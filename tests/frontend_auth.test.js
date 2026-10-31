const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'public', 'app.js'), 'utf8').replace(/\binitialize\(\);\s*$/, '');

function loadFrontend(hash = '') {
  const saved = new Map();
  const requests = [];
  const downloads = [];
  const location = { hash, search: '', pathname: '/' };
  const window = {
    location,
    history: { replaceState(_state, _title, url) { location.replacedWith = url; } },
    sessionStorage: { setItem(key, value) { saved.set(key, value); }, getItem(key) { return saved.get(key) || null; } },
    setTimeout() { return 1; },
  };
  const document = {
    querySelector() { return {}; },
    createElement() {
      const link = { click() { downloads.push({ href: link.href, download: link.download }); }, remove() {} };
      return link;
    },
    body: { appendChild() {} },
  };
  const URLStub = { createObjectURL() { return 'blob:dummy'; }, revokeObjectURL() {} };
  const fetch = async (url, options) => {
    requests.push({ url, options });
    return {
      ok: true,
      headers: { get(name) { return name === 'Content-Disposition' ? 'attachment; filename="example.csv"' : 'text/csv'; } },
      blob: async () => Buffer.from('dummy'),
    };
  };
  const context = vm.createContext({ window, document, URL: URLStub, URLSearchParams, fetch, console });
  vm.runInContext(source, context);
  return { context, requests, downloads, location, saved };
}

test('launch fragment becomes an authorization header and leaves the address bar', async () => {
  const frontend = loadFrontend('#access_token=dummy-capability');
  await vm.runInContext('apiFetch("/api/health")', frontend.context);
  assert.equal(frontend.location.replacedWith, '/');
  assert.equal(frontend.saved.get('schema-atlas-access-token'), 'dummy-capability');
  assert.equal(frontend.requests[0].options.headers.Authorization, 'Bearer dummy-capability');
  assert.equal(frontend.requests[0].options.cache, 'no-store');
});

test('exports use the same authorized fetch and a local download', async () => {
  const frontend = loadFrontend('#access_token=dummy-capability');
  await vm.runInContext('state.selectedObject = "example"; openExport("csv", "page")', frontend.context);
  assert.match(frontend.requests[0].url, /\/api\/objects\/example\/export\?/);
  assert.equal(frontend.requests[0].options.headers.Authorization, 'Bearer dummy-capability');
  assert.deepEqual(frontend.downloads, [{ href: 'blob:dummy', download: 'example.csv' }]);
});

test('a bare localhost page cannot call the API', async () => {
  const frontend = loadFrontend();
  await assert.rejects(vm.runInContext('apiFetch("/api/health")', frontend.context), /private Schema Atlas launch link/);
  assert.equal(frontend.requests.length, 0);
});
