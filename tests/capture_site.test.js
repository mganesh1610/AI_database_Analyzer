const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { PassThrough } = require('node:stream');
const { captureSite, parseCliArgs } = require('../scripts/capture_site.js');

function fakeBrowser(_executable, args, options) {
  assert.ok(args.includes('--remote-debugging-pipe'));
  assert.ok(!args.some((arg) => arg.startsWith('--remote-debugging-port=')));
  assert.ok(!args.includes('--no-sandbox'));
  assert.deepEqual(options.stdio, ['ignore', 'ignore', 'ignore', 'pipe', 'pipe']);

  const browser = new EventEmitter();
  const input = new PassThrough();
  const output = new PassThrough();
  browser.stdio = [null, null, null, input, output];
  browser.exitCode = null;
  browser.signalCode = null;
  browser.kill = () => {
    browser.exitCode = 0;
    output.end();
    browser.emit('exit', 0, null);
    return true;
  };

  function respond(message) {
    const encoded = JSON.stringify(message) + '\0';
    const split = Math.floor(encoded.length / 2);
    output.write(encoded.slice(0, split));
    output.write(encoded.slice(split));
  }

  let buffer = '';
  input.on('data', (chunk) => {
    buffer += chunk.toString('utf8');
    let end;
    while ((end = buffer.indexOf('\0')) !== -1) {
      const message = JSON.parse(buffer.slice(0, end));
      buffer = buffer.slice(end + 1);
      let result = {};
      if (message.method === 'Browser.getVersion') result = { product: 'Fake Chrome' };
      if (message.method === 'Target.createTarget') result = { targetId: 'dummy-target' };
      if (message.method === 'Target.attachToTarget') result = { sessionId: 'dummy-session' };
      if (message.method === 'Page.getLayoutMetrics') result = { cssContentSize: { width: 320, height: 240 } };
      if (message.method === 'Page.captureScreenshot') result = { data: Buffer.from('fake-image').toString('base64') };
      respond({ id: message.id, result });
      if (message.method === 'Page.navigate') respond({ sessionId: message.sessionId, method: 'Page.loadEventFired', params: {} });
    }
  });
  return browser;
}

test('capture uses a private CDP pipe and keeps the screenshot workflow', async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'atlas-capture-test-'));
  try {
    const screenshot = path.join(directory, 'capture.png');
    await captureSite({ chromePath: 'fake-browser', targetUrl: 'data:text/html,dummy', outputPath: screenshot, width: 320, height: 240, spawnBrowser: fakeBrowser });
    assert.equal(fs.readFileSync(screenshot, 'utf8'), 'fake-image');
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test('capture refuses the old TCP debugger port argument', () => {
  assert.throws(
    () => parseCliArgs(['browser', 'data:text/html,dummy', 'capture.png', '320', '240', '9222']),
    /TCP debugging ports are no longer supported/,
  );
});
