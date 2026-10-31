const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const MAX_CDP_MESSAGE_BYTES = 32 * 1024 * 1024;

function parseCliArgs(args) {
  if (!args[0] || !args[1] || !args[2] || args[5] !== undefined) {
    throw new Error('Usage: node scripts/capture_site.js <chromePath> <url> <outputPath> [width] [height]. TCP debugging ports are no longer supported.');
  }
  return {
    chromePath: args[0],
    targetUrl: args[1],
    outputPath: args[2],
    width: Number(args[3] || 1440),
    height: Number(args[4] || 2400),
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function waitForExit(browser, timeoutMs) {
  if (browser.exitCode !== null || browser.signalCode !== null) return Promise.resolve(true);
  return new Promise((resolve) => {
    const onExit = () => {
      clearTimeout(timer);
      resolve(true);
    };
    const timer = setTimeout(() => {
      browser.off('exit', onExit);
      resolve(false);
    }, timeoutMs);
    browser.once('exit', onExit);
  });
}

class DevToolsPipe {
  constructor(browser) {
    this.input = browser.stdio[3];
    this.output = browser.stdio[4];
    this.buffer = Buffer.alloc(0);
    this.pending = new Map();
    this.eventWaiters = new Map();
    this.nextId = 1;
    this.closed = false;

    this.output.on('data', (chunk) => this.onData(chunk));
    this.output.on('end', () => this.fail(new Error('Browser debugging pipe closed.')));
    this.output.on('error', (error) => this.fail(error));
    this.input.on('error', (error) => this.fail(error));
    browser.on('error', (error) => this.fail(error));
    browser.on('exit', () => this.fail(new Error('Browser exited before capture completed.')));
  }

  fail(error) {
    if (this.closed) return;
    this.closed = true;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    for (const waiters of this.eventWaiters.values()) {
      for (const waiter of waiters) {
        clearTimeout(waiter.timer);
        waiter.reject(error);
      }
    }
    this.eventWaiters.clear();
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    if (this.buffer.length > MAX_CDP_MESSAGE_BYTES) {
      this.fail(new Error('Browser debugging response exceeded the size limit.'));
      return;
    }
    let boundary;
    while ((boundary = this.buffer.indexOf(0)) !== -1) {
      const raw = this.buffer.subarray(0, boundary);
      this.buffer = this.buffer.subarray(boundary + 1);
      if (!raw.length) continue;
      let message;
      try {
        message = JSON.parse(raw.toString('utf8'));
      } catch {
        this.fail(new Error('Browser sent an invalid debugging response.'));
        return;
      }
      if (message.id !== undefined && this.pending.has(message.id)) {
        const pending = this.pending.get(message.id);
        this.pending.delete(message.id);
        clearTimeout(pending.timer);
        if (message.error) {
          pending.reject(new Error(message.error.message || 'Browser command failed.'));
        } else {
          pending.resolve(message.result || {});
        }
        continue;
      }
      const key = `${message.sessionId || ''}:${message.method || ''}`;
      const waiters = this.eventWaiters.get(key);
      if (waiters) {
        this.eventWaiters.delete(key);
        for (const waiter of waiters) {
          clearTimeout(waiter.timer);
          waiter.resolve(message.params || {});
        }
      }
    }
  }

  send(method, params = {}, sessionId = null, timeoutMs = 30000) {
    if (this.closed) return Promise.reject(new Error('Browser debugging pipe is closed.'));
    const id = this.nextId++;
    const message = { id, method, params };
    if (sessionId) message.sessionId = sessionId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Timed out waiting for ${method}.`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.input.write(`${JSON.stringify(message)}\0`, 'utf8', (error) => {
        if (error && this.pending.has(id)) {
          this.pending.delete(id);
          clearTimeout(timer);
          reject(error);
        }
      });
    });
  }

  waitFor(method, sessionId, timeoutMs = 15000) {
    const key = `${sessionId || ''}:${method}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.eventWaiters.set(key, (this.eventWaiters.get(key) || []).filter((item) => item !== waiter));
        reject(new Error(`Timed out waiting for ${method}.`));
      }, timeoutMs);
      const waiter = { resolve, reject, timer };
      this.eventWaiters.set(key, [...(this.eventWaiters.get(key) || []), waiter]);
    });
  }

  close() {
    this.input.end();
    this.fail(new Error('Browser debugging pipe closed.'));
  }
}

async function captureSite({ chromePath, targetUrl, outputPath, width = 1440, height = 2400, spawnBrowser = spawn }) {
  if (!chromePath || !targetUrl || !outputPath) {
    throw new Error('Chrome path, URL, and output path are required.');
  }
  if (!Number.isInteger(width) || !Number.isInteger(height) || width < 1 || height < 1 || width > 10000 || height > 10000) {
    throw new Error('Width and height must be integers between 1 and 10000.');
  }
  const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'schema-atlas-capture-'));
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  const chrome = spawnBrowser(
    chromePath,
    [
      '--headless=new',
      '--disable-gpu',
      '--disable-background-networking',
      '--hide-scrollbars',
      '--disable-crash-reporter',
      '--no-first-run',
      '--no-default-browser-check',
      '--remote-debugging-pipe',
      `--user-data-dir=${userDataDir}`,
      'about:blank',
    ],
    { stdio: ['ignore', 'ignore', 'ignore', 'pipe', 'pipe'], detached: false },
  );
  const client = new DevToolsPipe(chrome);

  try {
    const version = await client.send('Browser.getVersion');
    const { targetId } = await client.send('Target.createTarget', { url: 'about:blank' });
    const { sessionId } = await client.send('Target.attachToTarget', { targetId, flatten: true });
    await client.send('Page.enable', {}, sessionId);
    await client.send('Runtime.enable', {}, sessionId);
    await client.send('Emulation.setDeviceMetricsOverride', {
      width,
      height,
      deviceScaleFactor: 1,
      mobile: false,
    }, sessionId);
    await Promise.all([
      client.waitFor('Page.loadEventFired', sessionId),
      client.send('Page.navigate', { url: targetUrl }, sessionId),
    ]);
    await sleep(1000);

    const metrics = await client.send('Page.getLayoutMetrics', {}, sessionId);
    const contentSize = metrics.cssContentSize || metrics.contentSize || { width, height };
    const screenshot = await client.send('Page.captureScreenshot', {
      format: 'png',
      captureBeyondViewport: true,
      clip: {
        x: 0,
        y: 0,
        width: Math.min(Math.max(contentSize.width, width), 10000),
        height: Math.min(Math.max(contentSize.height, height), 10000),
        scale: 1,
      },
    }, sessionId);

    fs.writeFileSync(outputPath, Buffer.from(screenshot.data, 'base64'));
    console.log(`Saved screenshot to ${outputPath}`);
    console.log(`Debugger browser: ${version.product || version.Browser || 'Chrome'}`);
  } finally {
    client.close();
    if (chrome.exitCode === null && chrome.signalCode === null) {
      const exited = waitForExit(chrome, 3000);
      chrome.kill();
      if (!(await exited) && chrome.exitCode === null && chrome.signalCode === null) {
        const forcedExit = waitForExit(chrome, 2000);
        chrome.kill('SIGKILL');
        await forcedExit;
      }
    }
    fs.rmSync(userDataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  }
}

if (require.main === module) {
  try {
    const options = parseCliArgs(process.argv.slice(2));
    captureSite(options).catch((error) => {
      console.error(error.message || error);
      process.exitCode = 1;
    });
  } catch (error) {
    console.error(error.message || error);
    process.exitCode = 1;
  }
}

module.exports = { captureSite, parseCliArgs };
