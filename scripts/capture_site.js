const { spawn } = require('node:child_process');
const crypto = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
const path = require('node:path');

const chromePath = process.argv[2];
const targetUrl = process.argv[3];
const outputPath = process.argv[4];
const width = Number(process.argv[5] || 1440);
const height = Number(process.argv[6] || 2400);
const debugPort = Number(process.argv[7] || 9222);

if (!chromePath || !targetUrl || !outputPath) {
  console.error('Usage: node scripts/capture_site.js <chromePath> <url> <outputPath> [width] [height] [debugPort]');
  process.exit(1);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function httpJson(url, method = 'GET') {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method }, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (error) {
          reject(new Error(`Failed to parse JSON from ${url}: ${error.message}`));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

class DevToolsSocket {
  constructor(wsUrl) {
    const parsed = new URL(wsUrl);
    this.host = parsed.hostname;
    this.port = Number(parsed.port || 80);
    this.path = parsed.pathname + parsed.search;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.pending = new Map();
    this.nextId = 1;
    this.eventWaiters = new Map();
  }

  async connect() {
    this.socket = net.createConnection({ host: this.host, port: this.port });
    await new Promise((resolve, reject) => {
      this.socket.once('error', reject);
      this.socket.once('connect', resolve);
    });

    this.socket.on('data', (chunk) => this.onData(chunk));
    this.socket.on('error', (error) => {
      for (const pending of this.pending.values()) {
        pending.reject(error);
      }
      this.pending.clear();
    });

    const key = crypto.randomBytes(16).toString('base64');
    const request =
      `GET ${this.path} HTTP/1.1\r\n` +
      `Host: ${this.host}:${this.port}\r\n` +
      `Upgrade: websocket\r\n` +
      `Connection: Upgrade\r\n` +
      `Sec-WebSocket-Key: ${key}\r\n` +
      `Sec-WebSocket-Version: 13\r\n\r\n`;

    this.socket.write(request, 'utf8');
    await this.readHandshake();
  }

  async readHandshake() {
    while (!this.buffer.includes('\r\n\r\n')) {
      await sleep(10);
    }
    const boundary = this.buffer.indexOf('\r\n\r\n');
    const header = this.buffer.subarray(0, boundary).toString('utf8');
    if (!header.includes('101 Switching Protocols')) {
      throw new Error(`WebSocket handshake failed: ${header}`);
    }
    this.buffer = this.buffer.subarray(boundary + 4);
    this.processFrames();
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    this.processFrames();
  }

  processFrames() {
    while (true) {
      if (this.buffer.length < 2) {
        return;
      }

      const first = this.buffer[0];
      const second = this.buffer[1];
      const opcode = first & 0x0f;
      let offset = 2;
      let payloadLength = second & 0x7f;

      if (payloadLength === 126) {
        if (this.buffer.length < offset + 2) {
          return;
        }
        payloadLength = this.buffer.readUInt16BE(offset);
        offset += 2;
      } else if (payloadLength === 127) {
        if (this.buffer.length < offset + 8) {
          return;
        }
        const high = this.buffer.readUInt32BE(offset);
        const low = this.buffer.readUInt32BE(offset + 4);
        payloadLength = high * 2 ** 32 + low;
        offset += 8;
      }

      const masked = (second & 0x80) !== 0;
      let mask;
      if (masked) {
        if (this.buffer.length < offset + 4) {
          return;
        }
        mask = this.buffer.subarray(offset, offset + 4);
        offset += 4;
      }

      if (this.buffer.length < offset + payloadLength) {
        return;
      }

      let payload = this.buffer.subarray(offset, offset + payloadLength);
      this.buffer = this.buffer.subarray(offset + payloadLength);

      if (masked) {
        const unmasked = Buffer.alloc(payload.length);
        for (let index = 0; index < payload.length; index += 1) {
          unmasked[index] = payload[index] ^ mask[index % 4];
        }
        payload = unmasked;
      }

      if (opcode === 0x8) {
        this.socket.end();
        return;
      }

      if (opcode !== 0x1) {
        continue;
      }

      const message = JSON.parse(payload.toString('utf8'));
      if (message.id && this.pending.has(message.id)) {
        const pending = this.pending.get(message.id);
        this.pending.delete(message.id);
        if (message.error) {
          pending.reject(new Error(message.error.message || JSON.stringify(message.error)));
        } else {
          pending.resolve(message.result);
        }
        continue;
      }

      if (message.method && this.eventWaiters.has(message.method)) {
        const waiters = this.eventWaiters.get(message.method);
        this.eventWaiters.delete(message.method);
        for (const waiter of waiters) {
          waiter(message.params || {});
        }
      }
    }
  }

  send(method, params = {}) {
    const id = this.nextId;
    this.nextId += 1;
    const message = JSON.stringify({ id, method, params });
    const payload = Buffer.from(message, 'utf8');
    const mask = crypto.randomBytes(4);
    let header;

    if (payload.length < 126) {
      header = Buffer.alloc(2);
      header[1] = 0x80 | payload.length;
    } else if (payload.length < 65536) {
      header = Buffer.alloc(4);
      header[1] = 0x80 | 126;
      header.writeUInt16BE(payload.length, 2);
    } else {
      header = Buffer.alloc(10);
      header[1] = 0x80 | 127;
      header.writeUInt32BE(0, 2);
      header.writeUInt32BE(payload.length, 6);
    }

    header[0] = 0x81;
    const maskedPayload = Buffer.alloc(payload.length);
    for (let index = 0; index < payload.length; index += 1) {
      maskedPayload[index] = payload[index] ^ mask[index % 4];
    }

    this.socket.write(Buffer.concat([header, mask, maskedPayload]));

    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  waitFor(method, timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Timed out waiting for ${method}`));
      }, timeoutMs);
      const waiter = (params) => {
        clearTimeout(timeout);
        resolve(params);
      };
      const existing = this.eventWaiters.get(method) || [];
      existing.push(waiter);
      this.eventWaiters.set(method, existing);
    });
  }

  close() {
    if (this.socket) {
      this.socket.end();
    }
  }
}

async function waitForDebugger(port) {
  const url = `http://127.0.0.1:${port}/json/version`;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      return await httpJson(url);
    } catch {
      await sleep(100);
    }
  }
  throw new Error('Chrome debugger did not start in time.');
}

async function main() {
  const userDataDir = path.join(process.cwd(), '.chrome-headless');
  fs.mkdirSync(userDataDir, { recursive: true });
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });

  const chrome = spawn(
    chromePath,
    [
      '--headless=new',
      '--disable-gpu',
      '--no-sandbox',
      '--hide-scrollbars',
      '--disable-crash-reporter',
      '--no-first-run',
      '--no-default-browser-check',
      `--remote-debugging-port=${debugPort}`,
      `--user-data-dir=${userDataDir}`,
      'about:blank',
    ],
    {
      stdio: 'ignore',
      detached: false,
    },
  );

  try {
    const version = await waitForDebugger(debugPort);
    const target = await httpJson(`http://127.0.0.1:${debugPort}/json/new?${encodeURIComponent(targetUrl)}`, 'PUT');
    const client = new DevToolsSocket(target.webSocketDebuggerUrl);
    await client.connect();

    await client.send('Page.enable');
    await client.send('Runtime.enable');
    await client.send('Emulation.setDeviceMetricsOverride', {
      width,
      height,
      deviceScaleFactor: 1,
      mobile: false,
    });
    await client.send('Page.navigate', { url: targetUrl });
    await client.waitFor('Page.loadEventFired', 15000);
    await sleep(1000);

    const metrics = await client.send('Page.getLayoutMetrics');
    const contentSize = metrics.cssContentSize || metrics.contentSize || { width, height };
    const screenshot = await client.send('Page.captureScreenshot', {
      format: 'png',
      captureBeyondViewport: true,
      clip: {
        x: 0,
        y: 0,
        width: Math.max(contentSize.width, width),
        height: Math.max(contentSize.height, height),
        scale: 1,
      },
    });

    fs.writeFileSync(outputPath, Buffer.from(screenshot.data, 'base64'));
    client.close();
    console.log(`Saved screenshot to ${outputPath}`);
    console.log(`Debugger browser: ${version.Browser}`);
  } finally {
    chrome.kill();
  }
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
