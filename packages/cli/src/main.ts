#!/usr/bin/env bun
// Fling CLI v2: transfer tool over LiveKit data channels (fling-v2 protocol).
//   fling init              set up server URL + API key/secret (you type them yourself)
//   fling host              show a pairing code, listen in lobby; offers ask accept/decline
//   fling join <CODE>       interactive send: each line / file becomes one transfer task
//   fling send <CODE> TEXT  one-shot text send (or pipe stdin)

import { createInterface } from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { hostname } from 'node:os';
import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { basename, dirname, join } from 'node:path';
import {
  FlingRoom,
  MAX_TEXT_BYTES,
  generatePairingCode,
  lobbyNameForCode,
  mintToken,
  newTaskRoomName,
  normalizeCode,
  randomId,
  type FileLike,
  type FileManifest,
  type OfferMessage,
} from '@fling/core';
import { NodeTransport } from '@fling/core/node';
import { DEFAULT_SERVER_URL, configPath, loadConfig, saveConfig } from './config.js';
import { writeClipboard } from './clipboard.js';

const VERSION = '0.3.1';

/** v2 key contexts — must match the desktop app. */
const KEY_CTX_LOBBY = 'fling-v2-lobby';
const KEY_CTX_ROOM = 'fling-v2-room';

const WAIT_PEER_MS = 120_000;
const OFFER_REANNOUNCE_MS = 8_000;

function formatBytes(n: number): string {
  return n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(1)} MB`;
}

/** Non-colliding save path in dir for an incoming file (name sanitized). */
function uniqueSavePath(dir: string, name: string): string {
  const safe = basename(name) || 'file';
  let p = join(dir, safe);
  if (!existsSync(p)) return p;
  const dot = safe.lastIndexOf('.');
  const stem = dot > 0 ? safe.slice(0, dot) : safe;
  const ext = dot > 0 ? safe.slice(dot) : '';
  let i = 1;
  while (existsSync(p)) {
    p = join(dir, `${stem} (${i})${ext}`);
    i++;
  }
  return p;
}

/** Stable device id, persisted in the config file. */
function deviceId(): string {
  const cfg = loadConfig() as unknown as Record<string, unknown>;
  if (typeof cfg.deviceId === 'string' && cfg.deviceId) return cfg.deviceId;
  const id = 'cli-' + randomId(8);
  try {
    saveConfig({ ...(cfg as object), deviceId: id } as never);
  } catch {
    /* best effort */
  }
  return id;
}

function deviceName(): string {
  return `fling-cli-${hostname()}`;
}

/** Fresh LiveKit identity per connection so we never kick ourselves. */
function freshIdentity(): string {
  return `fling-${randomId(6)}`;
}

function usage(): never {
  console.log(`fling v${VERSION} — fling text & files between your devices
Usage:
  fling init                    set up config (server URL, API key/secret)
  fling host                    show pairing code, listen for incoming transfers
  fling join <CODE>             join with a pairing code, interactive send
                                (type text + Enter; /file <path> to send a file; /quit to exit)
  fling send <CODE> <TEXT...>   send once and exit (or pipe via stdin)
  fling config                  show config file path
  fling --version | -v`);
  process.exit(1);
}

async function prompt(q: string, def = ''): Promise<string> {
  const rl = createInterface({ input, output });
  const hint = def ? ` [${def}]` : '';
  const ans = (await rl.question(`${q}${hint}: `)).trim();
  rl.close();
  return ans || def;
}

async function cmdInit(): Promise<void> {
  console.log('Fling setup — your LiveKit server credentials stay on this machine only.\n');
  let serverUrl = '';
  let apiKey = '';
  let apiSecret = '';
  try {
    const prev = loadConfig();
    serverUrl = prev.serverUrl;
    apiKey = prev.apiKey;
    apiSecret = prev.apiSecret;
    console.log(`(existing config found at ${configPath()}, press Enter to keep values)`);
  } catch {
    /* fresh setup */
  }
  const cfg = {
    serverUrl: await prompt('Server URL', serverUrl || DEFAULT_SERVER_URL),
    apiKey: await prompt('API key', apiKey),
    apiSecret: await prompt('API secret', apiSecret),
  };
  if (!cfg.apiKey || !cfg.apiSecret) {
    console.error('API key and secret are required.');
    process.exit(1);
  }
  const p = saveConfig(cfg as never);
  console.log(`\nSaved to ${p}`);
}

/** Connect a FlingRoom; waits until the transport is actually up. */
async function connectRoom(opts: {
  roomName: string;
  keyMaterial: string;
  keyContext: string;
  events?: Parameters<FlingRoom['connect']>[3];
}): Promise<FlingRoom> {
  const cfg = loadConfig();
  const token = await mintToken(cfg.apiKey, cfg.apiSecret, opts.roomName, freshIdentity());
  const room = new FlingRoom(new NodeTransport(), deviceName());
  await room.connect(cfg.serverUrl, token, opts.keyMaterial, opts.events ?? {}, opts.keyContext);
  const deadline = Date.now() + 15_000;
  while (!room.isConnected && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 200));
  }
  if (!room.isConnected) {
    await room.close().catch(() => {});
    console.error('Could not connect to ' + cfg.serverUrl + ' — check server URL and key/secret.');
    process.exit(1);
  }
  return room;
}

function offerSummary(o: OfferMessage): string {
  return o.items.map((i) => `${i.kind === 'text' ? '文本' : i.kind === 'clipboard-image' ? '图片' : '文件'} ${i.name} (${formatBytes(i.size)})`).join(', ');
}

// ---------------------------------------------------------------------------
// Receiver (host)
// ---------------------------------------------------------------------------

async function receiveTask(code: string, offer: OfferMessage): Promise<void> {
  const taskRoom = offer.taskRoom;
  console.log(`\n⇅ joining task room…`);
  const textParts: string[] = [];
  const started = Date.now();
  let done = false;

  const room = await connectRoom({
    roomName: taskRoom,
    keyMaterial: normalizeCode(code),
    keyContext: KEY_CTX_ROOM,
    events: {
      onText: (t) => {
        textParts.push(t);
        output.write(`\r…receiving text (${formatBytes(textParts.join('').length)})`);
      },
      onFileManifest: (m) => console.log(`\n⇅ receiving "${m.name}" (${formatBytes(m.size)})`),
      onFileProgress: (_id, r, t) => {
        const pct = t > 0 ? Math.floor((r / t) * 100) : 100;
        output.write(`\r  ${pct}% (${formatBytes(r)}/${formatBytes(t)})   `);
      },
      onFileComplete: (_id, m: FileManifest, bytes: Uint8Array) => {
        output.write('\n');
        const p = uniqueSavePath(process.cwd(), m.name);
        mkdirSync(dirname(p), { recursive: true });
        writeFileSync(p, bytes);
        console.log(`✓ saved "${m.name}" → ${p} (sha256 verified)`);
        if (m.kind === 'clipboard-image') {
          console.log('  (image saved to file — no image clipboard on headless Linux)');
        }
      },
      onFileError: (id, err) => console.error(`\n✗ file ${id} error: ${err}`),
      onTaskDone: (taskId) => {
        if (taskId === offer.taskId) done = true;
      },
      onPeerLeft: () => {
        if (!done) console.log('\n- peer left before task-done');
        done = true;
      },
    },
  });

  // Wait for task-done (or peer leave), up to 10 min.
  const deadline = Date.now() + 600_000;
  while (!done && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 300));
  }
  const text = textParts.join('');
  if (text) {
    console.log(`\n<<< text from ${offer.fromDevice} (${text.length} chars):`);
    console.log(text.length > 500 ? text.slice(0, 500) + '…' : text);
    try {
      await writeClipboard(text);
      console.log('(copied to clipboard)');
    } catch (e) {
      console.log('(clipboard unavailable: ' + (e as Error).message + ')');
    }
  }
  console.log(`✓ transfer ${offer.taskId.slice(0, 8)} done in ${Math.round((Date.now() - started) / 1000)}s\n`);
  await room.close().catch(() => {});
}

async function cmdHost(): Promise<void> {
  const code = generatePairingCode();
  const myId = deviceId();
  console.log(`\n  Pairing code:  ${code}\n`);
  console.log('  On the other device: use this code to send (desktop: 用配对码发送)');
  console.log('  Waiting for offers… (Ctrl+C to quit)\n');

  const pending: OfferMessage[] = [];
  let busy = false;

  const lobby = await connectRoom({
    roomName: await lobbyNameForCode(code),
    keyMaterial: normalizeCode(code),
    keyContext: KEY_CTX_LOBBY,
    events: {
      onOffer: (offer) => {
        if (offer.fromDeviceId === myId) return; // our own echo
        pending.push(offer);
        console.log(`\n⬇ offer from ${offer.fromDevice}: ${offerSummary(offer)}`);
        void drainQueue();
      },
      onOfferWithdraw: (taskId) => {
        const i = pending.findIndex((o) => o.taskId === taskId);
        if (i >= 0) {
          pending.splice(i, 1);
          console.log(`\n- offer ${taskId.slice(0, 8)} withdrawn by sender`);
        }
      },
    },
  });

  async function drainQueue(): Promise<void> {
    if (busy) return;
    const offer = pending.shift();
    if (!offer) return;
    busy = true;
    const ans = (await prompt(`Accept transfer from ${offer.fromDevice}? [Y/n]`)).toLowerCase();
    if (ans === 'n' || ans === 'no') {
      await lobby.sendOfferRejected(offer.taskId).catch(() => {});
      console.log('declined.');
    } else {
      try {
        await receiveTask(code, offer);
      } catch (e) {
        console.error('receive failed: ' + (e as Error).message);
      }
    }
    busy = false;
    void drainQueue();
  }

  const shutdown = async () => {
    console.log('\nBye.');
    await lobby.close().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());
  await new Promise(() => {}); // run forever
}

// ---------------------------------------------------------------------------
// Sender (join / send)
// ---------------------------------------------------------------------------

interface SendItem {
  kind: 'text' | 'file';
  text?: string;
  path?: string;
  name: string;
  size: number;
}

function nodeFileLike(path: string): FileLike {
  const st = statSync(path);
  return {
    name: basename(path),
    size: st.size,
    slice: async (start: number, end: number) => {
      const buf = readFileSync(path).subarray(start, end);
      return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
    },
  };
}

/** One send action = one task = one offer + one dedicated task room. */
async function runSendTask(code: string, items: SendItem[]): Promise<boolean> {
  const myId = deviceId();
  const taskId = 'task-' + randomId(8);
  const taskRoom = newTaskRoomName();
  let peerJoined = false;
  let rejected = false;

  // 1. Sender joins the task room first.
  const room = await connectRoom({
    roomName: taskRoom,
    keyMaterial: normalizeCode(code),
    keyContext: KEY_CTX_ROOM,
    events: {
      onPeerJoined: () => {
        peerJoined = true;
      },
    },
  });

  // 2. Announce the offer in the lobby.
  const offer: OfferMessage = {
    kind: 'offer',
    taskId,
    fromDevice: deviceName(),
    fromDeviceId: myId,
    taskRoom,
    taskKey: '',
    items: items.map((i) => ({ name: i.name, size: i.size, kind: i.kind })),
  };
  const lobby = await connectRoom({
    roomName: await lobbyNameForCode(code),
    keyMaterial: normalizeCode(code),
    keyContext: KEY_CTX_LOBBY,
    events: {
      onOfferRejected: (tid) => {
        if (tid === taskId) rejected = true;
      },
    },
  });
  await lobby.sendOffer(offer);
  console.log(`offer sent, waiting for peer to accept… (${code})`);
  const announcer = setInterval(() => {
    if (peerJoined || rejected) return clearInterval(announcer);
    lobby.sendOffer(offer).catch(() => {});
  }, OFFER_REANNOUNCE_MS);

  // 3. Wait for the peer to join the task room.
  const deadline = Date.now() + WAIT_PEER_MS;
  while (!peerJoined && !rejected && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 300));
  }
  clearInterval(announcer);
  await lobby.close().catch(() => {});
  if (rejected) {
    console.log('✗ peer rejected the transfer.');
    await room.close().catch(() => {});
    return false;
  }
  if (!peerJoined) {
    console.log('✗ peer did not join in time.');
    await room.close().catch(() => {});
    return false;
  }

  // 4. Send items.
  for (const item of items) {
    if (item.kind === 'text') {
      const text = item.text ?? '';
      // chunk like the desktop does
      for (let i = 0; i < text.length; i += 15000) {
        await room.sendText(text.slice(i, i + 15000));
      }
      console.log(`✓ text sent (${text.length} chars)`);
    } else {
      const file = nodeFileLike(item.path!);
      await room.sendFile(file, (sent, total) => {
        const pct = total > 0 ? Math.floor((sent / total) * 100) : 100;
        output.write(`\r  ${item.name}: ${pct}% (${formatBytes(sent)}/${formatBytes(total)})   `);
      }, 'file');
      output.write('\n');
      console.log(`✓ file sent: ${item.name}`);
    }
  }
  await room.sendTaskDone(taskId).catch(() => {});
  await new Promise((r) => setTimeout(r, 1500));
  await room.close().catch(() => {});
  return true;
}

async function cmdJoin(code: string): Promise<void> {
  code = normalizeCode(code);
  if (!code) {
    console.error('Empty pairing code.');
    process.exit(1);
  }
  console.log(`Fling v2 — code ${code}. Each line is one transfer task (peer accepts each).`);
  console.log('Type text + Enter to send, /file <path> for a file, /quit to exit.\n');

  const rl = createInterface({ input, output, prompt: '> ' });
  const shutdown = () => {
    console.log('\nBye.');
    rl.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);

  rl.prompt();
  for await (const line of rl) {
    const text = line.replace(/\r?\n$/, '');
    if (text === '/quit') break;
    if (!text) {
      rl.prompt();
      continue;
    }
    if (text.startsWith('/file ')) {
      const p = text.slice(6).trim();
      if (!existsSync(p) || !statSync(p).isFile()) {
        console.error('not a file: ' + p);
        rl.prompt();
        continue;
      }
      const st = statSync(p);
      await runSendTask(code, [{ kind: 'file', path: p, name: basename(p), size: st.size }]);
    } else {
      if (Buffer.byteLength(text, 'utf8') > MAX_TEXT_BYTES) {
        console.error(`Too long (>${MAX_TEXT_BYTES} bytes), not sent.`);
        rl.prompt();
        continue;
      }
      await runSendTask(code, [{ kind: 'text', text, name: text.slice(0, 24) || '文本', size: Buffer.byteLength(text, 'utf8') }]);
    }
    rl.prompt();
  }
  shutdown();
}

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const c of input) chunks.push(c as Buffer);
  return Buffer.concat(chunks).toString('utf8');
}

async function cmdSend(code: string, args: string[]): Promise<void> {
  let text = args.join(' ');
  if (!text && !input.isTTY) text = (await readStdin()).replace(/\s+$/, '');
  if (!text) {
    console.error('Nothing to send. Usage: fling send <CODE> <TEXT>  (or pipe stdin)');
    process.exit(1);
  }
  if (Buffer.byteLength(text, 'utf8') > MAX_TEXT_BYTES) {
    console.error(`Too long (>${MAX_TEXT_BYTES} bytes).`);
    process.exit(1);
  }
  const ok = await runSendTask(normalizeCode(code), [
    { kind: 'text', text, name: text.slice(0, 24) || '文本', size: Buffer.byteLength(text, 'utf8') },
  ]);
  process.exit(ok ? 0 : 1);
}

async function main(): Promise<void> {
  const [, , cmd, ...args] = process.argv;
  if (!cmd || cmd === '-h' || cmd === '--help') usage();
  if (cmd === '-v' || cmd === '--version') {
    console.log(`fling v${VERSION}`);
    return;
  }
  switch (cmd) {
    case 'init':
      await cmdInit();
      break;
    case 'host':
      await cmdHost();
      break;
    case 'join':
      if (!args[0]) {
        console.error('Usage: fling join <CODE>');
        process.exit(1);
      }
      await cmdJoin(args[0]);
      break;
    case 'send':
      if (!args[0]) {
        console.error('Usage: fling send <CODE> <TEXT...>');
        process.exit(1);
      }
      await cmdSend(args[0], args.slice(1));
      break;
    case 'config':
      console.log(configPath());
      break;
    default:
      console.error(`Unknown command: ${cmd}\n`);
      usage();
  }
}

main().catch((e) => {
  console.error('Error: ' + (e as Error).message);
  process.exit(1);
});
