#!/usr/bin/env bun
// Fling CLI v1: fling text between devices over LiveKit data channels.
//   fling init              set up server URL + API key/secret (you type them yourself)
//   fling host              show a pairing code and listen; received text -> clipboard
//   fling join <CODE>       join a room; type lines to send, received text -> clipboard
//   fling send <CODE> TEXT  one-shot send (or pipe stdin)

import { createInterface } from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { hostname } from 'node:os';
import {
  FlingRoom,
  MAX_TEXT_BYTES,
  generatePairingCode,
  mintToken,
  normalizeCode,
  roomNameForCode,
} from '@fling/core';
import { DEFAULT_SERVER_URL, configPath, loadConfig, saveConfig } from './config.js';
import { writeClipboard } from './clipboard.js';

const VERSION = '0.1.0';

function deviceName(): string {
  const rand = Math.floor(Math.random() * 0xffff).toString(16).padStart(4, '0');
  return `fling-${hostname()}-${rand}`;
}

function usage(): never {
  console.log(`fling v${VERSION} — fling text between your devices
Usage:
  fling init                    set up config (server URL, API key/secret)
  fling host                    show pairing code, listen for incoming text
  fling join <CODE>             join with a pairing code, interactive send
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
  const p = saveConfig(cfg);
  console.log(`\nSaved to ${p}`);
}

async function connectRoom(code: string): Promise<{ room: FlingRoom; roomName: string }> {
  const cfg = loadConfig();
  const pairingCode = normalizeCode(code);
  if (!pairingCode) {
    console.error('Empty pairing code.');
    process.exit(1);
  }
  const roomName = await roomNameForCode(pairingCode);
  const identity = deviceName();
  const token = await mintToken(cfg.apiKey, cfg.apiSecret, roomName, identity);
  const room = new FlingRoom(identity);

  let peerSeen = false;
  const onPeer = (id: string, joined: boolean) => {
    if (id === identity) return;
    if (joined && !peerSeen) {
      peerSeen = true;
      console.log(`\n✓ peer joined: ${id}\n`);
    } else if (!joined) {
      console.log(`\n- peer left: ${id}\n`);
    }
  };

  room.connect(cfg.serverUrl, token, pairingCode, {
    onText: (text, from) => {
      console.log(`\n<<< from ${from} (${text.length} chars, copied to clipboard)`);
      const preview = text.length > 200 ? text.slice(0, 200) + '…' : text;
      console.log(preview + '\n');
      writeClipboard(text).catch((e) => console.error('(clipboard write failed: ' + e.message + ')'));
    },
    onPeerJoined: (id) => onPeer(id, true),
    onPeerLeft: (id) => onPeer(id, false),
  });

  // Wait until actually connected (or fail fast).
  const deadline = Date.now() + 15_000;
  while (!room.isConnected && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 200));
  }
  if (!room.isConnected) {
    console.error('Could not connect to ' + cfg.serverUrl + ' — check server URL and key/secret.');
    process.exit(1);
  }
  return { room, roomName };
}

async function cmdHost(): Promise<void> {
  const code = generatePairingCode();
  console.log(`\n  Pairing code:  ${code}\n`);
  console.log('  On the other device run:  fling join ' + code);
  console.log('  Waiting for peer… (Ctrl+C to quit)\n');
  const { room } = await connectRoom(code);

  const shutdown = async () => {
    console.log('\nBye.');
    await room.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());
  await new Promise(() => {}); // run forever
}

async function cmdJoin(code: string): Promise<void> {
  console.log(`Joining with code ${normalizeCode(code)}… (type text + Enter to send, /quit to exit)\n`);
  const { room } = await connectRoom(code);

  const rl = createInterface({ input, output, prompt: '> ' });
  const shutdown = async () => {
    console.log('\nBye.');
    rl.close();
    await room.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown());

  rl.prompt();
  for await (const line of rl) {
    const text = line.replace(/\r?\n$/, '');
    if (text === '/quit') break;
    if (!text) {
      rl.prompt();
      continue;
    }
    if (Buffer.byteLength(text, 'utf8') > MAX_TEXT_BYTES) {
      console.error(`Too long (>${MAX_TEXT_BYTES} bytes), not sent.`);
    } else {
      try {
        await room.sendText(text);
        console.log('(sent)');
      } catch (e) {
        console.error('Send failed: ' + (e as Error).message);
      }
    }
    rl.prompt();
  }
  await shutdown();
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
  const { room } = await connectRoom(code);
  // Give the peer a moment to be present; send anyway (server holds nothing,
  // but the peer's hello loop + our re-announce keep discovery live).
  await new Promise((r) => setTimeout(r, 1500));
  await room.sendText(text);
  console.log(`Sent ${text.length} chars.`);
  await room.close();
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
