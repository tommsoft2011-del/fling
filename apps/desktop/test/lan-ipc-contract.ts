import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const sourcePath = fileURLToPath(new URL('../src/lan.ts', import.meta.url));
const source = readFileSync(sourcePath, 'utf8');

const sourceMetadata = /interface SourceMetadata\s*\{([^}]*)\}/.exec(source)?.[1] ?? '';
assert.match(sourceMetadata, /sourceId:\s*string/);
assert.doesNotMatch(sourceMetadata, /name|size|path|sha256|handle/i);

const forbidden = [
  /\bArrayBuffer\b/,
  /\bUint8Array\b/,
  /\bBlob\b/,
  /read_file_bytes/,
  /append_received_file_chunk/,
  /finish_received_file/,
  /filePath/i,
  /FileChunkAck/i,
  /file-chunk-ack/i,
  /dataChannel/i,
];
for (const pattern of forbidden) {
  assert.doesNotMatch(source, pattern, `LAN WebView boundary must not contain ${pattern}`);
}

const allowedCommands = new Set([
  'native_lan_start_mode',
  'native_lan_stop_mode',
  'native_lan_pick_source',
  'native_lan_start_send',
  'native_lan_pair_decision',
  'native_lan_manifest_decision',
  'native_lan_cancel',
  'native_lan_ipc_counters',
]);
const commands = [...source.matchAll(/\binvoke(?:<[^>]+>)?\(\s*'([^']+)'/g)].map((match) => match[1]);
assert.ok(commands.length > 0, 'expected native LAN command calls');
for (const command of commands) {
  assert.ok(allowedCommands.has(command), `unexpected LAN IPC command: ${command}`);
}

const allowedEvents = new Set([
  'fling://lan/peer-up',
  'fling://lan/peer-down',
  'fling://lan/pairing',
  'fling://lan/manifest',
  'fling://lan/progress',
]);
const events = [...source.matchAll(/\blisten<[^>]+>\('([^']+)'/g)].map((match) => match[1]);
assert.ok(events.length > 0, 'expected aggregate LAN event listeners');
for (const event of events) {
  assert.ok(allowedEvents.has(event), `unexpected LAN IPC event: ${event}`);
}

console.log(`LAN IPC contract passed: ${commands.length} allowed command call sites, ${events.length} allowed event listeners; no file payload APIs.`);
