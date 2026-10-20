import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { NativeSourcesPicker } from '../src/native-source-picker';
const read = (file: string) => readFileSync(new URL(file, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
/** Deliberately executable policy: ordinary ingress may not regain byte/path IPC. */
export function checkOrdinaryIngress(send: string, tasks: string, bridge: string): void {
  assert.doesNotMatch(send, /tauri:\/\/drag-|openDialog|addLazyFileItem|addDirAsFolder|pickFoldersTauri|read_file_bytes|list_dir_recursive|\bfilePath\b|\bdiskPath\b/);
  assert.match(send, /new NativeSourcesPicker/);
  assert.match(send, /online_pick_sources/);
  assert.match(send, /fling:\/\/sources\/drop/);
  assert.match(send, /if \(isTauri\) return; \/\/ Native OS drop/);
  assert.doesNotMatch(tasks, /\bfilePath\b|\bdiskPath\b/);
  assert.match(tasks, /src\.sourceId\s*\? await this\.room!\.sendRegisteredSource/);
  const registered = bridge.slice(bridge.indexOf('  async sendRegisteredSource('), bridge.indexOf('  async cancelNative('));
  assert.match(registered, /online_send_registered_source/);
  assert.match(registered, /sessionId: this\.requireSession\(\), sourceId: source\.sourceId/);
  assert.doesNotMatch(registered, /ensureFilePath|arrayBuffer|Uint8Array|buildManifest|\bslice\(/);
  assert.doesNotMatch(bridge, /\bdiskPath\b/);
  assert.match(bridge, /Legacy Blob compatibility[\s\S]*NOT Rust-only/);
}
const send = read('../src/send.ts'); const tasks = read('../src/tasks.ts'); const bridge = read('../src/online-rust.ts');
checkOrdinaryIngress(send, tasks, bridge);
// Negative controls: the guard must fail for each former ordinary-file ingress regression.
for (const forbidden of ['tauri://drag-drop', 'read_file_bytes', 'list_dir_recursive', 'filePath', 'diskPath', 'openDialog']) {
  assert.throws(() => checkOrdinaryIngress(send + '\n' + forbidden, tasks, bridge), undefined, `must catch ${forbidden}`);
}
const caps = JSON.parse(read('../src-tauri/capabilities/default.json'));
for (const denied of ['core:event:deny-listen', 'core:event:deny-emit', 'core:event:deny-emit-to']) assert.ok(caps.permissions.includes(denied));
for (const denied of ['fs:default', 'dialog:default']) assert.ok(!caps.permissions.includes(denied));
const config = JSON.parse(read('../src-tauri/tauri.conf.json'));
assert.equal(config.build.beforeBuildCommand, 'bash ../../scripts/check-milestone.sh');
const checks = read('../../../scripts/check-milestone.sh');
assert.match(checks, /set -euo pipefail/);
assert.match(checks, /bash scripts\/test-typescript\.sh/);
assert.match(checks, /typescript\/bin\/tsc --noEmit/);
assert.match(checks, /cargo clippy --locked --all-targets/);
const native = read('../src-tauri/src/main.rs');
const handlers = native.slice(native.indexOf('.invoke_handler(tauri::generate_handler!['), native.indexOf('])\n', native.indexOf('.invoke_handler(tauri::generate_handler![')));
for (const command of ['read_file_bytes', 'file_size', 'is_dir', 'list_dir_recursive']) assert.doesNotMatch(handlers, new RegExp(`\\b${command}\\b`));
assert.match(native, /ordinary files require sourceId/);
assert.match(native, /sources\.busy_guard\(window.label\(\), &source_id\)/);
assert.match(native, /online_pick_sources[\s\S]*blocking_pick_folders[\s\S]*blocking_pick_files/);
const drop = read('../src-tauri/src/native_drop.rs');
const payload = /struct DropResult \{([^}]+)\}/.exec(drop)?.[1] ?? '';
assert.match(payload, /sources:/); assert.doesNotMatch(payload, /paths?:|PathBuf|bytes|data:/);
assert.match(drop, /registry.register_selection/);
const events = read('../src-tauri/src/ui_events.rs');
const allowed = events.slice(events.indexOf('fn allowed('), events.indexOf('#[tauri::command]'));
assert.doesNotMatch(allowed, /tauri:\/\/drag-/);
for (const file of readdirSync(new URL('../src', import.meta.url)).filter(f => f.endsWith('.ts'))) {
  assert.doesNotMatch(read('../src/' + file), /from ['"]@tauri-apps\/api\/event['"]/, `${file}: direct event subscription bypasses the allowlist`);
}
// Picker cancellation, errors and reentry are tested as behavior, not source spelling.
let pendingResolve!: (sources: any[]) => void;
let select: (folders: boolean) => Promise<any[]> = async () => [];
const modes: boolean[] = [];
const picker = new NativeSourcesPicker(async folders => { modes.push(folders); return select(folders); });
assert.deepEqual(await picker.pick(), []);
select = async () => { throw new Error('simulated picker error'); };
await assert.rejects(picker.pick(true), /simulated picker error/);
select = async () => [{ sourceId: 'id-1', name: 'a.bin', size: 0 }, { sourceId: 'id-2', name: 'b.bin', size: 6 }];
assert.equal((await picker.pick()).length, 2);
select = () => new Promise(resolve => { pendingResolve = resolve; });
const pending = picker.pick(true);
assert.deepEqual(await picker.pick(), []);
pendingResolve([]); await pending;
select = async () => [{ sourceId: 'folder-id', name: 'a.txt', size: 1, relPath: 'docs/a.txt', folder: 'docs' }];
assert.equal((await picker.pick(true))[0].relPath, 'docs/a.txt');
assert.deepEqual(modes, [false, true, false, true, true]);
console.log('ok - ordinary WebView boundary guard, six negative controls, denied raw path events, sourceId-only IPC and batch picker lifecycle');
console.log('Batch task orchestration remains compatible TypeScript; memory Blob/clipboard sources are NOT Rust-owned.');
