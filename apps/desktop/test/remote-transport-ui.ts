import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const read = (file: string) => readFileSync(new URL(file, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const bridge = read('../src/online-rust.ts');
const tasks = read('../src/tasks.ts');
const card = read('../src/taskcard.ts');
const history = read('../src/history.ts');

const p2pCase = /case 'p2p':[\s\S]*?break;/.exec(bridge)?.[0] ?? '';
assert.match(p2pCase, /ev\.state === 'relay_established'[\s\S]*?this\.directActive = true[\s\S]*?events\.onFileRoute\?\.\('relay'/, 'confirmed TURN relay is still an active RTCDataChannel route');
assert.match(p2pCase, /relay_candidate'[\s\S]*fallback_requested/, 'candidate and fallback intent are handled explicitly');
const fallbackIntent = /else if \(ev\.state === 'relay_candidate' \|\| ev\.state === 'fallback_requested'\) \{[\s\S]*?\n          \}/.exec(p2pCase)?.[0] ?? '';
assert.doesNotMatch(fallbackIntent, /directActive =|onFileRoute/, 'candidate/fallback intent preserves current active link state');
assert.match(p2pCase, /ev\.state === 'direct_established'[\s\S]*events\.onFileRoute\?\.\('p2p'/, 'direct established retains direct route');
assert.doesNotMatch(p2pCase, /ev\.state === 'failed'[\s\S]*?onFileRoute/, 'direct failure alone does not claim relay route');
assert.match(card, /connecting: '正在建立安全连接…'/, 'connection state has safe-connection label');
assert.match(card, /active && hasProgress && remainingSeconds !== null/, 'ETA is hidden until real progress');
assert.match(card, /el\('button', 'retry-failed', '重试连接'\)/, 'failed send row has a retry connection button');
assert.ok(card.includes("dir === 'send' && phase === 'error'") && card.includes("task.error?.includes('连接未建立')"), 'retry button only appears for failed connection sends');
assert.ok(card.includes('e.stopPropagation()') && card.includes("import('./tasks.js').then(({ manager }) => manager.retryFailed(task.taskId))"), 'retry click stops row expansion and dynamically calls retryFailed');
assert.ok(card.includes("dir === 'recv' && task.error?.includes('连接未建立')") && card.includes('请让发送方重试'), 'receiver connection failure asks sender to retry');
assert.match(tasks, /status\.error\?\.startsWith\('CONNECTION_FAILED:'\)/, 'native task error prefix is recognized');
assert.ok(tasks.includes('连接未建立：') && tasks.includes('可重试。'), 'connection failure is reported as not established and retryable');
assert.match(history, /retryFailed\(e\.taskId\)/, 'history row exposes existing retry action');
assert.match(tasks, /retryFailed\(taskId: string\)[\s\S]*?this\.startSend\(retrySources, task\.target\)/, 'retry action starts failed source items');
console.log('ok - remote Rust transport UI regression contracts');

assert.match(card, /bars\.hidden = !hasProgress/, 'overall progress is hidden before receive/ACK progress');
assert.match(card, /bytes === 0 && !complete[\s\S]*?等待接收进度/, 'file details do not show invented zero-percent progress');

assert.match(card, /task\.nativeRoute === 'relay'[\s\S]*?备用连接已建立/, 'backup-established label requires Rust confirmed route');
assert.match(bridge, /ev\.state === 'relay_established'[\s\S]*?events\.onNativeRoute\?\.\('relay'\)/, 'only actual Rust takeover sets relay confirmation');
