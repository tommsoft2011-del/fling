import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { safeConnectionDiagnostic } from '../src/connection-diagnostics.ts';
const base = { role: 'sender', path: 'Turn', stage: 'answer_apply', outcome: 'failed', reason: 'operation_failed', elapsed_ms: 4350, count: 1 };
const { path, reason, ...rest } = base;
const logged = { ...rest, transport_path: path, failure_category: reason };
const secret = 'candidate:raw SDP 192.0.2.1:3478 turn:private username password C:\\private\\file task-id';
assert.deepEqual(safeConnectionDiagnostic({ ...base, error: secret, sdp: secret, candidate: secret, session: secret }), logged);
for (const key of ['role', 'path', 'stage', 'outcome', 'reason']) assert.equal(safeConnectionDiagnostic({ ...base, [key]: secret }), null);
for (const n of [-1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1]) assert.equal(safeConnectionDiagnostic({ ...base, elapsed_ms: n }), null);
assert.equal(safeConnectionDiagnostic({ ...base, count: 0 }), null);
const pair = { route: 'turn_relay', pair_state: 'succeeded', local_type: 'relay', remote_type: 'relay', local_protocol: 'udp', remote_protocol: 'udp', selected: true, nominated: true };
assert.deepEqual(safeConnectionDiagnostic({ ...base, pair: { ...pair, address: secret, id: secret } }), { ...logged, pair });
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, local_type: secret } }), null);
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, selected: 'true' } }), null);
const aggregates = { pairs_total: 6, pairs_succeeded: 0, pairs_in_progress: 2, pairs_failed: 4, pairs_succeeded_direct: 0, pairs_succeeded_relay: 0, local_host: true, remote_host: true, local_srflx: true, remote_srflx: false };
assert.deepEqual(safeConnectionDiagnostic({ ...base, pair: { ...pair, ...aggregates, address: secret } }), { ...logged, pair: { ...pair, ...aggregates } });
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, pairs_total: -1 } }), null);
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, pairs_failed: 0.5 } }), null);
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, pairs_succeeded_direct: -1 } }), null);
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, pairs_succeeded_relay: 'x' } }), null);
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, local_srflx: 'yes' } }), null);
assert.equal(safeConnectionDiagnostic({ ...base, pair: { ...pair, remote_host: 'yes' } }), null);
const iceCategories = [
  ['ice_stats_unavailable', undefined],
  ['no_succeeded_pairs', { pairs_total: 0, pairs_succeeded: 0, pairs_in_progress: 0, pairs_failed: 0 }],
  ['ice_checks_in_progress', { pairs_total: 5, pairs_succeeded: 0, pairs_in_progress: 3, pairs_failed: 0 }],
  ['ice_checks_failed', { pairs_total: 2, pairs_succeeded: 0, pairs_in_progress: 0, pairs_failed: 2 }],
  ['ice_pair_succeeded', { pairs_total: 2, pairs_succeeded: 1, pairs_in_progress: 0, pairs_failed: 1 }],
] as const;
for (const [reason, counts] of iceCategories) {
  const stage = reason === 'ice_stats_unavailable' ? 'direct_channel_wait_terminal' : 'direct_ice_cutoff';
  const input = { ...base, stage, reason, ...(counts ? { pair: { ...pair, ...counts, address: secret, id: secret } } : {}) };
  const safe = safeConnectionDiagnostic(input);
  assert.ok(safe);
  assert.equal(safe.stage, stage);
  assert.equal(safe.failure_category, reason);
  if (counts) {
    const safePair = safe.pair as Record<string, unknown>;
    assert.equal(safePair.pairs_total, counts.pairs_total);
    assert.equal('address' in safePair || 'id' in safePair, false);
  }
}
const source = readFileSync(new URL('../src/online-rust.ts', import.meta.url), 'utf8');
assert.ok(source.includes("'connection-diagnostic'"));
assert.match(source, /safeConnectionDiagnostic\(ev.summary\)/);
assert.match(source, /if \(summary\) log\('p2p', 'connection diagnostic', summary\)/);
const native = readFileSync(new URL('../src-tauri/src/online_session.rs', import.meta.url), 'utf8');
const failed = native.split('TransportEvent::Failed(_error) => {')[1].split('TransportEvent::Started')[0];
assert.ok(failed.includes('crate::connection_diagnostics::connection_failure()'));
assert.ok(failed.includes('cancel_native(Some(error),false)'));
const stable = readFileSync(new URL('../src-tauri/src/connection_diagnostics.rs', import.meta.url), 'utf8');
assert.ok(stable.includes('CONNECTION_FAILED: 安全连接建立失败或已中断，请重试'));
const tasks = readFileSync(new URL('../src/tasks.ts', import.meta.url), 'utf8');
assert.ok(tasks.includes("status.error?.startsWith('CONNECTION_FAILED:')"));
assert.ok(tasks.includes('连接未建立：') && tasks.includes('可重试。'));
const card = readFileSync(new URL('../src/taskcard.ts', import.meta.url), 'utf8');
assert.ok(card.includes("task.error?.includes('连接未建立')") && card.includes('重试连接'));
console.log('connection diagnostics privacy, phase allowlist, sanitized native errors and retry UI contract passed');

for (const status of ['stats_unavailable', 'timeout', 'already_closed', 'zero_pairs', 'still_in_progress', 'failed_pairs', 'succeeded_direct', 'other_pairs']) {
  const checklist = { status, pc:'connected', ice:'checking', dc:'connecting', pair:null };
  const input = { ...base, stage:'direct_pre_close', attempt_no:2, checklist:{ ...checklist, sdp:secret, address:secret } };
  const out = safeConnectionDiagnostic(input);
  assert.deepEqual(out?.checklist, checklist);
  assert.equal(out?.attempt_no, 2);
  assert.equal(JSON.stringify(out).includes(secret), false);
  assert.equal(safeConnectionDiagnostic({ ...input, checklist:{...checklist, pc:secret} }), null);
  assert.equal(safeConnectionDiagnostic({ ...input, attempt_no:-1 }), null);
}
const preclose = safeConnectionDiagnostic({ ...base, stage:'direct_pre_close', checklist:{status:'succeeded_direct',pc:'connected',ice:'connected',dc:'open',pair:{...pair, address:secret}} });
assert.deepEqual((preclose?.checklist as Record<string, unknown>).pair, pair);
