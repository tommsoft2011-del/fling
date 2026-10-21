import assert from 'node:assert/strict';
import { mock } from 'bun:test';
const written: string[] = [];
mock.module('@tauri-apps/api/core', () => ({ invoke: async (_command: string, args: { lines: string[] }) => { written.push(...args.lines); } }));
const { log, flushLogs } = await import('../src/log.js');
log('p2p', 'path privacy regression', { taskId: 'RAW-TASK-IDENTIFIER', session_id: 'RAW-SESSION-IDENTIFIER', transferId: 'RAW-TRANSFER-IDENTIFIER',
  sourceId: 'RAW-SOURCE-IDENTIFIER', name: 'PRIVATE-FILENAME.txt', path: 'C:\\PRIVATE-DIRECTORY\\a', candidate: 'RAW-CANDIDATE', sdp: 'RAW-SDP',
  ip: '192.0.2.10', port: 54321, username: 'PRIVATE-USERNAME', password: 'PRIVATE-PASSWORD', url: 'turn:PRIVATE-ENDPOINT', payload: 'PRIVATE-CONTENT',
  route: 'turn_relay', local_type: 'relay', remote_type: 'host', local_protocol: 'udp', selected: true, nominated: true, elapsed_ms: 42 });
await flushLogs();
assert.equal(written.length, 1);
for (const value of ['RAW-', 'PRIVATE-', '192.0.2.10', '54321', 'turn:']) assert.ok(!written[0].includes(value), value);
for (const value of ['turn_relay', 'local_type', 'remote_type', 'local_protocol', 'selected', 'nominated', 'elapsed_ms']) assert.ok(written[0].includes(value), value);
console.log('PASS path log privacy: raw IDs, endpoints, credentials, paths, names and content excluded; route evidence retained');

const { safeConnectionDiagnostic } = await import('../src/connection-diagnostics.js');
const diagnostic = safeConnectionDiagnostic({ role: 'sender', path: 'Turn', stage: 'ice_add', outcome: 'failed', reason: 'timeout', count: 3, elapsed_ms: 42,
  error: 'PRIVATE-CREDENTIAL', candidate: 'RAW-CANDIDATE' });
assert.ok(diagnostic);
log('p2p', 'connection diagnostic', diagnostic);
await flushLogs();
const line = written.at(-1)!;
for (const value of ['transport_path','Turn','failure_category','timeout','ice_add','count']) assert.ok(line.includes(value), value);
for (const value of ['PRIVATE-', 'RAW-', 'candidate', 'password']) assert.ok(!line.includes(value), value);
console.log('PASS connection log boundary: safe path and failure category survive global privacy filtering');
