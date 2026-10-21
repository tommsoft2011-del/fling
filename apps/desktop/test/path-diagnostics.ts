import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { safeRouteDiagnostic, nativeRouteLabel } from '../src/route-diagnostics.js';
const read = (path: string) => readFileSync(new URL(path, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const summary = { transfer_no: null, role: 'initiator', phase: 'selected_pair', route: 'unknown', pair_state: 'unknown',
  local_type: 'unknown', remote_type: 'unknown', local_protocol: 'unknown', remote_protocol: 'unknown',
  selected: false, nominated: null, elapsed_ms: 4, snapshot_age_ms: null };
assert.deepEqual(safeRouteDiagnostic(summary), summary);
const privateValues = { taskId: 'raw-task-id', session: 'raw-session-id', filename: 'private.txt', path: 'C:\\private\\file',
  sdp: 'v=0 private SDP', candidate: 'candidate:raw', ip: '192.0.2.33', port: 54321, turnUrl: 'turn:private', password: 'private-password', payload: 'private-content' };
assert.deepEqual(safeRouteDiagnostic({ ...summary, ...privateValues }), summary);
for (const key of ['role', 'phase', 'route', 'pair_state', 'local_type', 'remote_type', 'local_protocol', 'remote_protocol']) {
  for (const value of Object.values(privateValues)) assert.equal(safeRouteDiagnostic({ ...summary, [key]: value }), null);
}
for (const value of [-1, NaN, Infinity, 'raw-id']) assert.equal(safeRouteDiagnostic({ ...summary, elapsed_ms: value }), null);
assert.equal(nativeRouteLabel('unknown'), '连接已建立，路径暂不可确认');
assert.equal(nativeRouteLabel('direct'), '已建立直连');
assert.equal(nativeRouteLabel('relay'), '已通过 TURN 中继连接');
assert.equal(nativeRouteLabel(undefined), '');
const session = read('../src-tauri/src/online_session.rs');
const bridge = read('../src/online-rust.ts');
const diagnostics = read('../src-tauri/src/online_diagnostics.rs');
const transfer = read('../src-tauri/src/online_transfer.rs');
const sender = session.slice(session.indexOf('async fn send_prepared'), session.indexOf('pub fn authorize'));
const responder = session.slice(session.indexOf('async fn install_route'), session.indexOf('async fn on_p2p_offer'));
assert.match(sender, /route.wait_selected\(\)\.await\.map_err\([\s\S]*?connection_diagnostics::connection_failure\(\)\)\?[\s\S]*?Phase::DataChannelOpen[\s\S]*?observe_route\(p2p\.clone\(\)\)/);
assert.match(responder, /TransportEvent::Started\(fence\)[\s\S]*?current_fence[\s\S]*?selected_peer\(fence\)[\s\S]*?Phase::DataChannelOpen[\s\S]*?observe_route\(peer\)/);
assert.match(responder, /TransportEvent::Failed\(_error\)[\s\S]*?let error=crate::connection_diagnostics::connection_failure\(\)[\s\S]*?Phase::ConnectionFailed/);
assert.match(responder, /TransportEvent::TurnStarted[\s\S]*?Phase::FallbackRequested/);
assert.match(read('../src-tauri/src/online_direct.rs'), /Effect::CreateTurnPeerConnection[\s\S]*?TransportEvent::TurnStarted/);
assert.match(sender, /install_route\(transfer_id.clone\(\), RouteRole::Sender/);
assert.match(session, /install_route\(id.clone\(\), RouteRole::Receiver/);
assert.match(sender, /if sent > 0[\s\S]*Phase::FirstRealBytes/);
assert.match(session, /if received_bytes > 0[\s\S]*Phase::FirstRealBytes/);
assert.match(session, /check_complete_observed\(\|phase\| self\.note_transfer\(&id, phase\)\)/);
for (const phase of ['DiskSynced', 'HashVerified', 'AtomicCommit']) assert.match(transfer, new RegExp('observe\\(Phase::' + phase + '\\)'));
for (const phase of ['DurableAckSent', 'DurableAckReceived', 'TaskAckSent', 'TaskAckReceived', 'TaskEnd', 'ConnectionClosed']) assert.ok(session.includes('Phase::' + phase));
assert.ok(transfer.indexOf('sync_all()') < transfer.indexOf('observe(Phase::DiskSynced)'));
assert.ok(transfer.indexOf('if digest != self.manifest.sha256', transfer.indexOf('check_complete_observed')) < transfer.indexOf('observe(Phase::HashVerified)'));
assert.ok(transfer.indexOf('std::fs::hard_link') < transfer.indexOf('observe(Phase::AtomicCommit)'));
assert.doesNotMatch(session, /P2pLog|sdp_len|frame_count|unknown transfer id \{\}|session_cb\.id, e/);
assert.match(bridge, /case 'path-diagnostic':[\s\S]*safeRouteDiagnostic\(ev\.summary\)/);
assert.match(bridge, /ev\.state === 'open' \|\| ev\.state === 'unknown_established'[\s\S]*onNativeRoute\?\.\('unknown'\)/);
assert.match(bridge, /ev\.state === 'failed' \|\| ev\.state === 'closed'[\s\S]*onNativeRoute\?\.\(undefined\)/);
assert.match(diagnostics, /seen\.insert\(phase\)/);
assert.match(session, /Duration::from_secs\(1\)/);
console.log('PASS path diagnostics: privacy allowlist, unknown mapping, both roles, sparse milestones, durable commit ordering');

const history = read('../src/history.ts');
assert.match(history, /transport === 'unknown' \? '路径暂不可确认'/);
for (const body of [sender, responder]) assert.match(body, /begin_connection\(Role::/);
for (const path of ['../package.json', '../src-tauri/tauri.conf.json']) assert.equal(JSON.parse(read(path)).version, '0.6.32');
assert.match(read('../src/main.ts'), /frontendBuildId: `v0\.6\.32\+/);
assert.match(read('../src/main.ts'), /buildId: await invoke<string>\('app_build_identity'\)/);
assert.match(read('../src-tauri/build.rs'), /FLING_BUILD_IDENTITY=v\{\}\+\{commit\}/);
