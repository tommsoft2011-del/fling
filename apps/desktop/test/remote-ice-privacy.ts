import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
const read = (name: string) => readFileSync(new URL(name, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const main = read('../src-tauri/src/main.rs');
const session = read('../src-tauri/src/online_session.rs');
const net = read('../src/net.ts');
const lan = read('../src-tauri/src/native_lan_transfer.rs');
const configBoundary = main.slice(main.indexOf('fn get_config('), main.indexOf('/// Load the config'));
assert.match(configBoundary, /Ok\(Some\(config_for_webview\(cfg\)\)\)/);
for (const field of ['turn_url', 'turn_username', 'turn_password']) {
  assert.match(configBoundary, new RegExp(`cfg\\.${field} = None;`));
}
assert.doesNotMatch(net, /turn_url|turn_username|turn_password|setCustomIceServers|turnHost/);
const connect = main.slice(main.indexOf('async fn online_connect('), main.indexOf('/// The only entrance'));
assert.match(connect, /effective_config\(&app\)/);
assert.match(connect, /online_p2p::default_ice_servers\(/);
assert.match(connect, /cfg\.turn_url\.as_deref\(\), cfg\.turn_username\.as_deref\(\), cfg\.turn_password\.as_deref\(\)/);
assert.match(session, /DirectAttempt::new_observed\(role,capability,self\.ice_servers\.clone\(\),send,observer\)/);
assert.match(session, /install_route\(transfer_id\.clone\(\), RouteRole::Sender/);
assert.match(session, /install_route\(id\.clone\(\), RouteRole::Receiver/);
const direct = read('../src-tauri/src/online_direct.rs');
assert.match(direct, /start_initiator_observed\(self\.servers\.clone\(\),mode,Some\(scope\.clone\(\)\)\)/);
assert.match(direct, /accept_offer_observed\(self\.servers\.clone\(\),mode,/);
const p2p = read('../src-tauri/src/online_p2p.rs');
assert.match(p2p, /IceMode::Turn[\s\S]*?IceTransportsType::Relay/);
// Since the v0.6.33 reference fix, the direct attempt intentionally uses the
// full-list Legacy configuration (the v0.6.2/v0.6.31 proven recipe); the
// STUN-only variant and its credential clearing are gone. Credentials still
// never reach the WebView (asserted above), and honest direct attribution
// is guarded by ReadyProof: a relay pair is valid only for the Turn path.
assert.match(direct, /path==Path::Direct \{IceMode::Legacy\}/);
assert.doesNotMatch(p2p, /IceMode::Direct|username\.clear\(\)|password\.clear\(\)/);
const choice = read('../src-tauri/src/online_route_choice.rs');
assert.match(choice, /relay == \(path == Path::Turn\)/);
assert.doesNotMatch(session, /default_ice_servers\(None, None, None\)|falling back to relay|using relay/);
assert.match(session, /TransportEvent::Failed\(_error\)[\s\S]*?connection_diagnostics::connection_failure\(\)[\s\S]*?Phase::ConnectionFailed/);
assert.match(session, /TransportEvent::TurnStarted[\s\S]*?state:\s*"fallback_requested"/);
assert.match(session, /p2p\.route_state\(\)\.await/);
assert.match(lan, /config\.ice_servers\.clear\(\)/);
assert.doesNotMatch(lan, /default_ice_servers|online_p2p::|LiveKit::|OnlineRoom/);
console.log('PASS remote ICE: native configuration injected on both roles; TURN never loaded into WebView; LAN host-only boundary preserved.');
