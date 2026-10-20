import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const read = (name: string) => readFileSync(new URL(`../src-tauri/src/${name}`, import.meta.url), 'utf8');
const runtime = read('native_lan_runtime.rs');
const transfer = read('native_lan_transfer.rs');
const diagnostics = read('pairing_diagnostics.rs');
const logger = read('discovery_diagnostics.rs');
const main = read('main.rs');

assert.match(runtime, /diagnostic:\s*FunnelTrace/);
assert.match(runtime, /diagnostic:\s*FunnelTrace::new\(trace.clone\(\)\)/);
assert.match(runtime, /fn decide_locally_observed[\s\S]*?pending.decide_locally\(approved, peer_code\)/);
for (const event of ['DecisionLocalSubmittedApproved', 'DecisionLocalSubmittedRejected', 'DecisionLocalApprovedMatch', 'DecisionLocalApprovedMismatch', 'DecisionLocalRejected', 'PeerDecisionConfirmed', 'PeerDecisionRejected', 'PeerDecisionInvalid', 'AuthConfirmed']) {
  assert.ok(runtime.includes(`FunnelEvent::${event}`), `native decision path missing ${event}`);
}
assert.match(runtime, /Ok\(Ok\(\(\)\)\)\s*=>\s*\{\s*session.observe_control_written\(frame.sequence\)/);
assert.match(transfer, /session.observe_outgoing_control\(&message, &frame\)/);
for (const event of ['PeerConnectionStart', 'PeerConnectionOk', 'PeerConnectionError', 'OfferGenerated', 'OfferQueued', 'OfferReceived', 'AnswerGenerated', 'AnswerQueued', 'AnswerReceived', 'IceGatheringComplete', 'IceConnectionChecking', 'DataChannelCreated', 'DataChannelReceived', 'DataChannelOpen', 'DataChannelClose', 'DataChannelError', 'ManifestQueued', 'ManifestReceived', 'ManifestAccepted', 'ManifestDenied', 'ManifestPeerAccepted', 'FirstFrameSendAttempt', 'FirstFrameNativeAccepted', 'FirstFrameReceiverArrival', 'FirstFrameAppendOk', 'FirstFrameAppendError']) {
  assert.ok(transfer.includes(`FunnelEvent::${event}`), `native transfer path missing ${event}`);
}
assert.match(transfer, /on_ice_gathering_state_change/);
assert.match(transfer, /IceGatheringState::Complete[\s\S]*?flush_candidates\(\)/);
assert.match(transfer, /candidate\(family, transport, category, rejection\);\s*if !is_local_host_candidate/);
assert.match(transfer, /if batch_seq == 0\s*\{\s*session\s*\.diagnostic\s*\.record\(FunnelEvent::FirstFrameSendAttempt\)/);
assert.match(transfer, /if frame.seq == 0\s*\{\s*session.diagnostic.record\(FunnelEvent::FirstFrameAppendOk\)/);
assert.match(transfer, /first_arrival.swap\(true/);
assert.match(transfer, /config.ice_servers.clear\(\)/);
assert.match(transfer, /ContinualGatheringPolicy::GatherOnce/);
assert.match(diagnostics, /insert\(event\)/, 'unit-enum events must be deduplicated');
assert.match(diagnostics, /candidates_flushed/);
assert.match(diagnostics, /trace:\s*PairTrace/);
const funnel = /pub enum FunnelEvent\s*\{([^}]+)\}/.exec(diagnostics)?.[1] ?? '';
assert.ok(funnel.length > 100);
assert.doesNotMatch(funnel, /[({]|String|Vec|str\b/, 'funnel event cannot carry arbitrary strings/payload');
assert.match(diagnostics, /Event::Pairing\(entry\)/);
assert.match(logger, /Event::Pairing\(entry\)\s*=>\s*\("pairing_stage", json!\(entry\)\)/);
assert.match(main, /discovery_diagnostics::initialize/);
assert.match(main, /discovery_diagnostics::export_text/);
assert.match(main, /mod native_lan_runtime;/);
assert.doesNotMatch(runtime, /#\[cfg\(not\(target_os\s*=\s*"windows"\)\)\]\s*(?:mod|fn)/);
console.log('LAN confirm observability contract passed: real native session/decision/write/ICE/DataChannel/manifest/first-frame wiring, enum-only payloads, bounded logs, local manual export. Static wiring evidence is not a Windows end-to-end result.');
