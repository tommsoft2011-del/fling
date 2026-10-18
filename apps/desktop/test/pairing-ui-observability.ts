import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const frontend = readFileSync(new URL('../src/lan.ts', import.meta.url), 'utf8');
const native = readFileSync(new URL('../src-tauri/src/main.rs', import.meta.url), 'utf8');
const modalReport = frontend.slice(frontend.indexOf('function reportPairModalMountedAndVisible'), frontend.indexOf('function showPair'));
function pairingCommand(source: string): string {
  const normalized = source.replace(/\r\n/g, '\n');
  const start = normalized.indexOf('async fn pairing_report_ui(');
  const end = normalized.indexOf('\n}\n', start);
  assert.ok(start >= 0 && end > start, 'expected complete native command regardless of checkout line endings');
  return normalized.slice(start, end + 2);
}
const command = pairingCommand(native);
assert.equal(pairingCommand(native.replace(/\r?\n/g, '\r\n')), command, 'Windows CRLF checkout must preserve all command phases');
assert.equal(pairingCommand(native.replace(/\r\n/g, '\n')), command, 'LF checkout must preserve all command phases');

assert.match(frontend, /reportPairingUi\('prompt-received', null, request\.diagnosticAttempt!\)/);
assert.match(modalReport, /requestAnimationFrame\(/);
assert.match(modalReport, /selectedPair\?\.diagnosticAttempt !== attempt \|\| !modal\.isConnected/);
assert.ok(modalReport.indexOf("reportPairingUi('modal-mounted'") < modalReport.indexOf('if (modal.hidden)'),
  'mounted acknowledgement must be independent from visibility checks');
assert.match(modalReport, /getComputedStyle\(modal\)/);
assert.match(modalReport, /modal\.getClientRects\(\)\.length === 0/);
assert.match(modalReport, /reportPairingUi\('modal-mounted', null, attempt\)/);
assert.match(modalReport, /reportPairingUi\('modal-visible', null, attempt\)/);
assert.match(modalReport, /proves only application DOM state, not native-window or physical-screen visibility/);
assert.match(frontend, /reportPairingUi\('decision-confirmed', null, request\.diagnosticAttempt!/);
assert.match(frontend, /reportPairingUi\('decision-rejected', null, request\.diagnosticAttempt!/);

for (const [phase, event] of [
  ['modal-mounted', 'UiModalMounted'],
  ['modal-visible', 'UiModalVisible'],
  ['decision-confirmed', 'UiDecisionConfirmed'],
  ['decision-rejected', 'UiDecisionRejected'],
  ['decision-cancelled', 'UiDecisionCancelled'],
]) {
  assert.ok(command.includes(`"${phase}"`), `missing allowlisted phase: ${phase}`);
  assert.ok(command.includes(`FunnelEvent::${event}`), `missing funnel mapping: ${event}`);
}
assert.match(command, /record_funnel_ui\(attempt\.unwrap\(\), event\)/);
assert.doesNotMatch(command, /session_id|task_id|device_id|verification_code|peer_code|sdp|ip_address/i);
console.log('pairing UI observability contract passed (static frontend/native command contract; DOM behavior is covered by lan-picker-ui mock test)');
