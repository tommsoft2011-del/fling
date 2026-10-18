import assert from 'node:assert/strict';
import { startFailureFeedback } from '../src/lan-start-error';

for (const [stage, reason, expected] of [
  ['endpoints', 'peer_unavailable', /重新搜索/],
  ['tcp_connect', 'connect_refused', /拒绝/],
  ['tcp_connect', 'connect_timeout', /连接对端超时/],
  ['tcp_connect', 'network_unreachable', /网络无法连接/],
  ['tcp_connect', 'permission_denied', /未获准/],
  ['hello_send', 'hello_timeout', /发送配对请求超时/],
  ['hello_send', 'hello_write', /配对请求发送失败/],
  ['response_receive', 'hello_timeout', /等待对端响应超时/],
  ['response_receive', 'hello_read_or_invalid', /有效.*配对响应/],
  ['identity_validation', 'unexpected_peer', /身份校验未通过/],
  ['secure_session', 'key_agreement_rejected', /安全会话未能建立/],
  ['session_install', 'session_install', /本地配对状态/],
  ['prompt_emit', 'prompt_emit_failed', /提示未能发送到界面/],
] as const) {
  const feedback = startFailureFeedback({ stage, reason, sourceRetained: true,
    message: 'RAW_PRIVATE_PATH 192.168.9.1 SESSION_ID SHORT_CODE' });
  assert.match(feedback.message, expected);
  assert.equal(feedback.sourceRetained, true);
  assert.doesNotMatch(feedback.message, /重新选择文件|RAW_PRIVATE|192\.168|SESSION_ID|SHORT_CODE/);
}
const lost = startFailureFeedback({ stage: 'request_start', reason: 'source_missing', sourceRetained: false });
assert.equal(lost.sourceRetained, false);
assert.match(lost.message, /重新选择文件/);
for (const error of ['RAW_PRIVATE_PATH 192.168.9.1', new Error('RAW_PRIVATE_PATH'),
  { stage: 'unsafe-stage', reason: 'io', sourceRetained: false },
  { stage: 'tcp_connect', reason: 'RAW_PRIVATE_PATH', sourceRetained: false }]) {
  const feedback = startFailureFeedback(error);
  assert.equal(feedback.sourceRetained, undefined);
  assert.match(feedback.message, /尚未确认失败阶段/);
  assert.doesNotMatch(feedback.message, /RAW_PRIVATE|192\.168|重新选择文件/);
}
assert.equal(startFailureFeedback({ stage: 'request_start', reason: 'source_registry_unavailable' }).sourceRetained, undefined);
assert.equal(startFailureFeedback({ stage: 'request_start', reason: 'source_missing', sourceRetained: true }).sourceRetained, undefined);
console.log('LAN start failure feedback passed: closed-stage mapping, timeout/error distinction, no raw error disclosure, no false reselect for retained/unknown sources.');
