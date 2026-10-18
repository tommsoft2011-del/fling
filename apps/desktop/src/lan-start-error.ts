// Only closed native stage/reason codes may affect a user-facing message.
// Never echo an IPC error string: it may contain paths, addresses or identifiers.
const stages = new Set(['ui_button', 'request_start', 'endpoints', 'tcp_connect', 'hello_send',
  'response_receive', 'identity_validation', 'secure_session', 'session_install', 'prompt_emit',
  'tcp_accept', 'hello_receive', 'hello_validation', 'response_send', 'ui_prompt']);
const reasons = new Set(['none', 'lan_disabled', 'source_missing', 'source_registry_unavailable',
  'peer_unavailable', 'endpoint_not_eligible', 'connect_refused', 'connect_timeout',
  'network_unreachable', 'permission_denied', 'io', 'hello_timeout', 'hello_read_or_invalid',
  'hello_write', 'unexpected_peer', 'replay_rejected', 'config_unavailable',
  'key_agreement_rejected', 'session_install', 'prompt_emit_failed']);

export interface StartFailureFeedback { message: string; sourceRetained: boolean | undefined }
export function startFailureFeedback(error: unknown): StartFailureFeedback {
  const fallback = { message: '配对请求未能启动，尚未确认失败阶段；可重试并导出两端同轮日志。', sourceRetained: undefined };
  if (!error || typeof error !== 'object') return fallback;
  const value = error as Record<string, unknown>;
  if (typeof value.stage !== 'string' || !stages.has(value.stage)
    || typeof value.reason !== 'string' || !reasons.has(value.reason)) return fallback;
  const sourceRetained = typeof value.sourceRetained === 'boolean' ? value.sourceRetained : undefined;
  let message: string;
  if (value.reason === 'source_missing') {
    message = '本机所选文件已失效，请重新选择文件后重试。';
  } else if (value.reason === 'lan_disabled') {
    message = '尚未启用局域网模式；请进入仅局域网测试后重试。';
  } else if (value.reason === 'source_registry_unavailable') {
    message = '本机暂时无法访问文件选择状态；请重试，仍失败请导出本机日志。';
  } else if (value.stage === 'endpoints') {
    message = '设备已发现，但暂无可用连接端点；请重新搜索附近设备后重试。';
  } else if (value.stage === 'tcp_connect') {
    message = value.reason === 'connect_timeout'
      ? '连接对端超时；请检查两端网络和防火墙后重试。'
      : value.reason === 'connect_refused'
        ? '对端拒绝了配对连接；请确认对端正在运行同一测试版并允许本地连接后重试。'
        : value.reason === 'network_unreachable'
          ? '当前网络无法连接对端；请检查两台设备在同一局域网后重试。'
          : value.reason === 'permission_denied'
            ? '本机未获准建立配对连接；请检查防火墙或安全软件后重试。'
            : '连接对端失败，尚未发送配对请求；请检查对端运行状态并导出两端日志。';
  } else if (value.stage === 'hello_send') {
    message = value.reason === 'hello_timeout'
      ? '已连接对端，但发送配对请求超时；请重试并导出两端同轮日志。'
      : '已连接对端，但配对请求发送失败；请重试并导出两端同轮日志。';
  } else if (value.stage === 'response_receive') {
    message = value.reason === 'hello_timeout'
      ? '已发送配对请求，等待对端响应超时；请保持对端运行并导出两端同轮日志。'
      : '未收到有效的对端配对响应；请确认两端使用同一测试版后重试。';
  } else if (value.stage === 'identity_validation' || value.stage === 'hello_validation') {
    message = value.reason === 'unexpected_peer'
      ? '配对身份校验未通过，已停止；请重新搜索并核对目标设备。'
      : '配对安全校验未通过，已停止；请重新搜索目标设备后重试。';
  } else if (value.stage === 'secure_session') {
    message = '配对安全会话未能建立，尚未进入短码确认；请重试并导出两端日志。';
  } else if (value.stage === 'prompt_emit') {
    message = '本地配对提示未能发送到界面；请重试，仍失败请导出本机日志。';
  } else if (value.stage === 'session_install') {
    message = '本地配对状态未能建立；请重试并导出本机日志。';
  } else {
    message = '配对准备未完成；请重试并导出两端同轮日志。';
  }
  // Only an explicit native source-loss result asks for a new selection.
  if (sourceRetained === true && value.reason === 'source_missing') return fallback;
  if (sourceRetained === false && value.reason !== 'source_missing') {
    message += ' 本机文件选择已失效，请重新选择文件后重试。';
  }
  return { message, sourceRetained };
}
