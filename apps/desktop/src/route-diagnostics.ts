// Defense in depth: native diagnostics must never forward arbitrary keys or strings to logs.
const choices = {
  role: ['initiator', 'responder', 'unknown'],
  phase: ['fallback_requested', 'relay_candidate', 'data_channel_open', 'first_real_bytes',
    'disk_synced', 'hash_verified', 'atomic_commit', 'durable_ack_sent', 'durable_ack_received',
    'task_ack_sent', 'task_ack_received', 'selected_pair', 'route_change', 'stats_unavailable',
    'connection_failed', 'connection_closed', 'transfer_failed', 'task_cancelled', 'task_end'],
  route: ['direct', 'turn_relay', 'unknown'],
  pair_state: ['frozen', 'waiting', 'in_progress', 'failed', 'succeeded', 'unknown'],
  local_type: ['host', 'srflx', 'prflx', 'relay', 'unknown'],
  remote_type: ['host', 'srflx', 'prflx', 'relay', 'unknown'],
  local_protocol: ['udp', 'tcp', 'unknown'],
  remote_protocol: ['udp', 'tcp', 'unknown'],
} as const;
export type NativeRoute = 'direct' | 'relay' | 'unknown';
export function safeRouteDiagnostic(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object') return null;
  const input = value as Record<string, unknown>;
  const result: Record<string, unknown> = {};
  for (const [key, allowed] of Object.entries(choices)) {
    const v = input[key];
    if (typeof v !== 'string' || !(allowed as readonly string[]).includes(v)) return null;
    result[key] = v;
  }
  for (const key of ['elapsed_ms', 'snapshot_age_ms']) {
    const v = input[key];
    if (key === 'snapshot_age_ms' && v === null) { result[key] = null; continue; }
    if (typeof v !== 'number' || !Number.isSafeInteger(v) || v < 0) return null;
    result[key] = v;
  }
  if (typeof input.selected !== 'boolean' || !(input.nominated === null || typeof input.nominated === 'boolean')) return null;
  if (input.transfer_no !== null && (typeof input.transfer_no !== 'number' || !Number.isSafeInteger(input.transfer_no) || input.transfer_no < 1)) return null;
  result.transfer_no = input.transfer_no;
  result.selected = input.selected;
  result.nominated = input.nominated;
  return result;
}
export function nativeRouteLabel(route: NativeRoute | undefined): string {
  return route === 'direct' ? '已建立直连' : route === 'relay' ? '已通过 TURN 中继连接'
    : route === 'unknown' ? '连接已建立，路径暂不可确认' : '';
}
