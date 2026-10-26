// Privacy defense in depth: rebuild a finite schema, never log the native object.
const enums: Record<string, readonly string[]> = {
  role: ['sender', 'receiver'], path: ['Direct', 'Turn'],
  stage: ['turn_decision', 'peer_create', 'channel_create', 'offer_setup', 'offer_create', 'offer_set', 'offer_send',
    'answer_setup', 'offer_parse', 'offer_apply', 'answer_create', 'answer_set', 'answer_send', 'answer_parse', 'answer_apply',
    'description_validate', 'ice_validate', 'ice_add', 'ice_send', 'channel_wait', 'direct_ice_cutoff', 'direct_channel_wait_terminal', 'channel_open',
    'pair_observe', 'ready_proof', 'ready', 'decision', 'decision_ack', 'request', 'request_ack', 'prepare', 'prepared',
    'commit', 'committed', 'start', 'start_ack', 'start_confirmed', 'abort', 'attempt_abort', 'control_receive', 'first_frame_permit', 'attempt_end'],
  outcome: ['started', 'ok', 'failed', 'rejected', 'observed'],
  reason: ['none', 'operation_failed', 'interrupted', 'timeout', 'bounds', 'generation', 'malformed', 'invalid_syntax',
    'ufrag_mismatch', 'bucket_limit', 'duplicate', 'stale_fence', 'peer_closed',
    'ice_stats_unavailable', 'no_succeeded_pairs', 'ice_checks_in_progress', 'ice_checks_failed', 'ice_pair_succeeded'],
};
const pairEnums: Record<string, readonly string[]> = {
  route: ['direct', 'turn_relay', 'unknown'], pair_state: ['frozen', 'waiting', 'in_progress', 'failed', 'succeeded', 'unknown'],
  local_type: ['host', 'srflx', 'prflx', 'relay', 'unknown'], remote_type: ['host', 'srflx', 'prflx', 'relay', 'unknown'],
  local_protocol: ['udp', 'tcp', 'unknown'], remote_protocol: ['udp', 'tcp', 'unknown'],
};
export function safeConnectionDiagnostic(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object') return null;
  const input = value as Record<string, unknown>;
  const result: Record<string, unknown> = {};
  for (const [key, allowed] of Object.entries(enums)) {
    const v = input[key];
    if (typeof v !== 'string' || !allowed.includes(v)) return null;
    result[key] = v;
  }
  for (const key of ['elapsed_ms', 'count']) {
    const v = input[key];
    if (typeof v !== 'number' || !Number.isSafeInteger(v) || v < (key === 'count' ? 1 : 0)) return null;
    result[key] = v;
  }
  if (input.pair !== undefined) {
    if (!input.pair || typeof input.pair !== 'object') return null;
    const source = input.pair as Record<string, unknown>;
    const pair: Record<string, unknown> = {};
    for (const [key, allowed] of Object.entries(pairEnums)) {
      const v = source[key];
      if (typeof v !== 'string' || !allowed.includes(v)) return null;
      pair[key] = v;
    }
    if (typeof source.selected !== 'boolean' || !(source.nominated === null || typeof source.nominated === 'boolean')) return null;
    pair.selected = source.selected; pair.nominated = source.nominated;
    // Checklist aggregates emitted by the native summary: pair counts by
    // state and host/srflx candidate presence per side. Counts/flags only, never IDs or
    // addresses. Same strictness as elapsed_ms/count: a present but
    // malformed value invalidates the whole record.
    for (const key of ['pairs_total', 'pairs_succeeded', 'pairs_in_progress', 'pairs_failed', 'pairs_succeeded_direct', 'pairs_succeeded_relay']) {
      const v = source[key];
      if (v === undefined) continue;
      if (typeof v !== 'number' || !Number.isSafeInteger(v) || v < 0) return null;
      pair[key] = v;
    }
    for (const key of ['local_host', 'remote_host', 'local_srflx', 'remote_srflx']) {
      const v = source[key];
      if (v === undefined) continue;
      if (typeof v !== 'boolean') return null;
      pair[key] = v;
    }
    result.pair = pair;
  }
  // The global logger deliberately strips generic path/reason keys. These
  // aliases carry ONLY validated finite enums; do not relax its privacy filter.
  result.transport_path = result.path; result.failure_category = result.reason;
  delete result.path; delete result.reason;
  return result;
}
