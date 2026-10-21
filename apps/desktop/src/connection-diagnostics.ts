// Privacy defense in depth: rebuild a finite schema, never log the native object.
const enums: Record<string, readonly string[]> = {
  role: ['sender', 'receiver'], path: ['Direct', 'Turn'],
  stage: ['turn_decision', 'peer_create', 'channel_create', 'offer_setup', 'offer_create', 'offer_set', 'offer_send',
    'answer_setup', 'offer_parse', 'offer_apply', 'answer_create', 'answer_set', 'answer_send', 'answer_parse', 'answer_apply',
    'description_validate', 'ice_validate', 'ice_add', 'ice_send', 'channel_wait', 'channel_open',
    'pair_observe', 'ready_proof', 'ready', 'decision', 'decision_ack', 'request', 'request_ack', 'prepare', 'prepared',
    'commit', 'committed', 'start', 'start_ack', 'start_confirmed', 'abort', 'attempt_abort', 'control_receive', 'first_frame_permit', 'attempt_end'],
  outcome: ['started', 'ok', 'failed', 'rejected', 'observed'],
  reason: ['none', 'operation_failed', 'interrupted', 'timeout', 'bounds', 'generation', 'malformed', 'invalid_syntax',
    'ufrag_mismatch', 'bucket_limit', 'duplicate', 'stale_fence', 'peer_closed'],
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
    pair.selected = source.selected; pair.nominated = source.nominated; result.pair = pair;
  }
  return result;
}
