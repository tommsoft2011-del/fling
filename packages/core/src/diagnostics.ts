/** Return a short, allow-listed diagnostic code for fallback/error messages. */
export function diagnosticReasonCode(reason: string): string {
  const value = reason.toLowerCase();
  if (value.includes('manifest-ack-timeout')) return 'manifest-ack-timeout';
  if (value.includes('chunk-ack-timeout')) return 'chunk-ack-timeout';
  if (value.includes('peer-progress-stalled')) return 'peer-progress-stalled';
  if (value.includes('direct-send-failed')) return 'direct-send-failed';
  if (value.includes('partial-replay-requires-relay')) return 'partial-replay-requires-relay';
  if (value.includes('relay-window-required')) return 'relay-window-required';
  if (value.includes('direct-channel-unavailable')) return 'direct-channel-unavailable';
  if (value.includes('p2p-connect-timeout')) return 'p2p-connect-timeout';
  if (value.includes('native-webrtc-failure')) return 'native-webrtc-failure';
  if (value.includes('manifest ack timeout')) return 'manifest-ack-timeout';
  if (value.includes('chunk ack timeout') || value.includes('window ack timeout')) return 'chunk-ack-timeout';
  if (value.includes('progress stalled') || value.includes('progress stall')) return 'peer-progress-stalled';
  if (value.includes('partial replay')) return 'partial-replay-requires-relay';
  if (value.includes('relay window requires')) return 'relay-window-required';
  if (value.includes('send failed') || value.includes('send failure') || value.includes('frame failed') || value.includes('direct chunk failed')) return 'direct-send-failed';
  if (value.includes('channel closed') || value.includes('channel is')) return 'direct-channel-unavailable';
  if (value.includes('connect timeout')) return 'p2p-connect-timeout';
  if (value.includes('cancel')) return 'cancelled';
  if (value.includes('ice') || value.includes('dtls') || value.includes('webrtc') || value.includes('native')) return 'native-webrtc-failure';
  return 'fallback-requested';
}
