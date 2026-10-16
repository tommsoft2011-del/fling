# Fling v0.6.3

## Changes

- Replaced periodic polling in direct-window and durable-finalization ACK waits with event-driven waiters, including timeout and room-close cancellation.
- Added negotiated, direct-only coalescing of unchanged 32 KiB logical chunks into frames bounded by both peers' advertised `maxMessageSize` and a 128 KiB payload ceiling. Older peers and relay sends retain single logical chunks.
- Kept the receive-to-disk batch default at 4 MiB and added a hard 16 MiB ceiling for bounded evaluation. Appends remain strictly sequential and awaited; hash verification, `fsync`, atomic no-overwrite commit, and final durable-save ACK are unchanged.
- Added a per-file, single-timer progress-ACK heartbeat for partial receive progress that lands between the normal cumulative-ACK boundaries. The timer is cancelled on finalize, receive error, or room close; it allows bounded relay repair windows to receive an ACK without waiting for a retry timeout.
- Kept production direct-window stages at 1/4/8 MiB (8 MiB maximum) and the production drain threshold at 256 KiB. The 16 MiB direct window and larger drain thresholds remain test-only experiment options.

## Validation

- Core TypeScript checks and file-stream, direct-window, routing, loopback-v2, desktop scroll, and saved/received-progress tests passed.
- Rust tests passed: 15 passed, 0 failed. The 4/8/16 MiB receive-save comparison verified serial writes, output size and SHA-256, and `sync_all` for each candidate.
- A size-matched seven-file in-memory FlingRoom comparison (717.64 MiB total) completed with both 8 MiB and 16 MiB experimental window caps. A separate 16 MiB-window silent-loss case completed via bounded relay repair and passed final hash/size checks.
- A shaped in-memory drain comparison covered 256 KiB, 512 KiB, 1 MiB, and 2 MiB settings, including lost-progress-ACK and direct-black-hole recovery.

The transfer A/B runs use synthetic payloads and mock channels; they are not physical LAN/WebRTC or durable-disk throughput claims. Defaults were kept conservative because the local measurements do not establish a reliable real-device performance gain.
