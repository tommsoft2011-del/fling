# Fling v0.6.2

## ACK-path diagnostics

Each receive-progress, final, failure, and repaired completion ACK now carries an optional diagnostic correlation ID. The receiver logs when an ACK is queued, begins local publication, and when the local publish call resolves; the sender logs relay-callback-to-handler delay, ACK-handler duration, and the waiter's unlock delay. Matching IDs let the two device logs be compared without subtracting unsynchronized cross-device wall clocks. A completed local publish call is not evidence of remote receipt or durable save; the sender still relies on the existing receive and final-save acknowledgements.

The ACK ID is optional, so older peers remain compatible. The existing 32 KiB logical chunks and adaptive direct windows (1/4/8 MiB, 8 MiB maximum) are unchanged. This release makes no throughput-improvement claim; paired real-device measurements are still needed to determine whether the remaining wait is in transport delivery or local event handling.

## Reliability retained

DTLS encryption, reliable ordered DataChannels, bounded send queues and backpressure, cumulative receive ACKs, replay/relay repair, SHA-256 verification, fsync/sync, atomic no-overwrite save, and final durable file/task acknowledgements are unchanged.

## Regression coverage

Added deterministic checks that correlate receiver publication and sender ACK logs with the same ID, record sender handler/unlock timings, and prove duplicate or out-of-order cumulative ACKs cannot move the accepted receive prefix backward. Existing tests continue to cover delayed/lost ACKs, missing chunks, slow receivers, direct-channel black holes/closure, relay repair, bounded in-flight buffering, multi-file ordering, hash verification, no-overwrite saving, and interrupted-stream cleanup.
