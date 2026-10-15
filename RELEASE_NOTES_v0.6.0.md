# Fling v0.6.0

## Faster direct transfers

Direct DataChannel send windows now adapt from 1 MiB to 4 MiB and up to a hard 8 MiB cap after timely receiver acknowledgements; slow progressing paths step back down. The 1 MiB initial stage retains early direct-black-hole detection. Receiver progress acknowledgements remain encrypted and continue every 1 MiB, and direct failures still trigger relay repair from the first missing contiguous chunk.

New diagnostics report per-window bytes and send-to-ACK timing, drain wait count/duration, `bufferedAmount` high-water, relay-repair bytes, selected ICE candidate/network type, RTT, and available outgoing bitrate. A deterministic A/B benchmark isolates ACK-window amortization; modeled rates are illustrative, not a prediction of a user's LAN throughput.

## Reliability retained

Receiver ACKs and final saved-file/task acknowledgements remain required. SHA-256 validation, fsync/sync, atomic no-overwrite commit, bounded DataChannel backpressure, DTLS encryption, missing-chunk repair, and direct-to-relay fallback are unchanged. Received/saved progress and task-row ordering behavior are unchanged.

Regression coverage includes adaptive window growth/backoff and caps, delayed/lost ACKs, missing chunks, direct black-hole/fallback, bounded sender buffering and receive batches, checksum/save, and multi-file final-ACK ordering. The existing release workflow is unchanged and will build the release assets from the exact v0.6.0 tag.
