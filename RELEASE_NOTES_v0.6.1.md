# Fling v0.6.1

## Faster multi-file handoff

When the receiver has all bytes for one file and is doing its durable save, the sender now streams a SHA-256 hash for at most the next already-loaded `Blob`. This overlaps manifest preparation with receiver write/sync time without sending the next file early. Lazy path-backed files are not read ahead, and the existing bounded transfer queues remain unchanged. `hashFile` reads fixed 256 KiB slices, so prehash adds at most one 256 KiB byte buffer plus small hash/manifest state, independent of Blob size; it does not copy the whole Blob. There is no additional Blob-size cap, but only one already-loaded next Blob can be in prehash at a time. Browser Blob/File content is immutable; a regression snapshots the manifest, verifies it is unchanged, verifies received content and the same SHA-256, and confirms `sendFile` does not hash the Blob a second time.

Handoff benefit is not yet quantified on a paired real-device run. The prior v0.6.0 single run recorded roughly 6.7 s average file-transition gaps and 2.7–5.3 s save/finalize tails; v0.6.1 logs prehash start/completion duration, but no comparable post-change Windows/peer measurement is available, so no latency reduction is claimed.

The DataChannel drain threshold remains 256 KiB. An isolated shaped in-memory channel compared 256 KiB with 512 KiB over three 8 MiB runs per setting (7.594 MB/s mock drain rate, 8 ms mock ACK publish delay): median throughput was 6.230 vs 6.233 MB/s (+0.05%); median waits/time were 232 / 935 ms vs 201 / 805 ms; maximum `bufferedAmount` was 262,352 vs 524,704 B; window-ACK P50/P90 was 44/128 vs 79/164 ms. Both settings recovered one deliberately lost progress ACK with no repair/retry/error, and repaired the same 70,000-byte black-holed chunk stream through relay in about 8.1 s with no error. The negligible modeled throughput difference does not justify doubling queued bytes, particularly with increased ACK latency, so production stays at 256 KiB. These are mock-channel measurements, not a physical LAN/WebRTC benchmark.

## Reliability retained

Receive-confirmed direct windows, relay repair/retry, DTLS encryption, SHA-256 verification, streaming save, `sync_all`, atomic no-overwrite commit, and final task ACK behavior are preserved. The direct window remains capped at 8 MiB.

Regression coverage includes delayed and lost progress ACKs, black-holed and partially lost direct paths, precomputed-manifest reuse, and a multi-file stress case that verifies the next file's first window cannot precede the previous file's durable-save ACK.
