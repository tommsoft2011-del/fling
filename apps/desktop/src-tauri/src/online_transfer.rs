//! Online file transfer engine in Rust.
//!
//! Replaces the TypeScript `FlingRoom.sendFile()` data plane and the
//! receiver reassembly. Two paths, mirroring the TS implementation:
//!
//! 1. **Direct P2P**: FLNG binary frames over the `fling-file` DataChannel
//!    (same wire format as the LAN path). Uses the optimized windowing:
//!    8 MiB in-flight window, 128 KiB coalesced batch frames, event-driven
//!    backpressure. Cumulative ACKs (`file-chunk-ack`) arrive over the
//!    encrypted LiveKit relay by design.
//! 2. **Relay fallback**: base64 `file-chunk` messages over the LiveKit data
//!    channel, for when P2P cannot be established.
//!
//! Wire compatibility with the TypeScript and CLI implementations is
//! mandatory: the remote peer may run any of them.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Mutex, Notify};

use crate::online_proto::{FileKind, FileManifest, FlingMessage};

// Reuse the FLNG codec and transfer constants from the LAN path.
use crate::native_lan::{
    decode_direct_file_frame, encode_direct_file_frame,
};
use crate::native_lan_runtime::NATIVE_CHUNK_SIZE;

/// In-flight window: 8 MiB (matches the v0.6.6 LAN optimization).
const SENDER_WINDOW_CHUNKS: u32 = 256;
/// Max chunks coalesced into one FLNG batch frame (4 x 32 KiB = 128 KiB).
const BATCH_CHUNKS_PER_FRAME: u32 = 4;
/// Receiver sends a cumulative ACK every N chunks.
const ACK_INTERVAL_CHUNKS: u32 = 8;
/// DataChannel backpressure threshold.
const MAX_BUFFERED_BYTES: u64 = 256 * 1024;
/// How long to wait for a manifest ACK / file-complete ACK.
const MANIFEST_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const FILE_COMPLETE_TIMEOUT: Duration = Duration::from_secs(60);

/// Build a `FileManifest` for a local file: streams the file once to
/// compute SHA-256 and the chunk count. Mirrors `buildManifest()`.
pub fn build_manifest(path: &Path, name: String, kind: FileKind) -> Result<FileManifest, String> {
    let mut file = File::open(path).map_err(|e| format!("open file for manifest: {e}"))?;
    let size = file
        .metadata()
        .map_err(|e| format!("stat file for manifest: {e}"))?
        .len();
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 128 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| format!("hash file: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let sha256 = format!("{:x}", hasher.finalize());
    let chunk_size = NATIVE_CHUNK_SIZE as u32;
    let total_chunks = ((size + u64::from(chunk_size) - 1) / u64::from(chunk_size)) as u32;
    // Transfer id: 24 hex chars (12 bytes), matching the FLNG id format.
    let mut id_bytes = [0u8; 12];
    rand::RngCore::fill_bytes(&mut rand::rng(), &mut id_bytes);
    let id: String = id_bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(FileManifest {
        id,
        name,
        size,
        sha256,
        chunk_size,
        total_chunks,
        kind,
        mime_type: None,
        path: None,
    })
}

/// Sender-side state for one file transfer.
pub struct OnlineSender {
    manifest: FileManifest,
    file: Mutex<File>,
    /// Next chunk sequence to send.
    seq: Mutex<u32>,
    /// Highest cumulatively ACKed chunk (from `file-chunk-ack`).
    acked: Arc<std::sync::atomic::AtomicU32>,
    ack_notify: Arc<Notify>,
    /// Set when the receiver confirms persistence (`task-ack` / complete).
    complete_notify: Arc<Notify>,
    sent_bytes: Mutex<u64>,
}

impl OnlineSender {
    pub fn new(manifest: FileManifest, path: &Path) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("open source file: {e}"))?;
        Ok(Self {
            manifest,
            file: Mutex::new(file),
            seq: Mutex::new(0),
            acked: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            ack_notify: Arc::new(Notify::new()),
            complete_notify: Arc::new(Notify::new()),
            sent_bytes: Mutex::new(0),
        })
    }

    pub fn manifest(&self) -> &FileManifest {
        &self.manifest
    }

    /// Handle a `file-chunk-ack` from the receiver.
    pub fn on_chunk_ack(&self, next_seq: u32) {
        let prev = self.acked.load(std::sync::atomic::Ordering::SeqCst);
        if next_seq > prev {
            self.acked.store(next_seq, std::sync::atomic::Ordering::SeqCst);
            self.ack_notify.notify_waiters();
        }
    }

    /// Mark the transfer complete (receiver persisted the file).
    pub fn on_complete(&self) {
        self.complete_notify.notify_waiters();
    }

    /// Wait until `acked >= target` or timeout. Returns true if satisfied.
    async fn wait_for_ack(&self, target: u32, timeout: Duration) -> bool {
        loop {
            let notified = self.ack_notify.notified();
            let acked = self.acked.load(std::sync::atomic::Ordering::Acquire);
            if acked >= target {
                return true;
            }
            if tokio::time::timeout(timeout, notified).await.is_err() {
                return false;
            }
        }
    }

    /// Send the file over a direct P2P DataChannel using FLNG batch frames.
    /// `send_frame` transmits one encoded frame; `on_progress` reports
    /// `(sent_bytes, total_bytes)`.
    pub async fn send_via_p2p(
        &self,
        send_frame: impl Fn(Vec<u8>) -> Result<(), String>,
        buffered_amount: impl Fn() -> u64,
        on_progress: impl Fn(u64, u64),
    ) -> Result<(), String> {
        let total_chunks = self.manifest.total_chunks;
        let size_bytes = self.manifest.size;
        let transfer_id = self.manifest.id.clone();

        // Backpressure is polling-based (see below).
        let mut seq_guard = self.seq.lock().await;
        let mut seq = *seq_guard;
        drop(seq_guard);

        let mut hasher = Sha256::new();
        let mut hashed_seq: u32 = 0;

        while seq < total_chunks {
            let acked = self.acked.load(std::sync::atomic::Ordering::Acquire);
            if seq.saturating_sub(acked) >= SENDER_WINDOW_CHUNKS {
                let target = acked.saturating_add(ACK_INTERVAL_CHUNKS);
                if !self.wait_for_ack(target, Duration::from_secs(30)).await {
                    return Err("P2P cumulative ACK timeout".to_string());
                }
                continue;
            }

            // Coalesce up to BATCH_CHUNKS_PER_FRAME chunks per FLNG frame.
            let window_left = SENDER_WINDOW_CHUNKS.saturating_sub(seq.saturating_sub(acked));
            let chunks_left = total_chunks.saturating_sub(seq);
            let batch_chunks = BATCH_CHUNKS_PER_FRAME
                .min(window_left)
                .min(chunks_left)
                .max(1);
            let batch_seq = seq;

            let mut batch = Vec::with_capacity(batch_chunks as usize * NATIVE_CHUNK_SIZE as usize);
            {
                let mut file = self.file.lock().await;
                for _ in 0..batch_chunks {
                    let chunk_offset = u64::from(seq)
                        .checked_mul(u64::from(NATIVE_CHUNK_SIZE))
                        .ok_or_else(|| "source offset overflow".to_string())?;
                    let remaining = size_bytes.saturating_sub(chunk_offset);
                    let expected = remaining.min(u64::from(NATIVE_CHUNK_SIZE)) as usize;
                    if expected == 0 {
                        return Err("source ended before declared chunk count".to_string());
                    }
                    let mut chunk = vec![0u8; expected];
                    file.seek(SeekFrom::Start(chunk_offset))
                        .map_err(|e| format!("seek source: {e}"))?;
                    file.read_exact(&mut chunk)
                        .map_err(|e| format!("read source: {e}"))?;
                    if seq == hashed_seq {
                        hasher.update(&chunk);
                        hashed_seq += 1;
                    }
                    batch.extend_from_slice(&chunk);
                    seq += 1;
                }
            }

            let frame = encode_direct_file_frame(
                &transfer_id,
                batch_seq,
                &batch,
                NATIVE_CHUNK_SIZE as u16,
                batch_chunks as u16,
            )
            .map_err(|e| format!("encode FLNG frame: {e}"))?;

            // Backpressure: poll buffered_amount() on a short sleep.
            let drain_started = Instant::now();
            while buffered_amount() > MAX_BUFFERED_BYTES {
                if drain_started.elapsed() > Duration::from_secs(15) {
                    return Err("P2P DataChannel send buffer did not drain".to_string());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }

            send_frame(frame)?;

            let sent = u64::from(batch_seq)
                .saturating_mul(u64::from(NATIVE_CHUNK_SIZE))
                .saturating_add(batch.len() as u64);
            *self.sent_bytes.lock().await = sent;
            on_progress(sent, size_bytes);

            // Persist seq for potential resume.
            *self.seq.lock().await = seq;
        }

        // Verify the running hash matches the manifest (sender-side sanity).
        let digest = format!("{:x}", hasher.finalize());
        if digest != self.manifest.sha256 {
            return Err("sender hash mismatch after P2P send".to_string());
        }

        // Wait for the receiver's persistence confirmation.
        if tokio::time::timeout(FILE_COMPLETE_TIMEOUT, self.complete_notify.notified())
            .await
            .is_err()
        {
            return Err("file complete ACK timeout".to_string());
        }
        Ok(())
    }

    /// Send the file via the LiveKit relay: base64 `file-chunk` messages.
    /// Mirrors the TS relay path in `FlingRoom.sendFile()`.
    pub async fn send_via_relay(
        &self,
        send_chunk: impl Fn(u32, String) -> Result<(), String>,
        on_progress: impl Fn(u64, u64),
    ) -> Result<(), String> {
        let total_chunks = self.manifest.total_chunks;
        let size_bytes = self.manifest.size;
        let transfer_id = self.manifest.id.clone();

        let mut seq: u32 = 0;
        let mut hasher = Sha256::new();

        while seq < total_chunks {
            let acked = self.acked.load(std::sync::atomic::Ordering::Acquire);
            if seq.saturating_sub(acked) >= SENDER_WINDOW_CHUNKS {
                let target = acked.saturating_add(ACK_INTERVAL_CHUNKS);
                if !self.wait_for_ack(target, Duration::from_secs(30)).await {
                    return Err("relay cumulative ACK timeout".to_string());
                }
                continue;
            }

            let chunk_offset = u64::from(seq)
                .checked_mul(u64::from(NATIVE_CHUNK_SIZE))
                .ok_or_else(|| "source offset overflow".to_string())?;
            let remaining = size_bytes.saturating_sub(chunk_offset);
            let expected = remaining.min(u64::from(NATIVE_CHUNK_SIZE)) as usize;
            if expected == 0 {
                return Err("source ended before declared chunk count".to_string());
            }
            let mut chunk = vec![0u8; expected];
            {
                let mut file = self.file.lock().await;
                file.seek(SeekFrom::Start(chunk_offset))
                    .map_err(|e| format!("seek source: {e}"))?;
                file.read_exact(&mut chunk)
                    .map_err(|e| format!("read source: {e}"))?;
            }
            hasher.update(&chunk);
            let b64 = B64.encode(&chunk);
            send_chunk(seq, b64)?;

            seq += 1;
            let sent = u64::from(seq).saturating_mul(u64::from(NATIVE_CHUNK_SIZE)).min(size_bytes);
            *self.sent_bytes.lock().await = sent;
            *self.seq.lock().await = seq;
            on_progress(sent, size_bytes);
            let _ = &transfer_id;
        }

        let digest = format!("{:x}", hasher.finalize());
        if digest != self.manifest.sha256 {
            return Err("sender hash mismatch after relay send".to_string());
        }
        if tokio::time::timeout(FILE_COMPLETE_TIMEOUT, self.complete_notify.notified())
            .await
            .is_err()
        {
            return Err("file complete ACK timeout".to_string());
        }
        Ok(())
    }
}

/// Receiver-side state for one file transfer.
pub struct OnlineReceiver {
    manifest: FileManifest,
    /// Reassembly buffer for out-of-order chunks (relay path).
    chunks: Mutex<HashMap<u32, Vec<u8>>>,
    next_seq: Mutex<u32>,
    received_bytes: Mutex<u64>,
    hasher: Mutex<Sha256>,
    /// Where the completed file goes.
    target_path: PathBuf,
    temp_path: PathBuf,
    complete_notify: Arc<Notify>,
}

impl OnlineReceiver {
    pub fn new(manifest: FileManifest, target_dir: &Path) -> Result<Self, String> {
        let file_name = Path::new(&manifest.name)
            .file_name()
            .ok_or_else(|| "manifest has no file name".to_string())?;
        let target_path = target_dir.join(file_name);
        let temp_path = target_dir.join(format!(".{}.part", manifest.id));
        Ok(Self {
            manifest,
            chunks: Mutex::new(HashMap::new()),
            next_seq: Mutex::new(0),
            received_bytes: Mutex::new(0),
            hasher: Mutex::new(Sha256::new()),
            target_path,
            temp_path,
            complete_notify: Arc::new(Notify::new()),
        })
    }

    pub fn manifest(&self) -> &FileManifest {
        &self.manifest
    }

    /// Handle one decoded FLNG batch frame from the P2P DataChannel.
    /// Returns the new `next_seq` (for ACK decisions).
    pub async fn on_p2p_frame(&self, frame_bytes: &[u8]) -> Result<u32, String> {
        let frame = decode_direct_file_frame(frame_bytes)
            .map_err(|e| format!("decode FLNG frame: {e}"))?
            .ok_or_else(|| "not a FLNG frame".to_string())?;
        if frame.id != self.manifest.id {
            return Err("FLNG frame transfer id mismatch".to_string());
        }
        let chunk_size = frame.chunk_size.map(u64::from).unwrap_or(u64::from(self.manifest.chunk_size));
        let count = u64::from(frame.chunk_count);

        // Validate the whole batch, then append contiguously.
        let mut offset = 0usize;
        let mut next = *self.next_seq.lock().await;
        for index in 0..count {
            if next + index as u32 >= self.manifest.total_chunks {
                return Err("FLNG frame exceeds declared chunk count".to_string());
            }
            let received = *self.received_bytes.lock().await;
            let remaining = self.manifest.size.saturating_sub(received);
            let expected = remaining.min(chunk_size) as usize;
            if expected == 0 || frame.payload.len() < offset + expected {
                return Err("FLNG payload length does not match manifest".to_string());
            }
            offset += expected;
        }
        if offset != frame.payload.len() {
            return Err("FLNG frame contains trailing payload bytes".to_string());
        }

        // Append to the temp file.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.temp_path)
                .map_err(|e| format!("open temp file: {e}"))?;
            f.write_all(&frame.payload[..offset])
                .map_err(|e| format!("write temp file: {e}"))?;
            self.hasher.lock().await.update(&frame.payload[..offset]);
        }
        *self.received_bytes.lock().await += offset as u64;
        next += count as u32;
        *self.next_seq.lock().await = next;
        Ok(next)
    }

    /// Handle one base64 `file-chunk` from the relay path.
    pub async fn on_relay_chunk(&self, seq: u32, b64data: &str) -> Result<u32, String> {
        let data = B64
            .decode(b64data)
            .map_err(|e| format!("decode relay chunk: {e}"))?;
        let mut chunks = self.chunks.lock().await;
        let mut next: u32 = *self.next_seq.lock().await;
        let mut received: u64 = *self.received_bytes.lock().await;
        let mut hasher = self.hasher.lock().await;

        chunks.insert(seq, data);
        // Drain contiguous chunks in order.
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.temp_path)
            .map_err(|e| format!("open temp file: {e}"))?;
        while let Some(data) = chunks.remove(&next) {
            f.write_all(&data).map_err(|e| format!("write temp file: {e}"))?;
            hasher.update(&data);
            received += data.len() as u64;
            next += 1;
        }
        *self.next_seq.lock().await = next;
        *self.received_bytes.lock().await = received;
        Ok(next)
    }

    /// Check whether the transfer is complete; if so, verify the hash and
    /// move the temp file into place. Returns true when done.
    pub async fn check_complete(&self) -> Result<bool, String> {
        let next = *self.next_seq.lock().await;
        let received = *self.received_bytes.lock().await;
        if next != self.manifest.total_chunks || received != self.manifest.size {
            return Ok(false);
        }
        let digest = {
            let hasher = self.hasher.lock().await;
            format!("{:x}", hasher.clone().finalize())
        };
        if digest != self.manifest.sha256 {
            return Err(format!(
                "sha256 mismatch after receive (got {digest}, want {})",
                self.manifest.sha256
            ));
        }
        std::fs::rename(&self.temp_path, &self.target_path)
            .map_err(|e| format!("move received file into place: {e}"))?;
        self.complete_notify.notify_waiters();
        Ok(true)
    }

    pub fn should_ack(&self, last_ack_seq: u32, next_seq: u32) -> bool {
        next_seq.saturating_sub(last_ack_seq) >= ACK_INTERVAL_CHUNKS
    }

    pub async fn received_bytes(&self) -> u64 {
        *self.received_bytes.lock().await
    }
}

/// Build a `file-chunk-ack` message for the current receiver state.
pub fn chunk_ack_message(
    receiver: &OnlineReceiver,
    next_seq: u32,
    received_bytes: u64,
    complete: bool,
) -> FlingMessage {
    FlingMessage::FileChunkAck {
        id: receiver.manifest().id.clone(),
        next_seq,
        received_bytes,
        ack_id: None,
        complete: complete.then_some(true),
        failed: None,
    }
}
