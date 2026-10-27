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
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, Notify};

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
/// DataChannel backpressure threshold (2 MiB; frames are 128 KiB, so the
/// old 256 KiB limit held barely 2 frames before stalling the pump).
const MAX_BUFFERED_BYTES: u64 = 2 * 1024 * 1024;
/// How long to wait for a manifest ACK / file-complete ACK.
const MANIFEST_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const FILE_COMPLETE_TIMEOUT: Duration = Duration::from_secs(60);
/// No-progress budget for cumulative ACKs while the send window is full.
/// The receiver writes every frame to disk under host antivirus/IO load, so
/// 30 s of ACK silence is reachable on a healthy but slow receiver; a truly
/// dead path still aborts promptly via the route failure event, which wakes
/// `wait_for_ack` through `on_failed`. 90 s only bounds silent stalls.
const ACK_PROGRESS_TIMEOUT: Duration = Duration::from_secs(90);
/// How long the DataChannel send buffer may stay above the backpressure
/// threshold before the send is declared stalled. Draining is paced by the
/// receiver's disk consumption, so this gets the same headroom rationale
/// as ACK_PROGRESS_TIMEOUT.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(45);

/// A sender-side source that is read **only** in place, never copied to a
/// staging directory:
///
/// * `File` — an already-open, read-only handle to the user's source file.
///   The sender seeks/reads from this handle directly; the source file is
///   never modified, moved, or copied anywhere (no `fling-online` spill,
///   no temp copy). The handle is opened read-only by the source registry.
/// * `Memory` — bytes that have no source file at all (OS clipboard image,
///   or a WebView-owned Blob staged in RAM by the new memory commands).
///   These are the only sources that are not a disk file, and they never
///   touch disk either: they are streamed from the in-memory buffer.
///
/// Protocol note: the chunk size (32 KiB, `NATIVE_CHUNK_SIZE`) is unchanged,
/// so manifests, chunk indices, FLNG frames and ACK geometry are identical
/// to already-published versions. Only the in-process reading path changed.
pub enum SendSource {
    File { file: File, pos: u64, len: u64 },
    Memory { data: Arc<Vec<u8>>, pos: usize },
}

impl SendSource {
    pub fn from_file(file: File) -> Result<Self, String> {
        let len = file.metadata().map_err(|e| format!("stat source: {e}"))?.len();
        Ok(Self::File { file, pos: 0, len })
    }

    pub fn from_memory(data: Arc<Vec<u8>>) -> Self {
        Self::Memory { data, pos: 0 }
    }

    /// Build a file source when the length is already known (source
    /// registry: the metadata size is the fallback if a metadata read
    /// fails; the manifest builder then checks the built size against
    /// the registered size, so a changed file still fails loudly).
    pub fn from_file_len(file: File, len: u64) -> Self {
        Self::File { file, pos: 0, len }
    }

    pub fn len(&self) -> u64 {
        match self {
            Self::File { len, .. } => *len,
            Self::Memory { data, .. } => data.len() as u64,
        }
    }

    fn rewind(&mut self) -> Result<(), String> {
        match self {
            Self::File { file, pos, .. } => {
                file.seek(SeekFrom::Start(0)).map_err(|e| format!("rewind source: {e}"))?;
                *pos = 0;
            }
            Self::Memory { pos, .. } => *pos = 0,
        }
        Ok(())
    }

    /// Sequential stream read used by the manifest hash pass. Advances the
    /// read position; callers rewind before the send pass.
    fn read_stream(&mut self, buf: &mut [u8]) -> Result<usize, String> {
        match self {
            Self::File { file, pos, .. } => {
                let n = file.read(buf).map_err(|e| format!("read source: {e}"))?;
                *pos += n as u64;
                Ok(n)
            }
            Self::Memory { data, pos } => {
                let n = (*pos + buf.len()).min(data.len()) - (*pos).min(data.len());
                buf[..n].copy_from_slice(&data[*pos..*pos + n]);
                *pos += n;
                Ok(n)
            }
        }
    }

    /// Read exactly `buf.len()` bytes starting at `offset`. For a file this
    /// seeks only when the current position differs from `offset`, so the
    /// normal in-order send path is a pure sequential read (no seek syscall
    /// per 32 KiB chunk, OS readahead stays effective). For memory this is
    /// a slice copy, never a disk operation.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), String> {
        match self {
            Self::File { file, pos, .. } => {
                if *pos != offset {
                    file.seek(SeekFrom::Start(offset)).map_err(|e| format!("seek source: {e}"))?;
                    *pos = offset;
                }
                file.read_exact(buf).map_err(|e| format!("read source: {e}"))?;
                *pos += buf.len() as u64;
                Ok(())
            }
            Self::Memory { data, pos } => {
                let start = usize::try_from(offset).map_err(|_| "source offset overflow")?;
                let end = start.checked_add(buf.len()).ok_or("source offset overflow")?;
                if end > data.len() {
                    return Err("source ended before declared chunk count".into());
                }
                buf.copy_from_slice(&data[start..end]);
                *pos = end;
                Ok(())
            }
        }
    }
}

/// Build a `FileManifest` for a source: streams it once to compute SHA-256
/// and the chunk count. This is the one mandatory hash pass — the manifest
/// must carry the SHA before any byte is sent, so it cannot be skipped.
/// The send pass does NOT hash again (see `send_via_p2p`); the receiver is
/// the authoritative verifier against this manifest SHA.
pub fn build_manifest(path: &Path, name: String, kind: FileKind) -> Result<FileManifest, String> {
    let file = File::open(path).map_err(|e| format!("open file for manifest: {e}"))?;
    let mut source = SendSource::from_file(file)?;
    build_manifest_source(&mut source, name, kind, &AtomicBool::new(false))
}

pub fn build_manifest_handle(file: &mut File, name: String, kind: FileKind, cancelled: &AtomicBool) -> Result<FileManifest, String> {
    // Kept for callers/tests that still hold a raw File handle: wrap it
    // (read-only, in place) and delegate to the shared manifest builder.
    let len = file.metadata().map_err(|e| format!("stat file for manifest: {e}"))?.len();
    // The caller retains ownership of `file`; clone the handle so the
    // manifest pass does not move it.
    let cloned = file.try_clone().map_err(|e| format!("clone source handle: {e}"))?;
    let mut source = SendSource::File { file: cloned, pos: 0, len };
    build_manifest_source(&mut source, name, kind, cancelled)
}

pub fn build_manifest_source(source: &mut SendSource, name: String, kind: FileKind, cancelled: &AtomicBool) -> Result<FileManifest, String> {
    source.rewind()?;
    let size = source.len();
    let mut hasher = Sha256::new();
    // 1 MiB hash buffer (was 128 KiB): fewer read syscalls on the one
    // mandatory hash pass, no protocol effect.
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        if cancelled.load(Ordering::Acquire) { return Err("已取消".into()); }
        let n = source.read_stream(&mut buf).map_err(|e| format!("hash source: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let sha256 = format!("{:x}", hasher.finalize());
    let chunk_size = NATIVE_CHUNK_SIZE as u32;
    let total_chunks = u32::try_from(size.div_ceil(u64::from(chunk_size))).map_err(|_| "file too large")?;
    source.rewind()?;
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

/// Length in bytes of chunk `seq` in a manifest (full size except the last
/// chunk, which may be short).
fn chunk_len(manifest: &FileManifest, seq: u32) -> u64 {
    manifest.size.saturating_sub(u64::from(seq) * u64::from(manifest.chunk_size))
        .min(u64::from(manifest.chunk_size))
}

/// Split a display file name into (stem, extension-with-dot) for the
/// sequence-rename rule: `report.pdf` -> (`report`, `.pdf`),
/// `archive.tar.gz` -> (`archive.tar`, `.gz`), `notes` -> (`notes`, "").
/// A leading dot (`.gitignore`) is a stem, not an extension.
fn split_name_ext(name: &str) -> (String, String) {
    match name.rfind('.') {
        Some(i) if i > 0 => (name[..i].to_string(), name[i..].to_string()),
        _ => (name.to_string(), String::new()),
    }
}

/// Rename `tmp_path` into `target_dir` under `name`, never overwriting:
/// if the candidate exists, or the rename fails (target occupied,
/// unwritable, …), try `name(1)`, `name(2)`, … with the sequence inserted
/// before the extension, until one rename succeeds. Only after every
/// candidate (up to 10 000, mirroring the old hard-link loop) has been
/// tried does this return an error — the caller then deletes the .tmp
/// file and reports failure. Returns the actual final path, which the
/// caller must report to the UI (it may differ from the original name).
/// This is a same-directory rename: no second copy, no hard link, and the
/// .tmp file itself is the only written copy.
pub fn rename_with_sequence(tmp_path: &Path, target_dir: &Path, name: &str) -> Result<PathBuf, String> {
    let (stem, ext) = split_name_ext(name);
    for i in 0..10_000u32 {
        let candidate = if i == 0 {
            target_dir.join(name)
        } else {
            target_dir.join(format!("{stem}({i}){ext}"))
        };
        // Existence check first: on Unix rename() would otherwise silently
        // overwrite. The check-then-rename window is the same one any
        // rename-based publisher has; a concurrent creator in the same
        // download directory during this millisecond loop is not a
        // supported scenario (the old hard-link loop had the same race
        // only in reverse — it failed instead of renaming).
        if std::fs::symlink_metadata(&candidate).is_ok() {
            continue;
        }
        match std::fs::rename(tmp_path, &candidate) {
            Ok(()) => return Ok(candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            // Occupied / unwritable / any other rename failure: do not
            // discard the transfer, try the next sequence name instead.
            Err(_) => continue,
        }
    }
    Err("rename failed after all sequence attempts".into())
}

/// Sender-side state for one file transfer.
pub struct OnlineSender {
    manifest: FileManifest,
    /// Read-only, in-place source (disk handle or in-memory bytes). Never
    /// a copy: ordinary files are read from their own handle, memory
    /// sources from their buffer.
    source: Mutex<SendSource>,
    /// Next chunk sequence to send.
    seq: Mutex<u32>,
    /// Highest cumulatively ACKed chunk (from `file-chunk-ack`).
    acked: Arc<std::sync::atomic::AtomicU32>,
    ack_notify: Arc<Notify>,
    /// Set when the receiver confirms persistence (`task-ack` / complete).
    complete_notify: Arc<Notify>,
    sent_bytes: Mutex<u64>,
    completed: AtomicBool,
    failed: AtomicBool,
}

impl OnlineSender {
    pub fn new(manifest: FileManifest, path: &Path) -> Result<Self, String> {
        // Read-only open of the user's own file: no copy, no move, no temp.
        let file = std::fs::OpenOptions::new().read(true).open(path)
            .map_err(|e| format!("open source file: {e}"))?;
        Ok(Self::from_source(manifest, SendSource::from_file(file)?))
    }

    /// Legacy name kept for callers/tests that hold a File handle.
    pub fn from_handle(manifest: FileManifest, file: File) -> Self {
        // from_file only fails on a metadata read; fall back to the
        // manifest size so a send attempt still fails loudly on the first
        // short read instead of silently sending an empty source.
        let len = file.metadata().map(|m| m.len()).unwrap_or(manifest.size);
        let source = SendSource::File { file, pos: 0, len };
        Self::from_source(manifest, source)
    }

    pub fn from_source(manifest: FileManifest, source: SendSource) -> Self {
        Self {
            manifest,
            source: Mutex::new(source),
            seq: Mutex::new(0),
            acked: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            ack_notify: Arc::new(Notify::new()),
            complete_notify: Arc::new(Notify::new()),
            sent_bytes: Mutex::new(0),
            completed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        }
    }

    pub fn manifest(&self) -> &FileManifest {
        &self.manifest
    }

    pub fn valid_ack_geometry(&self, next_seq:u32, received_bytes:u64) -> bool {
        next_seq <= self.manifest.total_chunks
            && received_bytes == (u64::from(next_seq)*u64::from(self.manifest.chunk_size)).min(self.manifest.size)
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
        self.completed.store(true, Ordering::Release);
        self.complete_notify.notify_one();
    }

    pub fn is_failed(&self) -> bool { self.failed.load(Ordering::Acquire) }

    pub fn on_failed(&self) {
        self.failed.store(true, Ordering::Release);
        self.ack_notify.notify_one();
        self.complete_notify.notify_one();
    }

    async fn wait_complete(&self) -> Result<(), String> {
        tokio::time::timeout(FILE_COMPLETE_TIMEOUT, async {
            loop {
                let notified = self.complete_notify.notified();
                if self.failed.load(Ordering::Acquire) { return Err("receiver failed transfer".into()); }
                if self.completed.load(Ordering::Acquire) { return Ok(()); }
                notified.await;
            }
        }).await.map_err(|_| "file complete ACK timeout".to_string())?
    }

    /// Wait until `acked >= target` or timeout. Returns true if satisfied.
    async fn wait_for_ack(&self, target: u32, timeout: Duration) -> bool {
        loop {
            let notified = self.ack_notify.notified();
            let acked = self.acked.load(std::sync::atomic::Ordering::Acquire);
            if self.failed.load(Ordering::Acquire) { return false; }
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
        let seq_guard = self.seq.lock().await;
        let mut seq = *seq_guard;
        drop(seq_guard);

        // NOTE: no sender-side re-hash here. The manifest pass already
        // hashed the source once, and the receiver verifies every byte
        // against that manifest SHA before it ACKs completion. Re-hashing
        // during send was a second full SHA pass (and a second full read
        // in the old per-chunk code) for no protocol benefit: if the
        // source changes mid-send, the receiver's hash check — not the
        // sender's — is what must fail the transfer.
        while seq < total_chunks {
            let acked = self.acked.load(std::sync::atomic::Ordering::Acquire);
            if seq.saturating_sub(acked) >= SENDER_WINDOW_CHUNKS {
                let target = acked.saturating_add(ACK_INTERVAL_CHUNKS);
                if !self.wait_for_ack(target, ACK_PROGRESS_TIMEOUT).await {
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

            // Read the whole batch straight into its final buffer: one
            // allocation per 128 KiB frame, no per-32 KiB chunk Vec and no
            // extend copy. SendSource::read_at seeks only if the position
            // moved, so the in-order path is a sequential read.
            let batch_len: usize = (0..batch_chunks)
                .map(|i| chunk_len(&self.manifest, seq + i) as usize)
                .sum();
            if batch_len == 0 {
                return Err("source ended before declared chunk count".to_string());
            }
            let mut batch = vec![0u8; batch_len];
            {
                let mut source = self.source.lock().await;
                let mut at = 0usize;
                for i in 0..batch_chunks {
                    let chunk_seq = seq + i;
                    let chunk_offset = u64::from(chunk_seq)
                        .checked_mul(u64::from(NATIVE_CHUNK_SIZE))
                        .ok_or_else(|| "source offset overflow".to_string())?;
                    let expected = chunk_len(&self.manifest, chunk_seq) as usize;
                    if expected == 0 {
                        return Err("source ended before declared chunk count".to_string());
                    }
                    source.read_at(chunk_offset, &mut batch[at..at + expected])?;
                    at += expected;
                }
            }
            seq += batch_chunks;

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
                if drain_started.elapsed() > DRAIN_TIMEOUT {
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

        // Wait for the receiver's persistence confirmation (the receiver
        // has verified the manifest SHA by now; see the note above).
        self.wait_complete().await
    }

    /// Send the file via the LiveKit relay: base64 `file-chunk` messages.
    /// Mirrors the TS relay path in `FlingRoom.sendFile()`. Not used by the
    /// current online session (its route is always a DataChannel, even for
    /// TURN relay); kept for protocol completeness. Same no-rehash rule as
    /// `send_via_p2p`: the receiver verifies against the manifest SHA.
    pub async fn send_via_relay(
        &self,
        send_chunk: impl Fn(u32, String) -> Result<(), String>,
        on_progress: impl Fn(u64, u64),
    ) -> Result<(), String> {
        let total_chunks = self.manifest.total_chunks;
        let size_bytes = self.manifest.size;
        let transfer_id = self.manifest.id.clone();

        let mut seq: u32 = 0;

        while seq < total_chunks {
            let acked = self.acked.load(std::sync::atomic::Ordering::Acquire);
            if seq.saturating_sub(acked) >= SENDER_WINDOW_CHUNKS {
                let target = acked.saturating_add(ACK_INTERVAL_CHUNKS);
                if !self.wait_for_ack(target, ACK_PROGRESS_TIMEOUT).await {
                    return Err("relay cumulative ACK timeout".to_string());
                }
                continue;
            }

            let chunk_offset = u64::from(seq)
                .checked_mul(u64::from(NATIVE_CHUNK_SIZE))
                .ok_or_else(|| "source offset overflow".to_string())?;
            let expected = chunk_len(&self.manifest, seq) as usize;
            if expected == 0 {
                return Err("source ended before declared chunk count".to_string());
            }
            let mut chunk = vec![0u8; expected];
            {
                let mut source = self.source.lock().await;
                source.read_at(chunk_offset, &mut chunk)?;
            }
            let b64 = B64.encode(&chunk);
            send_chunk(seq, b64)?;

            seq += 1;
            let sent = u64::from(seq).saturating_mul(u64::from(NATIVE_CHUNK_SIZE)).min(size_bytes);
            *self.sent_bytes.lock().await = sent;
            *self.seq.lock().await = seq;
            on_progress(sent, size_bytes);
            let _ = &transfer_id;
        }

        self.wait_complete().await
    }
}

/// Receiver-side state for one file transfer.
pub struct OnlineReceiver {
    manifest: FileManifest,
    /// Out-of-order relay chunks held ONLY until the incremental hash
    /// catches up to them (see `on_relay_chunk`). Their bytes are already
    /// on disk at the right offset; this map is hash bookkeeping, not
    /// reassembly storage, and it is bounded by the sender window
    /// (256 chunks x 32 KiB = 8 MiB). P2P frames are contiguous, so this
    /// map stays empty on the live path.
    pending: Mutex<HashMap<u32, Vec<u8>>>,
    /// Per-chunk receipt bitmap (true = already seek-written to the .tmp).
    received_map: Mutex<Vec<bool>>,
    received_count: Mutex<u32>,
    /// Next contiguous chunk still needed (ACK geometry + hash cursor).
    /// `received_bytes` is the contiguous byte count for this cursor, so
    /// ACKs keep exactly the old wire meaning; bytes already written
    /// beyond the cursor are on disk but not yet ACK-counted.
    next_seq: Mutex<u32>,
    received_bytes: Mutex<u64>,
    hasher: Mutex<Sha256>,
    /// Intended final path (manifest name in the target directory).
    target_path: PathBuf,
    /// Actual final path after a successful rename — may carry a
    /// `(n)` sequence suffix if the intended name was taken. Callers
    /// must use `target_path()` (which returns this when set) for the
    /// UI/saved-path, never the manifest name alone. A std RwLock (not
    /// a tokio Mutex) so `target_path()` can be read synchronously by
    /// session/UI code without awaiting.
    final_path: std::sync::RwLock<Option<PathBuf>>,
    /// The single .tmp file in the target directory (NOT the system temp
    /// directory, NOT a second staging copy): chunks are seek-written
    /// here, then this file is renamed once into place. No hard link,
    /// no copy, no second write pass.
    temp_path: PathBuf,
    /// Persistent handle for the .tmp file. Opening the file per frame
    /// cost tens of ms per 128 KiB frame on Windows (file open + AV
    /// scan) and capped P2P receive at ~2 MB/s; the handle is opened
    /// once and dropped before the final rename.
    file: Mutex<Option<std::fs::File>>,
    complete_notify: Arc<Notify>,
    gate: Mutex<()>,
    cancelled: AtomicBool,
    committed: AtomicBool,
    clipboard_image_claimed: AtomicBool,
}

/// Relative folder layout is display metadata, never a sender filesystem path.
fn receive_directory(manifest: &FileManifest, root: &Path) -> Result<PathBuf, String> {
    let Some(relative) = &manifest.path else { return Ok(root.to_path_buf()); };
    let components: Vec<_> = relative.split('/').collect();
    if components.len() > 65 || components.last().copied() != Some(manifest.name.as_str()) ||
        components.iter().any(|part| !crate::online_source::safe_component(part)) {
        return Err("invalid relative source path".into());
    }
    std::fs::create_dir_all(root).map_err(|_| "create receive root failed")?;
    let root = root.canonicalize().map_err(|_| "receive root unavailable")?;
    let mut directory = root.clone();
    for part in &components[..components.len() - 1] {
        directory.push(part);
        match std::fs::create_dir(&directory) {
            Ok(()) => {},
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {},
            Err(_) => return Err("create receive subdirectory failed".into()),
        }
        let stat = std::fs::symlink_metadata(&directory).map_err(|_| "receive subdirectory unavailable")?;
        if !stat.is_dir() || stat.file_type().is_symlink() { return Err("unsafe receive subdirectory".into()); }
        #[cfg(windows)] {
            use std::os::windows::fs::MetadataExt;
            if stat.file_attributes() & 0x400 != 0 { return Err("unsafe receive reparse point".into()); }
        }
        if !directory.canonicalize().map_err(|_| "receive subdirectory unavailable")?.starts_with(&root) {
            return Err("receive directory escaped root".into());
        }
    }
    Ok(directory)
}
impl OnlineReceiver {
    pub fn new(manifest: FileManifest, target_dir: &Path) -> Result<Self, String> {
        if manifest.id.len() != 24 || !manifest.id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("invalid transfer id".into());
        }
        if manifest.name.is_empty() || manifest.name.contains(['/', '\\', ':']) || matches!(manifest.name.as_str(), "." | "..") {
            return Err("invalid file name".into());
        }
        if u64::from(manifest.chunk_size) != NATIVE_CHUNK_SIZE || u64::from(manifest.total_chunks) != manifest.size.div_ceil(u64::from(NATIVE_CHUNK_SIZE)) {
            return Err("invalid manifest chunk accounting".into());
        }
        let target_dir = receive_directory(&manifest, target_dir)?;
        std::fs::create_dir_all(&target_dir).map_err(|e| format!("create receive directory: {e}"))?;
        let target_path = target_dir.join(&manifest.name);
        // Single .tmp in the target directory, unique per transfer id.
        // A pre-existing final file does NOT fail creation here: the
        // rename step resolves the conflict with a sequence suffix.
        let temp_path = target_dir.join(format!(".{}.tmp", manifest.id));
        let file = std::fs::OpenOptions::new().write(true).create_new(true).open(&temp_path)
            .map_err(|e| format!("create receive temp: {e}"))?;
        let total_chunks = manifest.total_chunks as usize;
        Ok(Self {
            manifest,
            pending: Mutex::new(HashMap::new()),
            received_map: Mutex::new(vec![false; total_chunks]),
            received_count: Mutex::new(0),
            next_seq: Mutex::new(0),
            received_bytes: Mutex::new(0),
            hasher: Mutex::new(Sha256::new()),
            target_path,
            final_path: std::sync::RwLock::new(None),
            temp_path,
            file: Mutex::new(Some(file)),
            complete_notify: Arc::new(Notify::new()),
            gate: Mutex::new(()),
            cancelled: AtomicBool::new(false),
            committed: AtomicBool::new(false),
            clipboard_image_claimed: AtomicBool::new(false),
        })
    }

    pub fn manifest(&self) -> &FileManifest {
        &self.manifest
    }

    /// Seek-write `bytes` for chunk `seq` into the .tmp file through the
    /// persistent handle. `bytes` must already be validated to be exactly
    /// this chunk's length. The handle position is set explicitly, so
    /// out-of-order chunks land at the right offset immediately instead
    /// of waiting in a RAM reassembly buffer.
    async fn write_chunk_at(&self, seq: u32, bytes: &[u8]) -> Result<(), String> {
        let offset = u64::from(seq) * u64::from(self.manifest.chunk_size);
        let mut guard = self.file.lock().await;
        let file = guard.as_mut().ok_or("receive handle closed")?;
        file.seek(SeekFrom::Start(offset)).map_err(|e| format!("seek receive temp: {e}"))?;
        file.write_all(bytes).map_err(|e| format!("write temp file: {e}"))?;
        Ok(())
    }

    /// Handle one decoded FLNG batch frame from the P2P DataChannel.
    /// Returns the new `next_seq` (for ACK decisions).
    pub async fn on_p2p_frame(&self, frame_bytes: &[u8]) -> Result<u32, String> {
        let _gate = self.gate.lock().await;
        if self.cancelled.load(Ordering::Acquire) || self.committed.load(Ordering::Acquire) { return Err("receive is terminal".into()); }
        let frame = decode_direct_file_frame(frame_bytes)
            .map_err(|e| format!("decode FLNG frame: {e}"))?
            .ok_or_else(|| "not a FLNG frame".to_string())?;
        if frame.id != self.manifest.id {
            return Err("FLNG frame transfer id mismatch".to_string());
        }
        let chunk_size = frame.chunk_size.map(u64::from).unwrap_or(u64::from(self.manifest.chunk_size));
        let count = u64::from(frame.chunk_count);
        if count == 0 || chunk_size != u64::from(self.manifest.chunk_size) || frame.seq != *self.next_seq.lock().await {
            return Err("FLNG frame is not contiguous or has invalid geometry".into());
        }

        // Validate the whole batch, then append contiguously.
        let mut offset = 0usize;
        let mut next = *self.next_seq.lock().await;
        for index in 0..count {
            let chunk_seq = next + index as u32;
            if chunk_seq >= self.manifest.total_chunks {
                return Err("FLNG frame exceeds declared chunk count".to_string());
            }
            // Chunks are full-size except the file's last chunk, which may
            // be short. Compute each chunk's expected length from its
            // absolute position in the file. (The old code derived it from
            // the transfer-wide `received`, which never advances inside
            // this loop — so a batch ending in a short chunk computed
            // count*chunk_size expected bytes and the frame was rejected
            // as having "trailing payload bytes".)
            let chunk_start = u64::from(chunk_seq) * chunk_size;
            let expected =
                self.manifest.size.saturating_sub(chunk_start).min(chunk_size) as usize;
            if expected == 0 || frame.payload.len() < offset + expected {
                return Err("FLNG payload length does not match manifest".to_string());
            }
            offset += expected;
        }
        if offset != frame.payload.len() {
            return Err("FLNG frame contains trailing payload bytes".to_string());
        }

        // Seek-write the whole batch to the .tmp file (one write for the
        // 128 KiB frame, not one per chunk) and hash it in order — P2P
        // frames are contiguous by the check above, so the incremental
        // hash stays in file order with no buffering.
        {
            let batch_seq = *self.next_seq.lock().await;
            let mut guard = self.file.lock().await;
            let file = guard.as_mut().ok_or("receive handle closed")?;
            let batch_offset = u64::from(batch_seq) * u64::from(self.manifest.chunk_size);
            file.seek(SeekFrom::Start(batch_offset)).map_err(|e| format!("seek receive temp: {e}"))?;
            file.write_all(&frame.payload[..offset]).map_err(|e| format!("write temp file: {e}"))?;
        }
        self.hasher.lock().await.update(&frame.payload[..offset]);
        {
            let mut map = self.received_map.lock().await;
            for i in 0..count as usize {
                if let Some(slot) = map.get_mut(next as usize + i) { *slot = true; }
            }
        }
        *self.received_count.lock().await += count as u32;
        *self.received_bytes.lock().await += offset as u64;
        next += count as u32;
        *self.next_seq.lock().await = next;
        Ok(next)
    }

    /// Handle one base64 `file-chunk` from the relay path.
    ///
    /// Seek-write first, hash second, in file order: an out-of-order
    /// chunk is written to its .tmp offset immediately (so disk I/O is
    /// not gated on contiguity), but it is only hashed once every chunk
    /// before it has been hashed — its bytes are held in `pending` until
    /// then, bounded by the sender window. ACK geometry is unchanged:
    /// `next_seq`/`received_bytes` still count only the contiguous
    /// prefix, which is what the sender's window and
    /// `valid_ack_geometry` expect.
    pub async fn on_relay_chunk(&self, seq: u32, b64data: &str) -> Result<u32, String> {
        let _gate = self.gate.lock().await;
        if self.cancelled.load(Ordering::Acquire) || self.committed.load(Ordering::Acquire) { return Err("receive is terminal".into()); }
        let next = *self.next_seq.lock().await;
        if seq < next { return Ok(next); }
        if seq >= self.manifest.total_chunks || seq.saturating_sub(next) >= SENDER_WINDOW_CHUNKS { return Err("relay sequence outside receive window".into()); }
        let expected = chunk_len(&self.manifest, seq) as usize;
        let data = B64
            .decode(b64data)
            .map_err(|e| format!("decode relay chunk: {e}"))?;
        if data.len() != expected { return Err("relay chunk length mismatch".into()); }
        {
            let map = self.received_map.lock().await;
            if map.get(seq as usize).copied().unwrap_or(false) { return Ok(next); }
        }
        // Disk first, at the chunk's own offset.
        self.write_chunk_at(seq, &data).await?;
        {
            let mut map = self.received_map.lock().await;
            if let Some(slot) = map.get_mut(seq as usize) { *slot = true; }
        }
        *self.received_count.lock().await += 1;
        // Hash only in file order.
        let mut next = next;
        let mut received = *self.received_bytes.lock().await;
        let mut hasher = self.hasher.lock().await;
        if seq == next {
            hasher.update(&data);
            received += data.len() as u64;
            next += 1;
            // Drain any already-written chunks that are now contiguous.
            loop {
                let pending_data = {
                    let mut pending = self.pending.lock().await;
                    pending.remove(&next)
                };
                let Some(pending_data) = pending_data else { break; };
                let len = chunk_len(&self.manifest, next);
                hasher.update(&pending_data);
                received += len;
                next += 1;
            }
        } else {
            self.pending.lock().await.insert(seq, data);
        }
        drop(hasher);
        *self.next_seq.lock().await = next;
        *self.received_bytes.lock().await = received;
        Ok(next)
    }

    /// Check whether the transfer is complete; if so, verify the hash and
    /// rename the .tmp file into place. Returns true when done.
    #[cfg(test)]
    pub async fn check_complete(&self) -> Result<bool, String> {
        self.check_complete_observed(|_| {}).await
    }

    pub async fn check_complete_observed(&self, observe: impl Fn(crate::online_diagnostics::Phase)) -> Result<bool, String> {
        use crate::online_diagnostics::Phase;
        let _gate = self.gate.lock().await;
        if self.cancelled.load(Ordering::Acquire) { return Err("receive cancelled".into()); }
        if self.committed.load(Ordering::Acquire) { return Ok(true); }
        let next = *self.next_seq.lock().await;
        let received = *self.received_bytes.lock().await;
        let count = *self.received_count.lock().await;
        if next != self.manifest.total_chunks || count != self.manifest.total_chunks || received != self.manifest.size {
            return Ok(false);
        }
        // Durability before integrity check and publication. No final ACK
        // is possible until this method succeeds.
        // SPEED CHANGE (deliberate trade-off, documented): the hash is
        // the incremental hash accumulated as chunks were written, NOT a
        // second full read of the .tmp file. That removes one whole-file
        // disk read per transfer (the old code re-read the file here).
        // It means an external modification of the .tmp file after a
        // chunk was written — e.g. another process editing it, or disk
        // corruption that the OS does not report on write — is NOT
        // detected here. The LAN store (ReceivedFileStore) already used
        // exactly this incremental-hash rule, so the online path is now
        // consistent with it. Wire bytes themselves are still verified:
        // a chunk whose bytes differ from the manifest fails this check.
        {
            let mut guard = self.file.lock().await;
            let file = guard.as_mut().ok_or("receive handle closed")?;
            file.flush().map_err(|e| format!("flush receive: {e}"))?;
            file.sync_all().map_err(|e| format!("sync receive: {e}"))?;
            *guard = None;
        }
        observe(Phase::DiskSynced);
        let digest = {
            let hasher = self.hasher.lock().await;
            format!("{:x}", hasher.clone().finalize())
        };
        if digest != self.manifest.sha256 {
            // Hash failure: delete the .tmp, mark cancelled so a later
            // check_complete cannot succeed on the same receiver, and let
            // the session's fail path (abort) clean up the rest.
            self.cancelled.store(true, Ordering::Release);
            let _ = std::fs::remove_file(&self.temp_path);
            return Err("sha256 mismatch after receive".into());
        }
        observe(Phase::HashVerified);
        // One same-directory rename, with a sequence suffix if the
        // intended name is taken or the rename fails. No hard link, no
        // copy: the .tmp file IS the only written copy.
        let target_dir = self.target_path.parent().ok_or("receive target has no parent")?.to_path_buf();
        let final_path = match rename_with_sequence(&self.temp_path, &target_dir, &self.manifest.name) {
            Ok(path) => path,
            Err(e) => {
                let _ = std::fs::remove_file(&self.temp_path);
                return Err(e);
            }
        };
        if let Ok(mut slot) = self.final_path.write() {
            *slot = Some(final_path);
        }
        self.committed.store(true, Ordering::Release);
        observe(Phase::AtomicCommit);
        self.complete_notify.notify_waiters();
        Ok(true)
    }

    /// Actual final path: the renamed path after commit, otherwise the
    /// intended (manifest-name) path. UI/saved-path callers must use this
    /// after commit, because a conflict may have renamed the file to
    /// `name(1).ext` etc.
    pub fn target_path(&self) -> PathBuf {
        if let Ok(slot) = self.final_path.read() {
            if let Some(path) = slot.as_ref() {
                return path.clone();
            }
        }
        self.target_path.clone()
    }

    pub fn is_committed(&self) -> bool { self.committed.load(Ordering::Acquire) }
    /// A verified clipboard image is applied to the OS clipboard once before
    /// the durable completion ACK is sent.
    pub fn claim_clipboard_image_delivery(&self) -> bool {
        self.manifest.kind == FileKind::ClipboardImage
            && !self.clipboard_image_claimed.swap(true, Ordering::AcqRel)
    }

    pub async fn abort(&self) {
        let _gate = self.gate.lock().await;
        if self.committed.load(Ordering::Acquire) { return; }
        self.cancelled.store(true, Ordering::Release);
        *self.file.lock().await = None;
        self.pending.lock().await.clear();
        // Only the .tmp is removed: no final file exists yet (rename is
        // the only publication step, and it has not happened), so there
        // is no half-finished final file to clean up.
        let _ = std::fs::remove_file(&self.temp_path);
    }

    pub fn should_ack(&self, last_ack_seq: u32, next_seq: u32) -> bool {
        next_seq.saturating_sub(last_ack_seq) >= ACK_INTERVAL_CHUNKS
    }

    pub async fn received_bytes(&self) -> u64 {
        *self.received_bytes.lock().await
    }
}

impl Drop for OnlineReceiver {
    fn drop(&mut self) {
        self.file.get_mut().take();
        if !self.committed.load(Ordering::Acquire) { let _ = std::fs::remove_file(&self.temp_path); }
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
        route_fence: None,
        id: receiver.manifest().id.clone(),
        next_seq,
        received_bytes,
        ack_id: None,
        complete: complete.then_some(true),
        failed: None,
    }
}

#[cfg(test)]
#[path = "online_transfer_tests.rs"]
mod tests;
