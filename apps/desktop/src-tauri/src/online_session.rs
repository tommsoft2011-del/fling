//! Online session orchestration: ties together the LiveKit room, the P2P
//! link, and the file transfer engine.
//!
//! This is the Rust replacement for the TypeScript `SendTask` / `RecvTask`
//! orchestration in `apps/desktop/src/tasks.ts` combined with `FlingRoom`.
//! The WebView only displays progress; all signaling and bytes flow here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{Duration, Instant};

use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::{mpsc, Mutex};

use crate::online_crypto::derive_room_key;
use crate::online_native::{NativeTask, NativeTaskConfig, NativeTaskStatus};
use crate::online_source::SourceLease;
use crate::online_p2p::{default_ice_servers, OnlineP2p};
use crate::online_proto::{FileKind, FlingMessage};
use crate::online_room::{OnlineRoom, OnlineRoomEvent};
use crate::online_transfer::{build_manifest, chunk_ack_message, OnlineReceiver, OnlineSender};

/// How long to wait for a manifest ACK.
const MANIFEST_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_PROGRESS_UI_INTERVAL: Duration = Duration::from_millis(500);
const PEER_PROGRESS_UI_BYTES: u64 = 4 * 1024 * 1024;

/// Events emitted to the WebView. The frontend only renders these.
#[derive(Clone, serde::Serialize)]
#[serde(tag = "kind")]
enum OnlineUiEvent {
    /// A text message arrived.
    #[serde(rename = "text")]
    Text { from: String, id: String, text: String },
    /// A file manifest arrived (receiver side). Frontend uses this to
    /// create the transferId -> item mapping for progress display.
    #[serde(rename = "file-manifest")]
    FileManifest {
        transfer_id: String,
        name: String,
        size: u64,
        file_kind: String,
    },
    /// Peer presence changed.
    #[serde(rename = "peer")]
    Peer { identity: String, joined: bool },
    /// Transfer progress (sender or receiver).
    #[serde(rename = "progress")]
    Progress {
        transfer_id: String,
        sent_bytes: u64,
        total_bytes: u64,
        direction: String,
    },
    /// Sender-side view of the receiver's cumulative ACK. `received_bytes`
    /// means appended to the receiver's temp file, not final durable commit.
    #[serde(rename = "peer-progress")]
    PeerProgress {
        transfer_id: String,
        received_bytes: u64,
        total_bytes: u64,
        complete: bool,
    },
    #[serde(rename = "file-send-started")]
    FileSendStarted {
        transfer_id: String,
        total_bytes: u64,
    },
    /// P2P link state changed.
    #[serde(rename = "p2p")]
    P2p { state: String },
    /// P2P diagnostic log (goes to the frontend log file via log('p2p', ...)).
    #[serde(rename = "p2p-log")]
    P2pLog { message: String },
    /// A transfer finished (or failed).
    #[serde(rename = "transfer-done")]
    TransferDone {
        transfer_id: String,
        ok: bool,
        error: Option<String>,
    },
    /// The room disconnected.
    #[serde(rename = "disconnected")]
    Disconnected { reason: String },
    /// A task-level protocol message for the frontend task orchestrator.
    /// The Rust session does not interpret these; tasks.ts does.
    #[serde(rename = "protocol")]
    Protocol { from: String, message: serde_json::Value },
    #[serde(rename = "native-task")]
    NativeTask { status: NativeTaskStatus },
}

fn emit(app: &AppHandle, session_id: &str, event: OnlineUiEvent) {
    let _ = app.emit(
        "fling://online/event",
        serde_json::json!({ "session": session_id, "event": event }),
    );
}

/// Limit peer-progress events to 500ms or 4MiB, while always emitting completion.
async fn should_emit_peer_progress(
    progress: &Mutex<HashMap<String, (u64, Instant)>>,
    id: &str,
    received_bytes: u64,
    complete: bool,
) -> bool {
    let now = Instant::now();
    let mut progress = progress.lock().await;
    let due = progress
        .get(id)
        .map_or(true, |(previous_bytes, previous_at)| {
            complete
                || received_bytes.saturating_sub(*previous_bytes) >= PEER_PROGRESS_UI_BYTES
                || now.saturating_duration_since(*previous_at) >= PEER_PROGRESS_UI_INTERVAL
        });
    if due {
        if complete {
            progress.remove(id);
        } else {
            progress.insert(id.to_string(), (received_bytes, now));
        }
    }
    due
}

/// One online session: a LiveKit room plus its transfers.
pub struct OnlineSession {
    id: String,
    owner: String,
    pub native: Option<Arc<NativeTask>>,
    cancelled: AtomicBool,
    cancel_notify: tokio::sync::Notify,
    peer_present: AtomicBool,
    peer_notify: tokio::sync::Notify,
    app: AppHandle,
    room: Arc<OnlineRoom>,
    senders: Arc<Mutex<HashMap<String, Arc<OnlineSender>>>>,
    receivers: Arc<Mutex<HashMap<String, Arc<OnlineReceiver>>>>,
    /// Last ACKed seq per receiver (to throttle `file-chunk-ack`).
    receiver_last_ack: Arc<Mutex<HashMap<String, u32>>>,
    p2p: Mutex<Option<Arc<OnlineP2p>>>,
    pending_ice: Mutex<Vec<String>>,
    download_dir: PathBuf,
    /// Notified when a `file-manifest-ack` arrives, keyed by transfer id.
    manifest_acks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,
    /// Rate-limit ACK progress events sent from Rust to the WebView.
    peer_progress_ui: Arc<Mutex<HashMap<String, (u64, Instant)>>>,
}

impl OnlineSession {
    /// Connect to the LiveKit room and start the dispatch loop.
    pub async fn connect(
        app: AppHandle,
        id: String,
        server_url: &str,
        token: &str,
        pairing_code: &str,
        key_context: &str,
        download_dir: PathBuf,
        owner: String,
        native_config: Option<NativeTaskConfig>,
    ) -> Result<Arc<Self>, String> {
        let room_key = derive_room_key(pairing_code, key_context);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let room = OnlineRoom::connect(server_url, token, room_key, event_tx).await?;
        let room = Arc::new(room);

        let session = Arc::new(Self {
            id: id.clone(),
            owner,
            native: native_config.map(NativeTask::new).transpose()?.map(Arc::new),
            cancelled: AtomicBool::new(false),
            cancel_notify: tokio::sync::Notify::new(),
            peer_present: AtomicBool::new(false),
            peer_notify: tokio::sync::Notify::new(),
            app: app.clone(),
            room,
            senders: Arc::new(Mutex::new(HashMap::new())),
            receivers: Arc::new(Mutex::new(HashMap::new())),
            receiver_last_ack: Arc::new(Mutex::new(HashMap::new())),
            p2p: Mutex::new(None),
            pending_ice: Mutex::new(Vec::new()),
            download_dir,
            manifest_acks: Arc::new(Mutex::new(HashMap::new())),
            peer_progress_ui: Arc::new(Mutex::new(HashMap::new())),
        });

        // Dispatch loop.
        let session_cb = session.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let Err(e) = session_cb.handle_room_event(event).await {
                    // Log and continue; a single bad message must not kill
                    // the session.
                    eprintln!("fling online session {}: {}", session_cb.id, e);
                    if session_cb.native.is_some() { let _ = session_cb.cancel_native(Some(e), true).await; }
                }
            }
        });

        // Announce presence, mirroring FlingRoom.connect().
        session
            .room
            .send(&FlingMessage::Hello {
                device: "fling-desktop".to_string(),
            })
            .await?;
        Ok(session)
    }

    async fn handle_room_event(self: &Arc<Self>, event: OnlineRoomEvent) -> Result<(), String> {
        match event {
            OnlineRoomEvent::Message { from, msg } => self.handle_message(from, msg).await,
            OnlineRoomEvent::PeerJoined(identity) => {
                self.peer_present.store(true, Ordering::Release);
                self.peer_notify.notify_one();
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::Peer { identity, joined: true },
                );
                Ok(())
            }
            OnlineRoomEvent::PeerLeft(identity) => {
                self.peer_present.store(false, Ordering::Release);
                if let Some(native) = &self.native {
                    if !native.status.lock().await.terminal() { self.cancel_native(Some("对方断开连接".into()), false).await?; }
                    self.receivers.lock().await.clear();
                    if let Some(p2p) = self.p2p.lock().await.as_ref() { p2p.close(); }
                    self.app.state::<OnlineSessionRegistry>().remove(&self.id).await;
                    let room = self.room.clone();
                    tokio::spawn(async move { let _ = room.disconnect().await; });
                }
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::Peer { identity, joined: false },
                );
                Ok(())
            }
            OnlineRoomEvent::Disconnected { reason } => {
                if self.native.is_some() { self.cancel_native(Some(reason.clone()), false).await?; }
                emit(&self.app, &self.id, OnlineUiEvent::Disconnected { reason });
                Ok(())
            }
            OnlineRoomEvent::ProtocolWarning { detail } => {
                eprintln!("fling online protocol warning: {detail}");
                Ok(())
            }
        }
    }

    async fn handle_message(self: &Arc<Self>, from: String, msg: FlingMessage) -> Result<(), String> {
        // Transfer data-plane messages are handled internally by the Rust
        // engine. Task-level signaling is forwarded to the frontend
        // orchestrator (tasks.ts) via the `protocol` UI event.
        let is_transfer_internal = matches!(
            msg,
            FlingMessage::FileManifest { .. }
                | FlingMessage::FileChunk { .. }
                | FlingMessage::FileManifestAck { .. }
                | FlingMessage::FileChunkAck { .. }
                | FlingMessage::P2pOffer { .. }
                | FlingMessage::P2pAnswer { .. }
                | FlingMessage::P2pIce { .. }
        );
        if !is_transfer_internal {
            let message = serde_json::to_value(&msg)
                .map_err(|e| format!("encode protocol message: {e}"))?;
            emit(
                &self.app,
                &self.id,
                OnlineUiEvent::Protocol {
                    from: from.clone(),
                    message,
                },
            );
        }
        match msg {
            FlingMessage::Text { id, text } => {
                emit(&self.app, &self.id, OnlineUiEvent::Text { from, id, text });
            }
            FlingMessage::FileManifest { manifest } => {
                self.on_file_manifest(manifest).await?;
            }
            FlingMessage::FileManifestAck { id, native_single_file, .. } => {
                if self.native.is_some() && native_single_file != Some(true) {
                    if let Some(sender) = self.senders.lock().await.get(&id) { sender.on_failed(); }
                }
                if let Some(notify) = self.manifest_acks.lock().await.get(&id) {
                    notify.notify_one();
                }
            }
            FlingMessage::FileChunk { id, seq, data } => {
                self.on_relay_chunk(id, seq, data).await?;
            }
            FlingMessage::FileChunkAck {
                id,
                next_seq,
                received_bytes,
                complete,
                failed,
                ..
            } => {
                if let Some(sender) = self.senders.lock().await.get(&id).cloned() {
                    if failed == Some(true) { sender.on_failed(); return Err(format!("receiver failed transfer {id}")); }
                    sender.on_chunk_ack(next_seq);
                    let is_complete = complete == Some(true);
                    if is_complete && (next_seq != sender.manifest().total_chunks || received_bytes != sender.manifest().size) {
                        sender.on_failed(); return Err("invalid final ACK accounting".into());
                    }
                    if is_complete { sender.on_complete(); }
                    let total_bytes = sender.manifest().size;
                    let received_bytes = received_bytes.min(total_bytes);
                    if should_emit_peer_progress(
                        &self.peer_progress_ui,
                        &id,
                        received_bytes,
                        is_complete,
                    )
                    .await
                    {
                    if let Some(native) = &self.native {
                        let mut status = native.status.lock().await;
                        if !status.terminal() {
                            status.peer_bytes = status.peer_bytes.max(received_bytes.min(status.total_bytes));
                            if is_complete { status.saved_bytes = status.total_bytes; }
                            emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
                        }
                    }
                        emit(
                            &self.app,
                            &self.id,
                            OnlineUiEvent::PeerProgress {
                                transfer_id: id,
                                received_bytes,
                                total_bytes,
                                complete: is_complete,
                            },
                        );
                    }
                }
            }
            FlingMessage::P2pOffer { sdp } => {
                self.on_p2p_offer(sdp).await?;
            }
            FlingMessage::P2pAnswer { sdp } => {
                if let Some(p2p) = self.p2p.lock().await.as_ref() {
                    p2p.handle_answer(&sdp).await?;
                }
            }
            FlingMessage::P2pIce { candidate } => {
                self.on_p2p_ice(candidate).await?;
            }
            FlingMessage::TaskAck { task_id } => {
                if let Some(native) = &self.native {
                    let status = native.status.lock().await;
                    if status.task_id == task_id && status.direction == "sending" {
                        native.task_acked.store(true, Ordering::Release);
                        native.ack.notify_one();
                    }
                }
            }
            FlingMessage::TaskCancel { task_id } => {
                if let Some(native) = &self.native {
                    let matches = native.status.lock().await.task_id == task_id;
                    if matches { self.cancel_native(None, false).await?; }
                }
            }
            FlingMessage::TaskDone { task_id } => {
                if let Some(native) = &self.native {
                    let status = native.status.lock().await;
                    if status.task_id == task_id && status.direction == "receiving" && status.stage == "done" {
                        self.room.send(&FlingMessage::TaskAck { task_id }).await?;
                    }
                }
            }
            FlingMessage::Hello { .. } => {
                self.peer_present.store(true, Ordering::Release);
                self.peer_notify.notify_one();
            }
            _ => {}
        }
        let _ = from;
        Ok(())
    }

    /// Receiver: a manifest arrived. Create the receiver state and ACK it.
    async fn on_file_manifest(
        &self,
        manifest: crate::online_proto::FileManifest,
    ) -> Result<(), String> {
        if self.cancelled.load(Ordering::Acquire) { return Err("session cancelled".into()); }
        if let Some(native) = &self.native {
            let status = native.status.lock().await;
            if status.direction != "receiving" || status.name != manifest.name || status.total_bytes != manifest.size || manifest.kind != FileKind::File {
                return Err("manifest does not match accepted single-file offer".into());
            }
            if native.claimed.swap(true, Ordering::AcqRel) { return Err("single-file task already has a manifest".into()); }
        }
        let id = manifest.id.clone();
        // Notify the frontend so it can create the transferId -> item mapping
        // for progress display. The manifest kind is an enum; convert to string.
        let kind_str = match &manifest.kind {
            crate::online_proto::FileKind::File => "file",
            crate::online_proto::FileKind::ClipboardImage => "clipboard-image",
        };
        emit(
            &self.app,
            &self.id,
            OnlineUiEvent::FileManifest {
                transfer_id: id.clone(),
                name: manifest.name.clone(),
                size: manifest.size,
                file_kind: kind_str.to_string(),
            },
        );
        let receiver = Arc::new(OnlineReceiver::new(manifest, &self.download_dir)?);
        self.receivers.lock().await.insert(id.clone(), receiver);
        self.receiver_last_ack.lock().await.insert(id.clone(), 0);
        // Advertise full capabilities: chunk ACKs, binary chunks, 128 KiB
        // direct frames (matching the v0.6.6 LAN engine).
        self.room
            .send(&FlingMessage::FileManifestAck {
                id: id.clone(),
                native_single_file: self.native.as_ref().map(|_| true),
                chunk_ack: Some(true),
                binary_chunks: Some(true),
                direct_frame_payload_bytes: Some(128 * 1024),
            })
            .await?;
        let receiver = self.receivers.lock().await.get(&id).cloned().ok_or("receiver missing")?;
        if receiver.manifest().size == 0 { self.maybe_ack(&receiver, 0).await?; }
        Ok(())
    }

    /// Receiver: a relay chunk arrived.
    async fn on_relay_chunk(&self, id: String, seq: u32, data: String) -> Result<(), String> {
        let receiver = self
            .receivers
            .lock()
            .await
            .get(&id)
            .cloned()
            .ok_or_else(|| format!("relay chunk for unknown transfer {id}"))?;
        let next_seq = receiver.on_relay_chunk(seq, &data).await?;
        self.maybe_ack(&receiver, next_seq).await?;
        Ok(())
    }

    /// Final ACKs are published only after flush, sync_all, disk SHA and
    /// atomic no-overwrite publication. A failed publish is retried, never
    /// silently converted into a successful task snapshot.
    async fn maybe_ack(&self, receiver: &Arc<OnlineReceiver>, next_seq: u32) -> Result<(), String> {
        let id = receiver.manifest().id.clone();
        let received_bytes = receiver.received_bytes().await;
        if self.cancelled.load(Ordering::Acquire) { receiver.abort().await; return Err("session cancelled".into()); }
        if self.native.is_some() && received_bytes == receiver.manifest().size { self.native_stage("finalizing").await; }
        let complete = match receiver.check_complete().await {
            Ok(complete) => complete,
            Err(error) => { self.fail_receive(receiver, &error).await; return Err(error); }
        };
        let due = should_emit_peer_progress(&self.peer_progress_ui, &id, received_bytes, complete).await;
        if !complete {
            if let Some(native) = &self.native {
                let mut status = native.status.lock().await;
                if !status.terminal() {
                    status.local_bytes = received_bytes;
                    status.stage = "transferring".into();
                    if due { emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() }); }
                }
            }
        }
        let last_ack = *self.receiver_last_ack.lock().await.get(&id).unwrap_or(&0);
        if complete {
            let mut published = false;
            let mut publish_error = String::new();
            for attempt in 0..3 {
                let result = async {
                    self.room.send(&chunk_ack_message(receiver, next_seq, received_bytes, true)).await?;
                    if let Some(native) = &self.native {
                        let task_id = native.status.lock().await.task_id.clone();
                        self.room.send(&FlingMessage::TaskAck { task_id }).await?;
                    }
                    Ok::<(), String>(())
                }.await;
                match result { Ok(()) => { published = true; break; }, Err(error) => { publish_error = error; } }
                if attempt < 2 { tokio::time::sleep(Duration::from_millis(500)).await; }
            }
            if !published {
                if let Some(native) = &self.native {
                    let mut status = native.status.lock().await;
                    if !status.terminal() {
                        status.stage = "error".into(); status.saved_path = Some(receiver.target_path().to_string_lossy().into_owned());
                        status.error = Some(format!("文件已保存，但最终确认发送失败：{publish_error}"));
                        emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
                    }
                }
                return Err(publish_error);
            }
            if let Some(native) = &self.native {
                let mut status = native.status.lock().await;
                if !status.terminal() {
                    status.local_bytes = received_bytes; status.saved_bytes = received_bytes;
                    status.sha256 = Some(receiver.manifest().sha256.clone());
                    status.saved_path = Some(receiver.target_path().to_string_lossy().into_owned());
                    status.stage = "done".into();
                    emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
                }
                // Retain committed metadata for idempotent final ACK replay.
                // It is released on peer departure/disconnect, not by the UI.
            } else { self.receivers.lock().await.remove(&id); }
            emit(&self.app, &self.id, OnlineUiEvent::TransferDone { transfer_id: id.clone(), ok: true, error: None });
        } else if receiver.should_ack(last_ack, next_seq) {
            self.room.send(&chunk_ack_message(receiver, next_seq, received_bytes, false)).await?;
            if due { emit(&self.app, &self.id, OnlineUiEvent::Progress { transfer_id: id.clone(), sent_bytes: received_bytes, total_bytes: receiver.manifest().size, direction: "receiving".into() }); }
        }
        self.receiver_last_ack.lock().await.insert(id, next_seq);
        Ok(())
    }

    /// Receiver: a P2P offer arrived. Become the responder.
    async fn on_p2p_offer(self: &Arc<Self>, sdp: String) -> Result<(), String> {
        if self.cancelled.load(Ordering::Acquire) { return Err("session cancelled".into()); }
        if self.native.is_some() { self.native_stage("connecting").await; }
        let t0 = std::time::Instant::now();
        emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
            message: format!("on_p2p_offer: received offer sdp_len={}", sdp.len()),
        });
        // TODO: read TURN config; for now use STUN defaults.
        let ice_servers = default_ice_servers(None, None, None);
        let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (p2p, ice_rx, answer_sdp) = OnlineP2p::accept_offer(ice_servers, &sdp, frame_tx).await.map_err(|e| {
            emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
                message: format!("on_p2p_offer: accept_offer FAILED: {}", e),
            });
            e
        })?;
        emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
            message: format!("on_p2p_offer: accept_offer ok, answer_sdp_len={}, elapsed_ms={}", answer_sdp.len(), t0.elapsed().as_millis()),
        });
        let p2p = Arc::new(p2p);
        *self.p2p.lock().await = Some(p2p.clone());

        // Forward our ICE candidates.
        let room = self.room.clone();
        tokio::spawn(async move {
            let mut ice_rx = ice_rx;
            while let Some(candidate) = ice_rx.recv().await {
                let _ = room.send(&FlingMessage::P2pIce { candidate }).await;
            }
        });

        self.room.send(&FlingMessage::P2pAnswer { sdp: answer_sdp }).await?;

        // Flush any ICE that arrived before the PC existed.
        let pending: Vec<String> = std::mem::take(&mut *self.pending_ice.lock().await);
        for candidate in pending {
            p2p.add_remote_ice(Some(candidate)).await?;
        }

        // Wait for the channel, then pump FLNG frames into the receivers.
        let session_id = self.id.clone();
        let app = self.app.clone();
        let room = self.room.clone();
        // We need &self in the async pump; clone the Arcs we need.
        let session = self.clone();
        let receivers = self.receivers.clone();
        tokio::spawn(async move {
            // Register the message handler BEFORE wait_open(): libwebrtc drops
            // messages received before on_message is set, so the handler must
            // be attached as soon as the DataChannel is created (via
            // on_data_channel), not after it opens.
            let wait_t0 = std::time::Instant::now();
            match p2p.wait_open().await {
                Ok(_dc) => {
                    emit(&app, &session_id, OnlineUiEvent::P2pLog {
                        message: format!("responder: wait_open OK, elapsed_ms={}", wait_t0.elapsed().as_millis()),
                    });
                    // Surface the P2P state to the UI (⚡ label): the sender
                    // emits this from send_file, the responder must too.
                    emit(&app, &session_id, OnlineUiEvent::P2p { state: "open".to_string() });
                    let _ = app.emit(
                        "fling://online/event",
                        serde_json::json!({
                            "session": session_id,
                            "event": { "kind": "p2p", "state": "open" },
                        }),
                    );
                    // Pump: on_message (native thread, attached early by
                    // OnlineP2p) -> channel -> async task that routes frames
                    // to receivers and ACKs.
                    let mut frame_count: u64 = 0;
                    let mut frame_bytes_total: u64 = 0;
                    let first_frame_at = std::time::Instant::now();
                    while let Some(frame_bytes) = frame_rx.recv().await {
                        frame_count += 1;
                        frame_bytes_total += frame_bytes.len() as u64;
                        if frame_count == 1 {
                            emit(&app, &session_id, OnlineUiEvent::P2pLog {
                                message: format!("responder: FIRST frame received, len={}, after_open_ms={}", frame_bytes.len(), first_frame_at.elapsed().as_millis()),
                            });
                        }
                        if frame_count % 100 == 0 {
                            emit(&app, &session_id, OnlineUiEvent::P2pLog {
                                message: format!("responder: frames={}, bytes={}", frame_count, frame_bytes_total),
                            });
                        }
                        // Decode just the transfer id synchronously.
                        let id = match crate::native_lan::decode_direct_file_frame(&frame_bytes) {
                            Ok(Some(f)) => f.id,
                            other => {
                                emit(&app, &session_id, OnlineUiEvent::P2pLog {
                                    message: format!(
                                        "responder: dropped frame #{frame_count}: decode failed ({:?})",
                                        other.as_ref().err().map(|e| e.as_str())
                                    ),
                                });
                                continue;
                            }
                        };
                        let receiver = match receivers.lock().await.get(&id).cloned() {
                            Some(r) => r,
                            None => {
                                emit(&app, &session_id, OnlineUiEvent::P2pLog {
                                    message: format!(
                                        "responder: dropped frame #{frame_count}: unknown transfer id {}",
                                        &id[..id.len().min(12)]
                                    ),
                                });
                                continue;
                            }
                        };
                        if receiver.is_committed() {
                            let _ = session.maybe_ack(&receiver, receiver.manifest().total_chunks).await;
                            continue;
                        }
                        let next_seq = match receiver.on_p2p_frame(&frame_bytes).await {
                            Ok(n) => n,
                            Err(e) => {
                                emit(&app, &session_id, OnlineUiEvent::P2pLog {
                                    message: format!(
                                        "responder: dropped frame #{frame_count}: on_p2p_frame failed: {e}"
                                    ),
                                });
                                session.fail_receive(&receiver, &e).await;
                                continue;
                            }
                        };
                        if let Err(error) = session.maybe_ack(&receiver, next_seq).await {
                            emit(&app, &session_id, OnlineUiEvent::P2pLog { message: format!("receive failed: {error}") });
                        }
                    }
                    let _ = room;
                }
                Err(e) => {
                    emit(&app, &session_id, OnlineUiEvent::P2pLog {
                        message: format!("responder: wait_open FAILED after {}ms: {}", wait_t0.elapsed().as_millis(), e),
                    });
                    emit(&app, &session_id, OnlineUiEvent::P2p { state: "failed".to_string() });
                    eprintln!("fling online P2P responder failed: {e}");
                }
            }
        });
        Ok(())
    }

    /// Handle a remote ICE candidate (or buffer it if the PC isn't ready).
    async fn on_p2p_ice(&self, candidate: Option<String>) -> Result<(), String> {
        if let Some(p2p) = self.p2p.lock().await.as_ref() {
            p2p.add_remote_ice(candidate).await?;
        } else if let Some(c) = candidate {
            self.pending_ice.lock().await.push(c);
        }
        Ok(())
    }

    /// Sender: transmit one file. Builds the manifest, exchanges it, tries
    /// P2P, falls back to relay. Mirrors `SendTask` + `FlingRoom.sendFile()`.
    pub async fn send_file(
        self: &Arc<Self>,
        path: &Path,
        name: String,
        kind: FileKind,
    ) -> Result<String, String> {
        let manifest = build_manifest(path, name, kind)?;
        let sender = Arc::new(OnlineSender::new(manifest, path)?);
        self.send_prepared(sender).await
    }

    async fn send_prepared(self: &Arc<Self>, sender: Arc<OnlineSender>) -> Result<String, String> {
        let manifest = sender.manifest();
        let transfer_id = manifest.id.clone();
        emit(
            &self.app,
            &self.id,
            OnlineUiEvent::FileSendStarted {
                transfer_id: transfer_id.clone(),
                total_bytes: manifest.size,
            },
        );
        self.senders
            .lock()
            .await
            .insert(transfer_id.clone(), sender.clone());

        // 1. Manifest + ACK.
        let manifest_notify = Arc::new(tokio::sync::Notify::new());
        self.manifest_acks
            .lock()
            .await
            .insert(transfer_id.clone(), manifest_notify.clone());
        self.room
            .send(&FlingMessage::FileManifest {
                manifest: sender.manifest().clone(),
            })
            .await?;
        // Wait for the receiver's manifest ACK (advertises capabilities).
        if tokio::time::timeout(MANIFEST_ACK_TIMEOUT, manifest_notify.notified())
            .await
            .is_err()
        {
            self.manifest_acks.lock().await.remove(&transfer_id);
            return Err("file manifest ACK timeout".to_string());
        }
        self.manifest_acks.lock().await.remove(&transfer_id);
        if sender.is_failed() { return Err("对方不支持 0.6.29 原生单文件确认，请两端使用此测试版本".into()); }

        if self.native.is_some() { self.native_stage("connecting").await; }
        // 2. P2P attempt (initiator).
        let ice_servers = default_ice_servers(None, None, None);
        let p2p_t0 = std::time::Instant::now();
        let p2p_result = OnlineP2p::start_initiator(ice_servers).await;
        let use_p2p = match p2p_result {
            Ok((p2p, ice_rx, offer_sdp)) => {
                emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
                    message: format!("initiator: start_initiator ok, offer_sdp_len={}, elapsed_ms={}", offer_sdp.len(), p2p_t0.elapsed().as_millis()),
                });
                let p2p = Arc::new(p2p);
                *self.p2p.lock().await = Some(p2p.clone());
                let room = self.room.clone();
                tokio::spawn(async move {
                    let mut ice_rx = ice_rx;
                    while let Some(candidate) = ice_rx.recv().await {
                        let _ = room.send(&FlingMessage::P2pIce { candidate }).await;
                    }
                });
                self.room.send(&FlingMessage::P2pOffer { sdp: offer_sdp }).await?;
                emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
                    message: "initiator: P2pOffer sent via room".to_string(),
                });
                // Flush pending ICE.
                let pending: Vec<String> = std::mem::take(&mut *self.pending_ice.lock().await);
                emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
                    message: format!("initiator: flushing {} pending ICE", pending.len()),
                });
                for candidate in pending {
                    p2p.add_remote_ice(Some(candidate)).await?;
                }
                let wait_t0 = std::time::Instant::now();
                match p2p.wait_open().await {
                    Ok(dc) => {
                        emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
                            message: format!("initiator: wait_open OK, elapsed_ms={}", wait_t0.elapsed().as_millis()),
                        });
                        emit(&self.app, &self.id, OnlineUiEvent::P2p { state: "open".to_string() });
                        Some((p2p, dc))
                    }
                    Err(e) => {
                        emit(&self.app, &self.id, OnlineUiEvent::P2pLog {
                            message: format!("initiator: wait_open FAILED after {}ms, falling back to relay: {}", wait_t0.elapsed().as_millis(), e),
                        });
                        eprintln!("fling online P2P initiator failed, falling back to relay: {e}");
                        emit(&self.app, &self.id, OnlineUiEvent::P2p { state: "failed".to_string() });
                        None
                    }
                }
            }
            Err(e) => {
                eprintln!("fling online P2P setup failed, using relay: {e}");
                None
            }
        };

        // 3. Transfer.
        let app = self.app.clone();
        let session_id = self.id.clone();
        let tid = transfer_id.clone();
        let native = self.native.clone();
        let last_native_emit = std::sync::Mutex::new(Instant::now() - Duration::from_secs(1));
        let on_progress = move |sent: u64, total: u64| {
            if let Some(native) = &native {
                if let Ok(mut status) = native.status.try_lock() {
                    if !status.terminal() {
                        status.local_bytes = sent;
                        status.stage = if sent == total { "finalizing" } else { "transferring" }.into();
                        let mut last = last_native_emit.lock().unwrap();
                        if last.elapsed() >= Duration::from_millis(300) || sent == total {
                            *last = Instant::now();
                            emit(&app, &session_id, OnlineUiEvent::NativeTask { status: status.clone() });
                        }
                    }
                }
            }
            emit(
                &app,
                &session_id,
                OnlineUiEvent::Progress {
                    transfer_id: tid.clone(),
                    sent_bytes: sent,
                    total_bytes: total,
                    direction: "sending".to_string(),
                },
            );
        };

        let result = if let Some((_p2p, dc)) = use_p2p {
            // P2P path: FLNG batch frames with polling backpressure.
            let dc_send = dc.clone();
            sender
                .send_via_p2p(
                    |frame| {
                        dc_send
                            .send(&frame, true)
                            .map_err(|e| format!("P2P send frame: {e}"))
                    },
                    || dc.buffered_amount(),
                    on_progress,
                )
                .await
        } else if self.native.is_some() {
            Err("未能建立直连；原生单文件测试路径不会静默改走 Blob 或中继，请重试".into())
        } else {
            // Relay path: base64 chunks over LiveKit. The engine emits
            // (seq, b64) pairs into a channel; a forwarder publishes them.
            let (chunk_tx, mut chunk_rx) = mpsc::unbounded_channel::<(u32, String)>();
            let room = self.room.clone();
            let tid2 = transfer_id.clone();
            let forwarder = tokio::spawn(async move {
                while let Some((seq, b64)) = chunk_rx.recv().await {
                    let _ = room
                        .send(&FlingMessage::FileChunk {
                            id: tid2.clone(),
                            seq,
                            data: b64,
                        })
                        .await;
                }
            });
            let relay_result = sender
                .send_via_relay(
                    |seq, b64| {
                        chunk_tx
                            .send((seq, b64))
                            .map_err(|_| "relay chunk channel closed".to_string())
                    },
                    on_progress,
                )
                .await;
            drop(chunk_tx);
            let _ = forwarder.await;
            relay_result
        };

        // 4. Report.
        // NOTE: Do NOT send FlingMessage::TaskDone here — that's a task-level
        // message for the whole task (all files), sent by the frontend.
        // We only emit the per-file TransferDone UI event.
        match &result {
            Ok(()) => {
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::TransferDone {
                        transfer_id: transfer_id.clone(),
                        ok: true,
                        error: None,
                    },
                );
            }
            Err(e) => {
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::TransferDone {
                        transfer_id: transfer_id.clone(),
                        ok: false,
                        error: Some(e.clone()),
                    },
                );
            }
        }
        self.senders.lock().await.remove(&transfer_id);
        self.peer_progress_ui.lock().await.remove(&transfer_id);
        result.map(|_| transfer_id)
    }

    pub fn authorize(&self, owner: &str) -> Result<(), String> {
        if self.owner != owner { return Err("online session unauthorized".into()); }
        Ok(())
    }

    pub async fn native_stage(&self, stage: &str) {
        if let Some(native) = &self.native {
            let mut status = native.status.lock().await;
            if !status.terminal() {
                status.stage = stage.into();
                emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
            }
        }
    }

    async fn fail_receive(&self, receiver: &Arc<OnlineReceiver>, error: &str) {
        receiver.abort().await;
        let id = receiver.manifest().id.clone();
        let _ = self.room.send(&FlingMessage::FileChunkAck { id: id.clone(), next_seq: 0, received_bytes: 0, ack_id: None, complete: None, failed: Some(true) }).await;
        self.receivers.lock().await.remove(&id);
        self.receiver_last_ack.lock().await.remove(&id);
        if let Some(native) = &self.native {
            let mut status = native.status.lock().await;
            if !status.terminal() { status.stage = "error".into(); status.error = Some(error.into()); emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() }); }
        }
        emit(&self.app, &self.id, OnlineUiEvent::TransferDone { transfer_id: id, ok: false, error: Some(error.into()) });
    }

    pub async fn send_source(self: &Arc<Self>, lease: SourceLease) -> Result<NativeTaskStatus, String> {
        let native = self.native.as_ref().ok_or("not a native single-file task")?;
        {
            let status = native.status.lock().await;
            if status.direction != "sending" || status.name != lease.metadata.name || status.total_bytes != lease.metadata.size { return Err("source does not match task".into()); }
        }
        if native.claimed.swap(true, Ordering::AcqRel) { return Err("native task already started".into()); }
        let result = async {
            tokio::time::timeout(Duration::from_secs(300), async {
                loop {
                    let notified = self.peer_notify.notified();
                    if self.peer_present.load(Ordering::Acquire) { break; }
                    if self.cancelled.load(Ordering::Acquire) { return Err("已取消".to_string()); }
                    tokio::select! { _ = notified => {}, _ = self.cancel_notify.notified() => return Err("已取消".to_string()) }
                }
                Ok(())
            }).await.map_err(|_| "等待对方加入超时".to_string())??;
            self.native_stage("preparing").await;
            let cancel = native.source_cancelled.clone();
            let (manifest, file) = tokio::task::spawn_blocking(move || {
                let mut file = lease.file;
                let manifest = crate::online_transfer::build_manifest_handle(&mut file, lease.metadata.name, FileKind::File, &cancel)?;
                Ok::<_, String>((manifest, file))
            }).await.map_err(|e| format!("hash worker failed: {e}"))??;
            {
                let mut status = native.status.lock().await;
                if manifest.size != status.total_bytes { return Err("文件大小已改变，请重新选择".into()); }
                status.sha256 = Some(manifest.sha256.clone());
            }
            if self.cancelled.load(Ordering::Acquire) { return Err("已取消".into()); }
            let sender = Arc::new(OnlineSender::from_handle(manifest, file));
            tokio::select! {
                result = self.send_prepared(sender) => { result?; }
                _ = self.cancel_notify.notified() => { return Err("已取消".into()); }
            }
            self.room.send(&FlingMessage::TaskDone { task_id: native.status.lock().await.task_id.clone() }).await?;
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let notified = native.ack.notified();
                    if native.task_acked.load(Ordering::Acquire) { return Ok(()); }
                    if self.cancelled.load(Ordering::Acquire) { return Err("已取消".to_string()); }
                    tokio::select! { _ = notified => {}, _ = self.cancel_notify.notified() => return Err("已取消".to_string()) }
                }
            }).await.map_err(|_| "最终任务确认超时".to_string())??;
            Ok::<(), String>(())
        }.await;
        self.senders.lock().await.clear();
        self.manifest_acks.lock().await.clear();
        self.peer_progress_ui.lock().await.clear();
        let mut status = native.status.lock().await;
        if !status.terminal() {
            match result {
                Ok(()) => { status.stage = "done".into(); status.local_bytes = status.total_bytes; status.peer_bytes = status.total_bytes; status.saved_bytes = status.total_bytes; }
                Err(error) => { status.stage = if self.cancelled.load(Ordering::Acquire) { "cancelled" } else { "error" }.into(); status.error = Some(error); }
            }
        }
        emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
        Ok(status.clone())
    }

    pub async fn cancel_native(&self, error: Option<String>, notify_peer: bool) -> Result<(), String> {
        if let Some(native) = &self.native {
            if native.status.lock().await.terminal() { return Ok(()); }
            native.cancel_source();
        }
        self.cancelled.store(true, Ordering::Release);
        self.cancel_notify.notify_one();
        let receivers: Vec<_> = self.receivers.lock().await.drain().map(|(_, receiver)| receiver).collect();
        for receiver in receivers { receiver.abort().await; }
        for sender in self.senders.lock().await.values() { sender.on_failed(); }
        if let Some(p2p) = self.p2p.lock().await.as_ref() { p2p.close(); }
        if let Some(native) = &self.native {
            let mut status = native.status.lock().await;
            if !status.terminal() {
                status.stage = if error.is_some() { "error" } else { "cancelled" }.into();
                status.error = error;
                emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
            }
            if notify_peer { self.room.send(&FlingMessage::TaskCancel { task_id: status.task_id.clone() }).await?; }
        }
        Ok(())
    }

    pub async fn send_text(&self, text: String) -> Result<(), String> {
        let id = format!("{:x}", rand::random::<u64>());
        self.room.send(&FlingMessage::Text { id, text }).await
    }

    /// Send a raw protocol message (used by the frontend adapter for
    /// offer / task lifecycle messages).
    pub async fn send_raw(&self, msg: FlingMessage) -> Result<(), String> {
        self.room.send(&msg).await
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        self.cancel_native(None, false).await?;
        if let Some(p2p) = self.p2p.lock().await.as_ref() {
            p2p.close();
        }
        self.room.disconnect().await
    }
}

/// Global registry of online sessions, keyed by session id.
pub struct OnlineSessionRegistry {
    sessions: Mutex<HashMap<String, Arc<OnlineSession>>>,
}

impl OnlineSessionRegistry {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn insert(&self, id: String, session: Arc<OnlineSession>) {
        self.sessions.lock().await.insert(id, session);
    }

    pub async fn get(&self, id: &str) -> Option<Arc<OnlineSession>> {
        self.sessions.lock().await.get(id).cloned()
    }

    pub async fn remove(&self, id: &str) -> Option<Arc<OnlineSession>> {
        self.sessions.lock().await.remove(id)
    }
}

#[cfg(test)]
mod ui_event_serialization_tests {
    use super::{should_emit_peer_progress, OnlineUiEvent, PEER_PROGRESS_UI_BYTES};
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    #[test]
    fn peer_progress_event_keeps_the_frontend_contract() {
        let value = serde_json::to_value(OnlineUiEvent::PeerProgress {
            transfer_id: "transfer-test".to_string(),
            received_bytes: 256,
            total_bytes: 1024,
            complete: false,
        })
        .unwrap();
        assert_eq!(value["kind"], "peer-progress");
        assert_eq!(value["transfer_id"], "transfer-test");
        assert_eq!(value["received_bytes"], 256);
        assert_eq!(value["total_bytes"], 1024);
        assert_eq!(value["complete"], false);
    }

    #[test]
    fn file_send_started_event_keeps_the_frontend_contract() {
        let value = serde_json::to_value(OnlineUiEvent::FileSendStarted {
            transfer_id: "transfer-test".to_string(),
            total_bytes: 1024,
        })
        .unwrap();
        assert_eq!(value["kind"], "file-send-started");
        assert_eq!(value["transfer_id"], "transfer-test");
        assert_eq!(value["total_bytes"], 1024);
    }

    #[tokio::test]
    async fn peer_progress_is_throttled_but_completion_is_always_emitted() {
        let progress = Mutex::new(HashMap::new());
        assert!(should_emit_peer_progress(&progress, "x", 0, false).await);
        assert!(!should_emit_peer_progress(&progress, "x", 1024, false).await);
        assert!(should_emit_peer_progress(&progress, "x", PEER_PROGRESS_UI_BYTES, false).await);
        assert!(should_emit_peer_progress(&progress, "x", PEER_PROGRESS_UI_BYTES, true).await);
        assert!(should_emit_peer_progress(&progress, "x", 0, false).await);
    }
}
