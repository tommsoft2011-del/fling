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
use crate::online_direct::{Capability, DirectAttempt, SignalSender, TransportEvent};
use crate::online_route_choice::{Fence, Role as RouteRole};
use crate::online_native::{NativeTask, NativeTaskConfig, NativeTaskStatus};
use crate::online_setup::{bounded_initial_effect, OfferAdmission, SetupLifecycle};
use crate::online_source::SourceLease;
use crate::online_diagnostics::{Diagnostic, Diagnostics, Phase, Role, Route, RouteObservation};
use crate::online_p2p::{has_relay_candidate, OnlineP2p};
use crate::online_proto::{FileKind, FlingMessage};
use crate::online_room::{OnlineRoom, OnlineRoomEvent};
use crate::online_transfer::{build_manifest, chunk_ack_message, OnlineReceiver, OnlineSender};

fn connection_failure() -> &'static str {
    "CONNECTION_FAILED: 未能建立安全连接，文件尚未开始传输；此路径不支持自动文件中继，请点击重试"
}

/// How long to wait for a manifest ACK.
const MANIFEST_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_PROGRESS_UI_INTERVAL: Duration = Duration::from_millis(500);
const PEER_PROGRESS_UI_BYTES: u64 = 4 * 1024 * 1024;

/// The cursor tracks successfully published ACKs, not received frames. In
/// particular, two 4-chunk frames must accumulate to the 8-chunk ACK threshold.
pub(crate) async fn publish_progress_ack(
    receiver: &OnlineReceiver,
    last_acks: &Mutex<HashMap<String, u32>>,
    next_seq: u32,
    publish: impl std::future::Future<Output = Result<(), String>>,
) -> Result<bool, String> {
    let id = &receiver.manifest().id;
    let last_ack = *last_acks.lock().await.get(id).unwrap_or(&0);
    if !receiver.should_ack(last_ack, next_seq) { return Ok(false); }
    publish.await?;
    last_acks.lock().await.insert(id.clone(), next_seq);
    Ok(true)
}

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
    #[serde(rename = "path-diagnostic")]
    PathDiagnostic { summary: Diagnostic },
    #[serde(rename = "connection-diagnostic")]
    ConnectionDiagnostic { summary: crate::connection_diagnostics::PhaseEvent },
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
    diagnostics: Diagnostics,
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
    route: Mutex<Option<(String, Arc<DirectAttempt>)>>,
    route_capabilities: Mutex<HashMap<String, Capability>>,
    // Remote online only. Never serialized or logged; LAN has its own transport.
    ice_servers: Vec<livekit::webrtc::peer_connection_factory::IceServer>,
    pending_ice: Mutex<Vec<String>>,
    download_dir: PathBuf,
    /// Notified when a `file-manifest-ack` arrives, keyed by transfer id.
    manifest_acks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,
    /// Rate-limit ACK progress events sent from Rust to the WebView.
    peer_progress_ui: Arc<Mutex<HashMap<String, (u64, Instant)>>>,
    /// Receiver-only native direct setup clock; starts after validated manifest, before ACK.
    setup: SetupLifecycle,
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
        ice_servers: Vec<livekit::webrtc::peer_connection_factory::IceServer>,
    ) -> Result<Arc<Self>, String> {
        let room_key = derive_room_key(pairing_code, key_context);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let room = OnlineRoom::connect(server_url, token, room_key, event_tx).await?;
        let room = Arc::new(room);

        let session = Arc::new(Self {
            diagnostics: Diagnostics::new(),
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
            route: Mutex::new(None),
            route_capabilities: Mutex::new(HashMap::new()),
            ice_servers,
            pending_ice: Mutex::new(Vec::new()),
            download_dir,
            manifest_acks: Arc::new(Mutex::new(HashMap::new())),
            peer_progress_ui: Arc::new(Mutex::new(HashMap::new())),
            setup: SetupLifecycle::new(),
        });

        // Dispatch loop.
        let session_cb = session.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let Err(e) = session_cb.handle_room_event(event).await {
                    // Log and continue; a single bad message must not kill
                    // the session.
                    eprintln!("fling online session event failed");
                    session_cb.note(Phase::TransferFailed);
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
                    self.record_route(RouteObservation::default(), false);
                    self.note(Phase::ConnectionClosed);
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
                self.record_route(RouteObservation::default(), false);
                self.note(Phase::ConnectionClosed);
                if self.native.is_some() { self.cancel_native(Some(reason.clone()), false).await?; }
                emit(&self.app, &self.id, OnlineUiEvent::Disconnected { reason });
                Ok(())
            }
            OnlineRoomEvent::ProtocolWarning { detail: _ } => {
                eprintln!("fling online protocol warning");
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
                | FlingMessage::NativeRoute { .. }
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
            FlingMessage::FileManifest { manifest, route } => {
                let capability = route.filter(|r| r.valid()).ok_or("对方不支持本版 Rust direct-first 协议，请两端使用同一测试版本")?;
                self.on_file_manifest(manifest, capability).await?;
            }
            FlingMessage::FileManifestAck { id, native_single_file, route, .. } => {
                let expected = self.route_capabilities.lock().await.get(&id).copied();
                if route != expected || expected.is_none() {
                    if let Some(sender) = self.senders.lock().await.get(&id) { sender.on_failed(); }
                }
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
                route_fence,
                next_seq,
                received_bytes,
                complete,
                failed,
                ..
            } => {
                let route = self.route.lock().await.as_ref().filter(|(tid,_)| tid == &id).map(|(_,r)| r.clone());
                let Some(route) = route else { return Ok(()); };
                let Some(fence) = route_fence else { return Ok(()); };
                let sender=self.senders.lock().await.get(&id).cloned();
                let Some(sender)=sender else {return Ok(());};
                if !sender.valid_ack_geometry(next_seq,received_bytes) {return Ok(());}
                if !route.confirm_ack(fence, received_bytes).await { return Ok(()); }
                {
                    if failed == Some(true) { sender.on_failed(); return Err(format!("receiver failed transfer {id}")); }
                    sender.on_chunk_ack(next_seq);
                    let is_complete = complete == Some(true);
                    if is_complete && (next_seq != sender.manifest().total_chunks || received_bytes != sender.manifest().size) {
                        sender.on_failed(); return Err("invalid final ACK accounting".into());
                    }
                    if is_complete { self.note_transfer(&id, Phase::DurableAckReceived); sender.on_complete(); }
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
            FlingMessage::NativeRoute { id, signal } => {
                let route = self.route.lock().await.as_ref().filter(|(tid,_)| tid == &id).map(|(_,r)|r.clone());
                if let Some(route) = route {
                    let initial_offer = matches!(&signal, crate::online_direct::Signal::Description { fence, answer: false, .. } if fence.path == crate::online_route_choice::Path::Direct && fence.epoch == route.capability.epoch);
                    let setup_begin = if initial_offer && self.native.is_some() {
                        matches!(self.setup.admit_offer(Instant::now())?, OfferAdmission::Begin(_))
                    } else {false};
                    route.receive(signal).await.map_err(|_|crate::connection_diagnostics::connection_failure())?;
                    if setup_begin && !self.setup.offer_ready(Instant::now()) {
                        route.fail("初始 offer 已超过 setup 期限".into()).await;
                        return Err("初始 offer 已超过 setup 期限".into());
                    }
                }
            }
            FlingMessage::P2pOffer { .. } | FlingMessage::P2pAnswer { .. } | FlingMessage::P2pIce { .. } => {
                return Err("旧版未围栏 WebRTC 信令不适用于本测试版本".into());
            }
            FlingMessage::TaskAck { task_id } => {
                if let Some(native) = &self.native {
                    let status = native.status.lock().await;
                    if status.task_id == task_id && status.direction == "sending" {
                        self.note(Phase::TaskAckReceived);
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
                        self.note(Phase::TaskAckSent);
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
        self: &Arc<Self>,
        manifest: crate::online_proto::FileManifest,
        capability: Capability,
    ) -> Result<(), String> {
        self.diagnostics.set_role(Role::Responder);
        if self.cancelled.load(Ordering::Acquire) { return Err("session cancelled".into()); }
        if let Some(native) = &self.native {
            let status = native.status.lock().await;
            if status.direction != "receiving" || status.name != manifest.name || status.total_bytes != manifest.size || manifest.kind != FileKind::File {
                return Err("manifest does not match accepted single-file offer".into());
            }
            if native.claimed.load(Ordering::Acquire) {
                drop(status);
                let existing = self.receivers.lock().await.get(&manifest.id).cloned();
                if self.route_capabilities.lock().await.get(&manifest.id) != Some(&capability)
                    || !existing.as_ref().is_some_and(|r| r.manifest() == &manifest) {
                    return Err("conflicting manifest for claimed native task".into());
                }
                // Idempotent reliable re-delivery: no file recreation, UI
                // transition, source clock reset, or second native PC.
                return self.send_manifest_ack(&manifest.id).await;
            }
            if native.claimed.swap(true, Ordering::AcqRel) { return Err("single-file task already has a manifest".into()); }
            drop(status);
            // No untrusted offer can start this clock: task and manifest have now both validated.
            if self.setup.start(Instant::now()) {
                let session = self.clone();
                tokio::spawn(async move {
                    if session.setup.wait_for_timeout().await { session.fail_setup_timeout().await; }
                });
            }
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
        self.route_capabilities.lock().await.insert(id.clone(), capability);
        self.install_route(id.clone(), RouteRole::Receiver, capability).await?;
        self.receiver_last_ack.lock().await.insert(id.clone(), 0);
        // Advertise full capabilities: chunk ACKs, binary chunks, 128 KiB
        // direct frames (matching the v0.6.6 LAN engine).
        self.send_manifest_ack(&id).await?;
        let receiver = self.receivers.lock().await.get(&id).cloned().ok_or("receiver missing")?;
        let _ = receiver; // Empty-file commit is gated by the receiver START handshake, too.
        Ok(())
    }

    async fn send_manifest_ack(&self, id: &str) -> Result<(), String> {
        let ack = FlingMessage::FileManifestAck {
            id: id.to_owned(), route: self.route_capabilities.lock().await.get(id).copied(), native_single_file: self.native.as_ref().map(|_| true),
            chunk_ack: Some(true), binary_chunks: Some(true), direct_frame_payload_bytes: Some(128 * 1024),
        };
        if self.native.is_some() { bounded_initial_effect(self.room.send(&ack)).await }
        else { self.room.send(&ack).await }
    }

    /// Receiver: a relay chunk arrived.
    async fn on_relay_chunk(&self, id: String, seq: u32, data: String) -> Result<(), String> {
        if self.route.lock().await.is_some() || self.native.is_some() { return Err("本测试版拒绝未选中路径的 relay 文件帧".into()); }
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
        if received_bytes > 0 { self.note_transfer(&id, Phase::FirstRealBytes); }
        if self.cancelled.load(Ordering::Acquire) { receiver.abort().await; return Err("session cancelled".into()); }
        if self.native.is_some() && received_bytes == receiver.manifest().size { self.native_stage("finalizing").await; }
        let complete = match receiver.check_complete_observed(|phase| self.note_transfer(&id, phase)).await {
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
        if complete {
            let mut published = false;
            let mut publish_error = String::new();
            for attempt in 0..3 {
                let result = async {
                    self.room.send(&self.fenced_ack(receiver, next_seq, received_bytes, true).await?).await?;
                    self.note_transfer(&id, Phase::DurableAckSent);
                    if let Some(native) = &self.native {
                        let task_id = native.status.lock().await.task_id.clone();
                        self.room.send(&FlingMessage::TaskAck { task_id }).await?;
                        self.note(Phase::TaskAckSent);
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
                self.note_transfer(&id, Phase::TransferFailed);
                self.snapshot_route().await;
                self.note_transfer(&id, Phase::TaskEnd);
                return Err(publish_error);
            }
            self.note_transfer(&id, Phase::DurableAckSent);
            if self.native.is_some() { self.note(Phase::TaskAckSent); }
            self.snapshot_route().await;
            self.note_transfer(&id, Phase::TaskEnd);
            self.receiver_last_ack.lock().await.insert(id.clone(), next_seq);
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
        } else {
            let ack = self.fenced_ack(receiver, next_seq, received_bytes, false).await?;
            if publish_progress_ack(receiver, &self.receiver_last_ack, next_seq, self.room.send(&ack)).await? {
                if due { emit(&self.app, &self.id, OnlineUiEvent::Progress { transfer_id: id.clone(), sent_bytes: received_bytes, total_bytes: receiver.manifest().size, direction: "receiving".into() }); }
            }
        }
        Ok(())
    }

    async fn current_fence(&self, id: &str) -> Option<Fence> {
        let route = self.route.lock().await.as_ref().filter(|(tid,_)| tid == id).map(|(_,r)|r.clone())?;
        route.active_fence().await
    }

    async fn fenced_ack(&self, receiver: &OnlineReceiver, next_seq: u32, received_bytes: u64, complete: bool) -> Result<FlingMessage,String> {
        let fence = self.current_fence(&receiver.manifest().id).await.ok_or("no selected receive path for ACK")?;
        let mut message = chunk_ack_message(receiver,next_seq,received_bytes,complete);
        if let FlingMessage::FileChunkAck {route_fence,..} = &mut message { *route_fence=Some(fence); }
        Ok(message)
    }

    async fn install_route(self: &Arc<Self>, id: String, role: RouteRole, capability: Capability) -> Result<Arc<DirectAttempt>,String> {
        if role==RouteRole::Receiver {self.diagnostics.begin_connection(Role::Responder);}
        let room = self.room.clone(); let signal_id=id.clone();
        let send: SignalSender = Arc::new(move |signal| {
            let room=room.clone();let id=signal_id.clone();
            Box::pin(async move {
                #[cfg(all(target_os="linux",debug_assertions))]
                if crate::online_probe::drop_signal(&signal) {return Ok(());}
                room.send(&FlingMessage::NativeRoute {id,signal}).await
            })
        });
        let app=self.app.clone();let session_id=self.id.clone();
        let observer=Arc::new(move |summary|emit(&app,&session_id,OnlineUiEvent::ConnectionDiagnostic {summary}));
        let (route,mut events)=DirectAttempt::new_observed(role,capability,self.ice_servers.clone(),send,observer);
        // Old callback objects retain old Arcs and cannot emit into the replacement.
        let old=self.route.lock().await.replace((id.clone(),route.clone()));
        if let Some((_,old))=old {old.close().await;}
        if role==RouteRole::Receiver {route.expect_offer(Instant::now()).await?;}
        let weak_session=Arc::downgrade(self);let weak_route=Arc::downgrade(&route);
        let mut terminal=route.terminal_watch();
        tokio::spawn(async move {
            loop {
                // Terminal state is sticky and independent of a full data-frame
                // queue. Do not hold a route Arc while awaiting (avoids a cycle).
                let terminated=*terminal.borrow();
                let event=if terminated {TransportEvent::Failed("安全连接已终止".into())} else {
                    tokio::select! {biased;
                        changed=terminal.changed()=>{
                            if changed.is_err() {break;}
                            TransportEvent::Failed("安全连接已终止".into())
                        },
                        event=events.recv()=>{let Some(event)=event else {break;};event}
                    }
                };
                let (Some(session),Some(route))=(weak_session.upgrade(),weak_route.upgrade()) else {break;};
                let current=session.route.lock().await.as_ref().is_some_and(|(tid,r)|tid==&id && Arc::ptr_eq(r,&route));
                if !current || session.cancelled.load(Ordering::Acquire) {break;}
                match event {
                    TransportEvent::TurnStarted => {
                        session.note(Phase::FallbackRequested);
                        emit(&session.app,&session.id,OnlineUiEvent::P2p {state:"fallback_requested".into()});
                    }
                    TransportEvent::Failed(_error) => {
                        let error=crate::connection_diagnostics::connection_failure();
                        session.note(Phase::ConnectionFailed);
                        if let Some(sender)=session.senders.lock().await.get(&id) {sender.on_failed();}
                        if session.native.is_some() {let _=session.cancel_native(Some(error),false).await;}
                        break;
                    }
                    TransportEvent::Started(fence) => {
                        if session.current_fence(&id).await!=Some(fence) {continue;}
                        if let Some(peer)=route.selected_peer(fence).await {
                            let first=session.p2p.lock().await.is_none();
                            *session.p2p.lock().await=Some(peer.clone());
                            if first {session.note(Phase::DataChannelOpen);session.observe_route(peer);}
                        }
                        let receiver=session.receivers.lock().await.get(&id).cloned();
                        if let Some(receiver)=receiver {if receiver.manifest().size==0 {let _=session.maybe_ack(&receiver,0).await;}}
                    }
                    TransportEvent::Frame {fence,bytes} => {
                        if session.current_fence(&id).await!=Some(fence) {continue;}
                        let decoded=match crate::native_lan::decode_direct_file_frame(&bytes) {Ok(Some(f))=>f,_=>continue};
                        if decoded.id!=id {continue;}
                        let receiver=session.receivers.lock().await.get(&id).cloned();
                        let Some(receiver)=receiver else {continue;};
                        let result=if receiver.is_committed() {Ok(receiver.manifest().total_chunks)} else {receiver.on_p2p_frame(&bytes).await};
                        match result {
                            Ok(next)=>{if let Err(error)=session.maybe_ack(&receiver,next).await {session.fail_receive(&receiver,&error).await;route.fail(error).await;}},
                            Err(error)=>{session.fail_receive(&receiver,&error).await;route.fail(error).await;}
                        }
                    }
                }
            }
        });
        Ok(route)
    }

    fn note(&self, phase: Phase) {
        if let Some(summary) = self.diagnostics.note(phase) {
            emit(&self.app, &self.id, OnlineUiEvent::PathDiagnostic { summary });
        }
    }

    fn note_transfer(&self, key: &str, phase: Phase) {
        if let Some(summary) = self.diagnostics.note_transfer(key, phase) {
            emit(&self.app, &self.id, OnlineUiEvent::PathDiagnostic { summary });
        }
    }

    fn record_route(&self, observation: RouteObservation, channel_open: bool) {
        if let Some(summary) = self.diagnostics.observe(observation) {
            let state = if !channel_open { "closed" } else { match summary.summary.route {
                Route::Direct => "direct_established", Route::TurnRelay => "relay_established",
                Route::Unknown => "unknown_established",
            } };
            emit(&self.app, &self.id, OnlineUiEvent::P2p { state: state.into() });
            emit(&self.app, &self.id, OnlineUiEvent::PathDiagnostic { summary });
        }
    }

    async fn snapshot_route(&self) {
        let p2p = self.p2p.lock().await.clone();
        if let Some(p2p) = p2p {
            let observation = p2p.route_state().await;
            let open=p2p.state().await == crate::online_p2p::P2pState::Open;
            let current=self.p2p.lock().await;
            if current.as_ref().is_some_and(|p|Arc::ptr_eq(p,&p2p)) {
                self.record_route(observation,open);
            }
        }
    }

    fn note_relay_candidate(&self, candidate: Option<&str>) {
        if candidate.is_some_and(has_relay_candidate) {
            self.note(Phase::RelayCandidate);
            // Candidate presence is NOT evidence for the UI route state.
        }
    }

    fn observe_route(self: &Arc<Self>, p2p: Arc<OnlineP2p>) {
        let session = self.clone();
        tokio::spawn(async move {
            // Advisory stats never block frames. One collection per second; no per-chunk stats.
            loop {
                let state = p2p.state().await;
                let current=session.p2p.lock().await;
                if !current.as_ref().is_some_and(|p|Arc::ptr_eq(p,&p2p)) {break;}
                if matches!(state, crate::online_p2p::P2pState::Failed | crate::online_p2p::P2pState::Closed) {
                    session.record_route(RouteObservation::default(), false);
                    session.note(if state == crate::online_p2p::P2pState::Failed { Phase::ConnectionFailed } else { Phase::ConnectionClosed });
                    emit(&session.app, &session.id, OnlineUiEvent::P2p { state: "closed".into() });
                    break;
                }
                drop(current);
                if session.cancelled.load(Ordering::Acquire) { break; }
                if let Some(native) = &session.native {
                    if native.status.lock().await.terminal() { break; }
                }
                if state != crate::online_p2p::P2pState::Open { break; }
                let observation = p2p.route_state().await;
                let open=p2p.state().await == crate::online_p2p::P2pState::Open;
                let current=session.p2p.lock().await;
                if !current.as_ref().is_some_and(|p|Arc::ptr_eq(p,&p2p)) {break;}
                session.record_route(observation,open);
                drop(current);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    /// Sender: transmit one file. Builds the manifest, exchanges it, tries
    /// P2P with optional configured TURN. Native single-file never falls back to LiveKit file relay.
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
        let id=sender.manifest().id.clone();
        let result=self.send_prepared_inner(sender).await;
        // All early exits share cleanup; native cancellation also has an outer scope.
        self.senders.lock().await.remove(&id);
        self.manifest_acks.lock().await.remove(&id);
        self.route_capabilities.lock().await.remove(&id);
        self.peer_progress_ui.lock().await.remove(&id);
        if result.is_err() {
            self.note_transfer(&id,Phase::TransferFailed);
            let route={let mut current=self.route.lock().await;
                if current.as_ref().is_some_and(|(tid,_)|tid==&id) {current.take()} else {None}};
            if let Some((_,route))=route {route.close().await;}
        }
        result
    }
    async fn send_prepared_inner(self: &Arc<Self>, sender: Arc<OnlineSender>) -> Result<String, String> {
        self.diagnostics.set_role(Role::Initiator);
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

        // 1. Manifest + exact Rust route capability/epoch echo.
        let capability = Capability::fresh();
        self.route_capabilities.lock().await.insert(transfer_id.clone(), capability);
        let manifest_notify = Arc::new(tokio::sync::Notify::new());
        self.manifest_acks
            .lock()
            .await
            .insert(transfer_id.clone(), manifest_notify.clone());
        self.room
            .send(&FlingMessage::FileManifest {
                manifest: sender.manifest().clone(),
                route: Some(capability),
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
        if sender.is_failed() { return Err("对方不支持本版 Rust direct-first 协议，请两端使用此测试版本".into()); }

        if self.native.is_some() { self.native_stage("connecting").await; }
        // 2. Rust owns both PCs, selected-pair evidence and the first-frame gate.
        self.diagnostics.begin_connection(Role::Initiator);
        let route = self.install_route(transfer_id.clone(), RouteRole::Sender, capability).await?;
        route.start().await.map_err(|_|crate::connection_diagnostics::connection_failure())?;
        let (selected_fence, p2p, dc) = route.wait_selected().await.map_err(|_|crate::connection_diagnostics::connection_failure())?;
        *self.p2p.lock().await = Some(p2p.clone());
        self.note(Phase::DataChannelOpen);
        self.observe_route(p2p.clone());
        // 3. Transfer.
        let app = self.app.clone();
        let session_id = self.id.clone();
        let tid = transfer_id.clone();
        let native = self.native.clone();
        let last_native_emit = std::sync::Mutex::new(Instant::now() - Duration::from_secs(1));
        let diagnostic_session = self.clone();
        let on_progress = move |sent: u64, total: u64| {
            if sent > 0 { diagnostic_session.note_transfer(&tid, Phase::FirstRealBytes); }
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

        let dc_send = dc.clone();
        let gate = route.clone();
        let result = sender.send_via_p2p(
            |frame| {
                if gate.is_terminal() || dc_send.state() != livekit::webrtc::data_channel::DataChannelState::Open {
                    return Err("选中路径已失败；本次传输不迁移路径".into());
                }
                // The only DC returned by wait_selected belongs to this immutable fence.
                let _ = selected_fence;
                dc_send.send(&frame, true).map_err(|e| format!("P2P send frame: {e}"))
            },
            || dc.buffered_amount(), on_progress,
        ).await;

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
        if result.is_err() { self.note_transfer(&transfer_id, Phase::TransferFailed); }
        self.snapshot_route().await;
        if self.native.is_none() { self.note_transfer(&transfer_id, Phase::TaskEnd); }
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
            if status.direction == "receiving" && !self.setup.active(Instant::now()) { return; }
            if !status.terminal() {
                status.stage = stage.into();
                emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
            }
        }
    }

    /// Commit local terminal status and reclaim resources before best-effort signaling.
    async fn fail_setup_timeout(&self) {
        self.cancelled.store(true, Ordering::Release);
        let route = self.route.lock().await.as_ref().map(|(_,r)|r.clone());
        if let Some(route)=route {route.close().await;}
        self.cancel_notify.notify_one();
        if let Some(native) = &self.native {
            native.cancel_source();
            let mut status = native.status.lock().await;
            if !status.terminal() {
                status.stage = "error".into();
                status.error = Some(crate::connection_diagnostics::connection_failure());
                emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() });
            }
        }
        let receivers: Vec<_> = self.receivers.lock().await.drain().map(|(_, receiver)| receiver).collect();
        for receiver in receivers { receiver.abort().await; }
        self.receiver_last_ack.lock().await.clear();
        if let Some(p2p) = self.p2p.lock().await.as_ref() { p2p.close(); }
        let task_id = self.native.as_ref().and_then(|n| n.status.try_lock().ok().map(|s| s.task_id.clone())).unwrap_or_default();
        if !task_id.is_empty() {
            let _ = tokio::time::timeout(Duration::from_millis(500), self.room.send(&FlingMessage::TaskCancel { task_id })).await;
        }
    }

    async fn fail_receive(&self, receiver: &Arc<OnlineReceiver>, error: &str) {
        let id = receiver.manifest().id.clone();
        self.note_transfer(&id, Phase::TransferFailed);
        self.snapshot_route().await;
        self.note_transfer(&id, Phase::TaskEnd);
        receiver.abort().await;
        let _ = self.room.send(&FlingMessage::FileChunkAck { route_fence: self.current_fence(&id).await, id: id.clone(), next_seq: 0, received_bytes: 0, ack_id: None, complete: None, failed: Some(true) }).await;
        self.receivers.lock().await.remove(&id);
        self.receiver_last_ack.lock().await.remove(&id);
        if let Some(native) = &self.native {
            let mut status = native.status.lock().await;
            if !status.terminal() { status.stage = "error".into(); status.error = Some(error.into()); emit(&self.app, &self.id, OnlineUiEvent::NativeTask { status: status.clone() }); }
        }
        emit(&self.app, &self.id, OnlineUiEvent::TransferDone { transfer_id: id, ok: false, error: Some(error.into()) });
    }

    /// Batch-source migration: TS still orchestrates the legacy task envelope, but
    /// ordinary source bytes, handles, hashes and file ACKs never cross the WebView.
    pub async fn send_registered_source(self: &Arc<Self>, lease: SourceLease) -> Result<String, String> {
        if self.cancelled.load(Ordering::Acquire) { return Err("已取消".into()); }
        let session = self.clone();
        let (manifest, file) = tokio::task::spawn_blocking(move || {
            let mut file = lease.file;
            let mut manifest = crate::online_transfer::build_manifest_handle(&mut file, lease.metadata.name, FileKind::File, &session.cancelled)?;
            if manifest.size != lease.metadata.size { return Err("文件大小已改变，请重新选择".to_string()); }
            manifest.path = lease.metadata.rel_path;
            Ok::<_, String>((manifest, file))
        }).await.map_err(|_| "源文件准备失败")??;
        if self.cancelled.load(Ordering::Acquire) { return Err("已取消".into()); }
        self.send_prepared(Arc::new(OnlineSender::from_handle(manifest, file))).await
    }
    pub async fn send_source(self: &Arc<Self>, lease: SourceLease) -> Result<NativeTaskStatus, String> {
        self.diagnostics.set_role(Role::Initiator);
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
        if result.is_err() {
            let route=self.route.lock().await.as_ref().map(|(_,r)|r.clone());
            if let Some(route)=route {route.close().await;}
            if let Some(p2p) = self.p2p.lock().await.take() { p2p.close(); }
            self.pending_ice.lock().await.clear();
            self.note(Phase::TransferFailed);
        }
        self.snapshot_route().await;
        self.note(Phase::TaskEnd);
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
        let route = self.route.lock().await.as_ref().map(|(_,r)|r.clone());
        if let Some(route)=&route {route.block_frames();}
        if error.is_some() { self.note(Phase::TransferFailed); } else { self.note(Phase::TaskCancelled); }
        self.snapshot_route().await;
        self.note(Phase::TaskEnd);
        if let Some(route)=route {route.close().await;}
        self.cancel_notify.notify_one();
        let receivers: Vec<_> = self.receivers.lock().await.drain().map(|(_, receiver)| receiver).collect();
        for receiver in receivers { receiver.abort().await; }
        for sender in self.senders.lock().await.values() { sender.on_failed(); }
        if let Some(p2p) = self.p2p.lock().await.as_ref() { p2p.close(); }
        self.note(Phase::ConnectionClosed);
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
        if !msg.webview_intent() {return Err("WebView 不得发送 Rust 所有的传输信令、帧或 ACK".into());}
        self.room.send(&msg).await
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        self.cancel_native(None, false).await?;
        if let Some(p2p) = self.p2p.lock().await.as_ref() {
            p2p.close();
        }
        self.note(Phase::ConnectionClosed);
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

#[cfg(test)]
mod connection_error_contract_tests {
    #[test]
    fn every_direct_first_ui_error_exit_uses_stable_mapping() {
        let source=include_str!("online_session.rs");
        assert!(source.contains("route.receive(signal).await.map_err(|_|crate::connection_diagnostics::connection_failure())?"));
        assert!(source.contains("route.start().await.map_err(|_|crate::connection_diagnostics::connection_failure())?"));
        assert!(source.contains("route.wait_selected().await.map_err(|_|crate::connection_diagnostics::connection_failure())?"));
        let branch=source.split("TransportEvent::Failed(_error) => {").nth(1).unwrap().split("TransportEvent::Started").next().unwrap();
        assert!(branch.contains("let error=crate::connection_diagnostics::connection_failure()"));
        assert!(branch.contains("cancel_native(Some(error),false)"));
    }
}
