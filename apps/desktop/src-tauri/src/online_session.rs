//! Online session orchestration: ties together the LiveKit room, the P2P
//! link, and the file transfer engine.
//!
//! This is the Rust replacement for the TypeScript `SendTask` / `RecvTask`
//! orchestration in `apps/desktop/src/tasks.ts` combined with `FlingRoom`.
//! The WebView only displays progress; all signaling and bytes flow here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tauri::{AppHandle, Emitter};
use tokio::sync::{mpsc, Mutex};

use crate::online_crypto::derive_room_key;
use crate::online_p2p::{default_ice_servers, OnlineP2p};
use crate::online_proto::{FileKind, FlingMessage};
use crate::online_room::{OnlineRoom, OnlineRoomEvent};
use crate::online_transfer::{build_manifest, chunk_ack_message, OnlineReceiver, OnlineSender};

/// How long to wait for a manifest ACK.
const MANIFEST_ACK_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// P2P link state changed.
    #[serde(rename = "p2p")]
    P2p { state: String },
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
}

fn emit(app: &AppHandle, session_id: &str, event: OnlineUiEvent) {
    let _ = app.emit(
        "fling://online/event",
        serde_json::json!({ "session": session_id, "event": event }),
    );
}

/// One online session: a LiveKit room plus its transfers.
pub struct OnlineSession {
    id: String,
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
    ) -> Result<Arc<Self>, String> {
        let room_key = derive_room_key(pairing_code, key_context);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let room = OnlineRoom::connect(server_url, token, room_key, event_tx).await?;
        let room = Arc::new(room);

        let session = Arc::new(Self {
            id: id.clone(),
            app: app.clone(),
            room,
            senders: Arc::new(Mutex::new(HashMap::new())),
            receivers: Arc::new(Mutex::new(HashMap::new())),
            receiver_last_ack: Arc::new(Mutex::new(HashMap::new())),
            p2p: Mutex::new(None),
            pending_ice: Mutex::new(Vec::new()),
            download_dir,
            manifest_acks: Arc::new(Mutex::new(HashMap::new())),
        });

        // Dispatch loop.
        let session_cb = session.clone();
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let Err(e) = session_cb.handle_room_event(event).await {
                    // Log and continue; a single bad message must not kill
                    // the session.
                    eprintln!("fling online session {}: {}", session_cb.id, e);
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

    async fn handle_room_event(&self, event: OnlineRoomEvent) -> Result<(), String> {
        match event {
            OnlineRoomEvent::Message { from, msg } => self.handle_message(from, msg).await,
            OnlineRoomEvent::PeerJoined(identity) => {
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::Peer { identity, joined: true },
                );
                Ok(())
            }
            OnlineRoomEvent::PeerLeft(identity) => {
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::Peer { identity, joined: false },
                );
                Ok(())
            }
            OnlineRoomEvent::Disconnected { reason } => {
                emit(&self.app, &self.id, OnlineUiEvent::Disconnected { reason });
                Ok(())
            }
            OnlineRoomEvent::ProtocolWarning { detail } => {
                eprintln!("fling online protocol warning: {detail}");
                Ok(())
            }
        }
    }

    async fn handle_message(&self, from: String, msg: FlingMessage) -> Result<(), String> {
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
            FlingMessage::FileManifestAck { id, .. } => {
                if let Some(notify) = self.manifest_acks.lock().await.get(&id) {
                    notify.notify_waiters();
                }
            }
            FlingMessage::FileChunk { id, seq, data } => {
                self.on_relay_chunk(id, seq, data).await?;
            }
            FlingMessage::FileChunkAck { id, next_seq, complete, failed, .. } => {
                if failed == Some(true) {
                    return Err(format!("receiver failed transfer {id}"));
                }
                if let Some(sender) = self.senders.lock().await.get(&id) {
                    sender.on_chunk_ack(next_seq);
                    if complete == Some(true) {
                        sender.on_complete();
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
                // Receiver confirmed persistence; complete any sender
                // waiting on this task.
                let _ = task_id;
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
                id,
                chunk_ack: Some(true),
                binary_chunks: Some(true),
                direct_frame_payload_bytes: Some(128 * 1024),
            })
            .await
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

    /// Send a cumulative `file-chunk-ack` when due, and finalize on complete.
    async fn maybe_ack(
        &self,
        receiver: &Arc<OnlineReceiver>,
        next_seq: u32,
    ) -> Result<(), String> {
        let id = receiver.manifest().id.clone();
        let last_ack = *self.receiver_last_ack.lock().await.get(&id).unwrap_or(&0);
        let received_bytes = receiver.received_bytes().await;
        let complete = receiver.check_complete().await?;
        if complete || receiver.should_ack(last_ack, next_seq) {
            self.receiver_last_ack.lock().await.insert(id.clone(), next_seq);
            self.room
                .send(&chunk_ack_message(receiver, next_seq, received_bytes, complete))
                .await?;
            if complete {
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::TransferDone {
                        transfer_id: id,
                        ok: true,
                        error: None,
                    },
                );
            } else {
                emit(
                    &self.app,
                    &self.id,
                    OnlineUiEvent::Progress {
                        transfer_id: id,
                        sent_bytes: received_bytes,
                        total_bytes: receiver.manifest().size,
                        direction: "receiving".to_string(),
                    },
                );
            }
        }
        Ok(())
    }

    /// Receiver: a P2P offer arrived. Become the responder.
    async fn on_p2p_offer(&self, sdp: String) -> Result<(), String> {
        // TODO: read TURN config; for now use STUN defaults.
        let ice_servers = default_ice_servers(None, None, None);
        let (p2p, ice_rx, answer_sdp) = OnlineP2p::accept_offer(ice_servers, &sdp).await?;
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
        let receivers = self.receivers.clone();
        let receiver_last_ack = self.receiver_last_ack.clone();
        tokio::spawn(async move {
            // Register the message handler BEFORE wait_open(): libwebrtc drops
            // messages received before on_message is set, so the handler must
            // be attached as soon as the DataChannel is created (via
            // on_data_channel), not after it opens.
            let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            p2p.set_message_handler(frame_tx);
            match p2p.wait_open().await {
                Ok(_dc) => {
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
                    while let Some(frame_bytes) = frame_rx.recv().await {
                        // Decode just the transfer id synchronously.
                        let id = match crate::native_lan::decode_direct_file_frame(&frame_bytes) {
                            Ok(Some(f)) => f.id,
                            _ => continue,
                        };
                        let receiver = match receivers.lock().await.get(&id).cloned() {
                            Some(r) => r,
                            None => continue,
                        };
                        let next_seq = match receiver.on_p2p_frame(&frame_bytes).await {
                            Ok(n) => n,
                            Err(e) => {
                                eprintln!("fling online P2P frame error: {e}");
                                continue;
                            }
                        };
                        // ACK + finalize, mirroring maybe_ack.
                        let last_ack =
                            *receiver_last_ack.lock().await.get(&id).unwrap_or(&0);
                        let received_bytes = receiver.received_bytes().await;
                        let complete = match receiver.check_complete().await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("fling online receive finalize error: {e}");
                                continue;
                            }
                        };
                        if complete || receiver.should_ack(last_ack, next_seq) {
                            receiver_last_ack.lock().await.insert(id.clone(), next_seq);
                            let _ = room
                                .send(&crate::online_transfer::chunk_ack_message(
                                    &receiver,
                                    next_seq,
                                    received_bytes,
                                    complete,
                                ))
                                .await;
                            if complete {
                                let _ = app.emit(
                                    "fling://online/event",
                                    serde_json::json!({
                                        "session": session_id,
                                        "event": {
                                            "kind": "transfer-done",
                                            "transfer_id": id,
                                            "ok": true,
                                        },
                                    }),
                                );
                            }
                        }
                    }
                    let _ = room;
                }
                Err(e) => {
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
        let transfer_id = manifest.id.clone();
        let sender = Arc::new(OnlineSender::new(manifest, path)?);
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

        // 2. P2P attempt (initiator).
        let ice_servers = default_ice_servers(None, None, None);
        let p2p_result = OnlineP2p::start_initiator(ice_servers).await;
        let use_p2p = match p2p_result {
            Ok((p2p, ice_rx, offer_sdp)) => {
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
                // Flush pending ICE.
                let pending: Vec<String> = std::mem::take(&mut *self.pending_ice.lock().await);
                for candidate in pending {
                    p2p.add_remote_ice(Some(candidate)).await?;
                }
                match p2p.wait_open().await {
                    Ok(dc) => {
                        emit(&self.app, &self.id, OnlineUiEvent::P2p { state: "open".to_string() });
                        Some((p2p, dc))
                    }
                    Err(e) => {
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
        let on_progress = move |sent: u64, total: u64| {
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
        result.map(|_| transfer_id)
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
