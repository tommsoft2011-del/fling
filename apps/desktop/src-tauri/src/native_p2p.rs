//! Experimental Rust-owned WebRTC data plane for the isolated v0.6.3 POC.
//!
//! LiveKit signaling remains in the existing WebView room (and therefore keeps
//! its current end-to-end encryption). This module only owns the native
//! RTCPeerConnection / DTLS-SCTP DataChannel and forwards the existing p2p-* SDP,
//! ICE, and FLNG binary frames. It does not introduce another transport.

use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::oneshot;
use webrtc::{
    api::{setting_engine::SettingEngine, APIBuilder},
    data::data_channel::DataChannel as DetachedDataChannel,
    data_channel::{data_channel_init::RTCDataChannelInit, RTCDataChannel},
    ice_transport::{
        ice_candidate::RTCIceCandidateInit, ice_credential_type::RTCIceCredentialType,
        ice_server::RTCIceServer,
    },
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription, RTCPeerConnection,
    },
};

const EVENT_NAME: &str = "fling-native-p2p";
const CHANNEL_LABEL: &str = "fling-file";
/// v0.6.3's largest legal coalesced binary FLNG frame (128 KiB + 30-byte header).
const MAX_FRAME_BYTES: usize = 128 * 1024 + 30;
/// Report a conservative value so the existing sender never coalesces beyond it.
const ADVERTISED_MAX_MESSAGE_SIZE: usize = 64 * 1024;

#[derive(Default)]
pub struct NativeP2pState {
    links: Mutex<HashMap<String, Arc<NativePeer>>>,
}

struct NativePeer {
    app: AppHandle,
    session_id: String,
    pc: Arc<RTCPeerConnection>,
    data_channel: tokio::sync::Mutex<Option<Arc<DetachedDataChannel>>>,
    receive_ack: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
    closed: AtomicBool,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct NativeP2pEvent {
    session_id: String,
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    signal: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_message_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl NativePeer {
    fn emit(
        &self,
        event: &str,
        signal: Option<Value>,
        data: Option<Vec<u8>>,
        error: Option<String>,
    ) {
        let payload = NativeP2pEvent {
            session_id: self.session_id.clone(),
            event: event.to_owned(),
            signal,
            data,
            max_message_size: (event == "open").then_some(ADVERTISED_MAX_MESSAGE_SIZE),
            error,
        };
        let _ = self.app.emit(EVENT_NAME, payload);
    }

    fn emit_signal(&self, signal: Value) {
        self.emit("signal", Some(signal), None, None);
    }
}

fn validate_session_id(session_id: &str) -> Result<(), String> {
    if session_id.len() != 32 || !session_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid native P2P session id".into());
    }
    Ok(())
}

fn get_peer(state: &NativeP2pState, session_id: &str) -> Result<Arc<NativePeer>, String> {
    validate_session_id(session_id)?;
    state
        .links
        .lock()
        .map_err(|e| format!("native P2P state lock: {e}"))?
        .get(session_id)
        .cloned()
        .ok_or_else(|| "native P2P session is not active".into())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IceServerInput {
    urls: Value,
    #[serde(default)]
    username: String,
    #[serde(default)]
    credential: String,
}

fn parse_ice_servers(input: Vec<IceServerInput>) -> Result<Vec<RTCIceServer>, String> {
    input
        .into_iter()
        .map(|entry| {
            let urls = match entry.urls {
                Value::String(url) => vec![url],
                Value::Array(values) => values
                    .into_iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| "ICE URL must be a string".to_owned())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err("ICE urls must be a string or string array".into()),
            };
            Ok(RTCIceServer {
                urls,
                username: entry.username,
                credential: entry.credential,
                credential_type: RTCIceCredentialType::Password,
            })
        })
        .collect()
}

fn wire_data_channel(peer: Arc<NativePeer>, channel: Arc<RTCDataChannel>) {
    let open_peer = peer.clone();
    let open_channel = channel.clone();
    channel.on_open(Box::new(move || {
        let peer = open_peer.clone();
        let channel = open_channel.clone();
        Box::pin(async move {
            match channel.detach().await {
                Ok(detached) => {
                    *peer.data_channel.lock().await = Some(detached.clone());
                    peer.emit("open", None, None, None);
                    tokio::spawn(read_data_channel(peer.clone(), detached));
                }
                Err(error) => {
                    peer.emit(
                        "failed",
                        None,
                        None,
                        Some(format!("DataChannel detach failed: {error}")),
                    );
                }
            }
        })
    }));

    let close_peer = peer.clone();
    channel.on_close(Box::new(move || {
        let peer = close_peer.clone();
        Box::pin(async move {
            if !peer.closed.load(Ordering::SeqCst) {
                peer.emit("closed", None, None, None);
            }
        })
    }));
}

async fn read_data_channel(peer: Arc<NativePeer>, channel: Arc<DetachedDataChannel>) {
    // One bounded buffer is sufficient for the v0.6.3 single-chunk and
    // coalesced-frame limits; no unbounded queue is introduced by the Rust side.
    let mut buffer = vec![0_u8; MAX_FRAME_BYTES];
    loop {
        if peer.closed.load(Ordering::SeqCst) {
            break;
        }
        match channel.read_data_channel(&mut buffer).await {
            Ok((0, _)) => break,
            Ok((n, is_string)) => {
                if is_string || n > MAX_FRAME_BYTES {
                    peer.emit(
                        "failed",
                        None,
                        None,
                        Some("invalid or oversized direct frame".into()),
                    );
                    break;
                }
                let (ack_tx, ack_rx) = oneshot::channel();
                *peer.receive_ack.lock().await = Some(ack_tx);
                peer.emit("message", None, Some(buffer[..n].to_vec()), None);
                match tokio::time::timeout(std::time::Duration::from_secs(15), ack_rx).await {
                    Ok(Ok(())) => {}
                    _ if peer.closed.load(Ordering::SeqCst) => break,
                    _ => {
                        peer.emit(
                            "failed",
                            None,
                            None,
                            Some("native receive handoff ACK timed out".into()),
                        );
                        break;
                    }
                }
            }
            Err(error) => {
                if !peer.closed.load(Ordering::SeqCst) {
                    peer.emit(
                        "failed",
                        None,
                        None,
                        Some(format!("DataChannel read failed: {error}")),
                    );
                }
                break;
            }
        }
    }
}

fn emit_local_candidate(
    peer: &NativePeer,
    candidate: Option<webrtc::ice_transport::ice_candidate::RTCIceCandidate>,
) {
    let candidate_json = match candidate {
        Some(candidate) => candidate
            .to_json()
            .ok()
            .and_then(|init| serde_json::to_string(&init).ok())
            .map(Value::String),
        None => Some(Value::Null),
    };
    if let Some(candidate) = candidate_json {
        peer.emit_signal(serde_json::json!({ "kind": "p2p-ice", "candidate": candidate }));
    }
}

#[tauri::command]
pub async fn native_p2p_start(
    app: AppHandle,
    state: State<'_, NativeP2pState>,
    session_id: String,
    role: String,
    ice_servers: Vec<IceServerInput>,
) -> Result<(), String> {
    validate_session_id(&session_id)?;
    if role != "initiator" && role != "responder" {
        return Err("invalid native P2P role".into());
    }
    if state
        .links
        .lock()
        .map_err(|e| e.to_string())?
        .contains_key(&session_id)
    {
        return Err("native P2P session already exists".into());
    }

    let mut setting = SettingEngine::default();
    // Required to receive the existing 32 KiB FLNG frames without the
    // webrtc-rs callback API's 16 KiB message-size limitation.
    setting.detach_data_channels();
    let api = APIBuilder::new().with_setting_engine(setting).build();
    let configuration = RTCConfiguration {
        ice_servers: parse_ice_servers(ice_servers)?,
        ..Default::default()
    };
    let pc = Arc::new(
        api.new_peer_connection(configuration)
            .await
            .map_err(|e| format!("create native RTCPeerConnection: {e}"))?,
    );
    let peer = Arc::new(NativePeer {
        app,
        session_id: session_id.clone(),
        pc: pc.clone(),
        data_channel: tokio::sync::Mutex::new(None),
        receive_ack: tokio::sync::Mutex::new(None),
        closed: AtomicBool::new(false),
    });

    let candidate_peer = peer.clone();
    pc.on_ice_candidate(Box::new(move |candidate| {
        let peer = candidate_peer.clone();
        Box::pin(async move {
            emit_local_candidate(&peer, candidate);
        })
    }));
    let state_peer = peer.clone();
    pc.on_peer_connection_state_change(Box::new(move |connection_state| {
        let peer = state_peer.clone();
        Box::pin(async move {
            if connection_state == RTCPeerConnectionState::Failed {
                peer.emit(
                    "failed",
                    None,
                    None,
                    Some("native ICE/DTLS peer connection failed".into()),
                );
            }
        })
    }));
    let incoming_peer = peer.clone();
    pc.on_data_channel(Box::new(move |channel| {
        let peer = incoming_peer.clone();
        Box::pin(async move {
            wire_data_channel(peer, channel);
        })
    }));

    state
        .links
        .lock()
        .map_err(|e| e.to_string())?
        .insert(session_id.clone(), peer.clone());

    if role == "initiator" {
        let channel = pc
            .create_data_channel(CHANNEL_LABEL, Some(RTCDataChannelInit::default()))
            .await
            .map_err(|e| format!("create native DataChannel: {e}"))?;
        wire_data_channel(peer.clone(), channel);
        let offer = pc
            .create_offer(None)
            .await
            .map_err(|e| format!("create native offer: {e}"))?;
        pc.set_local_description(offer)
            .await
            .map_err(|e| format!("set native local offer: {e}"))?;
        let local = pc
            .local_description()
            .await
            .ok_or_else(|| "native local offer missing".to_owned())?;
        peer.emit_signal(serde_json::json!({ "kind": "p2p-offer", "sdp": local.sdp }));
    }
    Ok(())
}

#[tauri::command]
pub async fn native_p2p_accept_offer(
    state: State<'_, NativeP2pState>,
    session_id: String,
    sdp: String,
) -> Result<(), String> {
    let peer = get_peer(state.inner(), &session_id)?;
    let offer =
        RTCSessionDescription::offer(sdp).map_err(|e| format!("parse native offer: {e}"))?;
    peer.pc
        .set_remote_description(offer)
        .await
        .map_err(|e| format!("set native remote offer: {e}"))?;
    let answer = peer
        .pc
        .create_answer(None)
        .await
        .map_err(|e| format!("create native answer: {e}"))?;
    peer.pc
        .set_local_description(answer)
        .await
        .map_err(|e| format!("set native local answer: {e}"))?;
    let local = peer
        .pc
        .local_description()
        .await
        .ok_or_else(|| "native local answer missing".to_owned())?;
    peer.emit_signal(serde_json::json!({ "kind": "p2p-answer", "sdp": local.sdp }));
    Ok(())
}

#[tauri::command]
pub async fn native_p2p_handle_signal(
    state: State<'_, NativeP2pState>,
    session_id: String,
    signal: Value,
) -> Result<(), String> {
    let peer = get_peer(state.inner(), &session_id)?;
    match signal.get("kind").and_then(Value::as_str) {
        Some("p2p-answer") => {
            let sdp = signal
                .get("sdp")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing answer SDP".to_owned())?;
            let answer = RTCSessionDescription::answer(sdp.to_owned())
                .map_err(|e| format!("parse native answer: {e}"))?;
            peer.pc
                .set_remote_description(answer)
                .await
                .map_err(|e| format!("set native remote answer: {e}"))?;
        }
        Some("p2p-ice") => match signal.get("candidate") {
            None | Some(Value::Null) => return Ok(()),
            Some(Value::String(raw)) => {
                let init: RTCIceCandidateInit = serde_json::from_str(raw)
                    .map_err(|e| format!("parse remote ICE candidate: {e}"))?;
                peer.pc
                    .add_ice_candidate(init)
                    .await
                    .map_err(|e| format!("add remote ICE candidate: {e}"))?;
            }
            _ => return Err("invalid remote ICE candidate".into()),
        },
        Some(kind) => return Err(format!("unsupported native P2P signal: {kind}")),
        None => return Err("missing native P2P signal kind".into()),
    }
    Ok(())
}

#[tauri::command]
pub async fn native_p2p_send(
    state: State<'_, NativeP2pState>,
    session_id: String,
    data: Vec<u8>,
) -> Result<usize, String> {
    if data.is_empty() || data.len() > MAX_FRAME_BYTES {
        return Err(format!("invalid direct frame size (max {MAX_FRAME_BYTES})"));
    }
    let peer = get_peer(state.inner(), &session_id)?;
    let channel = peer
        .data_channel
        .lock()
        .await
        .clone()
        .ok_or_else(|| "native DataChannel is not open".to_owned())?;
    channel
        .write_data_channel(&Bytes::from(data), false)
        .await
        .map_err(|e| format!("native DataChannel send failed: {e}"))
}

#[tauri::command]
pub async fn native_p2p_ack_receive(
    state: State<'_, NativeP2pState>,
    session_id: String,
) -> Result<(), String> {
    let peer = get_peer(state.inner(), &session_id)?;
    if let Some(ack) = peer.receive_ack.lock().await.take() {
        ack.send(())
            .map_err(|_| "native receive handoff is no longer waiting".to_owned())?;
    }
    Ok(())
}

#[tauri::command]
pub async fn native_p2p_close(
    state: State<'_, NativeP2pState>,
    session_id: String,
) -> Result<(), String> {
    let peer = {
        validate_session_id(&session_id)?;
        state
            .links
            .lock()
            .map_err(|e| e.to_string())?
            .remove(&session_id)
    };
    if let Some(peer) = peer {
        peer.closed.store(true, Ordering::SeqCst);
        if let Some(ack) = peer.receive_ack.lock().await.take() {
            let _ = ack.send(());
        }
        peer.pc
            .close()
            .await
            .map_err(|e| format!("close native peer connection: {e}"))?;
    }
    Ok(())
}
