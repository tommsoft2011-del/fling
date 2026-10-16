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

use crate::ReceivedFileStore;
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
    receive_store: ReceivedFileStore,
    files: tokio::sync::Mutex<HashMap<String, NativeFileBinding>>,
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
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_frame: Option<NativeFileFrame>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_message_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct NativeFileFrame {
    id: String,
    seq: u32,
    chunk_count: u32,
    next_seq: u32,
    received_bytes: u64,
}

#[derive(Clone)]
struct NativeFileBinding {
    save_session_id: String,
    chunk_size: usize,
    total_chunks: u32,
    expected_size: u64,
}

struct DecodedDirectFileFrame {
    id: String,
    seq: u32,
    chunk_count: u32,
    chunk_size: Option<usize>,
    payload_start: usize,
    payload_len: usize,
}

impl NativePeer {
    fn emit(
        &self,
        event: &str,
        signal: Option<Value>,
        text: Option<String>,
        file_frame: Option<NativeFileFrame>,
        error: Option<String>,
    ) {
        let payload = NativeP2pEvent {
            session_id: self.session_id.clone(),
            event: event.to_owned(),
            signal,
            text,
            file_frame,
            max_message_size: (event == "open").then_some(ADVERTISED_MAX_MESSAGE_SIZE),
            error,
        };
        let _ = self.app.emit(EVENT_NAME, payload);
    }

    fn emit_signal(&self, signal: Value) {
        self.emit("signal", Some(signal), None, None, None);
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
                    peer.emit("open", None, None, None, None);
                    tokio::spawn(read_data_channel(peer.clone(), detached));
                }
                Err(error) => {
                    peer.emit(
                        "failed",
                        None,
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
                peer.emit("closed", None, None, None, None);
            }
        })
    }));
}

fn decode_direct_file_frame(data: &[u8]) -> Result<Option<DecodedDirectFileFrame>, String> {
    if data.len() < 4 || &data[..4] != b"FLNG" {
        return Ok(None);
    }
    if data.len() < 26 || data.len() > MAX_FRAME_BYTES {
        return Err("truncated or oversized direct file frame".into());
    }
    if data[4] != 1 || (data[5] != 1 && data[5] != 2) {
        return Err("unsupported direct file frame".into());
    }
    let batch = data[5] == 2;
    let header_size = if batch { 30 } else { 26 };
    if data.len() < header_size {
        return Err("truncated direct file batch frame".into());
    }
    let seq = u32::from_be_bytes(data[18..22].try_into().unwrap());
    let payload_len = u32::from_be_bytes(data[22..26].try_into().unwrap()) as usize;
    if payload_len != data.len() - header_size {
        return Err("invalid direct file frame length".into());
    }
    let (chunk_size, chunk_count) = if batch {
        let size = u16::from_be_bytes(data[26..28].try_into().unwrap()) as usize;
        let count = u16::from_be_bytes(data[28..30].try_into().unwrap()) as u32;
        if size == 0
            || count < 2
            || seq.checked_add(count - 1).is_none()
            || payload_len <= (count as usize - 1) * size
            || payload_len > count as usize * size
        {
            return Err("invalid coalesced direct chunk metadata".into());
        }
        (Some(size), count)
    } else {
        (None, 1)
    };
    let mut id = String::with_capacity(24);
    for byte in &data[6..18] {
        use std::fmt::Write as _;
        let _ = write!(&mut id, "{byte:02x}");
    }
    Ok(Some(DecodedDirectFileFrame {
        id,
        seq,
        chunk_count,
        chunk_size,
        payload_start: header_size,
        payload_len,
    }))
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
            Ok((n, _is_string)) => {
                if n > MAX_FRAME_BYTES {
                    peer.emit(
                        "failed",
                        None,
                        None,
                        None,
                        Some("invalid or oversized direct frame".into()),
                    );
                    break;
                }
                let delivery = match decode_direct_file_frame(&buffer[..n]) {
                    Ok(Some(frame)) => {
                        let binding = peer.files.lock().await.get(&frame.id).cloned();
                        let Some(binding) = binding else {
                            peer.emit(
                                "failed",
                                None,
                                None,
                                None,
                                Some("direct file frame has no active manifest binding".into()),
                            );
                            break;
                        };
                        if frame.seq.checked_add(frame.chunk_count).is_none()
                            || frame.seq + frame.chunk_count > binding.total_chunks
                            || frame
                                .chunk_size
                                .is_some_and(|size| size != binding.chunk_size)
                        {
                            peer.emit(
                                "failed",
                                None,
                                None,
                                None,
                                Some("direct file frame does not match its manifest".into()),
                            );
                            break;
                        }
                        let mut payload_offset = frame.payload_start;
                        let payload_end = payload_offset + frame.payload_len;
                        let mut last_append = None;
                        let mut valid = true;
                        for index in 0..frame.chunk_count {
                            let seq = frame.seq + index;
                            let file_offset = seq as u64 * binding.chunk_size as u64;
                            let expected_len = std::cmp::min(
                                binding.chunk_size as u64,
                                binding.expected_size.saturating_sub(file_offset),
                            ) as usize;
                            if payload_offset + expected_len > payload_end {
                                valid = false;
                                break;
                            }
                            match peer.receive_store.append_chunk(
                                &binding.save_session_id,
                                seq,
                                &buffer[payload_offset..payload_offset + expected_len],
                            ) {
                                Ok(result) => {
                                    payload_offset += expected_len;
                                    last_append = Some(result);
                                }
                                Err(error) => {
                                    peer.emit(
                                        "failed",
                                        None,
                                        None,
                                        None,
                                        Some(format!("native file append failed: {error}")),
                                    );
                                    valid = false;
                                    break;
                                }
                            }
                        }
                        if !valid || payload_offset != payload_end {
                            if valid {
                                peer.emit(
                                    "failed",
                                    None,
                                    None,
                                    None,
                                    Some(
                                        "direct file frame payload does not match chunk lengths"
                                            .into(),
                                    ),
                                );
                            }
                            break;
                        }
                        let Some(append) = last_append else {
                            peer.emit(
                                "failed",
                                None,
                                None,
                                None,
                                Some("direct file frame contained no chunks".into()),
                            );
                            break;
                        };
                        Some(NativeFileFrame {
                            id: frame.id,
                            seq: frame.seq,
                            chunk_count: frame.chunk_count,
                            next_seq: append.next_seq,
                            received_bytes: append.cumulative_bytes,
                        })
                    }
                    Ok(None) => {
                        let text = match std::str::from_utf8(&buffer[..n]) {
                            Ok(text) => text.to_owned(),
                            Err(_) => {
                                peer.emit(
                                    "failed",
                                    None,
                                    None,
                                    None,
                                    Some("invalid UTF-8 control message".into()),
                                );
                                break;
                            }
                        };
                        let (ack_tx, ack_rx) = oneshot::channel();
                        *peer.receive_ack.lock().await = Some(ack_tx);
                        peer.emit("message", None, Some(text), None, None);
                        match tokio::time::timeout(std::time::Duration::from_secs(15), ack_rx).await
                        {
                            Ok(Ok(())) => continue,
                            _ if peer.closed.load(Ordering::SeqCst) => break,
                            _ => {
                                peer.emit(
                                    "failed",
                                    None,
                                    None,
                                    None,
                                    Some("native receive handoff ACK timed out".into()),
                                );
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        peer.emit("failed", None, None, None, Some(error));
                        break;
                    }
                };
                if let Some(file_frame) = delivery {
                    let (ack_tx, ack_rx) = oneshot::channel();
                    *peer.receive_ack.lock().await = Some(ack_tx);
                    peer.emit("file-frame", None, None, Some(file_frame), None);
                    match tokio::time::timeout(std::time::Duration::from_secs(15), ack_rx).await {
                        Ok(Ok(())) => {}
                        _ if peer.closed.load(Ordering::SeqCst) => break,
                        _ => {
                            peer.emit(
                                "failed",
                                None,
                                None,
                                None,
                                Some("native receive handoff ACK timed out".into()),
                            );
                            break;
                        }
                    }
                }
            }
            Err(error) => {
                if !peer.closed.load(Ordering::SeqCst) {
                    peer.emit(
                        "failed",
                        None,
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
    receive_store: State<'_, ReceivedFileStore>,
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
        receive_store: receive_store.inner().clone(),
        files: tokio::sync::Mutex::new(HashMap::new()),
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
pub async fn native_p2p_bind_file(
    state: State<'_, NativeP2pState>,
    session_id: String,
    transfer_id: String,
    save_session_id: String,
    chunk_size: usize,
    total_chunks: u32,
    expected_size: u64,
) -> Result<(), String> {
    let peer = get_peer(state.inner(), &session_id)?;
    if transfer_id.len() != 24 || !transfer_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid native transfer id".into());
    }
    validate_session_id(&save_session_id)?;
    if chunk_size == 0 || chunk_size > 128 * 1024 || total_chunks == 0 {
        return Err("invalid native receive manifest".into());
    }
    peer.files.lock().await.insert(
        transfer_id,
        NativeFileBinding {
            save_session_id,
            chunk_size,
            total_chunks,
            expected_size,
        },
    );
    Ok(())
}

#[tauri::command]
pub async fn native_p2p_unbind_file(
    state: State<'_, NativeP2pState>,
    session_id: String,
    transfer_id: String,
) -> Result<(), String> {
    let peer = get_peer(state.inner(), &session_id)?;
    peer.files.lock().await.remove(&transfer_id);
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
    request: tauri::ipc::Request<'_>,
) -> Result<usize, String> {
    let session_id = request
        .headers()
        .get("x-session-id")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "missing x-session-id header".to_string())?;
    validate_session_id(session_id)?;
    let data = match request.body() {
        tauri::ipc::InvokeBody::Raw(data) => data.clone(),
        tauri::ipc::InvokeBody::Json(_) => {
            return Err("native P2P send requires raw binary IPC".into())
        }
    };
    if data.is_empty() || data.len() > MAX_FRAME_BYTES {
        return Err(format!("invalid direct frame size (max {MAX_FRAME_BYTES})"));
    }
    let peer = get_peer(state.inner(), session_id)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(id: [u8; 12], seq: u32, payload: &[u8], batch: Option<(u16, u16)>) -> Vec<u8> {
        let header_size = if batch.is_some() { 30 } else { 26 };
        let mut bytes = vec![0u8; header_size + payload.len()];
        bytes[..4].copy_from_slice(b"FLNG");
        bytes[4] = 1;
        bytes[5] = if batch.is_some() { 2 } else { 1 };
        bytes[6..18].copy_from_slice(&id);
        bytes[18..22].copy_from_slice(&seq.to_be_bytes());
        bytes[22..26].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        if let Some((chunk_size, chunk_count)) = batch {
            bytes[26..28].copy_from_slice(&chunk_size.to_be_bytes());
            bytes[28..30].copy_from_slice(&chunk_count.to_be_bytes());
        }
        bytes[header_size..].copy_from_slice(payload);
        bytes
    }

    #[test]
    fn direct_file_frame_parser_handles_single_batch_and_rejects_corruption() {
        let id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let single = frame(id, 7, b"chunk", None);
        let decoded = decode_direct_file_frame(&single).unwrap().unwrap();
        assert_eq!(decoded.id, "000102030405060708090a0b");
        assert_eq!(decoded.seq, 7);
        assert_eq!(decoded.chunk_count, 1);
        assert_eq!(
            &single[decoded.payload_start..decoded.payload_start + decoded.payload_len],
            b"chunk"
        );

        let batch = frame(id, 12, b"1234567", Some((4, 2)));
        let decoded = decode_direct_file_frame(&batch).unwrap().unwrap();
        assert_eq!(decoded.seq, 12);
        assert_eq!(decoded.chunk_size, Some(4));
        assert_eq!(decoded.chunk_count, 2);
        assert_eq!(decoded.payload_len, 7);

        assert!(decode_direct_file_frame(b"control message")
            .unwrap()
            .is_none());
        let mut corrupt = single.clone();
        corrupt[22..26].copy_from_slice(&999u32.to_be_bytes());
        assert!(decode_direct_file_frame(&corrupt).is_err());
        let mut invalid_batch = frame(id, 12, b"1234", Some((4, 2)));
        assert!(decode_direct_file_frame(&invalid_batch).is_err());
        invalid_batch.truncate(25);
        assert!(decode_direct_file_frame(&invalid_batch).is_err());
    }
}
