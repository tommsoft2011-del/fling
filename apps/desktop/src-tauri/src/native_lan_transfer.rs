use super::{
    emit_lan_event, enqueue_ordered_control, is_local_network_address, task_event_with_metadata,
    try_enqueue_ordered_control, NativeLanRuntime, NativeLanSession, NativeSource,
    NATIVE_CHUNK_SIZE,
};
use crate::native_lan::{self, NativeControlMessage, NativeFileManifest};
use crate::transport_diagnostics::{CandidateStep as Cs, Outcome as To, Phase as Tp, Reason as Tr};
use crate::{
    ensure_config,
    pairing_diagnostics::{
        CandidateCategory, CandidateFamily, CandidateRejection, CandidateTransport, FunnelEvent,
    },
    ReceivedFileStore,
};
use livekit::webrtc::{
    data_channel::{DataChannel, DataChannelInit, DataChannelState},
    ice_candidate::IceCandidate,
    peer_connection::{AnswerOptions, IceConnectionState, OfferOptions, PeerConnection},
    peer_connection_factory::{
        ContinualGatheringPolicy, IceTransportsType, PeerConnectionFactory, RtcConfiguration,
    },
    session_description::{SdpType, SessionDescription},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tauri::{AppHandle, Manager};
use tokio::sync::{mpsc as async_mpsc, watch};
use tokio::time::timeout;

const DIRECT_CHANNEL_LABEL: &str = "fling-file";
const ACK_INTERVAL_CHUNKS: u32 = 8;
// 8 MiB in-flight window with 32 KiB chunks (was 16 chunks / 512 KiB).
const SENDER_WINDOW_CHUNKS: u32 = 256;
// Max chunks coalesced into one FLNG batch frame (4 x 32 KiB = 128 KiB payload ceiling).
const BATCH_CHUNKS_PER_FRAME: u32 = 4;
const MAX_BUFFERED_BYTES: u64 = 256 * 1024;
const MAX_ACK_RECOVERY_RETRIES: u8 = 3;
// 64 frames x 128 KiB batch frames = up to 8 MiB of shock absorption between the
// WebRTC network thread and the disk writer. Raised from 8 because the faster
// sender (8 MiB window, coalesced frames, event-driven backpressure) can burst
// faster than a single disk write drains; a full queue aborts the session.
const RECEIVE_QUEUE_FRAMES: usize = 64;
const ACK_TIMEOUT: Duration = Duration::from_secs(30);
const CHANNEL_OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const PROGRESS_MIN_BYTES: u64 = 1024 * 1024;
const PROGRESS_MIN_INTERVAL: Duration = Duration::from_millis(500);
const MAX_PENDING_ICE: usize = 128;
const MAX_REMOTE_ICE_INPUTS: u16 = 256;
const MAX_CANDIDATE_MISMATCHES: u8 = 16;
const CANDIDATE_MISMATCH_TIMEOUT: Duration = Duration::from_secs(30);
#[derive(Default)]
struct CandidateMismatchBudget {
    rejected: u8,
    added: bool,
    deadline: Option<tokio::time::Instant>,
}
impl CandidateMismatchBudget {
    fn reject(&mut self) -> Result<(), String> {
        self.rejected = self
            .rejected
            .saturating_add(1)
            .min(MAX_CANDIDATE_MISMATCHES);
        if !self.added && self.deadline.is_none() {
            self.deadline = Some(tokio::time::Instant::now() + CANDIDATE_MISMATCH_TIMEOUT);
        }
        if self.rejected >= MAX_CANDIDATE_MISMATCHES {
            return Err("LAN ICE endpoint mismatch budget exhausted".into());
        }
        Ok(())
    }
    fn added(&mut self) {
        self.added = true;
        self.deadline = None;
    }
}
pub(super) fn candidate_mismatch_deadline(
    session: &NativeLanSession,
) -> Result<Option<tokio::time::Instant>, String> {
    session
        .transfer
        .lock()
        .map(|transfer| transfer.candidate_mismatch.deadline)
        .map_err(|_| "LAN transfer state lock".into())
}
pub(super) struct TransferState {
    pc: Option<PeerConnection>,
    data_channel: Option<DataChannel>,
    pending_remote_ice: Vec<String>,
    candidate_mismatch: CandidateMismatchBudget,
    remote_ice_inputs: u16,
    remote_description_set: bool,
    pending_manifest: Option<NativeFileManifest>,
    outgoing_manifest: Option<NativeFileManifest>,
    seen_transfer_nonces: HashSet<String>,
    incoming: Option<IncomingTransfer>,
    last_sender_progress_bytes: u64,
    last_sender_progress_at: Instant,
}

impl Default for TransferState {
    fn default() -> Self {
        Self {
            pc: None,
            data_channel: None,
            pending_remote_ice: Vec::new(),
            candidate_mismatch: CandidateMismatchBudget::default(),
            remote_ice_inputs: 0,
            remote_description_set: false,
            pending_manifest: None,
            outgoing_manifest: None,
            seen_transfer_nonces: HashSet::new(),
            incoming: None,
            last_sender_progress_bytes: 0,
            last_sender_progress_at: Instant::now(),
        }
    }
}

fn claim_transfer_nonce(used: &mut HashSet<String>, nonce: &str) -> Result<(), String> {
    if used.contains(nonce) {
        return Err("file manifest transfer nonce replay rejected".to_string());
    }
    if used.len() >= 128 {
        return Err("LAN transfer nonce budget exhausted; re-pair required".to_string());
    }
    used.insert(nonce.to_string());
    Ok(())
}

fn fresh_transfer_nonce(used: &mut HashSet<String>) -> Result<String, String> {
    for _ in 0..8 {
        let nonce = super::random_hex_id();
        if !used.contains(&nonce) {
            claim_transfer_nonce(used, &nonce)?;
            return Ok(nonce);
        }
    }
    Err("cannot allocate a fresh transfer nonce".to_string())
}

fn outgoing_transfer_id(session: &NativeLanSession) -> Result<String, String> {
    session
        .transfer
        .lock()
        .map_err(|_| "LAN transfer state lock".to_string())?
        .outgoing_manifest
        .as_ref()
        .map(|manifest| manifest.id.clone())
        .ok_or_else(|| "sender manifest state missing".to_string())
}

fn progress_event(
    session: &NativeLanSession,
    phase: &str,
    received_bytes: u64,
    saved_bytes: u64,
    total_bytes: u64,
    speed_bps: u64,
    name: Option<String>,
) -> super::LanTaskEvent {
    let mut event = task_event_with_metadata(
        session,
        phase,
        received_bytes,
        saved_bytes,
        total_bytes,
        None,
        name,
    );
    event.speed_bps = speed_bps;
    event
}

struct IncomingTransfer {
    manifest: NativeFileManifest,
    store: ReceivedFileStore,
    store_session_id: String,
    next_seq: u32,
    received_bytes: u64,
    last_ack_seq: u32,
    last_progress_bytes: u64,
    last_progress_at: Instant,
    complete: bool,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct IncomingManifestEvent {
    session_id: String,
    task_id: String,
    peer_device_label: String,
    name: String,
    size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IceCandidateInit {
    candidate: String,
    sdp_mid: Option<String>,
    #[serde(rename = "sdpMLineIndex")]
    sdp_mline_index: Option<i32>,
    #[serde(default)]
    username_fragment: Option<String>,
}

pub(super) fn start_data_channel_router(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
    mut channels: async_mpsc::UnboundedReceiver<DataChannel>,
) {
    tauri::async_runtime::spawn(async move {
        while let Some(channel) = channels.recv().await {
            if channel.label() != DIRECT_CHANNEL_LABEL {
                channel.close();
                continue;
            }
            if let Err(error) =
                install_data_channel(app.clone(), runtime.clone(), session.clone(), channel).await
            {
                let kind = if error.contains("closed") {
                    super::LanFailureKind::Close
                } else {
                    super::LanFailureKind::Error
                };
                super::interrupt_session(&app, &runtime, &session, error, kind);
                return;
            }
        }
    });
}

fn initialize_peer(
    session: &NativeLanSession,
    install_callbacks: impl FnOnce(&PeerConnection),
) -> Result<Option<(PeerConnectionFactory, PeerConnection)>, String> {
    if session
        .webrtc_started
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .is_err()
    {
        return Ok(None);
    }
    let mut config = RtcConfiguration::default();
    config.ice_servers.clear(); // Offline LAN mode never uses STUN/TURN or LiveKit relay.
    config.continual_gathering_policy = ContinualGatheringPolicy::GatherOnce;
    config.ice_transport_type = IceTransportsType::All; // host candidates only: no ICE servers are configured.
    let factory = PeerConnectionFactory::default();
    session.diagnostic.record(FunnelEvent::PeerConnectionStart);
    let peer = match factory.create_peer_connection(config) {
        Ok(peer) => {
            session.diagnostic.record(FunnelEvent::PeerConnectionOk);
            peer
        }
        Err(error) => {
            session.diagnostic.record(FunnelEvent::PeerConnectionError);
            return Err(format!("create native LAN PeerConnection: {error}"));
        }
    };
    install_callbacks(&peer);
    {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        if let Some(previous) = transfer.pc.replace(peer.clone()) {
            previous.close();
        }
        transfer.remote_description_set = false;
        session
            .diagnostic
            .count(Cs::PendingAtPeerInstall, transfer.pending_remote_ice.len());
        session.diagnostic.phase(
            Tp::PeerInstall,
            To::Ok,
            Tr::None,
            transfer.pending_remote_ice.len(),
        );
        transfer.pending_remote_ice.clear();
    }
    Ok(Some((factory, peer)))
}

pub(super) async fn on_pair_confirmed(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &Arc<NativeLanSession>,
) -> Result<(), String> {
    let Some((_factory, peer)) = initialize_peer(session, |peer| {
        install_peer_callbacks(app.clone(), runtime.clone(), session.clone(), peer)
    })?
    else {
        return Ok(());
    };
    emit_lan_event(
        app,
        runtime,
        "fling://lan/progress",
        task_event_with_metadata(session, "connecting", 0, 0, 0, None, None),
    );
    begin_offer(session, &peer).await
}
async fn begin_offer(session: &NativeLanSession, peer: &PeerConnection) -> Result<(), String> {
    if session.role == super::PairingRole::Initiator {
        let data_channel = peer
            .create_data_channel(DIRECT_CHANNEL_LABEL, DataChannelInit::default())
            .map_err(|error| {
                session.diagnostic.record(FunnelEvent::DataChannelError);
                format!("create native LAN DataChannel: {error}")
            })?;
        session.diagnostic.record(FunnelEvent::DataChannelCreated);
        session
            .data_channel_tx
            .send(data_channel)
            .map_err(|_| "LAN DataChannel router is closed".to_string())?;
        let offer = peer
            .create_offer(OfferOptions::default())
            .await
            .map_err(|error| {
                session.diagnostic.record(FunnelEvent::OfferError);
                format!("create native LAN offer: {error}")
            })?;
        session.diagnostic.record(FunnelEvent::OfferGenerated);
        peer.set_local_description(offer.clone())
            .await
            .map_err(|error| {
                session.diagnostic.record(FunnelEvent::OfferError);
                format!("set native LAN local offer: {error}")
            })?;
        let sdp = signaling_sdp(&offer.to_string());
        send_authenticated_control(session, NativeControlMessage::P2pOffer { sdp }).await?;
    }
    Ok(())
}

fn observe_gathering_state(
    diagnostic: &crate::pairing_diagnostics::FunnelTrace,
    state: livekit::webrtc::peer_connection::IceGatheringState,
) {
    use livekit::webrtc::peer_connection::IceGatheringState;
    diagnostic.transition(
        Tp::IceGathering,
        match state {
            IceGatheringState::New => To::New,
            IceGatheringState::Gathering => To::Gathering,
            IceGatheringState::Complete => To::Complete,
        },
    );
}
fn observe_ice_state(
    diagnostic: &crate::pairing_diagnostics::FunnelTrace,
    state: IceConnectionState,
) {
    diagnostic.transition(
        Tp::IceConnection,
        match state {
            IceConnectionState::New => To::New,
            IceConnectionState::Checking => To::Checking,
            IceConnectionState::Connected => To::Connected,
            IceConnectionState::Completed => To::Completed,
            IceConnectionState::Failed => To::Failed,
            IceConnectionState::Disconnected => To::Disconnected,
            IceConnectionState::Closed => To::Closed,
            _ => To::Unknown,
        },
    );
}
fn install_peer_callbacks(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
    peer: &PeerConnection,
) {
    let dc_tx = session.data_channel_tx.clone();
    let dc_diag = session.diagnostic.clone();
    peer.on_data_channel(Some(Box::new(move |channel| {
        dc_diag.record(FunnelEvent::DataChannelReceived);
        let _ = dc_tx.send(channel);
    })));

    let candidate_session = session.clone();
    let candidate_app = app.clone();
    let candidate_runtime = runtime.clone();
    let gathering_session = session.clone();
    peer.on_ice_gathering_state_change(Some(Box::new(move |state| {
        use livekit::webrtc::peer_connection::IceGatheringState;
        observe_gathering_state(&gathering_session.diagnostic, state);
        match state {
            IceGatheringState::New => gathering_session
                .diagnostic
                .record(FunnelEvent::IceGatheringNew),
            IceGatheringState::Gathering => gathering_session
                .diagnostic
                .record(FunnelEvent::IceGatheringGathering),
            IceGatheringState::Complete => {
                gathering_session
                    .diagnostic
                    .record(FunnelEvent::IceGatheringComplete);
                gathering_session.diagnostic.flush_candidates();
                gathering_session.diagnostic.snapshot();
            }
            _ => {}
        }
    })));
    peer.on_ice_candidate(Some(Box::new(move |candidate| {
        candidate_session.diagnostic.count(Cs::Generated, 1);
        let candidate_line = candidate.candidate();
        let parsed = parse_candidate_summary(&candidate_line);
        let (family, transport, category, rejection) = parsed;
        candidate_session
            .diagnostic
            .candidate(family, transport, category, rejection);
        if !is_local_host_candidate(&candidate_line) {
            candidate_session.diagnostic.count(
                match rejection {
                    CandidateRejection::NonHost => Cs::FilteredNonHost,
                    CandidateRejection::NonLocal => Cs::FilteredNonLocal,
                    _ => Cs::FilteredMalformed,
                },
                1,
            );
            return;
        }
        let payload = IceCandidateInit {
            candidate: candidate_line,
            sdp_mid: Some(candidate.sdp_mid()),
            sdp_mline_index: Some(candidate.sdp_mline_index()),
            username_fragment: None,
        };
        let Ok(candidate_json) = serde_json::to_string(&payload) else {
            candidate_session
                .diagnostic
                .terminal(Tr::CandidatePayloadMalformed);
            let _ = candidate_session.cancel_tx.send(true);
            return;
        };
        let session_for_send = candidate_session.clone();
        let app_for_send = candidate_app.clone();
        let runtime_for_send = candidate_runtime.clone();
        tauri::async_runtime::spawn(async move {
            if let Err(error) = send_authenticated_control(
                &session_for_send,
                NativeControlMessage::P2pIce {
                    candidate: Some(candidate_json),
                },
            )
            .await
            {
                eprintln!(
                    "{}",
                    super::safe_failure_log("local-candidate-queue", &error)
                );
                super::interrupt_session(
                    &app_for_send,
                    &runtime_for_send,
                    &session_for_send,
                    error,
                    super::LanFailureKind::Error,
                );
            }
        });
    })));

    let state_app = app.clone();
    let state_runtime = runtime.clone();
    let state_session = session.clone();
    peer.on_ice_connection_state_change(Some(Box::new(move |state| {
        observe_ice_state(&state_session.diagnostic, state);
        match state {
            IceConnectionState::New => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionNew),
            IceConnectionState::Checking => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionChecking),
            IceConnectionState::Connected => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionConnected),
            IceConnectionState::Completed => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionCompleted),
            IceConnectionState::Failed => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionFailed),
            IceConnectionState::Disconnected => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionDisconnected),
            IceConnectionState::Closed => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionClosed),
            _ => state_session
                .diagnostic
                .record(FunnelEvent::IceConnectionUnknown),
        }
        if state_session
            .completed
            .load(std::sync::atomic::Ordering::Acquire)
            || *state_session.cancel_tx.borrow()
        {
            return;
        }
        eprintln!("[fling] LAN ICE state: {state:?}");
        let (error, kind) = match state {
            IceConnectionState::Failed => (
                "LAN host-candidate ICE failed",
                super::LanFailureKind::Error,
            ),
            IceConnectionState::Disconnected | IceConnectionState::Closed => (
                "LAN ICE connection interrupted",
                super::LanFailureKind::Close,
            ),
            _ => return,
        };
        let app = state_app.clone();
        let runtime = state_runtime.clone();
        let session = state_session.clone();
        tauri::async_runtime::spawn(async move {
            super::interrupt_session(&app, &runtime, &session, error.to_string(), kind);
        });
    })));
    observe_gathering_state(&session.diagnostic, peer.ice_gathering_state());
    observe_ice_state(&session.diagnostic, peer.ice_connection_state());
}

// Production and native-stack tests use this exact dispatch/handler boundary.
// UI/manifest events remain in the AppHandle wrapper; this is not a GUI E2E harness.
async fn route_transport_control(
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    message: NativeControlMessage,
) -> Result<Option<NativeControlMessage>, String> {
    match message {
        NativeControlMessage::P2pOffer { sdp } => handle_offer(session, sdp).await?,
        NativeControlMessage::P2pAnswer { sdp } => handle_answer(session, sdp).await?,
        NativeControlMessage::P2pIce { candidate } => {
            // Count every remote ICE input, including duplicate/matched candidates
            // after remote SDP and empty end-of-candidates frames. Never reset.
            let exhausted = {
                let mut transfer = session
                    .transfer
                    .lock()
                    .map_err(|_| "LAN transfer state lock".to_string())?;
                if transfer.remote_ice_inputs >= MAX_REMOTE_ICE_INPUTS {
                    true
                } else {
                    transfer.remote_ice_inputs += 1;
                    false
                }
            };
            if exhausted {
                session.diagnostic.terminal(Tr::CandidateInputBudget);
                return Err("LAN remote ICE input budget exhausted".into());
            }
            if let Some(candidate) = candidate {
                receive_ice_candidate(runtime, session, candidate).await?;
            }
        }
        message => return Ok(Some(message)),
    }
    Ok(None)
}
pub(super) async fn handle_authenticated_control(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &Arc<NativeLanSession>,
    message: NativeControlMessage,
) -> Result<(), String> {
    let Some(message) = route_transport_control(runtime, session, message).await? else {
        return Ok(());
    };
    match message {
        NativeControlMessage::P2pOffer { .. }
        | NativeControlMessage::P2pAnswer { .. }
        | NativeControlMessage::P2pIce { .. } => unreachable!("transport lane already dispatched"),
        NativeControlMessage::FileManifest { manifest } => {
            receive_manifest(app, runtime, session, manifest)
        }
        NativeControlMessage::FileManifestAck {
            id,
            chunk_ack,
            binary_chunks,
            direct_frame_payload_bytes,
        } => {
            if session.role != super::PairingRole::Initiator
                || id != outgoing_transfer_id(session)?
                || chunk_ack != Some(true)
                || binary_chunks != Some(true)
                || direct_frame_payload_bytes.unwrap_or_default() < NATIVE_CHUNK_SIZE as u32
            {
                return Err("recipient did not accept the native FLNG frame contract".to_string());
            }
            session.diagnostic.record(FunnelEvent::ManifestPeerAccepted);
            start_sender(app.clone(), runtime.clone(), session.clone()).await
        }
        NativeControlMessage::FileChunkAck {
            id,
            next_seq,
            received_bytes,
            saved_bytes,
            complete,
            failed,
        } => {
            handle_chunk_ack(
                app,
                runtime,
                session,
                id,
                next_seq,
                received_bytes,
                saved_bytes,
                complete,
                failed,
            )
            .await
        }
        NativeControlMessage::TaskAck { task_id } => {
            if session.role != super::PairingRole::Responder || task_id != session.task_id {
                return Err("invalid final task acknowledgement".to_string());
            }
            session
                .completed
                .store(true, std::sync::atomic::Ordering::Release);
            session.diagnostic.terminal(Tr::Completed);
            schedule_session_cleanup(runtime.clone(), session.clone());
            Ok(())
        }
        NativeControlMessage::TaskCancel { task_id } => {
            if task_id != session.task_id {
                return Err("task cancellation binding mismatch".to_string());
            }
            session.diagnostic.terminal(Tr::PeerCancel);
            Err("peer cancelled the transfer".to_string())
        }
        NativeControlMessage::TaskProgress { .. } => Ok(()),
        NativeControlMessage::PairConfirm { .. } | NativeControlMessage::PairReject { .. } => {
            Err("pairing control message received after authentication".to_string())
        }
    }
}

// This helper observes the exact Result returned by production handlers; it
// never serializes the error and never changes the Result or cleanup policy.
fn observed<T>(
    session: &NativeLanSession,
    phase: Tp,
    reason: Tr,
    result: Result<T, String>,
) -> Result<T, String> {
    match &result {
        Ok(_) => session.diagnostic.phase(phase, To::Ok, Tr::None, 0),
        Err(_) => {
            session.diagnostic.phase(phase, To::Error, reason, 0);
            session.diagnostic.terminal(reason);
        }
    }
    result
}
fn validate_description(session: &NativeLanSession, sdp: &str, answer: bool) -> Result<(), String> {
    let phase = if answer {
        Tp::AnswerValidate
    } else {
        Tp::OfferValidate
    };
    session.diagnostic.phase(phase, To::Start, Tr::None, 0);
    let expected = if answer {
        super::PairingRole::Initiator
    } else {
        super::PairingRole::Responder
    };
    if session.role != expected {
        return observed(
            session,
            phase,
            Tr::UnexpectedRole,
            Err("unexpected native LAN description role".to_string()),
        );
    }
    observed(
        session,
        phase,
        Tr::SdpPolicy,
        ensure_signaling_sdp_is_candidate_free(sdp),
    )
}
async fn apply_description(
    session: &NativeLanSession,
    sdp: &str,
    answer: bool,
) -> Result<PeerConnection, String> {
    validate_description(session, sdp, answer)?;
    let lookup = if answer {
        Tp::AnswerPeerLookup
    } else {
        Tp::OfferPeerLookup
    };
    session.diagnostic.phase(lookup, To::Start, Tr::None, 0);
    let peer = observed(session, lookup, Tr::PeerNotReady, current_peer(session))?;
    let parse = if answer {
        Tp::AnswerParse
    } else {
        Tp::OfferParse
    };
    session.diagnostic.phase(parse, To::Start, Tr::None, 0);
    let description = observed(
        session,
        parse,
        Tr::SdpParse,
        SessionDescription::parse(
            sdp,
            if answer {
                SdpType::Answer
            } else {
                SdpType::Offer
            },
        )
        .map_err(|error| format!("parse native LAN description: {error}")),
    )?;
    let remote = if answer {
        Tp::AnswerSetRemote
    } else {
        Tp::OfferSetRemote
    };
    session.diagnostic.phase(remote, To::Start, Tr::None, 0);
    session
        .diagnostic
        .phase(Tp::SetRemoteDescription, To::Start, Tr::None, 0);
    let applied = peer
        .set_remote_description(description)
        .await
        .map_err(|error| format!("set native LAN remote description: {error}"));
    let applied = observed(session, Tp::SetRemoteDescription, Tr::SetRemote, applied);
    observed(session, remote, Tr::SetRemote, applied)?;
    mark_remote_description_set(session).await?;
    Ok(peer)
}
async fn handle_offer(session: &NativeLanSession, sdp: String) -> Result<(), String> {
    session.diagnostic.record(FunnelEvent::OfferReceived);
    let peer = apply_description(session, &sdp, false).await?;
    let answer = peer
        .create_answer(AnswerOptions::default())
        .await
        .map_err(|error| {
            session.diagnostic.record(FunnelEvent::AnswerError);
            format!("create native LAN answer: {error}")
        })?;
    session.diagnostic.record(FunnelEvent::AnswerGenerated);
    peer.set_local_description(answer.clone())
        .await
        .map_err(|error| {
            session.diagnostic.record(FunnelEvent::AnswerError);
            format!("set native LAN local answer: {error}")
        })?;
    send_authenticated_control(
        session,
        NativeControlMessage::P2pAnswer {
            sdp: signaling_sdp(&answer.to_string()),
        },
    )
    .await
}
async fn handle_answer(session: &NativeLanSession, sdp: String) -> Result<(), String> {
    session.diagnostic.record(FunnelEvent::AnswerReceived);
    apply_description(session, &sdp, true).await.map(|_| ())
}
fn current_peer(session: &NativeLanSession) -> Result<PeerConnection, String> {
    session
        .transfer
        .lock()
        .map_err(|error| format!("LAN transfer state lock: {error}"))?
        .pc
        .clone()
        .ok_or_else(|| "native LAN PeerConnection is not ready".to_string())
}
async fn mark_remote_description_set(session: &NativeLanSession) -> Result<(), String> {
    let candidates = {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        transfer.remote_description_set = true;
        std::mem::take(&mut transfer.pending_remote_ice)
    };
    let total = candidates.len();
    session.diagnostic.count(Cs::PendingTaken, total);
    session
        .diagnostic
        .phase(Tp::PendingIceApply, To::Start, Tr::None, total);
    for (index, candidate) in candidates.into_iter().enumerate() {
        if let Err((reason, error)) = add_remote_ice_classified(session, &candidate).await {
            session
                .diagnostic
                .count(Cs::PendingAbandoned, total - index - 1);
            session
                .diagnostic
                .phase(Tp::PendingIceApply, To::Error, reason, index);
            return Err(error);
        }
        session.diagnostic.count(Cs::PendingApplied, 1);
    }
    session
        .diagnostic
        .phase(Tp::PendingIceApply, To::Ok, Tr::None, total);
    Ok(())
}
fn rejected<T>(session: &NativeLanSession, reason: Tr, error: String) -> Result<T, String> {
    session.diagnostic.candidate_rejected(reason);
    session.diagnostic.terminal(reason);
    Err(error)
}
async fn receive_ice_candidate(
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    candidate_json: String,
) -> Result<(), String> {
    session.diagnostic.count(Cs::Received, 1);
    if candidate_json.len() > 4096 {
        return rejected(
            session,
            Tr::CandidatePayloadBound,
            "ICE candidate exceeds the LAN control bound".into(),
        );
    }
    let init: IceCandidateInit = match serde_json::from_str(&candidate_json) {
        Ok(init) => init,
        Err(_) => {
            return rejected(
                session,
                Tr::CandidatePayloadMalformed,
                "invalid ICE candidate payload".into(),
            )
        }
    };
    if let Err((reason, error)) = classify_peer_candidate(runtime, session, &init.candidate) {
        if reason == Tr::CandidatePeerMismatch {
            // Only a fully well-formed host/LAN candidate from an authenticated
            // stream may be skipped. Never add or retain an unmatched candidate.
            let authenticated = session
                .auth
                .lock()
                .map(|auth| matches!(*auth, super::LocalAuthState::Authenticated(_)))
                .unwrap_or(false);
            if !authenticated {
                return rejected(
                    session,
                    Tr::ControlDecode,
                    "LAN ICE requires authentication".into(),
                );
            }
            parse_remote_ice(session, &init).map_err(|(_, error)| error)?;
            session.diagnostic.candidate_rejected(reason);
            let result = session
                .transfer
                .lock()
                .map_err(|_| "LAN transfer state lock".to_string())?
                .candidate_mismatch
                .reject();
            if result.is_err() {
                session.diagnostic.terminal(Tr::CandidateMismatchBudget);
            }
            return result;
        }
        return rejected(session, reason, error);
    }
    let ready = {
        let mut transfer = match session.transfer.lock() {
            Ok(transfer) => transfer,
            Err(error) => {
                return rejected(
                    session,
                    Tr::StateLock,
                    format!("LAN transfer state lock: {error}"),
                )
            }
        };
        if !transfer.remote_description_set || transfer.pc.is_none() {
            if transfer.pending_remote_ice.len() >= MAX_PENDING_ICE {
                return rejected(
                    session,
                    Tr::CandidatePendingBound,
                    "too many pending LAN ICE candidates".into(),
                );
            }
            transfer.pending_remote_ice.push(candidate_json.clone());
            session.diagnostic.count(Cs::Pending, 1);
            false
        } else {
            true
        }
    };
    if ready {
        add_remote_ice(session, &candidate_json).await?;
    }
    Ok(())
}
fn rejected_classified<T>(
    session: &NativeLanSession,
    reason: Tr,
    error: String,
) -> Result<T, (Tr, String)> {
    session.diagnostic.candidate_rejected(reason);
    session.diagnostic.terminal(reason);
    Err((reason, error))
}
async fn add_remote_ice(session: &NativeLanSession, candidate_json: &str) -> Result<(), String> {
    add_remote_ice_classified(session, candidate_json)
        .await
        .map_err(|(_, error)| error)
}
async fn add_remote_ice_classified(
    session: &NativeLanSession,
    candidate_json: &str,
) -> Result<(), (Tr, String)> {
    let init: IceCandidateInit = match serde_json::from_str(candidate_json) {
        Ok(init) => init,
        Err(_) => {
            return rejected_classified(
                session,
                Tr::CandidatePayloadMalformed,
                "invalid ICE candidate payload".into(),
            )
        }
    };
    let candidate = parse_remote_ice(session, &init)?;
    let peer = match current_peer(session) {
        Ok(peer) => peer,
        Err(error) => return rejected_classified(session, Tr::PeerNotReady, error),
    };
    match peer.add_ice_candidate(candidate).await {
        Ok(()) => {
            session.diagnostic.count(Cs::Added, 1);
            session
                .transfer
                .lock()
                .map_err(|_| (Tr::StateLock, "LAN transfer state lock".into()))?
                .candidate_mismatch
                .added();
            Ok(())
        }
        Err(error) => rejected_classified(
            session,
            Tr::CandidateAdd,
            format!("add LAN ICE candidate: {error}"),
        ),
    }
}
fn parse_remote_ice(
    session: &NativeLanSession,
    init: &IceCandidateInit,
) -> Result<IceCandidate, (Tr, String)> {
    let Some(mid) = init.sdp_mid.as_deref() else {
        return rejected_classified(
            session,
            Tr::CandidateMissingMid,
            "ICE candidate missing sdpMid".into(),
        );
    };
    let Some(mline) = init.sdp_mline_index else {
        return rejected_classified(
            session,
            Tr::CandidateMissingMline,
            "ICE candidate missing sdpMLineIndex".into(),
        );
    };
    match IceCandidate::parse(mid, mline, &init.candidate) {
        Ok(candidate) => Ok(candidate),
        Err(error) => rejected_classified(
            session,
            Tr::CandidateParse,
            format!("parse LAN ICE candidate: {error}"),
        ),
    }
}
fn classify_peer_candidate(
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    candidate: &str,
) -> Result<(), (Tr, String)> {
    let mut endpoint_matched = None;
    let result = (|| {
        let fields = candidate.split_whitespace().collect::<Vec<_>>();
        let malformed = || "LAN ICE candidate is not a numeric host candidate".to_string();
        if fields.len() < 8 || !fields[0].starts_with("candidate:") || fields[6] != "typ" {
            return Err((Tr::CandidateMalformedLine, malformed()));
        }
        if fields[7] != "host" {
            return Err((Tr::CandidateNonHost, malformed()));
        }
        let ip: IpAddr = fields[4]
            .parse()
            .map_err(|_| (Tr::CandidateNonNumeric, malformed()))?;
        if !is_local_network_address(ip, runtime.ipv6_listener_ready())
            && !is_loopback_for_tests(ip)
        {
            return Err((
                Tr::CandidateNonLocal,
                "non-local ICE candidate rejected in LAN-only mode".into(),
            ));
        }
        let matched = runtime.peer_matches_endpoint(&session.peer_device_id, ip);
        endpoint_matched = Some(matched);
        if !matched && !is_loopback_for_tests(ip) {
            return Err((
                Tr::CandidatePeerMismatch,
                "ICE candidate does not match the discovered peer endpoint".into(),
            ));
        }
        Ok(())
    })();
    if result.is_err() {
        session
            .diagnostic
            .rejected_candidate_context(rejected_candidate_context(candidate, endpoint_matched));
    }
    result
}
fn rejected_candidate_context(
    candidate: &str,
    endpoint_matched: Option<bool>,
) -> crate::transport_diagnostics::RejectedCandidate {
    use crate::transport_diagnostics::{
        CandidateAddressFamily as F, CandidateLanClass as A, RejectedCandidate,
    };
    let ip = candidate
        .split_whitespace()
        .nth(4)
        .and_then(|value| value.parse::<IpAddr>().ok());
    let family = match ip {
        Some(IpAddr::V4(_)) => F::Ipv4,
        Some(IpAddr::V6(_)) => F::Ipv6,
        None => F::Unknown,
    };
    let lan_class = match ip {
        Some(IpAddr::V4(ip)) if ip.is_unspecified() => A::Unspecified,
        Some(IpAddr::V4(ip)) if ip.is_loopback() => A::Loopback,
        Some(IpAddr::V4(ip)) if ip.is_private() => A::PrivateIpv4,
        Some(IpAddr::V4(ip)) if ip.is_link_local() => A::LinkLocalIpv4,
        Some(IpAddr::V4(ip)) if ip.is_multicast() => A::Multicast,
        Some(IpAddr::V6(ip)) if ip.is_unspecified() => A::Unspecified,
        Some(IpAddr::V6(ip)) if ip.is_loopback() => A::Loopback,
        Some(IpAddr::V6(ip)) if ip.to_ipv4_mapped().is_some() => A::Ipv4MappedIpv6,
        Some(IpAddr::V6(ip)) if (ip.segments()[0] & 0xfe00) == 0xfc00 => A::UlaIpv6,
        Some(IpAddr::V6(ip)) if (ip.segments()[0] & 0xffc0) == 0xfe80 => A::LinkLocalIpv6,
        Some(IpAddr::V6(ip)) if ip.is_multicast() => A::Multicast,
        Some(_) => A::Other,
        None => A::Unknown,
    };
    RejectedCandidate {
        family,
        lan_class,
        endpoint_matched,
    }
}
fn candidate_host_ip(candidate: &str) -> Option<IpAddr> {
    let fields = candidate.split_whitespace().collect::<Vec<_>>();
    if fields.len() < 8
        || !fields[0].starts_with("candidate:")
        || fields[6] != "typ"
        || fields[7] != "host"
    {
        return None;
    }
    fields[4].parse().ok()
}

fn parse_candidate_summary(
    candidate: &str,
) -> (
    CandidateFamily,
    CandidateTransport,
    CandidateCategory,
    CandidateRejection,
) {
    let fields = candidate.split_whitespace().collect::<Vec<_>>();
    let family = fields
        .get(4)
        .and_then(|v| v.parse::<IpAddr>().ok())
        .map(|ip| {
            if ip.is_ipv4() {
                CandidateFamily::Ipv4
            } else {
                CandidateFamily::Ipv6
            }
        })
        .unwrap_or(CandidateFamily::Unknown);
    if fields.len() < 8 || !fields[0].starts_with("candidate:") || fields[6] != "typ" {
        return (
            family,
            CandidateTransport::Unknown,
            CandidateCategory::Unknown,
            CandidateRejection::Malformed,
        );
    }
    let transport = match fields[2].to_ascii_lowercase().as_str() {
        "udp" => CandidateTransport::Udp,
        "tcp" => CandidateTransport::Tcp,
        _ => CandidateTransport::Unknown,
    };
    let category = if fields[7] == "host" {
        CandidateCategory::Host
    } else {
        CandidateCategory::Other
    };
    let rejection = if category != CandidateCategory::Host {
        CandidateRejection::NonHost
    } else {
        match candidate_host_ip(candidate) {
            None => CandidateRejection::Malformed,
            Some(ip) if is_local_network_address(ip, true) || is_loopback_for_tests(ip) => {
                CandidateRejection::None
            }
            Some(_) => CandidateRejection::NonLocal,
        }
    };
    (family, transport, category, rejection)
}

fn is_local_host_candidate(candidate: &str) -> bool {
    let Some(ip) = candidate_host_ip(candidate) else {
        return false;
    };
    is_local_network_address(ip, true) || is_loopback_for_tests(ip)
}

#[cfg(test)]
fn is_loopback_for_tests(ip: IpAddr) -> bool {
    ip.is_loopback()
}

#[cfg(not(test))]
fn is_loopback_for_tests(_ip: IpAddr) -> bool {
    false
}

fn signaling_sdp(sdp: &str) -> String {
    sdp.lines()
        .filter(|line| !line.starts_with("a=candidate:") && *line != "a=end-of-candidates")
        .collect::<Vec<_>>()
        .join("\r\n")
        + "\r\n"
}

fn ensure_signaling_sdp_is_candidate_free(sdp: &str) -> Result<(), String> {
    if sdp.len() > 128 * 1024 || sdp.lines().any(|line| line.starts_with("a=candidate:")) {
        return Err(
            "LAN SDP must be bounded and use the authenticated ICE control lane".to_string(),
        );
    }
    if !sdp.lines().any(|line| line.starts_with("m=application ")) {
        return Err("LAN SDP does not contain the expected DataChannel m-line".to_string());
    }
    Ok(())
}

fn receive_manifest(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    manifest: NativeFileManifest,
) -> Result<(), String> {
    if session.role != super::PairingRole::Responder {
        return Err("unexpected file manifest role".to_string());
    }
    session.diagnostic.record(FunnelEvent::ManifestReceived);
    validate_manifest(session, &manifest)?;
    let event = IncomingManifestEvent {
        session_id: session.session_id.clone(),
        task_id: session.task_id.clone(),
        peer_device_label: session.peer_device_label.clone(),
        name: manifest.name.clone(),
        size_bytes: manifest.size,
    };
    {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        if transfer.pending_manifest.is_some() || transfer.incoming.is_some() {
            return Err("duplicate file manifest".to_string());
        }
        claim_transfer_nonce(&mut transfer.seen_transfer_nonces, &manifest.transfer_nonce)?;
        transfer.pending_manifest = Some(manifest);
    }
    emit_lan_event(app, runtime, "fling://lan/manifest", event);
    Ok(())
}

fn validate_manifest(
    session: &NativeLanSession,
    manifest: &NativeFileManifest,
) -> Result<(), String> {
    validate_manifest_for_session(&session.session_id, &session.task_id, manifest)
}

fn validate_manifest_for_session(
    session_id: &str,
    task_id: &str,
    manifest: &NativeFileManifest,
) -> Result<(), String> {
    if manifest.transfer_nonce.len() != 32
        || !manifest
            .transfer_nonce
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || manifest.id
            != super::transfer_id_for_session(session_id, task_id, &manifest.transfer_nonce)
        || manifest.id.len() != 24
        || !manifest.id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("file manifest transfer id mismatch".to_string());
    }
    if manifest.name.is_empty()
        || manifest.name.len() > 255
        || manifest.name == "."
        || manifest.name == ".."
        || manifest
            .name
            .chars()
            .any(|character| character.is_control())
        || manifest.name.contains('/')
        || manifest.name.contains('\\')
    {
        return Err("invalid LAN file name".to_string());
    }
    if manifest.kind != "file"
        || manifest.chunk_size != NATIVE_CHUNK_SIZE as u32
        || manifest.sha256.len() != 64
        || !manifest.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("unsupported LAN manifest parameters".to_string());
    }
    let expected_chunks = if manifest.size == 0 {
        0
    } else {
        manifest
            .size
            .checked_add(NATIVE_CHUNK_SIZE - 1)
            .ok_or_else(|| "file size overflow".to_string())?
            / NATIVE_CHUNK_SIZE
    };
    if expected_chunks > u32::MAX as u64 || manifest.total_chunks as u64 != expected_chunks {
        return Err("LAN manifest chunk count mismatch".to_string());
    }
    Ok(())
}

pub(super) async fn decide_manifest(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &Arc<NativeLanSession>,
    accepted: bool,
) -> Result<(), String> {
    let manifest = {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        transfer
            .pending_manifest
            .take()
            .ok_or_else(|| "incoming file consent is no longer available".to_string())?
    };
    if !accepted {
        session.diagnostic.record(FunnelEvent::ManifestDenied);
        send_authenticated_control(
            session,
            NativeControlMessage::TaskCancel {
                task_id: session.task_id.clone(),
            },
        )
        .await?;
        runtime.remove_session(&session.session_id);
        emit_lan_event(
            app,
            runtime,
            "fling://lan/progress",
            task_event_with_metadata(
                session,
                "cancelled",
                0,
                0,
                manifest.size,
                None,
                Some(manifest.name),
            ),
        );
        return Ok(());
    }
    session.diagnostic.record(FunnelEvent::ManifestAccepted);
    let cfg = ensure_config(app)?.1;
    let store = app.state::<ReceivedFileStore>().inner().clone();
    let store_session_id = store.begin(
        cfg.download_dir,
        manifest.name.clone(),
        None,
        manifest.size,
        manifest.sha256.clone(),
    )?;
    {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        transfer.incoming = Some(IncomingTransfer {
            manifest: manifest.clone(),
            store: store.clone(),
            store_session_id,
            next_seq: 0,
            received_bytes: 0,
            last_ack_seq: 0,
            last_progress_bytes: 0,
            last_progress_at: Instant::now(),
            complete: false,
        });
    }
    send_authenticated_control(
        session,
        NativeControlMessage::FileManifestAck {
            id: manifest.id.clone(),
            chunk_ack: Some(true),
            binary_chunks: Some(true),
            direct_frame_payload_bytes: Some(NATIVE_CHUNK_SIZE as u32),
        },
    )
    .await?;
    emit_lan_event(
        app,
        runtime,
        "fling://lan/progress",
        task_event_with_metadata(
            session,
            "ready-to-transfer",
            0,
            0,
            manifest.size,
            None,
            Some(manifest.name.clone()),
        ),
    );
    if manifest.size == 0 {
        finalize_zero_byte_receive(app, runtime, session).await?;
    }
    Ok(())
}

async fn finalize_zero_byte_receive(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
) -> Result<(), String> {
    let (manifest, store, store_session_id) = {
        let transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        let incoming = transfer
            .incoming
            .as_ref()
            .ok_or_else(|| "zero-byte receive state missing".to_string())?;
        let result = (
            incoming.manifest.clone(),
            incoming.store.clone(),
            incoming.store_session_id.clone(),
        );
        result
    };
    store.finish(&store_session_id)?;
    if let Ok(mut transfer) = session.transfer.lock() {
        if let Some(incoming) = transfer.incoming.as_mut() {
            incoming.complete = true;
        }
    }
    send_file_chunk_ack(session, &manifest.id, 0, 0, Some(0), Some(true), None).await?;
    session
        .durable_complete
        .store(true, std::sync::atomic::Ordering::Release);
    if !session.claim_terminal() {
        return Ok(());
    }
    emit_lan_event(
        app,
        runtime,
        "fling://lan/progress",
        task_event_with_metadata(session, "complete", 0, 0, 0, None, Some(manifest.name)),
    );
    Ok(())
}

fn observe_channel_state(
    diagnostic: &crate::pairing_diagnostics::FunnelTrace,
    state: DataChannelState,
    initial: bool,
) {
    diagnostic.transition(
        if initial {
            Tp::DataChannelInitial
        } else {
            Tp::DataChannelTransition
        },
        match state {
            DataChannelState::Connecting => To::Connecting,
            DataChannelState::Open => To::Open,
            DataChannelState::Closing => To::Closing,
            DataChannelState::Closed => To::Closed,
        },
    );
}
async fn wait_channel_open(
    diagnostic: &crate::pairing_diagnostics::FunnelTrace,
    state_rx: &mut watch::Receiver<DataChannelState>,
    duration: Duration,
) -> Result<(), String> {
    let open_result = timeout(duration, async {
        loop {
            match *state_rx.borrow() {
                DataChannelState::Open => return Ok(()),
                DataChannelState::Closing | DataChannelState::Closed => {
                    diagnostic.terminal(Tr::DataChannelClosed);
                    return Err("LAN DataChannel closed before opening".to_string());
                }
                DataChannelState::Connecting => {}
            }
            state_rx.changed().await.map_err(|_| {
                diagnostic.terminal(Tr::DataChannelObserver);
                "LAN DataChannel state observer closed".to_string()
            })?;
        }
    })
    .await
    .map_err(|_| {
        diagnostic.record(FunnelEvent::DataChannelError);
        diagnostic.terminal(Tr::DataChannelTimeout);
        "LAN DataChannel open timed out".to_string()
    })?;
    open_result
}
async fn install_data_channel(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
    channel: DataChannel,
) -> Result<(), String> {
    let (frame_tx, frame_rx) = async_mpsc::channel(RECEIVE_QUEUE_FRAMES);
    let message_tx = frame_tx.clone();
    let overflow_channel = channel.clone();
    let overflow_cancel = session.cancel_tx.clone();
    let arrival_diag = session.diagnostic.clone();
    let overflow_diag = session.diagnostic.clone();
    let receiver_role = session.role == super::PairingRole::Responder;
    let first_arrival = std::sync::atomic::AtomicBool::new(false);
    channel.on_message(Some(Box::new(move |message| {
        if receiver_role && !first_arrival.swap(true, std::sync::atomic::Ordering::Relaxed) {
            arrival_diag.record(FunnelEvent::FirstFrameReceiverArrival);
        }
        let frame = if message.binary && message.data.len() <= 128 * 1024 + 30 {
            message.data.to_vec()
        } else {
            vec![0]
        };
        if message_tx.try_send(frame).is_err() {
            overflow_diag.terminal(Tr::ReceiveOverflow);
            overflow_diag.cleanup(Tr::ReceiveOverflow);
            let _ = overflow_cancel.send(true);
            overflow_diag.local_close_requested();
            overflow_channel.close();
        }
    })));
    let initial_state = channel.state();
    observe_channel_state(&session.diagnostic, initial_state, true);
    match initial_state {
        DataChannelState::Open => session.diagnostic.record(FunnelEvent::DataChannelOpen),
        DataChannelState::Closed => session.diagnostic.record(FunnelEvent::DataChannelClose),
        _ => {}
    }
    let (state_tx, mut state_rx) = watch::channel(initial_state);
    let worker_state_rx = state_rx.clone();
    eprintln!("[fling] LAN DataChannel installed: {:?}", channel.state());
    let state_diag = session.diagnostic.clone();
    channel.on_state_change(Some(Box::new(move |state| {
        observe_channel_state(&state_diag, state, false);
        match state {
            DataChannelState::Open => state_diag.record(FunnelEvent::DataChannelOpen),
            DataChannelState::Closed => state_diag.record(FunnelEvent::DataChannelClose),
            _ => {}
        }
        eprintln!("[fling] LAN DataChannel state: {state:?}");
        let _ = state_tx.send(state);
    })));
    {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        transfer.data_channel = Some(channel.clone());
    }
    let worker_app = app.clone();
    let worker_runtime = runtime.clone();
    let worker_session = session.clone();
    tauri::async_runtime::spawn(async move {
        receive_worker(
            worker_app,
            worker_runtime,
            worker_session,
            frame_rx,
            worker_state_rx,
        )
        .await;
    });

    wait_channel_open(&session.diagnostic, &mut state_rx, CHANNEL_OPEN_TIMEOUT).await?;
    if *session.cancel_tx.borrow() {
        return Err("LAN session cancelled".to_string());
    }
    emit_lan_event(
        &app,
        &runtime,
        "fling://lan/progress",
        task_event_with_metadata(&session, "channel-ready", 0, 0, 0, None, None),
    );

    if session.role == super::PairingRole::Initiator
        && !session
            .manifest_sent
            .swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        let transfer_nonce = {
            let mut transfer = session
                .transfer
                .lock()
                .map_err(|_| "LAN transfer state lock".to_string())?;
            fresh_transfer_nonce(&mut transfer.seen_transfer_nonces)?
        };
        let manifest = {
            let source = session
                .source
                .lock()
                .map_err(|error| format!("LAN source state lock: {error}"))?;
            let source = source
                .as_ref()
                .ok_or_else(|| "native LAN source file is unavailable".to_string())?;
            NativeFileManifest {
                id: super::transfer_id_for_session(
                    &session.session_id,
                    &session.task_id,
                    &transfer_nonce,
                ),
                transfer_nonce,
                name: source.name.clone(),
                size: source.size_bytes,
                sha256: source.sha256.clone(),
                chunk_size: NATIVE_CHUNK_SIZE as u32,
                total_chunks: source.total_chunks,
                kind: "file".to_string(),
            }
        };
        {
            let mut transfer = session
                .transfer
                .lock()
                .map_err(|error| format!("LAN transfer state lock: {error}"))?;
            transfer.outgoing_manifest = Some(manifest.clone());
        }
        send_authenticated_control(
            &session,
            NativeControlMessage::FileManifest {
                manifest: manifest.clone(),
            },
        )
        .await?;
        emit_lan_event(
            &app,
            &runtime,
            "fling://lan/progress",
            task_event_with_metadata(
                &session,
                "waiting-recipient",
                0,
                0,
                manifest.size,
                None,
                Some(manifest.name),
            ),
        );
    }
    Ok(())
}

async fn receive_worker(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
    mut receiver: async_mpsc::Receiver<Vec<u8>>,
    mut state_rx: watch::Receiver<DataChannelState>,
) {
    loop {
        if receiver.is_empty()
            && matches!(
                *state_rx.borrow(),
                DataChannelState::Closing | DataChannelState::Closed
            )
        {
            break;
        }
        let frame = tokio::select! {
            biased;
            frame = receiver.recv() => frame,
            changed = state_rx.changed() => {
                if changed.is_err()
                    || matches!(*state_rx.borrow(), DataChannelState::Closing | DataChannelState::Closed)
                {
                    None
                } else {
                    continue;
                }
            }
        };
        let Some(frame) = frame else {
            break;
        };
        if let Err(error) = receive_one_frame(&app, &runtime, &session, &frame).await {
            fail_session(&app, &runtime, &session, error);
            return;
        }
    }
    if !session
        .durable_complete
        .load(std::sync::atomic::Ordering::Acquire)
        && !*session.cancel_tx.borrow()
    {
        super::interrupt_session(
            &app,
            &runtime,
            &session,
            "LAN DataChannel closed before durable completion".to_string(),
            super::LanFailureKind::Close,
        );
    }
}

async fn receive_one_frame(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    bytes: &[u8],
) -> Result<(), String> {
    let frame = native_lan::decode_direct_file_frame(bytes)?
        .ok_or_else(|| "non-FLNG payload received on the file DataChannel".to_string())?;
    let (ack, progress, complete, manifest_name, manifest_size) = {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        let incoming = transfer
            .incoming
            .as_mut()
            .ok_or_else(|| "file bytes arrived before recipient consent".to_string())?;
        if frame.id != incoming.manifest.id {
            return Err("FLNG frame transfer id mismatch".to_string());
        }
        if incoming.complete {
            return Err("file frame received after durable completion".to_string());
        }
        if frame.seq < incoming.next_seq || frame.seq > incoming.next_seq {
            let ack = cumulative_ack(incoming, false, None);
            (
                Some(ack),
                None,
                false,
                incoming.manifest.name.clone(),
                incoming.manifest.size,
            )
        } else {
            let count = u32::from(frame.chunk_count);
            let chunk_size = frame
                .chunk_size
                .map(u32::from)
                .unwrap_or(incoming.manifest.chunk_size);
            if chunk_size != incoming.manifest.chunk_size || count == 0 {
                return Err("FLNG frame chunk geometry does not match manifest".to_string());
            }
            // Validate the whole batch first, then commit it with a single disk
            // write (was: one write per 32 KiB chunk).
            let mut offset = 0usize;
            let mut frame_bytes = 0u64;
            for index in 0..count {
                if incoming.next_seq.saturating_add(index) >= incoming.manifest.total_chunks {
                    return Err("FLNG frame exceeds declared chunk count".to_string());
                }
                let remaining = incoming
                    .manifest
                    .size
                    .checked_sub(incoming.received_bytes + frame_bytes)
                    .ok_or_else(|| "received byte counter overflow".to_string())?;
                let expected = remaining.min(u64::from(chunk_size)) as usize;
                if expected == 0
                    || offset.checked_add(expected).unwrap_or(usize::MAX) > frame.payload.len()
                {
                    return Err("FLNG payload length does not match manifest".to_string());
                }
                if index + 1 < count
                    && incoming.received_bytes + frame_bytes + expected as u64
                        >= incoming.manifest.size
                {
                    return Err("FLNG batch exceeds declared file size".to_string());
                }
                offset += expected;
                frame_bytes += expected as u64;
            }
            if offset != frame.payload.len() {
                return Err("FLNG frame contains trailing payload bytes".to_string());
            }
            if let Err(error) = incoming.store.append(
                &incoming.store_session_id,
                &frame.payload[..offset],
            ) {
                if frame.seq == 0 {
                    session
                        .diagnostic
                        .record(FunnelEvent::FirstFrameAppendError);
                }
                return Err(error);
            }
            // A frame may batch chunks: only report success once the entire first frame appended.
            if frame.seq == 0 {
                session.diagnostic.record(FunnelEvent::FirstFrameAppendOk);
            }
            incoming.received_bytes += frame_bytes;
            incoming.next_seq += count;
            let is_complete = incoming.next_seq == incoming.manifest.total_chunks
                && incoming.received_bytes == incoming.manifest.size;
            if incoming.next_seq == incoming.manifest.total_chunks
                && incoming.received_bytes != incoming.manifest.size
            {
                return Err("received chunk count does not match declared file size".to_string());
            }
            let should_ack = is_complete
                || incoming.next_seq.saturating_sub(incoming.last_ack_seq) >= ACK_INTERVAL_CHUNKS;
            let ack = if should_ack {
                incoming.last_ack_seq = incoming.next_seq;
                Some(cumulative_ack(incoming, false, None))
            } else {
                None
            };
            let progress_due = incoming
                .received_bytes
                .saturating_sub(incoming.last_progress_bytes)
                >= PROGRESS_MIN_BYTES
                || incoming.last_progress_at.elapsed() >= PROGRESS_MIN_INTERVAL
                || is_complete;
            let progress = if progress_due {
                let elapsed_ms = incoming.last_progress_at.elapsed().as_millis().max(1) as u64;
                let delta = incoming
                    .received_bytes
                    .saturating_sub(incoming.last_progress_bytes);
                let speed_bps = delta.saturating_mul(1000) / elapsed_ms;
                incoming.last_progress_bytes = incoming.received_bytes;
                incoming.last_progress_at = Instant::now();
                Some((
                    incoming.received_bytes,
                    incoming.manifest.name.clone(),
                    incoming.manifest.size,
                    speed_bps,
                ))
            } else {
                None
            };
            (
                ack,
                progress,
                is_complete,
                incoming.manifest.name.clone(),
                incoming.manifest.size,
            )
        }
    };
    if let Some(ack) = ack {
        send_file_chunk_ack(
            session,
            &ack.id,
            ack.next_seq,
            ack.received_bytes,
            ack.saved_bytes,
            ack.complete,
            ack.failed,
        )
        .await?;
    }
    if let Some((received, name, total, speed_bps)) = progress {
        emit_lan_event(
            app,
            runtime,
            "fling://lan/progress",
            progress_event(
                session,
                if complete { "finalizing" } else { "receiving" },
                received,
                received,
                total,
                speed_bps,
                Some(name),
            ),
        );
    }
    if complete {
        finalize_received_file(app, runtime, session, &manifest_name, manifest_size).await?;
    }
    Ok(())
}

struct AckFields {
    id: String,
    next_seq: u32,
    received_bytes: u64,
    saved_bytes: Option<u64>,
    complete: Option<bool>,
    failed: Option<bool>,
}

fn cumulative_ack(incoming: &IncomingTransfer, complete: bool, failed: Option<bool>) -> AckFields {
    AckFields {
        id: incoming.manifest.id.clone(),
        next_seq: incoming.next_seq,
        received_bytes: incoming.received_bytes,
        saved_bytes: Some(incoming.received_bytes),
        complete: Some(complete),
        failed,
    }
}

async fn finalize_received_file(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    _name: &str,
    _size: u64,
) -> Result<(), String> {
    let (manifest, store, store_session_id) = {
        let transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        let incoming = transfer
            .incoming
            .as_ref()
            .ok_or_else(|| "receive finalization state missing".to_string())?;
        if incoming.complete {
            return Ok(());
        }
        let manifest = incoming.manifest.clone();
        let store = incoming.store.clone();
        let store_session_id = incoming.store_session_id.clone();
        (manifest, store, store_session_id)
    };
    // finish() verifies SHA-256, syncs the temporary file and atomically commits
    // without overwriting an existing destination. Only after it succeeds is the
    // durable-completion ACK sent.
    store.finish(&store_session_id)?;
    if let Ok(mut transfer) = session.transfer.lock() {
        if let Some(incoming) = transfer.incoming.as_mut() {
            incoming.complete = true;
        }
    }
    send_file_chunk_ack(
        session,
        &manifest.id,
        manifest.total_chunks,
        manifest.size,
        Some(manifest.size),
        Some(true),
        None,
    )
    .await?;
    session
        .durable_complete
        .store(true, std::sync::atomic::Ordering::Release);
    if !session.claim_terminal() {
        return Ok(());
    }
    emit_lan_event(
        app,
        runtime,
        "fling://lan/progress",
        task_event_with_metadata(
            session,
            "complete",
            manifest.size,
            manifest.size,
            manifest.size,
            None,
            Some(manifest.name),
        ),
    );
    Ok(())
}

async fn start_sender(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
) -> Result<(), String> {
    let channel = {
        let transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        transfer
            .data_channel
            .clone()
            .ok_or_else(|| "accepted manifest has no open DataChannel".to_string())?
    };
    let source = session
        .source
        .lock()
        .map_err(|error| format!("LAN source state lock: {error}"))?
        .take()
        .ok_or_else(|| "native source file was already consumed".to_string())?;
    if channel.state() != DataChannelState::Open {
        return Err("LAN DataChannel closed before manifest acceptance completed".to_string());
    }
    emit_lan_event(
        &app,
        &runtime,
        "fling://lan/progress",
        task_event_with_metadata(
            &session,
            "ready-to-transfer",
            0,
            0,
            source.size_bytes,
            None,
            Some(source.name.clone()),
        ),
    );
    tauri::async_runtime::spawn(async move {
        if let Err(error) = run_sender(
            app.clone(),
            runtime.clone(),
            session.clone(),
            channel,
            source,
        )
        .await
        {
            fail_session(&app, &runtime, &session, error);
        }
    });
    Ok(())
}

async fn run_sender(
    app: AppHandle,
    runtime: NativeLanRuntime,
    session: Arc<NativeLanSession>,
    channel: DataChannel,
    mut source: NativeSource,
) -> Result<(), String> {
    if channel.state() != DataChannelState::Open {
        return Err("LAN DataChannel is not open after manifest acceptance".to_string());
    }
    let transfer_id = outgoing_transfer_id(&session)?;
    emit_lan_event(
        &app,
        &runtime,
        "fling://lan/progress",
        task_event_with_metadata(
            &session,
            "sending",
            0,
            0,
            source.size_bytes,
            None,
            Some(source.name.clone()),
        ),
    );
    let mut sent_hasher = Sha256::new();
    let mut seq = 0u32;
    let mut hashed_seq = 0u32;
    let mut sent_bytes = 0u64;
    let mut recovery_retries = 0u8;
    let mut last_progress_bytes = 0u64;
    let mut last_progress_at = Instant::now();
    // Event-driven backpressure: the sender waits on this watch channel instead of
    // polling buffered_amount() on a fixed sleep. The native stack fires
    // on_buffered_amount_change whenever the SCTP buffer drains; the wait loop
    // re-checks buffered_amount() itself, so spurious wakeups are harmless.
    // (A 200 ms timeout inside the wait loop is only a safety net.)
    let (drain_tx, mut drain_rx) = watch::channel(());
    channel.on_buffered_amount_change(Some(Box::new(move |_sent_bytes: u64| {
        let _ = drain_tx.send(());
    })));
    while seq < source.total_chunks {
        let acked = session
            .ack_next_seq
            .load(std::sync::atomic::Ordering::Acquire);
        if seq < acked {
            seq = acked;
            let offset = u64::from(seq)
                .checked_mul(NATIVE_CHUNK_SIZE)
                .ok_or_else(|| "resume offset overflow".to_string())?;
            source
                .file
                .seek(SeekFrom::Start(offset))
                .map_err(|error| format!("seek native source: {error}"))?;
            sent_bytes = sent_bytes.max(offset.min(source.size_bytes));
        }
        if seq.saturating_sub(acked) >= SENDER_WINDOW_CHUNKS {
            let target = acked.saturating_add(ACK_INTERVAL_CHUNKS);
            if !wait_for_ack(&session, target, false).await? {
                let latest_acked = session
                    .ack_next_seq
                    .load(std::sync::atomic::Ordering::Acquire);
                if latest_acked > acked {
                    recovery_retries = 0;
                } else {
                    if recovery_retries >= MAX_ACK_RECOVERY_RETRIES {
                        return Err(
                            "cumulative file ACK timed out after 3 recovery retries".to_string()
                        );
                    }
                    recovery_retries += 1;
                    seq = latest_acked;
                    let offset = u64::from(seq)
                        .checked_mul(NATIVE_CHUNK_SIZE)
                        .ok_or_else(|| "recovery offset overflow".to_string())?;
                    source
                        .file
                        .seek(SeekFrom::Start(offset))
                        .map_err(|error| format!("seek native source for recovery: {error}"))?;
                }
            } else {
                recovery_retries = 0;
            }
            continue;
        }
        // Coalesce up to BATCH_CHUNKS_PER_FRAME chunks into a single FLNG batch
        // frame (protocol ceiling: 128 KiB payload). The batch size is bounded by
        // the remaining in-flight window so a frame never overshoots the cap.
        // The receiver already decodes batch frames (see decode_direct_file_frame).
        let window_left = SENDER_WINDOW_CHUNKS.saturating_sub(seq.saturating_sub(acked));
        let chunks_left = source.total_chunks.saturating_sub(seq);
        let batch_chunks = BATCH_CHUNKS_PER_FRAME
            .min(window_left)
            .min(chunks_left)
            .max(1);
        let batch_seq = seq;
        let mut batch = Vec::with_capacity(batch_chunks as usize * NATIVE_CHUNK_SIZE as usize);
        for _ in 0..batch_chunks {
            let cs = seq;
            let chunk_offset = u64::from(cs)
                .checked_mul(NATIVE_CHUNK_SIZE)
                .ok_or_else(|| "source offset overflow".to_string())?;
            let remaining = source.size_bytes.saturating_sub(chunk_offset);
            let expected = remaining.min(NATIVE_CHUNK_SIZE) as usize;
            if expected == 0 {
                return Err("native source ended before declared chunk count".to_string());
            }
            let mut chunk = vec![0u8; expected];
            source
                .file
                .read_exact(&mut chunk)
                .map_err(|error| format!("read native source: {error}"))?;
            if cs == hashed_seq {
                sent_hasher.update(&chunk);
                hashed_seq += 1;
            } else if cs > hashed_seq {
                return Err("sender recovery skipped an unhashed source chunk".to_string());
            }
            // cs < hashed_seq: chunk was already hashed before a recovery rewind;
            // re-send it as-is without re-hashing.
            batch.extend_from_slice(&chunk);
            seq += 1;
        }
        let frame = native_lan::encode_direct_file_frame(
            &transfer_id,
            batch_seq,
            &batch,
            NATIVE_CHUNK_SIZE as u16,
            batch_chunks as u16,
        )?;
        // Event-driven backpressure: wait until the DataChannel buffer drains below
        // MAX_BUFFERED_BYTES instead of polling on a fixed 5 ms sleep.
        let drain_started = Instant::now();
        loop {
            if channel.buffered_amount() <= MAX_BUFFERED_BYTES {
                break;
            }
            if channel.state() != DataChannelState::Open {
                return Err("LAN DataChannel closed while applying backpressure".to_string());
            }
            if drain_started.elapsed() > Duration::from_secs(15) {
                return Err("LAN DataChannel send buffer did not drain".to_string());
            }
            let _ = timeout(Duration::from_millis(200), drain_rx.changed()).await;
        }
        if batch_seq == 0 {
            session
                .diagnostic
                .record(FunnelEvent::FirstFrameSendAttempt);
        }
        if let Err(error) = channel.send(&frame, true) {
            session.diagnostic.record(FunnelEvent::DataChannelError);
            if batch_seq == 0 {
                session.diagnostic.record(FunnelEvent::FirstFrameSendError);
            }
            return Err(format!("send FLNG frame: {error}"));
        }
        if batch_seq == 0 {
            session
                .diagnostic
                .record(FunnelEvent::FirstFrameNativeAccepted);
        }
        sent_bytes = sent_bytes.max(
            u64::from(batch_seq)
                .saturating_mul(NATIVE_CHUNK_SIZE)
                .saturating_add(batch.len() as u64),
        );
        if sent_bytes.saturating_sub(last_progress_bytes) >= PROGRESS_MIN_BYTES
            || last_progress_at.elapsed() >= PROGRESS_MIN_INTERVAL
        {
            let elapsed_ms = last_progress_at.elapsed().as_millis().max(1) as u64;
            let delta = sent_bytes.saturating_sub(last_progress_bytes);
            let speed_bps = delta.saturating_mul(1000) / elapsed_ms;
            let saved_bytes = u64::from(
                session
                    .ack_next_seq
                    .load(std::sync::atomic::Ordering::Acquire),
            )
            .saturating_mul(NATIVE_CHUNK_SIZE)
            .min(source.size_bytes);
            last_progress_bytes = sent_bytes;
            last_progress_at = Instant::now();
            emit_lan_event(
                &app,
                &runtime,
                "fling://lan/progress",
                progress_event(
                    &session,
                    "sending",
                    sent_bytes,
                    saved_bytes,
                    source.size_bytes,
                    speed_bps,
                    Some(source.name.clone()),
                ),
            );
        }
        if seq.saturating_sub(acked) >= SENDER_WINDOW_CHUNKS {
            let target = seq.saturating_sub(ACK_INTERVAL_CHUNKS);
            if !wait_for_ack(&session, target, false).await? {
                let latest_acked = session
                    .ack_next_seq
                    .load(std::sync::atomic::Ordering::Acquire);
                if latest_acked > acked {
                    recovery_retries = 0;
                } else {
                    if recovery_retries >= MAX_ACK_RECOVERY_RETRIES {
                        return Err(
                            "cumulative file ACK timed out after 3 recovery retries".to_string()
                        );
                    }
                    recovery_retries += 1;
                    seq = latest_acked;
                    let offset = u64::from(seq)
                        .checked_mul(NATIVE_CHUNK_SIZE)
                        .ok_or_else(|| "recovery offset overflow".to_string())?;
                    source
                        .file
                        .seek(SeekFrom::Start(offset))
                        .map_err(|error| format!("seek native source for recovery: {error}"))?;
                }
            } else {
                recovery_retries = 0;
            }
        }
    }
    let digest = format!("{:x}", sent_hasher.finalize());
    if !digest.eq_ignore_ascii_case(&source.sha256) {
        return Err("source changed during transfer; SHA-256 mismatch".to_string());
    }
    if !wait_for_ack(&session, source.total_chunks, true).await? {
        return Err("durable completion ACK timed out".to_string());
    }
    send_authenticated_control(
        &session,
        NativeControlMessage::TaskAck {
            task_id: session.task_id.clone(),
        },
    )
    .await?;
    session
        .completed
        .store(true, std::sync::atomic::Ordering::Release);
    session.diagnostic.terminal(Tr::Completed);
    emit_lan_event(
        &app,
        &runtime,
        "fling://lan/progress",
        task_event_with_metadata(
            &session,
            "complete",
            source.size_bytes,
            source.size_bytes,
            source.size_bytes,
            None,
            Some(source.name),
        ),
    );
    schedule_session_cleanup(runtime, session);
    Ok(())
}

async fn wait_for_ack(
    session: &NativeLanSession,
    target_seq: u32,
    require_durable: bool,
) -> Result<bool, String> {
    loop {
        let notified = session.ack_notify.notified();
        let acked = session
            .ack_next_seq
            .load(std::sync::atomic::Ordering::Acquire);
        let durable = session
            .durable_complete
            .load(std::sync::atomic::Ordering::Acquire);
        if acked >= target_seq && (!require_durable || durable) {
            return Ok(true);
        }
        if timeout(ACK_TIMEOUT, notified).await.is_err() {
            return Ok(false);
        }
    }
}

async fn handle_chunk_ack(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    id: String,
    next_seq: u32,
    received_bytes: u64,
    saved_bytes: Option<u64>,
    complete: Option<bool>,
    failed: Option<bool>,
) -> Result<(), String> {
    if session.role != super::PairingRole::Initiator || id != outgoing_transfer_id(session)? {
        return Err("file ACK transfer binding mismatch".to_string());
    }
    if session
        .source
        .lock()
        .map_err(|_| "LAN source state lock".to_string())?
        .is_some()
    {
        return Err("file ACK received before manifest acceptance".to_string());
    }
    let (expected_bytes, total_bytes, total_chunks, name) = {
        let source = session
            .source
            .lock()
            .map_err(|error| format!("LAN source state lock: {error}"))?;
        let (size, chunks, name) = if let Some(source) = source.as_ref() {
            (source.size_bytes, source.total_chunks, source.name.clone())
        } else {
            // Sender worker owns the source after manifest acceptance; manifest
            // metadata is retained by this per-session transfer descriptor below.
            let transfer = session
                .transfer
                .lock()
                .map_err(|error| format!("LAN transfer state lock: {error}"))?;
            let manifest = transfer
                .outgoing_manifest
                .as_ref()
                .ok_or_else(|| "sender manifest state missing".to_string())?;
            (manifest.size, manifest.total_chunks, manifest.name.clone())
        };
        if next_seq > chunks {
            return Err("cumulative ACK exceeds declared chunks".to_string());
        }
        let offset = u64::from(next_seq)
            .checked_mul(NATIVE_CHUNK_SIZE)
            .ok_or_else(|| "ACK byte offset overflow".to_string())?;
        (offset.min(size), size, chunks, name)
    };
    if received_bytes != expected_bytes || saved_bytes.is_some_and(|saved| saved > received_bytes) {
        return Err("cumulative ACK byte count mismatch".to_string());
    }
    if failed == Some(true) {
        return Err("recipient failed while saving the file".to_string());
    }
    session
        .ack_next_seq
        .fetch_max(next_seq, std::sync::atomic::Ordering::AcqRel);
    if complete == Some(true) {
        if received_bytes != expected_bytes || next_seq != total_chunks {
            return Err("durable ACK does not cover the complete source".to_string());
        }
        session
            .durable_complete
            .store(true, std::sync::atomic::Ordering::Release);
    }
    session.ack_notify.notify_waiters();
    let progress = {
        let mut transfer = session
            .transfer
            .lock()
            .map_err(|error| format!("LAN transfer state lock: {error}"))?;
        let elapsed = transfer.last_sender_progress_at.elapsed();
        let delta = received_bytes.saturating_sub(transfer.last_sender_progress_bytes);
        if delta >= PROGRESS_MIN_BYTES || elapsed >= PROGRESS_MIN_INTERVAL || complete == Some(true)
        {
            let speed_bps = delta.saturating_mul(1000) / elapsed.as_millis().max(1) as u64;
            transfer.last_sender_progress_bytes = received_bytes;
            transfer.last_sender_progress_at = Instant::now();
            Some(speed_bps)
        } else {
            None
        }
    };
    if let Some(speed_bps) = progress {
        emit_lan_event(
            app,
            runtime,
            "fling://lan/progress",
            progress_event(
                session,
                "sending",
                received_bytes,
                saved_bytes.unwrap_or_default(),
                total_bytes,
                speed_bps,
                Some(name),
            ),
        );
    }
    Ok(())
}

async fn send_file_chunk_ack(
    session: &NativeLanSession,
    id: &str,
    next_seq: u32,
    received_bytes: u64,
    saved_bytes: Option<u64>,
    complete: Option<bool>,
    failed: Option<bool>,
) -> Result<(), String> {
    send_authenticated_control(
        session,
        NativeControlMessage::FileChunkAck {
            id: id.to_string(),
            next_seq,
            received_bytes,
            saved_bytes,
            complete,
            failed,
        },
    )
    .await
}

fn send_file_chunk_ack_nowait(
    session: &NativeLanSession,
    id: &str,
    next_seq: u32,
    received_bytes: u64,
    saved_bytes: Option<u64>,
    complete: Option<bool>,
    failed: Option<bool>,
) -> Result<(), String> {
    send_authenticated_control_nowait(
        session,
        NativeControlMessage::FileChunkAck {
            id: id.to_string(),
            next_seq,
            received_bytes,
            saved_bytes,
            complete,
            failed,
        },
    )
}

async fn send_authenticated_control(
    session: &NativeLanSession,
    message: NativeControlMessage,
) -> Result<(), String> {
    let queued_event = match &message {
        NativeControlMessage::P2pOffer { .. } => Some(FunnelEvent::OfferQueued),
        NativeControlMessage::P2pAnswer { .. } => Some(FunnelEvent::AnswerQueued),
        NativeControlMessage::FileManifest { .. } => Some(FunnelEvent::ManifestQueued),
        _ => None,
    };
    let is_ice = matches!(
        &message,
        NativeControlMessage::P2pIce { candidate: Some(_) }
    );
    let result = enqueue_ordered_control(&session.control_tx, &session.control_send_lock, || {
        let frame = encrypt_authenticated_control(session, &message)?;
        session.observe_outgoing_control(&message, &frame);
        if matches!(
            &message,
            NativeControlMessage::P2pIce { candidate: Some(_) }
        ) {
            session.diagnostic.count(Cs::Queued, 1);
        }
        // This closure runs after bounded queue reservation; no await can intervene before permit.send.
        if let Some(event) = queued_event {
            session.diagnostic.record(event);
        }
        Ok((frame, ()))
    })
    .await;
    if is_ice && result.is_err() {
        session.diagnostic.count(Cs::QueueFailed, 1);
    }
    result
}
fn send_authenticated_control_nowait(
    session: &NativeLanSession,
    message: NativeControlMessage,
) -> Result<(), String> {
    try_enqueue_ordered_control(&session.control_tx, &session.control_send_lock, || {
        let frame = encrypt_authenticated_control(session, &message)?;
        Ok((frame, ()))
    })
}

fn encrypt_authenticated_control(
    session: &NativeLanSession,
    message: &NativeControlMessage,
) -> Result<crate::native_lan::EncryptedControlFrame, String> {
    let mut auth = session
        .auth
        .lock()
        .map_err(|error| format!("LAN auth state lock: {error}"))?;
    match &mut *auth {
        super::LocalAuthState::Authenticated(authenticated) => {
            authenticated.encrypt_control(message)
        }
        _ => Err("LAN control message requires an authenticated session".to_string()),
    }
}

fn fail_session(
    app: &AppHandle,
    runtime: &NativeLanRuntime,
    session: &NativeLanSession,
    reason: String,
) {
    if session.completed.load(std::sync::atomic::Ordering::Acquire) || *session.cancel_tx.borrow() {
        return;
    }
    if reason.contains("DataChannel") && (reason.contains("closed") || reason.contains("not open"))
    {
        let kind = if reason.contains("closed") {
            super::LanFailureKind::Close
        } else {
            super::LanFailureKind::Error
        };
        super::interrupt_session(app, runtime, session, reason, kind);
        return;
    }
    session.diagnostic.terminal(Tr::TransferFailure);
    if !session.claim_terminal() {
        runtime.remove_session(&session.session_id);
        return;
    }
    eprintln!("{}", super::safe_failure_log("transfer-failed", &reason));
    let failed_ack = session.transfer.lock().ok().and_then(|mut transfer| {
        transfer.incoming.take().map(|incoming| {
            let ack = cumulative_ack(&incoming, false, Some(true));
            if !incoming.complete {
                let _ = incoming.store.abort(&incoming.store_session_id);
            }
            ack
        })
    });
    if session.role == super::PairingRole::Responder {
        if let Some(ack) = failed_ack {
            let _ = send_file_chunk_ack_nowait(
                session,
                &ack.id,
                ack.next_seq,
                ack.received_bytes,
                ack.saved_bytes,
                ack.complete,
                ack.failed,
            );
        } else {
            let _ = send_authenticated_control_nowait(
                session,
                NativeControlMessage::TaskCancel {
                    task_id: session.task_id.clone(),
                },
            );
        }
    } else {
        let _ = send_authenticated_control_nowait(
            session,
            NativeControlMessage::TaskCancel {
                task_id: session.task_id.clone(),
            },
        );
    }
    emit_lan_event(
        app,
        runtime,
        "fling://lan/progress",
        task_event_with_metadata(session, "failed", 0, 0, 0, Some(reason), None),
    );
    runtime.remove_session(&session.session_id);
}

fn schedule_session_cleanup(runtime: NativeLanRuntime, session: Arc<NativeLanSession>) {
    tauri::async_runtime::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        session.diagnostic.cleanup(Tr::Completed);
        runtime.remove_session(&session.session_id);
    });
}

pub(super) fn close(session: &NativeLanSession) {
    session.diagnostic.cleanup(
        if session.completed.load(std::sync::atomic::Ordering::Acquire) {
            Tr::Completed
        } else {
            Tr::SessionRemoval
        },
    );
    if let Ok(mut transfer) = session.transfer.lock() {
        if let Some(channel) = transfer.data_channel.take() {
            session.diagnostic.local_close_requested();
            channel.close();
        }
        if let Some(peer) = transfer.pc.take() {
            session.diagnostic.local_close_requested();
            peer.close();
        }
        if let Some(incoming) = transfer.incoming.take() {
            if !incoming.complete {
                let _ = incoming.store.abort(&incoming.store_session_id);
            }
        }
    }
    session.diagnostic.snapshot();
}

#[cfg(test)]
mod contract_tests {
    #[test]
    fn diagnostic_candidate_classifier_is_allowlisted_and_matches_original_filter() {
        use crate::pairing_diagnostics::{new_pair_trace, FunnelTrace, PairRole};
        for role in [PairRole::Sender, PairRole::Receiver] {
            let trace = new_pair_trace(role, None);
            let diagnostic = FunnelTrace::new(trace.clone());
            for (candidate, transport, category, rejection) in [
                ("candidate:CANDIDATE_SENTINEL 1 udp 2122260223 192.168.246.19 49157 typ host", CandidateTransport::Udp, CandidateCategory::Host, CandidateRejection::None),
                ("candidate:CANDIDATE_SENTINEL 1 tcp 2122260223 fd12:3456:789a::42 49157 typ host tcptype passive", CandidateTransport::Tcp, CandidateCategory::Host, CandidateRejection::None),
                ("candidate:CANDIDATE_SENTINEL 1 udp 2122260223 203.0.113.42 49157 typ srflx", CandidateTransport::Udp, CandidateCategory::Other, CandidateRejection::NonHost),
                ("candidate:CANDIDATE_SENTINEL 1 udp 2122260223 203.0.113.42 49157 typ host", CandidateTransport::Udp, CandidateCategory::Host, CandidateRejection::NonLocal),
                ("SDP_SENTINEL INVALID_CANDIDATE", CandidateTransport::Unknown, CandidateCategory::Unknown, CandidateRejection::Malformed),
            ] {
                let (family, got_transport, got_category, got_rejection) = parse_candidate_summary(candidate);
                assert_eq!(got_transport, transport);
                assert_eq!(got_category, category);
                assert_eq!(got_rejection, rejection);
                assert_eq!(is_local_host_candidate(candidate), rejection == CandidateRejection::None);
                diagnostic.candidate(family, got_transport, got_category, got_rejection);
            }
            diagnostic.flush_candidates();
            let encoded = serde_json::to_string(&trace.captured_entries()).unwrap();
            for sentinel in [
                "CANDIDATE_SENTINEL",
                "192.168.246.19",
                "49157",
                "fd12:3456:789a::42",
                "203.0.113.42",
                "SDP_SENTINEL",
            ] {
                assert!(!encoded.contains(sentinel));
            }
        }
    }

    use super::*;

    #[test]
    fn concurrent_nonce_allocations_are_unique_and_ids_are_not_reused() {
        let used = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let allocations = (0..8)
            .map(|_| {
                let used = Arc::clone(&used);
                std::thread::spawn(move || {
                    (0..12)
                        .map(|_| {
                            let mut used = used.lock().unwrap();
                            fresh_transfer_nonce(&mut used).unwrap()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let nonces = allocations
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(nonces.len(), 96);
        let ids = nonces
            .iter()
            .map(|nonce| super::super::transfer_id_for_session("session", "task", nonce))
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 96);
    }

    fn manifest(session_id: &str, task_id: &str, nonce: &str) -> NativeFileManifest {
        NativeFileManifest {
            id: super::super::transfer_id_for_session(session_id, task_id, nonce),
            transfer_nonce: nonce.to_string(),
            name: "contract-test.bin".to_string(),
            size: 1,
            sha256: "a".repeat(64),
            chunk_size: NATIVE_CHUNK_SIZE as u32,
            total_chunks: 1,
            kind: "file".to_string(),
        }
    }

    #[test]
    fn matching_manifest_binding_is_accepted_and_wrong_bindings_are_rejected() {
        let session = "session-contract-sentinel";
        let task = "task-contract-sentinel";
        let nonce = "0123456789abcdef0123456789abcdef";
        let valid = manifest(session, task, nonce);
        assert!(validate_manifest_for_session(session, task, &valid).is_ok());
        assert!(validate_manifest_for_session("other-session", task, &valid).is_err());
        assert!(validate_manifest_for_session(session, "other-task", &valid).is_err());

        let mut wrong_nonce = valid.clone();
        wrong_nonce.transfer_nonce = "fedcba9876543210fedcba9876543210".to_string();
        assert!(validate_manifest_for_session(session, task, &wrong_nonce).is_err());

        let mut wrong_id = valid;
        wrong_id.id = "000000000000000000000000".to_string();
        assert!(validate_manifest_for_session(session, task, &wrong_id).is_err());
    }

    #[test]
    fn transfer_nonce_replay_is_rejected() {
        let mut seen = HashSet::new();
        let nonce = "0123456789abcdef0123456789abcdef";
        assert!(claim_transfer_nonce(&mut seen, nonce).is_ok());
        assert!(claim_transfer_nonce(&mut seen, nonce).is_err());
    }
}

#[cfg(test)]
#[path = "transport_regression_tests.rs"]
mod transport_regression_tests;
