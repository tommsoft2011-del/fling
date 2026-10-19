//! Direct P2P WebRTC link for Fling online transfers.
//!
//! Rust port of `apps/desktop/src/p2p.ts` (`P2PLink`). The sender is the
//! initiator, the receiver is the responder. SDP and ICE candidates travel
//! over the end-to-end encrypted LiveKit signaling channel (`OnlineRoom`);
//! file bytes flow over the direct `fling-file` RTCDataChannel using the
//! same FLNG binary frames as the LAN path.
//!
//! If the handshake fails or the channel drops, the caller falls back to
//! the LiveKit relay path (per-chunk routing), mirroring the TypeScript.

use std::sync::Arc;
use std::time::Duration;

use livekit::webrtc::{
    data_channel::{DataChannel, DataChannelState},
    ice_candidate::IceCandidate,
    peer_connection::OfferOptions,
    peer_connection_factory::{
        ContinualGatheringPolicy, IceServer, IceTransportsType, PeerConnectionFactory,
        RtcConfiguration,
    },
    session_description::{SdpType, SessionDescription},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};

/// Default STUN servers, matching `DEFAULT_ICE_SERVERS` in `p2p.ts`.
/// No TURN here by default: when a direct path can't be built we fall back
/// to the LiveKit relay instead of paying for a TURN server. A configured
/// TURN (config.json `turn_url`/`turn_username`/`turn_password`) is appended.
pub fn default_ice_servers(
    turn_url: Option<&str>,
    turn_username: Option<&str>,
    turn_password: Option<&str>,
) -> Vec<IceServer> {
    let mut servers = vec![
        IceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            username: String::new(),
            password: String::new(),
        },
        IceServer {
            urls: vec!["stun:stun1.l.google.com:19302".to_string()],
            username: String::new(),
            password: String::new(),
        },
    ];
    if let Some(url) = turn_url {
        if !url.trim().is_empty() {
            servers.push(IceServer {
                urls: vec![url.trim().to_string()],
                username: turn_username.unwrap_or_default().to_string(),
                password: turn_password.unwrap_or_default().to_string(),
            });
        }
    }
    servers
}

pub const P2P_DC_LABEL: &str = "fling-file";
/// P2P handshake timeout, matching the 12s `p2pReady` timeout in `tasks.ts`.
pub const P2P_CONNECT_TIMEOUT: Duration = Duration::from_secs(12);

/// The JSON shape of an ICE candidate on the signaling channel, matching
/// `RTCIceCandidate.toJSON()` as sent by `p2p.ts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalingIceCandidate {
    pub candidate: String,
    pub sdp_mid: Option<String>,
    pub sdp_mline_index: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pState {
    Idle,
    Connecting,
    Open,
    Failed,
}

/// A direct P2P link. The initiator creates the offer; the responder
/// answers. Both sides feed remote ICE candidates via `add_remote_ice`.
pub struct OnlineP2p {
    peer: livekit::webrtc::peer_connection::PeerConnection,
    _factory: PeerConnectionFactory,
    state: Arc<Mutex<P2pState>>,
    /// Fires when a local ICE candidate is gathered (JSON, or None at end).
    ice_tx: mpsc::UnboundedSender<Option<String>>,
    /// Fires when the DataChannel opens (initiator: our channel;
    /// responder: the remote channel).
    open_tx: mpsc::UnboundedSender<DataChannel>,
    open_rx: Mutex<mpsc::UnboundedReceiver<DataChannel>>,
}

impl OnlineP2p {
    fn new_common(
        ice_servers: Vec<IceServer>,
        ice_tx: mpsc::UnboundedSender<Option<String>>,
        open_tx: mpsc::UnboundedSender<DataChannel>,
        open_rx: mpsc::UnboundedReceiver<DataChannel>,
    ) -> Result<Self, String> {
        let mut config = RtcConfiguration::default();
        config.ice_servers = ice_servers;
        config.continual_gathering_policy = ContinualGatheringPolicy::GatherOnce;
        config.ice_transport_type = IceTransportsType::All;
        let factory = PeerConnectionFactory::default();
        let peer = factory
            .create_peer_connection(config)
            .map_err(|e| format!("create P2P PeerConnection: {e}"))?;

        // Forward gathered ICE candidates to the signaling channel.
        let ice_tx_cb = ice_tx.clone();
        peer.on_ice_candidate(Some(Box::new(move |candidate: IceCandidate| {
            let sig = SignalingIceCandidate {
                candidate: candidate.candidate(),
                sdp_mid: Some(candidate.sdp_mid()),
                sdp_mline_index: Some(candidate.sdp_mline_index()),
            };
            let json = serde_json::to_string(&sig).unwrap_or_default();
            let _ = ice_tx_cb.send(Some(json));
        })));

        // Connection failure surfaces via the wait_open timeout; the data
        // channel state is the authoritative signal.
        let state = Arc::new(Mutex::new(P2pState::Connecting));

        Ok(Self {
            peer,
            _factory: factory,
            state,
            ice_tx,
            open_tx,
            open_rx: Mutex::new(open_rx),
        })
    }

    /// Initiator: create the PeerConnection + DataChannel and return the
    /// offer SDP to send as `p2p-offer`. Mirrors `P2PLink.start()`.
    pub async fn start_initiator(
        ice_servers: Vec<IceServer>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        let (ice_tx, ice_rx) = mpsc::unbounded_channel();
        let (open_tx, open_rx) = mpsc::unbounded_channel();
        let link = Self::new_common(ice_servers, ice_tx, open_tx.clone(), open_rx)?;

        let dc = link
            .peer
            .create_data_channel(
                P2P_DC_LABEL,
                livekit::webrtc::data_channel::DataChannelInit::default(),
            )
            .map_err(|e| format!("create P2P DataChannel: {e}"))?;
        link.wire_data_channel(dc);

        let offer = link
            .peer
            .create_offer(OfferOptions::default())
            .await
            .map_err(|e| format!("create P2P offer: {e}"))?;
        let sdp = offer.to_string();
        link.peer
            .set_local_description(offer)
            .await
            .map_err(|e| format!("set P2P local offer: {e}"))?;
        Ok((link, ice_rx, sdp))
    }

    /// Responder: accept the initiator's offer, return the answer SDP to
    /// send as `p2p-answer`. Mirrors `P2PLink.acceptOffer()`.
    pub async fn accept_offer(
        ice_servers: Vec<IceServer>,
        offer_sdp: &str,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        let (ice_tx, ice_rx) = mpsc::unbounded_channel();
        let (open_tx, open_rx) = mpsc::unbounded_channel();
        let link = Self::new_common(ice_servers, ice_tx, open_tx.clone(), open_rx)?;

        // The responder's DataChannel arrives via on_data_channel.
        let link_open_tx = link.open_tx.clone();
        link.peer.on_data_channel(Some(Box::new(move |dc: DataChannel| {
            // Wire open-state delivery like the initiator path.
            let open_tx = link_open_tx.clone();
            let dc_for_open = dc.clone();
            dc.on_state_change(Some(Box::new(move |state| {
                if state == DataChannelState::Open {
                    let _ = open_tx.send(dc_for_open.clone());
                }
            })));
        })));

        let offer = SessionDescription::parse(offer_sdp, SdpType::Offer)
            .map_err(|e| format!("parse P2P offer: {e}"))?;
        link.peer
            .set_remote_description(offer)
            .await
            .map_err(|e| format!("set P2P remote offer: {e}"))?;
        let answer = link
            .peer
            .create_answer(livekit::webrtc::peer_connection::AnswerOptions::default())
            .await
            .map_err(|e| format!("create P2P answer: {e}"))?;
        let sdp = answer.to_string();
        link.peer
            .set_local_description(answer)
            .await
            .map_err(|e| format!("set P2P local answer: {e}"))?;
        Ok((link, ice_rx, sdp))
    }

    /// Initiator: apply the responder's answer. Mirrors the `p2p-answer`
    /// branch of `P2PLink.handleSignal()`.
    pub async fn handle_answer(&self, answer_sdp: &str) -> Result<(), String> {
        let answer = SessionDescription::parse(answer_sdp, SdpType::Answer)
            .map_err(|e| format!("parse P2P answer: {e}"))?;
        self.peer
            .set_remote_description(answer)
            .await
            .map_err(|e| format!("set P2P remote answer: {e}"))
    }

    /// Add a remote ICE candidate from a `p2p-ice` signal. `None` marks
    /// end-of-gathering. Candidates arriving before the remote description
    /// is set are queued by the caller.
    pub async fn add_remote_ice(&self, candidate_json: Option<String>) -> Result<(), String> {
        let Some(json) = candidate_json else {
            return Ok(());
        };
        if json.is_empty() {
            return Ok(());
        }
        let sig: SignalingIceCandidate =
            serde_json::from_str(&json).map_err(|e| format!("parse ICE candidate: {e}"))?;
        let candidate = IceCandidate::parse(
            sig.sdp_mid.as_deref().unwrap_or(""),
            sig.sdp_mline_index.unwrap_or(0),
            &sig.candidate,
        )
        .map_err(|e| format!("parse ICE candidate SDP: {e}"))?;
        self.peer
            .add_ice_candidate(candidate)
            .await
            .map_err(|e| format!("add remote ICE candidate: {e}"))
    }

    /// Wait for the DataChannel to open, with the P2P handshake timeout.
    /// Returns the open channel, or an error on timeout/failure so the
    /// caller can fall back to the relay path.
    pub async fn wait_open(&self) -> Result<DataChannel, String> {
        let mut rx = self.open_rx.lock().await;
        match tokio::time::timeout(P2P_CONNECT_TIMEOUT, rx.recv()).await {
            Ok(Some(dc)) => {
                *self.state.lock().await = P2pState::Open;
                Ok(dc)
            }
            Ok(None) => {
                *self.state.lock().await = P2pState::Failed;
                Err("P2P DataChannel closed before open".to_string())
            }
            Err(_) => {
                *self.state.lock().await = P2pState::Failed;
                Err(format!(
                    "P2P handshake timed out after {}s",
                    P2P_CONNECT_TIMEOUT.as_secs()
                ))
            }
        }
    }

    pub async fn state(&self) -> P2pState {
        *self.state.lock().await
    }

    fn wire_data_channel(&self, dc: DataChannel) {
        // Deliver the channel via open_tx only once it reaches Open state.
        let open_tx = self.open_tx.clone();
        let dc_for_open = dc.clone();
        dc.on_state_change(Some(Box::new(move |state| {
            if state == DataChannelState::Open {
                let _ = open_tx.send(dc_for_open.clone());
            }
        })));
    }

    pub fn close(&self) {
        self.peer.close();
    }
}
