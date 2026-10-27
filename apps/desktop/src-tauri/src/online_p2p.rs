//! Direct P2P WebRTC link for Fling online transfers.
//!
//! Rust port of `apps/desktop/src/p2p.ts` (`P2PLink`). The sender is the
//! initiator, the receiver is the responder. SDP and ICE candidates travel
//! over the end-to-end encrypted LiveKit signaling channel (`OnlineRoom`);
//! file bytes flow over the direct `fling-file` RTCDataChannel using the
//! same FLNG binary frames as the LAN path.
//!
//! Configured TURN may carry this encrypted DataChannel. The caller decides
//! whether file relay is supported; native single-file tasks do not silently
//! switch to LiveKit file relay when connection establishment fails.

use std::sync::{Arc, Mutex as StdMutex};
use crate::connection_diagnostics::{Scope, Stage, Outcome, Reason};
use std::time::{Duration, Instant};

use livekit::webrtc::{
    data_channel::{DataChannel, DataChannelState},
    ice_candidate::IceCandidate,
    peer_connection::OfferOptions,
    peer_connection_factory::{
        ContinualGatheringPolicy, IceServer, IceTransportsType, PeerConnectionFactory,
        RtcConfiguration,
    },
    session_description::{SdpType, SessionDescription},
    stats::{IceCandidatePairState, IceCandidateType, RtcStats},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};

/// Remote online ICE only: default STUN plus a complete, validated TURN
/// configuration. Pure LAN never calls this function.
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
    if let (Some(url), Some(username), Some(password)) = (turn_url, turn_username, turn_password) {
        let url = url.trim();
        if valid_turn_url(url)
            && !username.trim().is_empty()
            && !password.trim().is_empty()
            && !username.contains(['\r', '\n', '\0'])
            && !password.contains(['\r', '\n', '\0'])
        {
            // coturn answers STUN binding requests on the same host:port as
            // TURN. Offer that STUN endpoint to every PC built from this
            // list: the direct attempt gains a reflexive-candidate source on
            // infrastructure both peers already prove reachable through
            // their TURN allocations, instead of depending solely on public
            // STUN servers some networks cannot reach. The Turn mode filter
            // drops non-TURN URLs, so relay purity is unchanged.
            if let Some(stun_url) = stun_url_from_turn(url) {
                servers.push(IceServer {
                    urls: vec![stun_url],
                    username: String::new(),
                    password: String::new(),
                });
            }
            servers.push(IceServer {
                urls: vec![url.to_string()],
                username: username.to_string(),
                password: password.to_string(),
            });
        }
    }
    servers
}

fn valid_turn_url(url: &str) -> bool {
    let Some(rest) = url
        .strip_prefix("turn:")
        .or_else(|| url.strip_prefix("turns:"))
    else {
        return false;
    };
    !rest.is_empty()
        && !rest.contains('@')
        && !rest.contains('#')
        && !rest.contains(['\r', '\n', '\0', ' ', '\t'])
        && !rest.starts_with('/')
}

fn valid_stun_url(url: &str) -> bool {
    let Some(rest) = url
        .strip_prefix("stun:")
        .or_else(|| url.strip_prefix("stuns:"))
    else {
        return false;
    };
    !rest.is_empty()
        && !rest.contains('@')
        && !rest.contains('#')
        && !rest.contains(['\r', '\n', '\0', ' ', '\t'])
        && !rest.starts_with('/')
}

/// Derive the STUN URL served by the same coturn host as a TURN URL:
/// `turn:host:3478?transport=udp` -> `stun:host:3478`,
/// `turns:host:5349` -> `stuns:host:5349`. Returns None for a host part
/// that would not pass the same hygiene checks as `valid_turn_url`.
fn stun_url_from_turn(turn_url: &str) -> Option<String> {
    let (scheme, rest) = if let Some(rest) = turn_url.strip_prefix("turns:") {
        ("stuns", rest)
    } else if let Some(rest) = turn_url.strip_prefix("turn:") {
        ("stun", rest)
    } else {
        return None;
    };
    let host = rest.split(['?', ';']).next().unwrap_or_default();
    if host.is_empty()
        || host.contains('@')
        || host.contains('#')
        || host.contains(['\r', '\n', '\0', ' ', '\t'])
        || host.starts_with('/')
    {
        return None;
    }
    Some(format!("{scheme}:{host}"))
}

/// Returns true only when an SDP candidate has an exact `typ relay` token pair.
pub fn has_relay_candidate(sdp: &str) -> bool {
    sdp.lines().any(|line| {
        let tokens: Vec<&str> = line.split_ascii_whitespace().collect();
        tokens.windows(2).any(|pair| pair == ["typ", "relay"])
    })
}

// Select only the pair referenced by a transport, never any candidate merely present.
fn classify_stats(stats: &[RtcStats]) -> crate::online_diagnostics::RouteObservation {
    use crate::online_diagnostics::{Candidate, PairState, Protocol, Route, RouteObservation};
    fn candidate(t: Option<IceCandidateType>) -> Candidate {
        match t { Some(IceCandidateType::Host) => Candidate::Host,
            Some(IceCandidateType::Srflx) => Candidate::Srflx,
            Some(IceCandidateType::Prflx) => Candidate::Prflx,
            Some(IceCandidateType::Relay) => Candidate::Relay, None => Candidate::Unknown }
    }
    fn protocol(p: &str) -> Protocol {
        if p.eq_ignore_ascii_case("udp") { Protocol::Udp }
        else if p.eq_ignore_ascii_case("tcp") { Protocol::Tcp } else { Protocol::Unknown }
    }
    // Aggregates over the whole checklist, computed independently of pair
    // selection so a PC that never nominates still reports whether pairs
    // formed, what state they are stuck in, and whether srflx candidates
    // exist on either side. Counts and type presence only; no IDs.
    let mut local_types = std::collections::HashMap::new();
    let mut remote_types = std::collections::HashMap::new();
    for stat in stats {
        match stat {
            RtcStats::LocalCandidate(c) => {
                local_types.insert(c.rtc.id.as_str(), c.local_candidate.candidate_type);
            }
            RtcStats::RemoteCandidate(c) => {
                remote_types.insert(c.rtc.id.as_str(), c.remote_candidate.candidate_type);
            }
            _ => {}
        }
    }
    let mut aggregate = crate::online_diagnostics::RouteSummary::default();
    for stat in stats {
        match stat {
            RtcStats::CandidatePair(p) => {
                aggregate.pairs_total += 1;
                match p.candidate_pair.state {
                    Some(IceCandidatePairState::Succeeded) => {
                        aggregate.pairs_succeeded += 1;
                        // Split succeeded pairs by relay involvement so the
                        // diagnostic stream can prove whether a non-relay
                        // pair ever connected, not just the selected one.
                        let local_type = local_types
                            .get(p.candidate_pair.local_candidate_id.as_str())
                            .copied()
                            .flatten();
                        let remote_type = remote_types
                            .get(p.candidate_pair.remote_candidate_id.as_str())
                            .copied()
                            .flatten();
                        if let (Some(local_type), Some(remote_type)) = (local_type, remote_type) {
                            if local_type == IceCandidateType::Relay
                                || remote_type == IceCandidateType::Relay
                            {
                                aggregate.pairs_succeeded_relay += 1;
                            } else {
                                aggregate.pairs_succeeded_direct += 1;
                            }
                        }
                    }
                    Some(IceCandidatePairState::InProgress) => aggregate.pairs_in_progress += 1,
                    Some(IceCandidatePairState::Failed) => aggregate.pairs_failed += 1,
                    _ => {}
                }
            }
            RtcStats::LocalCandidate(c) => {
                if c.local_candidate.candidate_type == Some(IceCandidateType::Host) {
                    aggregate.local_host = true;
                }
                if c.local_candidate.candidate_type == Some(IceCandidateType::Srflx) {
                    aggregate.local_srflx = true;
                }
            }
            RtcStats::RemoteCandidate(c) => {
                if c.remote_candidate.candidate_type == Some(IceCandidateType::Host) {
                    aggregate.remote_host = true;
                }
                if c.remote_candidate.candidate_type == Some(IceCandidateType::Srflx) {
                    aggregate.remote_srflx = true;
                }
            }
            _ => {}
        }
    }
    for stat in stats {
        let RtcStats::Transport(transport) = stat else { continue; };
        let id = &transport.transport.selected_candidate_pair_id;
        if id.is_empty() { continue; }
        let Some(pair) = stats.iter().find_map(|s| match s {
            RtcStats::CandidatePair(p) if p.rtc.id == *id => Some(&p.candidate_pair), _ => None,
        }) else { continue; };
        let mut observation = RouteObservation { pair_key: Some(id.clone()), summary: aggregate.clone() };
        let summary = &mut observation.summary;
        summary.selected = true;
        summary.pair_state = match pair.state {
            Some(IceCandidatePairState::Succeeded) => PairState::Succeeded,
            Some(IceCandidatePairState::Failed) => PairState::Failed,
            Some(IceCandidatePairState::Frozen) => PairState::Frozen,
            Some(IceCandidatePairState::Waiting) => PairState::Waiting,
            Some(IceCandidatePairState::InProgress) => PairState::InProgress,
            None => PairState::Unknown,
        };
        summary.nominated = Some(pair.nominated);
        let local = stats.iter().find_map(|s| match s {
            RtcStats::LocalCandidate(c) if c.rtc.id == pair.local_candidate_id => Some(&c.local_candidate), _ => None,
        });
        let remote = stats.iter().find_map(|s| match s {
            RtcStats::RemoteCandidate(c) if c.rtc.id == pair.remote_candidate_id => Some(&c.remote_candidate), _ => None,
        });
        if let Some(c) = local { summary.local_type = candidate(c.candidate_type); summary.local_protocol = protocol(&c.protocol); }
        if let Some(c) = remote { summary.remote_type = candidate(c.candidate_type); summary.remote_protocol = protocol(&c.protocol); }
        if summary.pair_state == PairState::Succeeded && summary.local_type != Candidate::Unknown && summary.remote_type != Candidate::Unknown {
            summary.route = if summary.local_type == Candidate::Relay || summary.remote_type == Candidate::Relay {
                Route::TurnRelay
            } else { Route::Direct };
        }
        return observation;
    }
    // No selected pair: still surface the checklist aggregates so the
    // diagnostic stream can show pair formation/check progress instead of
    // an opaque all-unknown snapshot.
    RouteObservation { summary: aggregate, ..Default::default() }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IceMode { Legacy, Direct, Turn }

/// Filter before allocating a PC.
/// - Turn: Relay policy, TURN URLs only.
/// - Legacy (baseline session PC): the full configured list, All policy.
/// - Direct (direct-first attempt): STUN URLs only, All policy, credentials
///   cleared. Its candidate pool therefore cannot contain a relay candidate
///   at all, so ICE can never nominate a relay pair on this PC and a
///   ReadyProof earned here is genuinely non-relay.
///
/// Why Direct is STUN-only again after the v0.6.33 full-list detour: the
/// v0.6.33 field test (2026-10-22 dual-side logs) showed the full-list
/// direct PC nominating a relay pair within ~2s; the coordinator rightly
/// refuses to credit that as Direct, so the already-open connection was
/// discarded and a separate TURN PC rebuilt from scratch. The original
/// 0.6.32 STUN-only failure is not repeated here: that build's server list
/// contained NO reachable STUN (only public Google STUN, unreachable from
/// the user's network), so it gathered host candidates only. The list now
/// also carries the coturn-derived STUN endpoint, and srflx gathering
/// through it is field-verified on both peers. ReadyProof verification
/// stays in place as a second, independent guard on attribution.
fn mode_configuration(servers: Vec<IceServer>, mode: IceMode) -> Result<RtcConfiguration, String> {
    let mut config = RtcConfiguration::default();
    config.continual_gathering_policy = ContinualGatheringPolicy::GatherOnce;
    config.ice_transport_type = if mode == IceMode::Turn { IceTransportsType::Relay } else { IceTransportsType::All };
    config.ice_servers = servers.into_iter().filter_map(|mut server| {
        server.urls.retain(|url| match mode {
            IceMode::Legacy => true,
            IceMode::Direct => valid_stun_url(url),
            IceMode::Turn => valid_turn_url(url),
        });
        if mode == IceMode::Direct {
            // No TURN credentials may ride into the direct attempt, even on
            // an entry that originally mixed STUN and TURN URLs.
            server.username.clear();
            server.password.clear();
        }
        (!server.urls.is_empty()).then_some(server)
    }).collect();
    if mode == IceMode::Turn && config.ice_servers.is_empty() { return Err("未配置可用 TURN；本次连接失败，请重试".into()); }
    Ok(config)
}

/// Bound both allocation size and backlog BEFORE copying native callback data.
const MAX_INCOMING_FRAME: usize = crate::native_lan::DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES
    + crate::native_lan::DIRECT_FILE_BATCH_FRAME_HEADER_SIZE;
fn enqueue_frame(tx: &mpsc::Sender<Vec<u8>>, data: &[u8], closed: &std::sync::atomic::AtomicBool) {
    if data.len() > MAX_INCOMING_FRAME {
        closed.store(true, std::sync::atomic::Ordering::Release); return;
    }
    match tx.try_reserve() {
        Ok(permit) => permit.send(data.to_vec()),
        Err(_) => { closed.store(true, std::sync::atomic::Ordering::Release); }
    }
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
    Closed,
}

/// A direct P2P link. The initiator creates the offer; the responder
/// answers. Both sides feed remote ICE candidates via `add_remote_ice`.
pub struct OnlineP2p {
    diagnostic_dc: Arc<StdMutex<crate::connection_diagnostics::LinkState>>,
    diagnostics: Option<Scope>,
    gathering_origin: Option<Instant>,
    channel_closed: Arc<std::sync::atomic::AtomicBool>,
    peer: livekit::webrtc::peer_connection::PeerConnection,
    _factory: PeerConnectionFactory,
    state: Arc<Mutex<P2pState>>,
    /// Fires when a local ICE candidate is gathered (JSON, or None at end).
    ice_tx: mpsc::UnboundedSender<Option<String>>,
    /// Fires when the DataChannel opens (initiator: our channel;
    /// responder: the remote channel).
    open_tx: mpsc::UnboundedSender<DataChannel>,
    open_rx: Mutex<mpsc::UnboundedReceiver<DataChannel>>,
    /// Early message handler: attached to the DataChannel as soon as it is
    /// created (before Open), so no frames are dropped by libwebrtc.
    /// If not set before the channel is created, messages may be lost.
    msg_handler: Arc<StdMutex<Option<mpsc::Sender<Vec<u8>>>>>,
}

impl OnlineP2p {
    fn new_common(
        ice_servers: Vec<IceServer>,
        ice_tx: mpsc::UnboundedSender<Option<String>>,
        open_tx: mpsc::UnboundedSender<DataChannel>,
        open_rx: mpsc::UnboundedReceiver<DataChannel>,
    ) -> Result<Self, String> {
        Self::new_mode(ice_servers, IceMode::Legacy, ice_tx, open_tx, open_rx, None)
    }

    fn new_mode(ice_servers: Vec<IceServer>, mode: IceMode, ice_tx: mpsc::UnboundedSender<Option<String>>, open_tx: mpsc::UnboundedSender<DataChannel>, open_rx: mpsc::UnboundedReceiver<DataChannel>, diagnostics: Option<Scope>) -> Result<Self, String> {
        let mut phase = diagnostics.as_ref().map(|d| d.begin(Stage::PeerCreate));
        let config = mode_configuration(ice_servers, mode).map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} e})?;
        let factory = PeerConnectionFactory::default();
        let peer = factory
            .create_peer_connection(config)
            .map_err(|_| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} "create P2P PeerConnection failed".to_string()})?;

        if let Some(phase) = phase {phase.finish(Outcome::Ok, Reason::None);}

        // Forward gathered ICE candidates to the signaling channel.
        let ice_tx_cb = ice_tx.clone();
        let generated_diagnostics = diagnostics.clone();
        peer.on_ice_candidate(Some(Box::new(move |candidate: IceCandidate| {
            if let Some(scope)=&generated_diagnostics {scope.note(Stage::IceGenerated,Outcome::Observed,Reason::None);}
            let sig = SignalingIceCandidate {
                candidate: candidate.candidate(),
                sdp_mid: Some(candidate.sdp_mid()),
                sdp_mline_index: Some(candidate.sdp_mline_index()),
            };
            let json = serde_json::to_string(&sig).unwrap_or_default();
            let _ = ice_tx_cb.send(Some(json));
        })));

        // Transition events contain enums only; checklist reads remain at most 1 Hz.
        let state_scope=diagnostics.clone();
        peer.on_ice_connection_state_change(Some(Box::new(move |state| {
            use crate::connection_diagnostics::LinkState as S;
            if let Some(scope)=&state_scope {scope.transition(S::Unknown,S::from_native(state),S::Unknown);}
        })));
        let state_scope=diagnostics.clone();
        peer.on_connection_state_change(Some(Box::new(move |state| {
            use crate::connection_diagnostics::LinkState as S;
            if let Some(scope)=&state_scope {scope.transition(S::from_native(state),S::Unknown,S::Unknown);}
        })));

        // Connection failure surfaces via the wait_open timeout; the data
        // channel state is the authoritative signal.
        let state = Arc::new(Mutex::new(P2pState::Connecting));

        Ok(Self {
            diagnostic_dc: Arc::new(StdMutex::new(crate::connection_diagnostics::LinkState::Unknown)),
            diagnostics,
            gathering_origin: None,
            channel_closed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            peer,
            _factory: factory,
            state,
            ice_tx,
            open_tx,
            open_rx: Mutex::new(open_rx),
            msg_handler: Arc::new(StdMutex::new(None)),
        })
    }

    fn phase(&self, stage: Stage) -> Option<crate::connection_diagnostics::Guard> {self.diagnostics.as_ref().map(|d|d.begin(stage))}
    fn phase_ok(guard: Option<crate::connection_diagnostics::Guard>) {if let Some(g)=guard {g.finish(Outcome::Ok,Reason::None);}}

    /// Set the message handler BEFORE the DataChannel is created/opened.
    /// The handler will be attached as soon as the channel exists, preventing
    /// libwebrtc from dropping early frames (it does not queue messages
    /// received before on_message is registered).
    pub fn set_message_handler(&self, tx: mpsc::Sender<Vec<u8>>) {
        *self.msg_handler.lock().unwrap() = Some(tx);
    }

    /// Attach the early message handler to a DataChannel, if one was set.
    /// Called as soon as the channel is created (before Open).
    fn attach_early_handler(&self, dc: &DataChannel) {
        if let Some(tx) = self.msg_handler.lock().unwrap().clone() {
            let closed = self.channel_closed.clone();
            dc.on_message(Some(Box::new(move |msg| {
                if msg.binary {
                    enqueue_frame(&tx, &msg.data, &closed);
                }
            })));
        }
    }

    /// Initiator: create the PeerConnection + DataChannel and return the
    /// offer SDP to send as `p2p-offer`. Mirrors `P2PLink.start()`.
    pub async fn start_initiator(
        ice_servers: Vec<IceServer>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        Self::start_initiator_mode(ice_servers, IceMode::Legacy).await
    }

    pub async fn start_initiator_mode(ice_servers: Vec<IceServer>, mode: IceMode) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        Self::start_initiator_observed(ice_servers, mode, None).await
    }
    pub async fn start_initiator_observed(ice_servers: Vec<IceServer>, mode: IceMode, diagnostics: Option<Scope>) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        let (ice_tx, ice_rx) = mpsc::unbounded_channel();
        let (open_tx, open_rx) = mpsc::unbounded_channel();
        let mut link = Self::new_mode(ice_servers, mode, ice_tx, open_tx.clone(), open_rx, diagnostics)?;

        let mut phase = link.phase(Stage::ChannelCreate);
        let dc = link
            .peer
            .create_data_channel(
                P2P_DC_LABEL,
                livekit::webrtc::data_channel::DataChannelInit::default(),
            )
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("create P2P DataChannel: {e}")})?;
        Self::phase_ok(phase);
        link.wire_data_channel(dc);

        let mut phase = link.phase(Stage::OfferCreate);
        let offer = link
            .peer
            .create_offer(OfferOptions::default())
            .await
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("create P2P offer: {e}")})?;
        Self::phase_ok(phase);
        let sdp = offer.to_string();
        let mut phase = link.phase(Stage::OfferSet);
        link.peer
            .set_local_description(offer)
            .await
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("set P2P local offer: {e}")})?;
        link.gathering_origin = Some(Instant::now());
        Self::phase_ok(phase);
        Ok((link, ice_rx, sdp))
    }

    /// Responder: accept the initiator's offer, return the answer SDP to
    /// send as `p2p-answer`. Mirrors `P2PLink.acceptOffer()`.
    pub async fn accept_offer(
        ice_servers: Vec<IceServer>,
        offer_sdp: &str,
        frame_tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        Self::accept_offer_mode(ice_servers, IceMode::Legacy, offer_sdp, frame_tx).await
    }

    pub async fn accept_offer_mode(ice_servers: Vec<IceServer>, mode: IceMode, offer_sdp: &str, frame_tx: mpsc::Sender<Vec<u8>>) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        Self::accept_offer_observed(ice_servers, mode, offer_sdp, frame_tx, None).await
    }
    pub async fn accept_offer_observed(ice_servers: Vec<IceServer>, mode: IceMode, offer_sdp: &str, frame_tx: mpsc::Sender<Vec<u8>>, diagnostics: Option<Scope>) -> Result<(Self, mpsc::UnboundedReceiver<Option<String>>, String), String> {
        let (ice_tx, ice_rx) = mpsc::unbounded_channel();
        let (open_tx, open_rx) = mpsc::unbounded_channel();
        let mut link = Self::new_mode(ice_servers, mode, ice_tx, open_tx.clone(), open_rx, diagnostics)?;

        link.set_message_handler(frame_tx);

        // The responder's DataChannel arrives via on_data_channel.
        let link_open_tx = link.open_tx.clone();
        let link_msg_handler = link.msg_handler.clone();
        let link_closed = link.channel_closed.clone();
        let link_diagnostics = link.diagnostics.clone();
        let diagnostic_dc = link.diagnostic_dc.clone();
        link.peer
            .on_data_channel(Some(Box::new(move |dc: DataChannel| {
                if let Some(d)=&link_diagnostics {d.note(Stage::ChannelCreate,Outcome::Started,Reason::None);}
                // Attach early message handler BEFORE Open, so libwebrtc doesn't
                // drop frames that arrive before the handler is registered.
                if let Some(tx) = link_msg_handler.lock().unwrap().clone() {
                    let tx_clone = tx.clone();
                    let ingress_closed = link_closed.clone();
                    dc.on_message(Some(Box::new(move |msg| {
                        if msg.binary {
                            enqueue_frame(&tx_clone, &msg.data, &ingress_closed);
                        }
                    })));
                }
                // Wire open-state delivery like the initiator path.
                let open_tx = link_open_tx.clone();
                let dc_for_open = dc.clone();
                *diagnostic_dc.lock().unwrap()=crate::connection_diagnostics::LinkState::from_native(dc.state());
                let dc_scope=link_diagnostics.clone();
                let diagnostic_dc=diagnostic_dc.clone();
                let closed = link_closed.clone();
                dc.on_state_change(Some(Box::new(move |state| {
                    let finite=crate::connection_diagnostics::LinkState::from_native(state);
                    *diagnostic_dc.lock().unwrap()=finite;
                    if let Some(scope)=&dc_scope {scope.transition(crate::connection_diagnostics::LinkState::Unknown,crate::connection_diagnostics::LinkState::Unknown,finite);}
                    if state == DataChannelState::Closed { closed.store(true, std::sync::atomic::Ordering::Release); }
                    if state == DataChannelState::Open {
                        let _ = open_tx.send(dc_for_open.clone());
                    }
                })));
                if let Some(d)=&link_diagnostics {d.note(Stage::ChannelCreate,Outcome::Ok,Reason::None);}
            })));

        let mut phase=link.phase(Stage::OfferParse);
        let offer = SessionDescription::parse(offer_sdp, SdpType::Offer)
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("parse P2P offer: {e}")})?;
        Self::phase_ok(phase);
        let mut phase=link.phase(Stage::OfferApply);
        link.peer
            .set_remote_description(offer)
            .await
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("set P2P remote offer: {e}")})?;
        Self::phase_ok(phase);
        let mut phase=link.phase(Stage::AnswerCreate);
        let answer = link
            .peer
            .create_answer(livekit::webrtc::peer_connection::AnswerOptions::default())
            .await
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("create P2P answer: {e}")})?;
        Self::phase_ok(phase);
        let sdp = answer.to_string();
        let mut phase=link.phase(Stage::AnswerSet);
        link.peer
            .set_local_description(answer)
            .await
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("set P2P local answer: {e}")})?;
        link.gathering_origin = Some(Instant::now());
        Self::phase_ok(phase);
        Ok((link, ice_rx, sdp))
    }

    /// Initiator: apply the responder's answer. Mirrors the `p2p-answer`
    /// branch of `P2PLink.handleSignal()`.
    pub async fn handle_answer(&self, answer_sdp: &str) -> Result<(), String> {
        let mut phase=self.phase(Stage::AnswerParse);
        let answer = SessionDescription::parse(answer_sdp, SdpType::Answer)
            .map_err(|e| {if let Some(g)=phase.take() {g.finish(Outcome::Failed,Reason::OperationFailed);} format!("parse P2P answer: {e}")})?;
        Self::phase_ok(phase);
        let phase=self.phase(Stage::AnswerApply);
        let result=self.peer
            .set_remote_description(answer)
            .await
            .map_err(|e| format!("set P2P remote answer: {e}"));
        if result.is_ok() {Self::phase_ok(phase);} else if let Some(g)=phase {g.finish(Outcome::Failed,Reason::OperationFailed);}
        result
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
        crate::ice_syntax::validate(&sig.candidate)?;
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
        let phase=self.phase(Stage::ChannelWait);
        let mut rx = self.open_rx.lock().await;
        match tokio::time::timeout(P2P_CONNECT_TIMEOUT, rx.recv()).await {
            Ok(Some(dc)) => {
                Self::phase_ok(phase);
                *self.state.lock().await = P2pState::Open;
                Ok(dc)
            }
            Ok(None) => {
                if let Some(g)=phase {g.finish(Outcome::Failed,Reason::PeerClosed);}
                *self.state.lock().await = P2pState::Failed;
                Err("P2P DataChannel closed before open".to_string())
            }
            Err(_) => {
                if let Some(g)=phase {g.finish(Outcome::Failed,Reason::Timeout);}
                *self.state.lock().await = P2pState::Failed;
                Err(format!(
                    "P2P handshake timed out after {}s",
                    P2P_CONNECT_TIMEOUT.as_secs()
                ))
            }
        }
    }

    pub async fn state(&self) -> P2pState {
        use livekit::webrtc::peer_connection::PeerConnectionState;
        if self.channel_closed.load(std::sync::atomic::Ordering::Acquire) || self.peer.connection_state() == PeerConnectionState::Closed { return P2pState::Closed; }
        if self.peer.connection_state() == PeerConnectionState::Failed { return P2pState::Failed; }
        *self.state.lock().await
    }

    pub fn gathering_origin(&self) -> Instant { self.gathering_origin.expect("successful setLocalDescription source") }

    /// Native readiness, not a UI classification. Unknown or transitional
    /// stats cannot authorize transfer, including the diagnostic flicker cache.
    pub async fn ready_proof(&self, dc: &DataChannel) -> crate::online_route_choice::ReadyProof {
        use crate::online_diagnostics::{Candidate, PairState, Route};
        let observation = self.route_state().await;
        if let Some(d)=&self.diagnostics {d.pair(observation.summary.clone());}
        let s = observation.summary;
        crate::online_route_choice::ReadyProof {
            dc_open: dc.state() == DataChannelState::Open,
            pc_connected: self.peer.connection_state() == livekit::webrtc::peer_connection::PeerConnectionState::Connected,
            selected: s.selected, nominated: s.nominated == Some(true),
            succeeded: s.pair_state == PairState::Succeeded,
            known: s.local_type != Candidate::Unknown && s.remote_type != Candidate::Unknown,
            relay: s.route == Route::TurnRelay,
        }
    }

    /// Advisory only: pre-open ICE stats must NEVER authorize READY or frames.
    pub async fn diagnostic_pair_state(&self) -> crate::online_diagnostics::RouteSummary {
        match tokio::time::timeout(Duration::from_secs(2),self.peer.get_stats()).await {
            Ok(Ok(stats))=>classify_stats(&stats).summary,
            _=>Default::default(),
        }
    }

    /// A privacy-safe checklist snapshot for terminal diagnostics. `None`
    /// means stats could not be collected; `Some(default)` means collection
    /// succeeded but the report contained no candidate pairs.
    pub async fn diagnostic_pair_snapshot(&self) -> Option<crate::online_diagnostics::RouteSummary> {
        match tokio::time::timeout(Duration::from_secs(2), self.peer.get_stats()).await {
            Ok(Ok(stats)) => Some(classify_stats(&stats).summary),
            _ => None,
        }
    }

    /// Determine the selected ICE route only after the DataChannel is open.
    /// Stats are never logged or exposed, and collection is bounded.
    pub async fn route_state(&self) -> crate::online_diagnostics::RouteObservation {
        if self.state().await != P2pState::Open {
            return Default::default();
        }
        if self.peer.connection_state()
            != livekit::webrtc::peer_connection::PeerConnectionState::Connected
        {
            return Default::default();
        }
        match tokio::time::timeout(Duration::from_secs(2), self.peer.get_stats()).await {
            Ok(Ok(stats)) if self.state().await == P2pState::Open && self.peer.connection_state()
                == livekit::webrtc::peer_connection::PeerConnectionState::Connected => classify_stats(&stats),
            _ => Default::default(),
        }
    }

    fn wire_data_channel(&self, dc: DataChannel) {
        // Attach early message handler BEFORE the channel opens, so no
        // frames are dropped by libwebrtc (it doesn't queue pre-handler msgs).
        self.attach_early_handler(&dc);
        // Deliver the channel via open_tx only once it reaches Open state.
        let open_tx = self.open_tx.clone();
        let dc_for_open = dc.clone();
        *self.diagnostic_dc.lock().unwrap()=crate::connection_diagnostics::LinkState::from_native(dc.state());
        let dc_scope=self.diagnostics.clone();
        let diagnostic_dc=self.diagnostic_dc.clone();
        let closed = self.channel_closed.clone();
        dc.on_state_change(Some(Box::new(move |state| {
            let finite=crate::connection_diagnostics::LinkState::from_native(state);
            *diagnostic_dc.lock().unwrap()=finite;
            if let Some(scope)=&dc_scope {scope.transition(crate::connection_diagnostics::LinkState::Unknown,crate::connection_diagnostics::LinkState::Unknown,finite);}
            if state == DataChannelState::Closed { closed.store(true, std::sync::atomic::Ordering::Release); }
            if state == DataChannelState::Open {
                let _ = open_tx.send(dc_for_open.clone());
            }
        })));
    }

    pub async fn direct_checklist(&self, stage: Stage) {
        use crate::connection_diagnostics::{Checklist, ChecklistStatus, LinkState, checklist_status};
        let pc=LinkState::from_native(self.peer.connection_state());
        let ice=LinkState::from_native(self.peer.ice_connection_state());
        let dc=*self.diagnostic_dc.lock().unwrap();
        let (mut status,mut pair)=if pc==LinkState::Closed {
            (ChecklistStatus::AlreadyClosed,None)
        } else {
            match tokio::time::timeout(Duration::from_millis(200),self.peer.get_stats()).await {
                Ok(Ok(stats))=>{let pair=classify_stats(&stats).summary;(checklist_status(&pair),Some(pair))},
                Ok(Err(_))=>(ChecklistStatus::StatsUnavailable,None),
                Err(_)=>(ChecklistStatus::Timeout,None),
            }
        };
        let pc_after=LinkState::from_native(self.peer.connection_state());
        if pc_after==LinkState::Closed {status=ChecklistStatus::AlreadyClosed;pair=None;}
        if let Some(scope)=&self.diagnostics {scope.checklist(stage,Checklist {status,pc:pc_after,ice,dc,pair});}
    }
    pub async fn close_for_path(&self,path:crate::online_route_choice::Path) {
        if path==crate::online_route_choice::Path::Direct {
            crate::connection_diagnostics::observe_before_close(
                self.direct_checklist(Stage::DirectPreClose),||self.close()).await;
        } else {self.close();}
    }
    pub fn close(&self) {
        self.channel_closed.store(true, std::sync::atomic::Ordering::Release);
        self.peer.close();
    }
}

// Initial createOffer/setLocalDescription futures may fail or be cancelled
// before the link is installed in OnlineSession. Close that partial allocation
// as well; session-level CloseAll cannot reach a not-yet-returned link.
impl Drop for OnlineP2p {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use livekit::webrtc::stats::dictionaries;
    use livekit::webrtc::stats::{
        CandidatePairStats, LocalCandidateStats, RemoteCandidateStats, TransportStats,
    };

    fn stats(
        pair_state: Option<IceCandidatePairState>,
        local: Option<IceCandidateType>,
        remote: Option<IceCandidateType>,
        selected: bool,
    ) -> Vec<RtcStats> {
        let mut out = vec![
            RtcStats::Transport(TransportStats {
                rtc: dictionaries::RtcStats {
                    id: "transport".into(),
                    ..Default::default()
                },
                transport: dictionaries::TransportStats {
                    selected_candidate_pair_id: if selected { "pair" } else { "other" }.into(),
                    ..Default::default()
                },
            }),
            RtcStats::CandidatePair(CandidatePairStats {
                rtc: dictionaries::RtcStats {
                    id: "pair".into(),
                    ..Default::default()
                },
                candidate_pair: dictionaries::CandidatePairStats {
                    local_candidate_id: "local".into(),
                    remote_candidate_id: "remote".into(),
                    state: pair_state,
                    ..Default::default()
                },
            }),
        ];
        if let Some(t) = local {
            out.push(RtcStats::LocalCandidate(LocalCandidateStats {
                rtc: dictionaries::RtcStats {
                    id: "local".into(),
                    ..Default::default()
                },
                local_candidate: dictionaries::IceCandidateStats {
                    candidate_type: Some(t),
                    ..Default::default()
                },
            }));
        }
        if let Some(t) = remote {
            out.push(RtcStats::RemoteCandidate(RemoteCandidateStats {
                rtc: dictionaries::RtcStats {
                    id: "remote".into(),
                    ..Default::default()
                },
                remote_candidate: dictionaries::IceCandidateStats {
                    candidate_type: Some(t),
                    ..Default::default()
                },
            }));
        }
        out
    }

    #[test]
    fn turn_validation_and_credentials() {
        // 2 public STUN + 1 STUN derived from the TURN host + 1 TURN.
        assert_eq!(
            default_ice_servers(Some("turn:host:3478"), Some("u"), Some("p")).len(),
            4
        );
        for url in [
            "https://host",
            "turn://user@host",
            "turn:user:pass@host",
            "turn:/path",
            "turn:host\nInjected",
        ] {
            assert_eq!(
                default_ice_servers(Some(url), Some("u"), Some("p")).len(),
                2,
                "{url}"
            );
        }
        assert_eq!(
            default_ice_servers(Some("turn:host"), Some("u"), None).len(),
            2
        );
        assert_eq!(
            default_ice_servers(Some("turn:host"), Some(" "), Some("p")).len(),
            2
        );
    }

    #[test]
    fn turn_host_also_offers_stun_for_direct() {
        let servers = default_ice_servers(
            Some("turn:turn.example:3478?transport=udp"),
            Some("u"),
            Some("p"),
        );
        let urls: Vec<&str> = servers.iter().flat_map(|s| s.urls.iter().map(String::as_str)).collect();
        assert!(urls.contains(&"stun:turn.example:3478"), "{urls:?}");
        assert!(urls.contains(&"turn:turn.example:3478?transport=udp"), "{urls:?}");
        // The derived STUN entry carries no credentials.
        let derived = servers.iter().find(|s| s.urls.iter().any(|u| u == "stun:turn.example:3478")).unwrap();
        assert!(derived.username.is_empty() && derived.password.is_empty());
        // turns: maps to stuns:.
        assert_eq!(stun_url_from_turn("turns:turn.example:5349").as_deref(), Some("stuns:turn.example:5349"));
        assert_eq!(stun_url_from_turn("turn:turn.example:3478").as_deref(), Some("stun:turn.example:3478"));
        assert_eq!(stun_url_from_turn("turn:user@host:3478"), None);
        assert_eq!(stun_url_from_turn("stun:host:3478"), None);
        // The direct-attempt (Direct) configuration keeps only STUN URLs —
        // the derived STUN included — and carries no credentials; the
        // baseline Legacy configuration keeps the full list, and Turn mode
        // keeps only TURN URLs.
        let direct = mode_configuration(servers.clone(), IceMode::Direct).unwrap();
        assert!(direct.ice_servers.iter().any(|s| s.urls.iter().any(|u| u == "stun:turn.example:3478")));
        assert!(direct.ice_servers.iter().all(|s| s.urls.iter().all(|u| u.starts_with("stun"))));
        assert!(direct.ice_servers.iter().all(|s| s.username.is_empty() && s.password.is_empty()));
        let legacy = mode_configuration(servers.clone(), IceMode::Legacy).unwrap();
        assert!(legacy.ice_servers.iter().any(|s| s.urls.iter().any(|u| u == "turn:turn.example:3478?transport=udp")));
        let turn = mode_configuration(servers, IceMode::Turn).unwrap();
        assert!(turn.ice_servers.iter().all(|s| s.urls.iter().all(|u| !u.starts_with("stun"))));
    }

    #[test]
    fn route_classifies_only_selected_succeeded_pair() {
        use crate::online_diagnostics::{Route, Candidate, Protocol, PairState};
        for (state, local, remote, selected, expected) in [
            (Some(IceCandidatePairState::Succeeded), Some(IceCandidateType::Relay), Some(IceCandidateType::Host), true, Route::TurnRelay),
            (Some(IceCandidatePairState::Succeeded), Some(IceCandidateType::Host), Some(IceCandidateType::Srflx), true, Route::Direct),
            (Some(IceCandidatePairState::Succeeded), Some(IceCandidateType::Prflx), Some(IceCandidateType::Host), true, Route::Direct),
            (Some(IceCandidatePairState::Succeeded), Some(IceCandidateType::Relay), Some(IceCandidateType::Host), false, Route::Unknown),
            (Some(IceCandidatePairState::Failed), Some(IceCandidateType::Relay), Some(IceCandidateType::Host), true, Route::Unknown),
            (Some(IceCandidatePairState::Succeeded), Some(IceCandidateType::Relay), None, true, Route::Unknown),
        ] { assert_eq!(classify_stats(&stats(state, local, remote, selected)).summary.route, expected); }
        assert_eq!(classify_stats(&[]).summary.route, Route::Unknown);
        let mut input = stats(Some(IceCandidatePairState::Succeeded), Some(IceCandidateType::Relay), Some(IceCandidateType::Host), true);
        for item in &mut input {
            match item {
                RtcStats::CandidatePair(p) => p.candidate_pair.nominated = true,
                RtcStats::LocalCandidate(c) => { c.local_candidate.protocol = "UDP".into(); c.local_candidate.address = "192.0.2.44".into(); c.local_candidate.port = 54321; c.local_candidate.url = "turn:private".into(); },
                RtcStats::RemoteCandidate(c) => c.remote_candidate.protocol = "tcp".into(), _ => {},
            }
        }
        let summary = classify_stats(&input).summary;
        assert!(summary.selected); assert_eq!(summary.nominated, Some(true));
        assert_eq!(summary.local_type, Candidate::Relay); assert_eq!(summary.pair_state, PairState::Succeeded);
        assert_eq!(summary.local_protocol, Protocol::Udp); assert_eq!(summary.remote_protocol, Protocol::Tcp);
        let json = serde_json::to_string(&summary).unwrap();
        for forbidden in ["192.0.2.44", "54321", "turn:private", "address", "url", "port\""] { assert!(!json.contains(forbidden)); }
    }

    #[test]
    fn classify_stats_reports_checklist_aggregates_without_selected_pair() {
        // No transport points at the only pair: nothing is selected, yet the
        // aggregates must still show the pair exists, is mid-check, and that
        // an srflx candidate was gathered locally but none was received.
        let input = stats(
            Some(IceCandidatePairState::InProgress),
            Some(IceCandidateType::Srflx),
            Some(IceCandidateType::Host),
            false,
        );
        let summary = classify_stats(&input).summary;
        assert!(!summary.selected);
        assert_eq!(summary.pairs_total, 1);
        assert_eq!(summary.pairs_in_progress, 1);
        assert_eq!(summary.pairs_succeeded, 0);
        assert_eq!(summary.pairs_failed, 0);
        assert!(summary.local_srflx);
        assert!(!summary.remote_srflx);
        assert!(!summary.local_host);
        assert!(summary.remote_host);
        // And with a selected succeeded srflx pair, aggregates ride along.
        let input = stats(
            Some(IceCandidatePairState::Succeeded),
            Some(IceCandidateType::Srflx),
            Some(IceCandidateType::Srflx),
            true,
        );
        let summary = classify_stats(&input).summary;
        assert!(summary.selected);
        assert_eq!(summary.pairs_succeeded, 1);
        assert!(summary.local_srflx && summary.remote_srflx);
        // The succeeded-pair split attributes this srflx<->srflx pair as
        // direct, and a relay-involved success as relay.
        assert_eq!(summary.pairs_succeeded_direct, 1);
        assert_eq!(summary.pairs_succeeded_relay, 0);
        let input = stats(
            Some(IceCandidatePairState::Succeeded),
            Some(IceCandidateType::Relay),
            Some(IceCandidateType::Srflx),
            false,
        );
        let summary = classify_stats(&input).summary;
        assert_eq!(summary.pairs_succeeded, 1);
        assert_eq!(summary.pairs_succeeded_direct, 0);
        assert_eq!(summary.pairs_succeeded_relay, 1);
    }

    #[test]
    fn relay_sdp_requires_token_pair() {
        assert!(has_relay_candidate(
            "a=candidate:1 1 udp 1 1.2.3.4 9 typ relay raddr 0.0.0.0 rport 0"
        ));
        assert!(!has_relay_candidate(
            "a=candidate:1 1 udp 1 1.2.3.4 9 typ host"
        ));
        assert!(!has_relay_candidate("a=x relay typ host"));
    }
    #[tokio::test]
    async fn initial_effect_timeout_closes_unregistered_native_peer_connection() {
        use livekit::webrtc::peer_connection::PeerConnectionState;
        let (ice_tx, _ice_rx) = mpsc::unbounded_channel();
        let (open_tx, open_rx) = mpsc::unbounded_channel();
        let link = OnlineP2p::new_common(vec![], ice_tx, open_tx, open_rx).unwrap();
        let observer = link.peer.clone();
        let result = crate::online_setup::bounded_initial_effect(async move {
            let owned = link;
            std::future::pending::<()>().await;
            drop(owned);
            Ok::<(), String>(())
        }).await;
        assert!(result.is_err());
        tokio::time::timeout(Duration::from_secs(2), async {
            while observer.connection_state() != PeerConnectionState::Closed {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("partial native PC must close even before session registration");
    }
}

#[cfg(test)]
mod isolation_tests {
    use super::*;
    #[test]
    fn direct_attempt_is_stun_only_and_turn_is_relay_only() {
        let servers=vec![IceServer {urls:vec!["stun:stun.example:3478".into(),"turn:turn.example:3478".into()],username:"user".into(),password:"password".into()}];
        // The direct-first attempt is built with the Direct (STUN-only)
        // configuration: no TURN URL survives, so no relay candidate can
        // ever form on it, and credentials are cleared rather than merely
        // unused. ReadyProof remains as the independent attribution guard.
        let direct=mode_configuration(servers.clone(),IceMode::Direct).unwrap();
        assert_eq!(direct.ice_transport_type,IceTransportsType::All);
        assert_eq!(direct.ice_servers.len(),1);
        assert_eq!(direct.ice_servers[0].urls,vec!["stun:stun.example:3478"]);
        assert_eq!(direct.ice_servers[0].username,"");assert_eq!(direct.ice_servers[0].password,"");
        // The baseline keeps the full list with credentials untouched.
        let legacy=mode_configuration(servers.clone(),IceMode::Legacy).unwrap();
        assert_eq!(legacy.ice_servers[0].urls,vec!["stun:stun.example:3478","turn:turn.example:3478"]);
        assert_eq!(legacy.ice_servers[0].username,"user");assert_eq!(legacy.ice_servers[0].password,"password");
        let turn=mode_configuration(servers,IceMode::Turn).unwrap();
        assert_eq!(turn.ice_transport_type,IceTransportsType::Relay);
        assert_eq!(turn.ice_servers[0].urls,vec!["turn:turn.example:3478"]);
        assert_eq!(turn.ice_servers[0].username,"user");
        assert!(mode_configuration(vec![],IceMode::Turn).is_err());
        assert!(mode_configuration(vec![],IceMode::Legacy).unwrap().ice_servers.is_empty());
        assert!(mode_configuration(vec![],IceMode::Direct).unwrap().ice_servers.is_empty());
    }
    #[test]
    fn callback_queue_is_bounded_before_copy_and_overflow_fences_link() {
        use std::sync::atomic::{AtomicBool,Ordering};
        let (tx,mut rx)=mpsc::channel(1);let closed=AtomicBool::new(false);
        enqueue_frame(&tx,&vec![0;MAX_INCOMING_FRAME+1],&closed);
        assert!(closed.load(Ordering::Acquire));assert!(rx.try_recv().is_err());
        closed.store(false,Ordering::Release);
        enqueue_frame(&tx,b"first",&closed);assert!(!closed.load(Ordering::Acquire));
        enqueue_frame(&tx,b"overflow",&closed);assert!(closed.load(Ordering::Acquire));
        assert_eq!(rx.try_recv().unwrap(),b"first");assert!(rx.try_recv().is_err());
    }
}

#[cfg(test)]
mod preclose_diagnostic_regression {
    use super::*;
    #[tokio::test] async fn real_direct_pc_is_sampled_before_native_close() {
        use crate::connection_diagnostics::Diagnostics;
        use crate::online_route_choice::{Path,Role};
        let records=Arc::new(StdMutex::new(Vec::new()));let output=records.clone();
        let diagnostics=Diagnostics::new(Role::Sender,Arc::new(move |event| {
            output.lock().unwrap().push(serde_json::to_value(event).unwrap());
        }));
        let (link,_,_)=OnlineP2p::start_initiator_observed(vec![],IceMode::Direct,Some(diagnostics.scope(Path::Direct))).await.unwrap();
        link.close_for_path(Path::Direct).await;
        let records=records.lock().unwrap();
        let snapshot=records.iter().find(|e|e["stage"]=="direct_pre_close").expect("production close snapshot");
        assert_ne!(snapshot["checklist"]["pc"],"closed");
        assert_ne!(snapshot["checklist"]["status"],"already_closed");
        assert_eq!(link.peer.connection_state(),livekit::webrtc::peer_connection::PeerConnectionState::Closed);
    }
}
