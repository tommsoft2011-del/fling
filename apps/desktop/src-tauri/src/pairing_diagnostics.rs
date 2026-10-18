use serde::Serialize;
use sha2::{Digest, Sha256};
use std::net::{IpAddr, SocketAddr};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::Instant,
};

const MAX_TRACES: usize = 128;
static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(1);
static TRACES: OnceLock<Mutex<VecDeque<PairTrace>>> = OnceLock::new();
static FINGERPRINT_SALT: OnceLock<[u8; 32]> = OnceLock::new();

// A process-local secret salt prevents offline guessing of low-entropy service
// names. Neither salt nor original identifiers are serialized; restarts unlink them.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(transparent)]
pub struct SourceFingerprint(String);

impl SourceFingerprint {
    pub fn from_fullname(fullname: &str) -> Self {
        let mut hash = Sha256::new();
        hash.update(FINGERPRINT_SALT.get_or_init(rand::random));
        hash.update(b"fling/pair-candidate-source/v1\0");
        hash.update((fullname.len() as u64).to_be_bytes());
        hash.update(fullname.as_bytes());
        Self(
            hash.finalize()[..12]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
        )
    }
}

fn address_class(ip: IpAddr) -> &'static str {
    match ip {
        IpAddr::V4(ip) if ip.is_unspecified() => "unspecified",
        IpAddr::V4(ip) if ip.is_loopback() => "loopback",
        IpAddr::V4(ip) if ip.is_private() => "private_ipv4",
        IpAddr::V4(ip) if ip.is_link_local() => "link_local_ipv4",
        IpAddr::V4(ip) if ip.is_multicast() => "multicast",
        IpAddr::V6(ip) if ip.is_unspecified() => "unspecified",
        IpAddr::V6(ip) if ip.is_loopback() => "loopback",
        IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some() => "ipv4_mapped_ipv6",
        IpAddr::V6(ip) if (ip.segments()[0] & 0xfe00) == 0xfc00 => "ula_ipv6",
        IpAddr::V6(ip) if (ip.segments()[0] & 0xffc0) == 0xfe80 => "link_local_ipv6",
        IpAddr::V6(ip) if ip.is_multicast() => "multicast",
        _ => "other",
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateDiagnostic {
    // One-based position in the original sorted/deduplicated candidate snapshot,
    // including ineligible entries. The connect records retain this exact index.
    index: u32,
    family: &'static str,
    address_class: &'static str,
    port_nonzero: bool,
    eligible: bool,
    source_fingerprints: Vec<SourceFingerprint>,
    source: &'static str,
    cache_origin: &'static str,
    freshness: &'static str,
    route_class: &'static str,
    interface_class: &'static str,
}

impl CandidateDiagnostic {
    pub fn new(
        index: usize,
        endpoint: SocketAddr,
        eligible: bool,
        mut sources: Vec<SourceFingerprint>,
    ) -> Self {
        sources.sort_unstable();
        sources.dedup();
        Self {
            index: index.min(u32::MAX as usize) as u32,
            family: if endpoint.is_ipv4() { "ipv4" } else { "ipv6" },
            address_class: address_class(endpoint.ip()),
            port_nonzero: endpoint.port() != 0,
            eligible,
            source_fingerprints: sources,
            source: "peer_directory_from_mdns_service_resolved",
            cache_origin: "unknown",
            freshness: "unknown",
            route_class: "unknown",
            interface_class: "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PairRole {
    Sender,
    Receiver,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PairStage {
    UiButton,
    RequestStart,
    Endpoints,
    TcpConnect,
    HelloSend,
    ResponseReceive,
    IdentityValidation,
    SecureSession,
    SessionInstall,
    PromptEmit,
    TcpAccept,
    HelloReceive,
    HelloValidation,
    ResponseSend,
    UiPrompt,
    PostPrompt,
    DecisionLocal,
    PeerDecisionReceived,
    AuthConfirmed,
    PeerConnection,
    Offer,
    Answer,
    IceGathering,
    IceConnection,
    DataChannel,
    Manifest,
    FirstFrame,
    UiModalMounted,
    UiModalVisible,
    UiDecisionConfirmed,
    UiDecisionRejected,
    UiDecisionCancelled,
    IceCandidateSummary,
    Transport,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PairState {
    Start,
    Ok,
    Timeout,
    Error,
    Rejected,
    Closed,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PairReason {
    None,
    LanDisabled,
    SourceMissing,
    SourceRegistryUnavailable,
    PeerUnavailable,
    EndpointNotEligible,
    ConnectRefused,
    ConnectTimeout,
    NetworkUnreachable,
    PermissionDenied,
    Io,
    HelloTimeout,
    HelloReadOrInvalid,
    HelloWrite,
    UnexpectedPeer,
    ReplayRejected,
    ConfigUnavailable,
    KeyAgreementRejected,
    SessionInstall,
    PromptEmitFailed,
    DecisionRejected,
    DecisionInvalid,
    CodeMismatch,
    ControlWriteFailed,
    ControlWriteTimeout,
    LocalOperationFailed,
    NotReported,
}

#[derive(Clone, Debug)]
pub struct PairTrace {
    attempt: u64,
    role: PairRole,
    ui_attempt: Option<u32>,
    started: Instant,
    #[cfg(test)]
    captured: std::sync::Arc<Mutex<Vec<PairEntry>>>,
    funnel_events: std::sync::Arc<Mutex<HashSet<FunnelEvent>>>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PairEntry {
    attempt: u64,
    role: PairRole,
    #[serde(skip_serializing_if = "Option::is_none")]
    ui_attempt: Option<u32>,
    attempt_elapsed_ms: u64,
    stage: PairStage,
    state: PairState,
    reason: PairReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate: Option<CandidateDiagnostic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_address_class: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reachability: Option<&'static str>,
    evidence: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    funnel_event: Option<FunnelEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code_match: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_counts: Option<CandidateCounts>,
    #[serde(skip_serializing_if = "Option::is_none")]
    approved: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transport: Option<crate::transport_diagnostics::Detail>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FunnelEvent {
    DecisionLocalSubmittedApproved,
    DecisionLocalSubmittedRejected,
    DecisionLocalUnavailable,
    UiModalMounted,
    UiModalVisible,
    UiDecisionConfirmed,
    UiDecisionRejected,
    UiDecisionCancelled,
    DecisionLocalApprovedMatch,
    DecisionLocalApprovedMismatch,
    DecisionLocalRejected,
    PeerDecisionConfirmed,
    PeerDecisionRejected,
    PeerDecisionInvalid,
    AuthConfirmed,
    PeerConnectionStart,
    PeerConnectionOk,
    PeerConnectionError,
    OfferGenerated,
    OfferError,
    OfferQueued,
    OfferSent,
    OfferWriteStart,
    OfferWriteError,
    OfferWriteTimeout,
    OfferReceived,
    AnswerGenerated,
    AnswerError,
    AnswerQueued,
    AnswerSent,
    AnswerWriteStart,
    AnswerWriteError,
    AnswerWriteTimeout,
    AnswerReceived,
    IceCandidateWritten,
    IceGatheringNew,
    IceGatheringGathering,
    IceGatheringComplete,
    IceConnectionNew,
    IceConnectionChecking,
    IceConnectionConnected,
    IceConnectionCompleted,
    IceConnectionFailed,
    IceConnectionDisconnected,
    IceConnectionClosed,
    IceConnectionUnknown,
    DataChannelCreated,
    DataChannelReceived,
    DataChannelOpen,
    DataChannelClose,
    DataChannelError,
    ManifestQueued,
    ManifestSent,
    ManifestWriteStart,
    ManifestWriteError,
    ManifestWriteTimeout,
    ManifestReceived,
    ManifestAccepted,
    ManifestDenied,
    ManifestPeerAccepted,
    FirstFrameSendAttempt,
    FirstFrameNativeAccepted,
    FirstFrameSendError,
    FirstFrameReceiverArrival,
    FirstFrameAppendOk,
    FirstFrameAppendError,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateFamily {
    Ipv4,
    Ipv6,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateTransport {
    Udp,
    Tcp,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateCategory {
    Host,
    Other,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRejection {
    None,
    NonHost,
    NonLocal,
    Malformed,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateCounts {
    ipv4: u32,
    ipv6: u32,
    unknown_family: u32,
    udp: u32,
    tcp: u32,
    unknown_transport: u32,
    host: u32,
    other: u32,
    unknown_category: u32,
    accepted: u32,
    non_host: u32,
    non_local: u32,
    malformed: u32,
}
#[derive(Default)]
struct FunnelState {
    transport: crate::transport_diagnostics::State,
    candidates: HashMap<
        (
            CandidateFamily,
            CandidateTransport,
            CandidateCategory,
            CandidateRejection,
        ),
        u32,
    >,
    candidates_flushed: bool,
}
#[derive(Clone)]
pub struct FunnelTrace {
    trace: PairTrace,
    state: std::sync::Arc<Mutex<FunnelState>>,
}
impl FunnelTrace {
    pub fn new(trace: PairTrace) -> Self {
        Self {
            trace,
            state: std::sync::Arc::new(Mutex::new(FunnelState::default())),
        }
    }
    pub fn record(&self, event: FunnelEvent) {
        if !self
            .trace
            .funnel_events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(event)
        {
            return;
        }
        let (stage, pair_state, reason, approved, code_match) = funnel_mapping(event);
        let entry = PairEntry {
            attempt: self.trace.attempt,
            role: self.trace.role,
            ui_attempt: self.trace.ui_attempt,
            attempt_elapsed_ms: self
                .trace
                .started
                .elapsed()
                .as_millis()
                .min(u64::MAX as u128) as u64,
            stage,
            state: pair_state,
            reason,
            endpoint_count: None,
            candidate: None,
            local_address_class: None,
            reachability: None,
            evidence: match event {
                FunnelEvent::UiModalMounted | FunnelEvent::UiModalVisible => {
                    "application_dom_state_not_physical_screen_proof"
                }
                FunnelEvent::UiDecisionConfirmed
                | FunnelEvent::UiDecisionRejected
                | FunnelEvent::UiDecisionCancelled => {
                    "application_button_handler_not_native_decision_or_auth_proof"
                }
                FunnelEvent::OfferSent | FunnelEvent::AnswerSent | FunnelEvent::ManifestSent => {
                    "control_write_returned_ok_not_peer_receipt_or_nic_proof"
                }
                FunnelEvent::FirstFrameNativeAccepted => {
                    "native_send_returned_ok_not_receiver_or_nic_proof"
                }
                FunnelEvent::AuthConfirmed => {
                    "both_authenticated_pair_decisions_not_transport_proof"
                }
                _ => "post_prompt_application_observation_not_nic_proof",
            },
            funnel_event: Some(event),
            code_match,
            candidate_counts: None,
            approved,
            transport: None,
        };
        self.trace.emit(entry);
    }
    pub fn detail(&self, detail: crate::transport_diagnostics::Detail) {
        let mut entry = self.trace.base_transport_entry();
        entry.transport = Some(detail);
        self.trace.emit(entry);
    }
    pub fn phase(
        &self,
        phase: crate::transport_diagnostics::Phase,
        outcome: crate::transport_diagnostics::Outcome,
        reason: crate::transport_diagnostics::Reason,
        count: usize,
    ) {
        self.detail(crate::transport_diagnostics::Detail::new(
            phase, outcome, reason, count,
        ));
    }
    pub fn count(&self, step: crate::transport_diagnostics::CandidateStep, n: usize) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .transport
            .count(step, n);
    }
    pub fn candidate_rejected(&self, reason: crate::transport_diagnostics::Reason) {
        use crate::transport_diagnostics::CandidateStep;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.transport.count(CandidateStep::Rejected, 1);
        let n = state.transport.rejections.entry(reason).or_default();
        *n = n.saturating_add(1);
    }
    pub fn rejected_candidate_context(
        &self,
        observation: crate::transport_diagnostics::RejectedCandidate,
    ) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .transport
            .first_rejected_candidate
            .get_or_insert(observation);
        // No per-candidate event. One immutable slot appears in existing terminal snapshots.
    }
    pub fn terminal(&self, reason: crate::transport_diagnostics::Reason) -> bool {
        use crate::transport_diagnostics::{Detail, Outcome, Phase};
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.transport.first_terminal.is_some() {
            return false;
        }
        state.transport.first_terminal = Some(reason);
        let mut detail = Detail::new(
            Phase::FirstTerminal,
            if reason == crate::transport_diagnostics::Reason::Completed {
                Outcome::Ok
            } else {
                Outcome::Error
            },
            reason,
            0,
        );
        detail.first_terminal = Some(reason);
        // Emit while holding the diagnostic-only lock: first terminal is ordered
        // before a concurrent cleanup/close, and cannot be overwritten.
        self.detail(detail);
        drop(state);
        self.snapshot();
        self.flush_candidates();
        true
    }
    pub fn local_close_requested(&self) {
        use crate::transport_diagnostics::{CloseOrigin, Detail, Outcome, Phase, Reason};
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.transport.local_close_requested {
            return;
        }
        state.transport.local_close_requested = true;
        let mut detail = Detail::new(
            Phase::LocalCloseRequested,
            Outcome::Start,
            state.transport.cleanup.unwrap_or(Reason::NotReported),
            0,
        );
        detail.close_origin = Some(CloseOrigin::LocalCleanup);
        detail.close_observed = Some(false);
        detail.local_close_requested = Some(true);
        self.detail(detail);
    }
    pub fn cleanup(&self, reason: crate::transport_diagnostics::Reason) {
        use crate::transport_diagnostics::{Detail, Outcome, Phase};
        self.terminal(reason);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.transport.cleanup.is_some() {
            return;
        }
        state.transport.cleanup = Some(reason);
        let mut detail = Detail::new(Phase::Cleanup, Outcome::Start, reason, 0);
        detail.first_terminal = state.transport.first_terminal;
        self.detail(detail);
        drop(state);
        self.snapshot();
        self.flush_candidates();
    }
    pub fn snapshot(&self) {
        use crate::transport_diagnostics::{Detail, Outcome, Phase, Reason};
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut detail = Detail::new(Phase::CandidateSummary, Outcome::Snapshot, Reason::None, 0);
        detail.candidate_steps = Some(state.transport.steps.clone());
        detail.candidate_rejections = Some(state.transport.rejections.clone());
        detail.first_rejected_candidate = state.transport.first_rejected_candidate;
        self.detail(detail);
    }
    pub fn transition(
        &self,
        phase: crate::transport_diagnostics::Phase,
        outcome: crate::transport_diagnostics::Outcome,
    ) {
        use crate::transport_diagnostics::{CloseOrigin, Detail, Outcome, Phase, Reason};
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Initial and subsequent DC states share a single previous-state slot.
        let key = if phase == Phase::DataChannelInitial {
            Phase::DataChannelTransition
        } else {
            phase
        };
        if state.transport.transitions.get(&key) == Some(&outcome) {
            return;
        }
        state.transport.transitions.insert(key, outcome);
        let mut detail = Detail::new(phase, outcome, Reason::NotReported, 0);
        if matches!(
            phase,
            Phase::DataChannelInitial | Phase::DataChannelTransition
        ) {
            detail.close_observed = Some(outcome == Outcome::Closed);
            // A local request and a later Closed callback are not causal proof.
            detail.close_origin = Some(CloseOrigin::Unknown);
            detail.local_close_requested = Some(state.transport.local_close_requested);
        }
        self.detail(detail);
        let terminal = match (phase, outcome) {
            (Phase::IceConnection, Outcome::Failed) => Some(Reason::IceFailed),
            (Phase::IceConnection, Outcome::Disconnected) => Some(Reason::IceDisconnected),
            (Phase::IceConnection, Outcome::Closed) => Some(Reason::IceClosed),
            (Phase::DataChannelInitial | Phase::DataChannelTransition, Outcome::Closed) => {
                Some(Reason::DataChannelClosed)
            }
            _ => None,
        };
        let first = terminal.filter(|_| {
            state.transport.cleanup.is_none() && state.transport.first_terminal.is_none()
        });
        if let Some(reason) = first {
            state.transport.first_terminal = Some(reason);
            let mut detail = Detail::new(Phase::FirstTerminal, Outcome::Error, reason, 0);
            detail.first_terminal = Some(reason);
            self.detail(detail);
        }
        drop(state);
        if first.is_some() {
            self.snapshot();
            self.flush_candidates();
        }
    }

    pub fn candidate(
        &self,
        family: CandidateFamily,
        transport: CandidateTransport,
        category: CandidateCategory,
        rejection: CandidateRejection,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.candidates_flushed {
            *state
                .candidates
                .entry((family, transport, category, rejection))
                .or_insert(0);
            *state
                .candidates
                .get_mut(&(family, transport, category, rejection))
                .unwrap() = state
                .candidates
                .get(&(family, transport, category, rejection))
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
        }
    }
    pub fn flush_candidates(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.candidates_flushed {
            return;
        }
        state.candidates_flushed = true;
        let mut c = CandidateCounts {
            ipv4: 0,
            ipv6: 0,
            unknown_family: 0,
            udp: 0,
            tcp: 0,
            unknown_transport: 0,
            host: 0,
            other: 0,
            unknown_category: 0,
            accepted: 0,
            non_host: 0,
            non_local: 0,
            malformed: 0,
        };
        for ((f, t, k, r), n) in state.candidates.drain() {
            match f {
                CandidateFamily::Ipv4 => c.ipv4 = c.ipv4.saturating_add(n),
                CandidateFamily::Ipv6 => c.ipv6 = c.ipv6.saturating_add(n),
                CandidateFamily::Unknown => c.unknown_family = c.unknown_family.saturating_add(n),
            };
            match t {
                CandidateTransport::Udp => c.udp = c.udp.saturating_add(n),
                CandidateTransport::Tcp => c.tcp = c.tcp.saturating_add(n),
                CandidateTransport::Unknown => {
                    c.unknown_transport = c.unknown_transport.saturating_add(n)
                }
            };
            match k {
                CandidateCategory::Host => c.host = c.host.saturating_add(n),
                CandidateCategory::Other => c.other = c.other.saturating_add(n),
                CandidateCategory::Unknown => {
                    c.unknown_category = c.unknown_category.saturating_add(n)
                }
            };
            match r {
                CandidateRejection::None => c.accepted = c.accepted.saturating_add(n),
                CandidateRejection::NonHost => c.non_host = c.non_host.saturating_add(n),
                CandidateRejection::NonLocal => c.non_local = c.non_local.saturating_add(n),
                CandidateRejection::Malformed => c.malformed = c.malformed.saturating_add(n),
            };
        }
        self.trace.emit(PairEntry {
            attempt: self.trace.attempt,
            role: self.trace.role,
            ui_attempt: self.trace.ui_attempt,
            attempt_elapsed_ms: self
                .trace
                .started
                .elapsed()
                .as_millis()
                .min(u64::MAX as u128) as u64,
            stage: PairStage::IceCandidateSummary,
            state: PairState::Ok,
            reason: PairReason::None,
            endpoint_count: None,
            candidate: None,
            local_address_class: None,
            reachability: None,
            evidence: "local_ice_gathering_counts_not_selected_pair_or_route_proof",
            funnel_event: None,
            code_match: None,
            candidate_counts: Some(c),
            approved: None,
            transport: None,
        });
    }
}
fn funnel_mapping(
    e: FunnelEvent,
) -> (PairStage, PairState, PairReason, Option<bool>, Option<bool>) {
    use FunnelEvent::*;
    let stage = match e {
        DecisionLocalSubmittedApproved
        | DecisionLocalSubmittedRejected
        | DecisionLocalUnavailable
        | DecisionLocalApprovedMatch
        | DecisionLocalApprovedMismatch
        | DecisionLocalRejected => PairStage::DecisionLocal,
        PeerDecisionConfirmed | PeerDecisionRejected | PeerDecisionInvalid => {
            PairStage::PeerDecisionReceived
        }
        AuthConfirmed => PairStage::AuthConfirmed,
        PeerConnectionStart | PeerConnectionOk | PeerConnectionError => PairStage::PeerConnection,
        OfferGenerated | OfferError | OfferQueued | OfferSent | OfferReceived | OfferWriteStart
        | OfferWriteError | OfferWriteTimeout => PairStage::Offer,
        AnswerGenerated | AnswerError | AnswerQueued | AnswerSent | AnswerReceived
        | AnswerWriteStart | AnswerWriteError | AnswerWriteTimeout => PairStage::Answer,
        IceCandidateWritten => PairStage::IceCandidateSummary,
        IceGatheringNew | IceGatheringGathering | IceGatheringComplete => PairStage::IceGathering,
        IceConnectionNew
        | IceConnectionChecking
        | IceConnectionConnected
        | IceConnectionCompleted
        | IceConnectionFailed
        | IceConnectionDisconnected
        | IceConnectionClosed
        | IceConnectionUnknown => PairStage::IceConnection,
        DataChannelCreated | DataChannelReceived | DataChannelOpen | DataChannelClose
        | DataChannelError => PairStage::DataChannel,
        ManifestWriteStart | ManifestWriteError | ManifestWriteTimeout | ManifestQueued
        | ManifestSent | ManifestReceived | ManifestAccepted | ManifestDenied
        | ManifestPeerAccepted => PairStage::Manifest,
        FirstFrameSendAttempt
        | FirstFrameNativeAccepted
        | FirstFrameSendError
        | FirstFrameReceiverArrival
        | FirstFrameAppendOk
        | FirstFrameAppendError => PairStage::FirstFrame,
        UiModalMounted => PairStage::UiModalMounted,
        UiModalVisible => PairStage::UiModalVisible,
        UiDecisionConfirmed => PairStage::UiDecisionConfirmed,
        UiDecisionRejected => PairStage::UiDecisionRejected,
        UiDecisionCancelled => PairStage::UiDecisionCancelled,
    };
    let state = match e {
        DecisionLocalSubmittedApproved
        | DecisionLocalSubmittedRejected
        | PeerConnectionStart
        | OfferWriteStart
        | AnswerWriteStart
        | ManifestWriteStart
        | FirstFrameSendAttempt => PairState::Start,
        OfferWriteTimeout | AnswerWriteTimeout | ManifestWriteTimeout => PairState::Timeout,
        OfferWriteError | AnswerWriteError | ManifestWriteError | OfferError | AnswerError => {
            PairState::Error
        }
        DecisionLocalUnavailable
        | DecisionLocalRejected
        | PeerDecisionRejected
        | PeerDecisionInvalid
        | PeerConnectionError
        | IceConnectionFailed
        | DataChannelError
        | ManifestDenied
        | FirstFrameSendError
        | FirstFrameAppendError => PairState::Error,
        DataChannelClose | IceConnectionClosed => PairState::Closed,
        DecisionLocalApprovedMismatch => PairState::Rejected,
        _ => PairState::Ok,
    };
    let reason = match e {
        OfferWriteTimeout | AnswerWriteTimeout | ManifestWriteTimeout => {
            PairReason::ControlWriteTimeout
        }
        OfferWriteError | AnswerWriteError | ManifestWriteError => PairReason::ControlWriteFailed,
        OfferError | AnswerError => PairReason::LocalOperationFailed,
        DecisionLocalApprovedMismatch => PairReason::CodeMismatch,
        DecisionLocalRejected | PeerDecisionRejected | ManifestDenied => {
            PairReason::DecisionRejected
        }
        DataChannelClose => PairReason::NotReported,
        PeerDecisionInvalid => PairReason::DecisionInvalid,
        _ => PairReason::None,
    };
    let approved = match e {
        DecisionLocalSubmittedApproved
        | DecisionLocalApprovedMatch
        | DecisionLocalApprovedMismatch => Some(true),
        DecisionLocalSubmittedRejected | DecisionLocalRejected => Some(false),
        _ => None,
    };
    let code_match = match e {
        DecisionLocalApprovedMatch => Some(true),
        DecisionLocalApprovedMismatch => Some(false),
        _ => None,
    };
    (stage, state, reason, approved, code_match)
}

impl PairTrace {
    fn base_transport_entry(&self) -> PairEntry {
        PairEntry {
            attempt: self.attempt,
            role: self.role,
            ui_attempt: self.ui_attempt,
            attempt_elapsed_ms: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            stage: PairStage::Transport,
            state: PairState::Start,
            reason: PairReason::None,
            endpoint_count: None,
            candidate: None,
            local_address_class: None,
            reachability: None,
            evidence: "typed_transport_observation_not_root_cause_or_remote_close_proof",
            funnel_event: None,
            code_match: None,
            candidate_counts: None,
            approved: None,
            transport: None,
        }
    }
    pub fn id(&self) -> u64 {
        self.attempt
    }
    pub fn record(
        &self,
        stage: PairStage,
        state: PairState,
        reason: PairReason,
        endpoint_count: Option<u32>,
    ) {
        let entry = PairEntry {
            attempt: self.attempt,
            role: self.role,
            ui_attempt: self.ui_attempt,
            attempt_elapsed_ms: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            stage,
            state,
            reason,
            endpoint_count,
            candidate: None,
            local_address_class: None,
            reachability: None,
            evidence: "application_stage_not_nic_or_user_ack",
            funnel_event: None,
            code_match: None,
            candidate_counts: None,
            approved: None,
            transport: None,
        };
        // A pairing attempt is not a discovery round. Its independent local sequence
        // and monotonic clock are explicit, without any session/task/device identity.
        self.emit(entry);
    }

    fn emit(&self, entry: PairEntry) {
        #[cfg(test)]
        self.captured.lock().unwrap().push(entry.clone());
        crate::discovery_diagnostics::record_for_round(
            0,
            0,
            crate::discovery_diagnostics::Event::Pairing(entry),
        );
    }

    pub fn candidate_entry(
        &self,
        stage: PairStage,
        state: PairState,
        reason: PairReason,
        candidate: &CandidateDiagnostic,
        local: Option<SocketAddr>,
    ) -> PairEntry {
        PairEntry {
            attempt: self.attempt,
            role: self.role,
            ui_attempt: self.ui_attempt,
            attempt_elapsed_ms: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            stage,
            state,
            reason,
            endpoint_count: None,
            candidate: Some(candidate.clone()),
            local_address_class: if stage == PairStage::TcpConnect && state == PairState::Ok {
                Some(
                    local
                        .map(|addr| address_class(addr.ip()))
                        .unwrap_or("unknown"),
                )
            } else {
                None
            },
            reachability: if stage != PairStage::TcpConnect {
                None
            } else {
                Some(match state {
                    PairState::Start => "pending",
                    PairState::Ok => "tcp_connected_not_pairhello_or_nic_proof",
                    PairState::Timeout => "timeout_not_proof_of_no_route_or_no_syn",
                    _ => "os_connect_error_not_nic_proof",
                })
            },
            evidence: "application_stage_not_nic_or_user_ack",
            funnel_event: None,
            code_match: None,
            candidate_counts: None,
            approved: None,
            transport: None,
        }
    }

    pub fn record_candidate(
        &self,
        stage: PairStage,
        state: PairState,
        reason: PairReason,
        candidate: &CandidateDiagnostic,
        local: Option<SocketAddr>,
        endpoint_count: Option<u32>,
    ) {
        let mut entry = self.candidate_entry(stage, state, reason, candidate, local);
        entry.endpoint_count = endpoint_count;
        self.emit(entry);
    }

    #[cfg(test)]
    pub(crate) fn captured_entries(&self) -> Vec<PairEntry> {
        self.captured.lock().unwrap().clone()
    }
}

pub fn new_pair_trace(role: PairRole, ui_attempt: Option<u32>) -> PairTrace {
    let trace = PairTrace {
        attempt: NEXT_ATTEMPT.fetch_add(1, Ordering::Relaxed),
        role,
        ui_attempt,
        started: Instant::now(),
        #[cfg(test)]
        captured: Default::default(),
        funnel_events: Default::default(),
    };
    let mut registry = TRACES
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    store_trace(&mut registry, trace.clone());
    trace
}

fn store_trace(registry: &mut VecDeque<PairTrace>, trace: PairTrace) {
    if registry.len() >= MAX_TRACES {
        registry.pop_front();
    }
    registry.push_back(trace);
}

pub fn record_ui_button(ui_attempt: u32) {
    crate::discovery_diagnostics::record_for_round(
        0,
        0,
        crate::discovery_diagnostics::Event::PairingUiButton { ui_attempt },
    );
}

pub fn record_funnel_ui(attempt: u64, event: FunnelEvent) -> bool {
    if !matches!(
        event,
        FunnelEvent::UiModalMounted
            | FunnelEvent::UiModalVisible
            | FunnelEvent::UiDecisionConfirmed
            | FunnelEvent::UiDecisionRejected
            | FunnelEvent::UiDecisionCancelled
    ) {
        return false;
    }
    let trace = TRACES
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .find(|t| t.attempt == attempt)
        .cloned();
    if let Some(trace) = trace {
        FunnelTrace::new(trace).record(event);
        true
    } else {
        false
    }
}

pub fn record_ui_prompt(attempt: u64) -> bool {
    let trace = TRACES
        .get_or_init(|| Mutex::new(VecDeque::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .find(|trace| trace.attempt == attempt)
        .cloned();
    if let Some(trace) = trace {
        trace.record(PairStage::UiPrompt, PairState::Ok, PairReason::None, None);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn candidate_categories_ports_and_process_local_fingerprints_are_allowlisted() {
        for (socket, family, class, port) in [
            ("192.168.246.19:49157", "ipv4", "private_ipv4", true),
            ("169.254.2.3:0", "ipv4", "link_local_ipv4", false),
            ("[fd12:3456:789a::42]:49157", "ipv6", "ula_ipv6", true),
            ("[fe80::42]:0", "ipv6", "link_local_ipv6", false),
            ("127.0.0.1:0", "ipv4", "loopback", false),
            ("0.0.0.0:0", "ipv4", "unspecified", false),
            (
                "[::ffff:192.168.246.19]:0",
                "ipv6",
                "ipv4_mapped_ipv6",
                false,
            ),
            ("8.8.8.8:0", "ipv4", "other", false),
        ] {
            let diagnostic = CandidateDiagnostic::new(2, socket.parse().unwrap(), false, vec![]);
            let data = serde_json::to_value(diagnostic).unwrap();
            assert_eq!(data["index"], 2);
            assert_eq!(data["family"], family);
            assert_eq!(data["addressClass"], class);
            assert_eq!(data["portNonzero"], port);
            assert_eq!(data["eligible"], false);
            for key in ["cacheOrigin", "freshness", "routeClass", "interfaceClass"] {
                assert_eq!(data[key], "unknown");
            }
            assert!(!data.to_string().contains(socket));
            assert!(data.get("port").is_none());
            assert!(data.get("ageMs").is_none());
        }
        let one = SourceFingerprint::from_fullname("SERVICE_ID_SENTINEL");
        assert_eq!(one, SourceFingerprint::from_fullname("SERVICE_ID_SENTINEL"));
        assert_ne!(
            one,
            SourceFingerprint::from_fullname("OTHER_SERVICE_SENTINEL")
        );
        let encoded = serde_json::to_value(one).unwrap();
        let fingerprint = encoded.as_str().unwrap();
        assert_eq!(fingerprint.len(), 24);
        assert!(fingerprint.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn attempt_entries_have_only_local_sequence_enums_counts_and_elapsed() {
        let trace = new_pair_trace(PairRole::Sender, Some(12));
        let entry = PairEntry {
            attempt: trace.id(),
            role: PairRole::Sender,
            ui_attempt: Some(12),
            attempt_elapsed_ms: 4,
            stage: PairStage::TcpConnect,
            state: PairState::Timeout,
            reason: PairReason::ConnectTimeout,
            endpoint_count: Some(1),
            candidate: None,
            local_address_class: None,
            reachability: None,
            evidence: "application_stage_not_nic_or_user_ack",
            funnel_event: None,
            code_match: None,
            candidate_counts: None,
            approved: None,
            transport: None,
        };
        let value = serde_json::to_value(entry).unwrap();
        assert_eq!(value["uiAttempt"], 12);
        assert_eq!(value["stage"], "tcp_connect");
        assert_eq!(value["reason"], "connect_timeout");
        assert_eq!(value["endpointCount"], 1);
        assert!(!value.to_string().contains("session") && !value.to_string().contains("deviceId"));
        assert!(record_ui_prompt(trace.id()));
        assert!(!record_ui_prompt(u64::MAX));
    }
    #[test]
    fn registry_is_bounded_and_old_traces_expire_without_private_keys() {
        let mut registry = VecDeque::new();
        for i in 0..(MAX_TRACES + 1) {
            store_trace(
                &mut registry,
                PairTrace {
                    attempt: (i + 1) as u64,
                    role: PairRole::Receiver,
                    ui_attempt: None,
                    started: Instant::now(),
                    captured: Default::default(),
                    funnel_events: Default::default(),
                },
            );
        }
        assert!(registry.len() <= MAX_TRACES);
        assert!(registry.iter().all(|trace| trace.attempt > 0));
    }
    #[test]
    fn funnel_mapping_and_dedup_are_allowlisted() {
        use FunnelEvent::*;
        let t = new_pair_trace(PairRole::Sender, Some(5));
        let f = FunnelTrace::new(t.clone());
        for e in [
            DecisionLocalSubmittedApproved,
            DecisionLocalSubmittedRejected,
            DecisionLocalUnavailable,
            DecisionLocalApprovedMatch,
            DecisionLocalApprovedMismatch,
            DecisionLocalRejected,
            PeerDecisionConfirmed,
            AuthConfirmed,
        ] {
            f.record(e);
            f.record(e);
        }
        let rows = t.captured_entries();
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0].stage, PairStage::DecisionLocal);
        assert_eq!(rows[0].state, PairState::Start);
        assert_eq!(rows[0].approved, Some(true));
        assert_eq!(rows[0].code_match, None);
        assert_eq!(rows[1].approved, Some(false));
        assert_eq!(rows[3].code_match, Some(true));
        assert_eq!(rows[4].code_match, Some(false));
        assert_eq!(rows[6].stage, PairStage::PeerDecisionReceived);
        assert_eq!(rows[7].stage, PairStage::AuthConfirmed);
        let json = serde_json::to_string(&rows).unwrap();
        for raw in [
            "192.168.1.9",
            "49157",
            "SDP_SENTINEL",
            "PAYLOAD_SENTINEL",
            "DEVICE_ID_SENTINEL",
        ] {
            assert!(!json.contains(raw));
        }
    }
    #[test]
    fn ui_funnel_recording_is_allowlisted_and_deduplicated_across_handles() {
        let t = new_pair_trace(PairRole::Receiver, None);
        let id = t.id();
        assert!(record_funnel_ui(id, FunnelEvent::UiModalMounted));
        assert!(record_funnel_ui(id, FunnelEvent::UiModalMounted));
        assert!(!record_funnel_ui(id, FunnelEvent::AuthConfirmed));
        assert_eq!(t.captured_entries().len(), 1);
        assert_eq!(t.captured_entries()[0].stage, PairStage::UiModalMounted);
    }
    #[test]
    fn session_context_clones_keep_attempt_and_zero_candidates_are_explicit() {
        for role in [PairRole::Sender, PairRole::Receiver] {
            let trace = new_pair_trace(role, None);
            let original = FunnelTrace::new(trace.clone());
            let callback = original.clone();
            drop(original);
            callback.record(FunnelEvent::IceGatheringComplete);
            callback.flush_candidates();
            callback.flush_candidates();
            let rows = trace.captured_entries();
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|row| row.attempt == trace.id()));
            let counts = serde_json::to_value(rows[1].candidate_counts.as_ref().unwrap()).unwrap();
            assert!(counts.as_object().unwrap().values().all(|v| v == 0));
        }
    }

    #[test]
    fn candidate_aggregation_flushes_once_and_emits_counts_only() {
        use FunnelEvent::*;
        let trace = new_pair_trace(PairRole::Receiver, None);
        let f = FunnelTrace::new(trace.clone());
        f.candidate(
            CandidateFamily::Ipv4,
            CandidateTransport::Udp,
            CandidateCategory::Host,
            CandidateRejection::None,
        );
        f.candidate(
            CandidateFamily::Ipv4,
            CandidateTransport::Udp,
            CandidateCategory::Host,
            CandidateRejection::None,
        );
        f.candidate(
            CandidateFamily::Unknown,
            CandidateTransport::Unknown,
            CandidateCategory::Unknown,
            CandidateRejection::Malformed,
        );
        f.flush_candidates();
        f.flush_candidates();
        let rows = trace.captured_entries();
        assert_eq!(rows.len(), 1);
        let c = rows[0].candidate_counts.as_ref().unwrap();
        assert_eq!(c.ipv4, 2);
        assert_eq!(c.accepted, 2);
        assert_eq!(c.malformed, 1);
        let value = serde_json::to_string(&rows).unwrap();
        assert!(!value.contains("candidateId"));
        assert!(!value.contains("address"));
        let _ = DecisionLocalUnavailable;
    }
}
