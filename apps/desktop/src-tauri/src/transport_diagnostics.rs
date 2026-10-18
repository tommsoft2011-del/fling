//! Privacy-safe transport observations. No API accepts SDP, candidates, IDs or errors.
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    AnswerValidate,
    AnswerPeerLookup,
    AnswerParse,
    AnswerSetRemote,
    SetRemoteDescription,
    OfferValidate,
    OfferPeerLookup,
    OfferParse,
    OfferSetRemote,
    PendingIceApply,
    CandidateSummary,
    IceConnection,
    IceGathering,
    DataChannelInitial,
    DataChannelTransition,
    LocalCloseRequested,
    FirstTerminal,
    Cleanup,
    PeerInstall,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Start,
    Ok,
    Error,
    Snapshot,
    New,
    Checking,
    Connected,
    Completed,
    Failed,
    Disconnected,
    Closed,
    Unknown,
    Gathering,
    Complete,
    Connecting,
    Open,
    Closing,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    None,
    NotReported,
    UnexpectedRole,
    SdpPolicy,
    PeerNotReady,
    StateLock,
    SdpParse,
    SetRemote,
    CandidatePayloadBound,
    CandidatePayloadMalformed,
    CandidateMalformedLine,
    CandidateNonHost,
    CandidateNonNumeric,
    CandidateNonLocal,
    CandidatePeerMismatch,
    CandidatePendingBound,
    CandidateMissingMid,
    CandidateMissingMline,
    CandidateParse,
    CandidateAdd,
    ControlRead,
    ControlDecode,
    ControlWrite,
    ControlWriteTimeout,
    PeerInitialization,
    AuthenticatedHandler,
    IceFailed,
    IceDisconnected,
    IceClosed,
    DataChannelClosed,
    DataChannelTimeout,
    DataChannelObserver,
    ReceiveOverflow,
    TransferFailure,
    LocalCancel,
    PeerCancel,
    PairRejected,
    Completed,
    SessionRemoval,
    RuntimeClear,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateStep {
    Generated,
    FilteredNonHost,
    FilteredNonLocal,
    FilteredMalformed,
    Queued,
    Written,
    WriteFailed,
    QueueFailed,
    Received,
    Pending,
    Added,
    Rejected,
    PendingTaken,
    PendingApplied,
    PendingAbandoned,
    PendingAtPeerInstall,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseOrigin {
    LocalCleanup,
    Unknown,
}
// No remote-or-transport classification: this API supplies no evidence for it.
/// First rejected incoming candidate only. No raw address or caller-defined text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateAddressFamily {
    Ipv4,
    Ipv6,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateLanClass {
    PrivateIpv4,
    LinkLocalIpv4,
    UlaIpv6,
    LinkLocalIpv6,
    Ipv4MappedIpv6,
    Loopback,
    Unspecified,
    Multicast,
    Other,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectedCandidate {
    pub family: CandidateAddressFamily,
    pub lan_class: CandidateLanClass,
    // None means an earlier gate rejected before the original endpoint lookup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint_matched: Option<bool>,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Detail {
    pub phase: Phase,
    pub outcome: Outcome,
    pub reason: Reason,
    pub count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_observed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_origin: Option<CloseOrigin>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_close_requested: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_terminal: Option<Reason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_steps: Option<BTreeMap<CandidateStep, u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidate_rejections: Option<BTreeMap<Reason, u32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_rejected_candidate: Option<RejectedCandidate>,
}
impl Detail {
    pub fn new(phase: Phase, outcome: Outcome, reason: Reason, count: usize) -> Self {
        Self {
            phase,
            outcome,
            reason,
            count: count.min(u32::MAX as usize) as u32,
            close_observed: None,
            close_origin: None,
            local_close_requested: None,
            first_terminal: None,
            candidate_steps: None,
            candidate_rejections: None,
            first_rejected_candidate: None,
        }
    }
}
pub struct State {
    pub first_terminal: Option<Reason>,
    pub local_close_requested: bool,
    pub cleanup: Option<Reason>,
    pub steps: BTreeMap<CandidateStep, u32>,
    pub rejections: BTreeMap<Reason, u32>,
    pub transitions: BTreeMap<Phase, Outcome>,
    pub first_rejected_candidate: Option<RejectedCandidate>,
}
impl State {
    pub fn count(&mut self, step: CandidateStep, n: usize) {
        let value = self.steps.entry(step).or_default();
        *value = value.saturating_add(n.min(u32::MAX as usize) as u32);
    }
}

impl Default for State {
    fn default() -> Self {
        use CandidateStep::*;
        Self {
            first_terminal: None,
            local_close_requested: false,
            cleanup: None,
            steps: [
                Generated,
                FilteredNonHost,
                FilteredNonLocal,
                FilteredMalformed,
                Queued,
                Written,
                WriteFailed,
                QueueFailed,
                Received,
                Pending,
                Added,
                Rejected,
                PendingTaken,
                PendingApplied,
                PendingAbandoned,
                PendingAtPeerInstall,
            ]
            .into_iter()
            .map(|step| (step, 0))
            .collect(),
            rejections: BTreeMap::new(),
            transitions: BTreeMap::new(),
            first_rejected_candidate: None,
        }
    }
}
