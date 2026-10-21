//! Privacy boundary: only finite enums, booleans and elapsed time leave this module.
//! TURN carries encrypted WebRTC DataChannel packets, NOT LiveKit file relay.
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Route {
    Direct,
    TurnRelay,
    #[default]
    Unknown,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Candidate {
    Host,
    Srflx,
    Prflx,
    Relay,
    #[default]
    Unknown,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Udp,
    Tcp,
    #[default]
    Unknown,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairState {
    Frozen,
    Waiting,
    InProgress,
    Failed,
    Succeeded,
    #[default]
    Unknown,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RouteSummary {
    pub route: Route,
    pub selected: bool,
    pub pair_state: PairState,
    pub nominated: Option<bool>,
    pub local_type: Candidate,
    pub remote_type: Candidate,
    pub local_protocol: Protocol,
    pub remote_protocol: Protocol,
    /// Privacy-safe checklist aggregates over ALL candidate pairs and
    /// candidates in a stats snapshot (counts and type presence only;
    /// never IDs or addresses). These stay populated even when no pair is
    /// selected yet, so diagnostics can distinguish "no pairs formed",
    /// "pairs exist but checks are failing", and "no srflx gathered".
    pub pairs_total: u32,
    pub pairs_succeeded: u32,
    pub pairs_in_progress: u32,
    pub pairs_failed: u32,
    /// Succeeded pairs split by whether either side is a relay candidate.
    /// `pairs_succeeded_direct + pairs_succeeded_relay <= pairs_succeeded`
    /// (a succeeded pair whose candidate types are missing counts in
    /// neither), so diagnostics can prove whether a genuinely non-relay
    /// pair ever connected, and when, without exposing pair identities.
    pub pairs_succeeded_direct: u32,
    pub pairs_succeeded_relay: u32,
    pub local_srflx: bool,
    pub remote_srflx: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteObservation {
    pub summary: RouteSummary,
    // Comparison only. Never serialize raw stats IDs, even when switching same-type pairs.
    pub pair_key: Option<String>,
}
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    FallbackRequested,
    RelayCandidate,
    DataChannelOpen,
    FirstRealBytes,
    DiskSynced,
    HashVerified,
    AtomicCommit,
    DurableAckSent,
    DurableAckReceived,
    TaskAckSent,
    TaskAckReceived,
    SelectedPair,
    RouteChange,
    StatsUnavailable,
    ConnectionFailed,
    ConnectionClosed,
    TransferFailed,
    TaskCancelled,
    TaskEnd,
}
#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Initiator,
    Responder,
    #[default]
    Unknown,
}
#[derive(Clone, Debug, Serialize)]
pub struct Diagnostic {
    pub transfer_no: Option<u64>,
    pub role: Role,
    pub phase: Phase,
    pub elapsed_ms: u64,
    pub snapshot_age_ms: Option<u64>,
    #[serde(flatten)]
    pub summary: RouteSummary,
}
struct State {
    role: Role,
    observation: RouteObservation,
    observed_at: Option<Instant>,
    seen: HashSet<Phase>,
    transfer_seen: HashSet<(String, Phase)>,
    transfer_numbers: HashMap<String, u64>,
}
pub struct Diagnostics {
    started: Instant,
    state: Mutex<State>,
}
impl Diagnostics {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            state: Mutex::new(State {
                role: Role::Unknown,
                observation: RouteObservation::default(),
                observed_at: None,
                seen: HashSet::new(),
                transfer_seen: HashSet::new(),
                transfer_numbers: HashMap::new(),
            }),
        }
    }
    pub fn set_role(&self, role: Role) {
        self.state.lock().unwrap().role = role;
    }
    pub fn begin_connection(&self, role: Role) {
        let mut state = self.state.lock().unwrap();
        state.role = role;
        state.observation = RouteObservation::default();
        state.observed_at = None;
        for phase in [
            Phase::FallbackRequested,
            Phase::RelayCandidate,
            Phase::DataChannelOpen,
            Phase::ConnectionFailed,
            Phase::ConnectionClosed,
        ] {
            state.seen.remove(&phase);
        }
    }
    fn event(&self, state: &State, phase: Phase) -> Diagnostic {
        Diagnostic {
            transfer_no: None,
            role: state.role,
            phase,
            elapsed_ms: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            snapshot_age_ms: state
                .observed_at
                .map(|at| at.elapsed().as_millis().min(u64::MAX as u128) as u64),
            summary: state.observation.summary.clone(),
        }
    }
    /// One event per lifecycle milestone, not per chunk, candidate or ACK replay.
    pub fn note(&self, phase: Phase) -> Option<Diagnostic> {
        let mut state = self.state.lock().unwrap();
        if !state.seen.insert(phase) {
            return None;
        }
        Some(self.event(&state, phase))
    }
    /// Internal transfer IDs are used only for deduplication. The output contains a
    /// session-local ordinal, not a task/session/transfer identifier from the protocol.
    pub fn note_transfer(&self, key: &str, phase: Phase) -> Option<Diagnostic> {
        let mut state = self.state.lock().unwrap();
        if !state.transfer_seen.insert((key.to_owned(), phase)) {
            return None;
        }
        let next = state.transfer_numbers.len() as u64 + 1;
        let number = *state.transfer_numbers.entry(key.to_owned()).or_insert(next);
        let mut event = self.event(&state, phase);
        event.transfer_no = Some(number);
        Some(event)
    }
    /// Poll silently. Emit only on selected-pair/state/route change, including loss of stats.
    pub fn observe(&self, observation: RouteObservation) -> Option<Diagnostic> {
        let mut state = self.state.lock().unwrap();
        let changed = state.observed_at.is_none() || state.observation != observation;
        let phase = if observation.summary.selected
            && observation.pair_key.is_some()
            && state.observation.pair_key != observation.pair_key
        {
            Phase::SelectedPair
        } else if state.observed_at.is_none() {
            Phase::StatsUnavailable
        } else {
            Phase::RouteChange
        };
        state.observation = observation;
        state.observed_at = Some(Instant::now());
        changed.then(|| self.event(&state, phase))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn intent_candidate_open_never_prove_relay_and_milestones_are_sparse() {
        let d = Diagnostics::new();
        for phase in [
            Phase::FallbackRequested,
            Phase::RelayCandidate,
            Phase::DataChannelOpen,
            Phase::FirstRealBytes,
            Phase::DiskSynced,
            Phase::HashVerified,
            Phase::AtomicCommit,
            Phase::DurableAckSent,
            Phase::DurableAckReceived,
            Phase::TaskAckSent,
            Phase::TaskAckReceived,
            Phase::ConnectionFailed,
            Phase::ConnectionClosed,
            Phase::TransferFailed,
            Phase::TaskEnd,
        ] {
            assert_eq!(d.note(phase).unwrap().summary.route, Route::Unknown);
            assert!(d.note(phase).is_none());
        }
    }
    #[test]
    fn private_pair_identity_never_serialized_and_same_type_switch_is_observed() {
        let d = Diagnostics::new();
        let mut observation = RouteObservation {
            pair_key: Some(
                "SECRET 192.0.2.1:3478 turn:private candidate:raw C:\\private\\file.txt session-id"
                    .into(),
            ),
            ..Default::default()
        };
        let json = serde_json::to_string(&d.observe(observation.clone()).unwrap()).unwrap();
        for forbidden in [
            "SECRET",
            "192.0.2.1",
            "3478",
            "turn:",
            "candidate:",
            "private",
            "session-id",
            "pair_key",
        ] {
            assert!(!json.contains(forbidden));
        }
        assert!(d.observe(observation.clone()).is_none());
        observation.pair_key = Some("different-private-pair".into());
        assert!(d.observe(observation).is_some());
    }
    #[test]
    fn independent_transfer_scopes_never_leak_raw_keys() {
        let d = Diagnostics::new();
        for (index, key) in ["raw-secret-task-A", "raw-secret-task-B"]
            .iter()
            .enumerate()
        {
            for phase in [
                Phase::FirstRealBytes,
                Phase::DiskSynced,
                Phase::HashVerified,
                Phase::AtomicCommit,
                Phase::DurableAckSent,
                Phase::TaskEnd,
            ] {
                let event = d.note_transfer(key, phase).unwrap();
                assert_eq!(event.transfer_no, Some(index as u64 + 1));
                assert!(!serde_json::to_string(&event)
                    .unwrap()
                    .contains("raw-secret"));
                assert!(d.note_transfer(key, phase).is_none());
            }
        }
    }
    #[test]
    fn new_channel_open_cannot_inherit_previous_relay_route() {
        let d = Diagnostics::new();
        let mut observation = RouteObservation::default();
        observation.summary.route = Route::TurnRelay;
        d.observe(observation);
        d.note(Phase::DataChannelOpen);
        d.begin_connection(Role::Initiator);
        assert_eq!(
            d.note(Phase::DataChannelOpen).unwrap().summary.route,
            Route::Unknown
        );
    }
    #[test]
    fn first_missing_stats_is_not_a_selected_pair_event() {
        let d = Diagnostics::new();
        assert_eq!(
            d.observe(RouteObservation::default()).unwrap().phase,
            Phase::StatsUnavailable
        );
    }
    #[test]
    fn stats_loss_resets_confirmed_route_to_unknown() {
        let d = Diagnostics::new();
        let mut observation = RouteObservation::default();
        observation.summary.route = Route::TurnRelay;
        d.observe(observation);
        assert_eq!(
            d.observe(RouteObservation::default())
                .unwrap()
                .summary
                .route,
            Route::Unknown
        );
        assert!(d.observe(RouteObservation::default()).is_none());
    }
}
