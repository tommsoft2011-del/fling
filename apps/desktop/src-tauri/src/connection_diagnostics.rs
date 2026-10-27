//! Attempt-local privacy boundary. No API accepts errors, SDP, candidates or IDs.
use crate::online_route_choice::{Path, Role, Wire};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    TurnDecision,
    PeerCreate,
    ChannelCreate,
    OfferSetup,
    OfferCreate,
    OfferSet,
    OfferSend,
    AnswerSetup,
    OfferParse,
    OfferApply,
    AnswerCreate,
    AnswerSet,
    AnswerSend,
    AnswerParse,
    AnswerApply,
    DescriptionValidate,
    IceValidate,
    IceAdd,
    IceSend,
    ChannelWait,
    DirectIceCutoff,
    DirectPreClose,
    DirectStateChange,
    DirectSample,
    IceGenerated,
    DirectChannelWaitTerminal,
    ChannelOpen,
    PairObserve,
    ReadyProof,
    Ready,
    Decision,
    DecisionAck,
    Request,
    RequestAck,
    Prepare,
    Prepared,
    Commit,
    Committed,
    Start,
    StartAck,
    StartConfirmed,
    Abort,
    AttemptAbort,
    ControlReceive,
    FirstFramePermit,
    AttemptEnd,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Started,
    Ok,
    Failed,
    Rejected,
    Observed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    None,
    OperationFailed,
    Interrupted,
    Timeout,
    Bounds,
    Generation,
    Malformed,
    InvalidSyntax,
    UfragMismatch,
    BucketLimit,
    Duplicate,
    StaleFence,
    PeerClosed,
    IceStatsUnavailable,
    NoSucceededPairs,
    IceChecksInProgress,
    IceChecksFailed,
    IcePairSucceeded,
}
#[derive(Clone, Debug, Serialize)]
pub struct PhaseEvent {
    attempt_no: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    checklist: Option<Checklist>,
    role: &'static str,
    path: Path,
    stage: Stage,
    outcome: Outcome,
    reason: Reason,
    elapsed_ms: u64,
    count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pair: Option<crate::online_diagnostics::RouteSummary>,
}
/// Independent diagnostic observation; never passed to route selection or its cache.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Checklist {
    pub status: ChecklistStatus,
    pub pc: LinkState,
    pub ice: LinkState,
    pub dc: LinkState,
    pub pair: Option<crate::online_diagnostics::RouteSummary>,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub enum ChecklistStatus { StateChange, StatsUnavailable, Timeout, AlreadyClosed, ZeroPairs, StillInProgress, FailedPairs, SucceededDirect, OtherPairs }
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub enum LinkState { New, Checking, Connecting, Connected, Completed, Disconnected, Failed, Closed, Open, Closing, Unknown }
impl LinkState {
    pub fn from_native(value: impl std::fmt::Debug) -> Self {
        match format!("{value:?}").as_str() {
            "New"=>Self::New,"Checking"=>Self::Checking,"Connecting"=>Self::Connecting,
            "Connected"=>Self::Connected,"Completed"=>Self::Completed,"Disconnected"=>Self::Disconnected,
            "Failed"=>Self::Failed,"Closed"=>Self::Closed,"Open"=>Self::Open,"Closing"=>Self::Closing,_=>Self::Unknown,
        }
    }
}
pub fn checklist_status(pair: &crate::online_diagnostics::RouteSummary) -> ChecklistStatus {
    if pair.pairs_succeeded_direct>0 {ChecklistStatus::SucceededDirect}
    else if pair.pairs_in_progress>0 {ChecklistStatus::StillInProgress}
    else if pair.pairs_failed>0 {ChecklistStatus::FailedPairs}
    else if pair.pairs_total==0 {ChecklistStatus::ZeroPairs}
    else {ChecklistStatus::OtherPairs}
}
/// Await the diagnostic observation before invoking the real close, including timeout/error observations.
pub async fn observe_before_close(observe: impl Future<Output=()>, close: impl FnOnce()) {
    observe.await;
    close();
}
pub type Observer = Arc<dyn Fn(PhaseEvent) + Send + Sync>;
pub struct Diagnostics {
    attempt_no: u64,
    started: Instant,
    role: &'static str,
    observer: Observer,
    counts: Mutex<HashMap<(Path, Stage, Outcome, Reason), u32>>,
    pairs: Mutex<HashMap<Path, crate::online_diagnostics::RouteSummary>>,
    checkpoints: Mutex<HashSet<(Path, Stage)>>,
}
#[derive(Clone)]
pub struct Scope {
    diagnostics: Arc<Diagnostics>,
    path: Path,
}
impl Diagnostics {
    pub fn new(role: Role, observer: Observer) -> Arc<Self> {
        static NEXT_ATTEMPT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Arc::new(Self {
            attempt_no: NEXT_ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            started: Instant::now(),
            role: if role == Role::Sender {
                "sender"
            } else {
                "receiver"
            },
            observer,
            counts: Mutex::new(HashMap::new()),
            pairs: Mutex::new(HashMap::new()),
            checkpoints: Mutex::new(HashSet::new()),
        })
    }
    pub fn scope(self: &Arc<Self>, path: Path) -> Scope {
        Scope {
            diagnostics: self.clone(),
            path,
        }
    }
    pub fn flush(&self) {
        let counts = self.counts.lock().unwrap().clone();
        for ((path, stage, outcome, reason), count) in counts {
            if matches!(stage, Stage::IceValidate | Stage::IceAdd | Stage::IceSend | Stage::IceGenerated) {
                self.emit(path, stage, outcome, reason, count, None);
            }
        }
    }
    fn emit(
        &self,
        path: Path,
        stage: Stage,
        outcome: Outcome,
        reason: Reason,
        count: u32,
        pair: Option<crate::online_diagnostics::RouteSummary>,
    ) {
        (self.observer)(PhaseEvent {
            attempt_no: self.attempt_no,
            checklist: None,
            role: self.role,
            path,
            stage,
            outcome,
            reason,
            elapsed_ms: self
                .started
                .elapsed()
                .as_millis()
                .min(9_007_199_254_740_991) as u64,
            count,
            pair,
        });
    }
}
impl Scope {
    pub fn transition(&self, pc: LinkState, ice: LinkState, dc: LinkState) {
        if self.path!=Path::Direct {return;}
        let mut counts=self.diagnostics.counts.lock().unwrap();
        let n=counts.entry((self.path,Stage::DirectStateChange,Outcome::Observed,Reason::None)).or_default();
        *n=n.saturating_add(1);let emit=*n<=32;drop(counts);
        if emit {self.checklist(Stage::DirectStateChange,Checklist {status:ChecklistStatus::StateChange,pc,ice,dc,pair:None});}
    }
    pub fn checklist(&self, stage: Stage, checklist: Checklist) {
        debug_assert_eq!(self.path, Path::Direct);
        // A route decision is retransmitted until it is acknowledged, so the
        // receiver used to emit one DirectIceCutoff snapshot per Decision
        // (17 in the 2026-10-27 field log, against 1 on the sender) for a
        // single unchanged checklist. Emit the cutoff snapshot once per
        // attempt, sharing the checkpoint set with ice_checklist_checkpoint,
        // while DirectSample and DirectPreClose keep their own cadence.
        if stage == Stage::DirectIceCutoff
            && !self
                .diagnostics
                .checkpoints
                .lock()
                .unwrap()
                .insert((self.path, stage))
        {
            return;
        }
        (self.diagnostics.observer)(PhaseEvent {
            attempt_no: self.diagnostics.attempt_no, checklist: Some(checklist),
            role:self.diagnostics.role, path:self.path, stage, outcome:Outcome::Observed,
            reason:Reason::None, elapsed_ms:self.diagnostics.started.elapsed().as_millis().min(9_007_199_254_740_991) as u64,
            count:1, pair:None,
        });
    }

    /// Milestones emit once per tuple; ICE emits at powers of two plus terminal totals.
    pub fn note(&self, stage: Stage, outcome: Outcome, reason: Reason) {
        let mut counts = self.diagnostics.counts.lock().unwrap();
        let count = counts
            .entry((self.path, stage, outcome, reason))
            .or_default();
        *count = count.saturating_add(1);
        let n = *count;
        drop(counts);
        if n == 1
            || (matches!(stage, Stage::IceValidate | Stage::IceAdd | Stage::IceSend | Stage::IceGenerated)
                && n.is_power_of_two())
        {
            self.diagnostics
                .emit(self.path, stage, outcome, reason, n, None);
        }
    }
    pub fn pair(&self, summary: crate::online_diagnostics::RouteSummary) {
        let mut pairs = self.diagnostics.pairs.lock().unwrap();
        if pairs.get(&self.path) == Some(&summary) {
            return;
        }
        pairs.insert(self.path, summary.clone());
        drop(pairs);
        // Bound changes independently of polling frequency; never use raw pair IDs.
        let mut counts = self.diagnostics.counts.lock().unwrap();
        let n = counts
            .entry((
                self.path,
                Stage::PairObserve,
                Outcome::Observed,
                Reason::None,
            ))
            .or_default();
        *n = n.saturating_add(1);
        let n = *n;
        drop(counts);
        if n <= 16 {
            self.diagnostics.emit(
                self.path,
                Stage::PairObserve,
                Outcome::Observed,
                Reason::None,
                n,
                Some(summary),
            );
        }
    }
    /// Emit one Direct ICE checklist snapshot at a route decision/terminal.
    /// Only finite pair counts, candidate-type flags and enums are serialized.
    pub fn ice_checklist_checkpoint(
        &self,
        stage: Stage,
        summary: Option<crate::online_diagnostics::RouteSummary>,
    ) {
        debug_assert_eq!(self.path, Path::Direct);
        debug_assert!(matches!(stage, Stage::DirectIceCutoff | Stage::DirectChannelWaitTerminal));
        let reason = match summary.as_ref() {
            None => Reason::IceStatsUnavailable,
            Some(summary) if summary.pairs_succeeded > 0 => Reason::IcePairSucceeded,
            Some(summary) if summary.pairs_in_progress > 0 => Reason::IceChecksInProgress,
            Some(summary) if summary.pairs_failed > 0 => Reason::IceChecksFailed,
            Some(_) => Reason::NoSucceededPairs,
        };
        if !self
            .diagnostics
            .checkpoints
            .lock()
            .unwrap()
            .insert((self.path, stage))
        {
            return;
        }
        self.diagnostics.emit(self.path, stage, Outcome::Observed, reason, 1, summary);
    }
    pub fn begin(&self, stage: Stage) -> Guard {
        self.note(stage, Outcome::Started, Reason::None);
        Guard {
            scope: self.clone(),
            stage,
            done: false,
        }
    }
    pub async fn run<T>(
        &self,
        stage: Stage,
        future: impl Future<Output = Result<T, String>>,
    ) -> Result<T, String> {
        let guard = self.begin(stage);
        let result = future.await;
        guard.finish(
            if result.is_ok() {
                Outcome::Ok
            } else {
                Outcome::Failed
            },
            if result.is_ok() {
                Reason::None
            } else {
                Reason::OperationFailed
            },
        );
        result
    }
    pub async fn bounded<T>(
        &self,
        stage: Stage,
        future: impl Future<Output = Result<T, String>>,
    ) -> Result<T, String> {
        let guard = self.begin(stage);
        match tokio::time::timeout(crate::online_setup::INITIAL_EFFECT_TIMEOUT, future).await {
            Ok(result) => {
                guard.finish(
                    if result.is_ok() {
                        Outcome::Ok
                    } else {
                        Outcome::Failed
                    },
                    if result.is_ok() {
                        Reason::None
                    } else {
                        Reason::OperationFailed
                    },
                );
                result
            }
            Err(_) => {
                guard.finish(Outcome::Failed, Reason::Timeout);
                Err("原生连接建立副作用超时，请重试".into())
            }
        }
    }
}
pub struct Guard {
    scope: Scope,
    stage: Stage,
    done: bool,
}
impl Guard {
    pub fn finish(mut self, outcome: Outcome, reason: Reason) {
        self.done = true;
        self.scope.note(self.stage, outcome, reason);
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if !self.done {
            self.scope
                .note(self.stage, Outcome::Failed, Reason::Interrupted);
        }
    }
}
pub fn control_stage(wire: Wire) -> Option<Stage> {
    Some(match wire {
        Wire::Ready(_) => Stage::Ready,
        Wire::Decision => Stage::Decision,
        Wire::DecisionAck => Stage::DecisionAck,
        Wire::Request => Stage::Request,
        Wire::RequestAck => Stage::RequestAck,
        Wire::Prepare(_) => Stage::Prepare,
        Wire::Prepared(_) => Stage::Prepared,
        Wire::Commit => Stage::Commit,
        Wire::Committed => Stage::Committed,
        Wire::Start => Stage::Start,
        Wire::StartAck => Stage::StartAck,
        Wire::StartConfirmed => Stage::StartConfirmed,
        Wire::Abort => Stage::Abort,
        Wire::AttemptAbort => Stage::AttemptAbort,
        Wire::Ack { .. } => return None,
    })
}
/// Stable UI contract. Deliberately accepts no underlying error text.
pub fn connection_failure() -> String {
    "CONNECTION_FAILED: 安全连接建立失败或已中断，请重试".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn capture() -> (Arc<Diagnostics>, Arc<Mutex<Vec<PhaseEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let saved = events.clone();
        (
            Diagnostics::new(
                Role::Sender,
                Arc::new(move |e| saved.lock().unwrap().push(e)),
            ),
            events,
        )
    }
    #[tokio::test]
    async fn privacy_and_failure_categories_are_not_raw_error_strings() {
        let (d, events) = capture();
        let s = d.scope(Path::Turn);
        let secret = "SDP candidate:secret 192.0.2.1:3478 turn:private password C:\\file task-id";
        assert_eq!(
            s.run::<()>(Stage::AnswerApply, async { Err(secret.into()) })
                .await
                .unwrap_err(),
            secret
        );
        drop(s.begin(Stage::OfferSet));
        let json = serde_json::to_string(&*events.lock().unwrap()).unwrap();
        for value in [
            "candidate:",
            "192.0.2.1",
            "3478",
            "turn:",
            "password",
            "task-id",
            "SDP",
        ] {
            assert!(!json.contains(value));
        }
        assert!(json.contains("operation_failed"));
        assert!(json.contains("interrupted"));
        assert!(connection_failure().starts_with("CONNECTION_FAILED:"));
        assert!(!connection_failure().contains(secret));
    }
    #[test]
    fn sparse_counts_and_attempt_clock_are_independent() {
        let (d, events) = capture();
        let s = d.scope(Path::Direct);
        for _ in 0..100 {
            s.note(Stage::IceValidate, Outcome::Rejected, Reason::InvalidSyntax);
        }
        assert_eq!(events.lock().unwrap().len(), 7);
        d.flush();
        assert_eq!(events.lock().unwrap().last().unwrap().count, 100);
        let (next, next_events) = capture();
        next.scope(Path::Direct)
            .note(Stage::PeerCreate, Outcome::Started, Reason::None);
        assert_eq!(next_events.lock().unwrap()[0].count, 1);
        assert!(next_events.lock().unwrap()[0].elapsed_ms < 100);
    }
    #[test]
    fn direct_checkpoints_report_finite_ice_categories_and_aggregate_counts_once() {
        let (d, events) = capture();
        let scope = d.scope(Path::Direct);
        let summary = crate::online_diagnostics::RouteSummary {
            pairs_total: 5,
            pairs_in_progress: 3,
            local_host: true,
            remote_host: true,
            local_srflx: true,
            remote_srflx: true,
            ..Default::default()
        };
        scope.ice_checklist_checkpoint(Stage::DirectIceCutoff, Some(summary.clone()));
        // A duplicate route decision must not emit a second, possibly stale snapshot.
        scope.ice_checklist_checkpoint(Stage::DirectIceCutoff, None);
        scope.ice_checklist_checkpoint(Stage::DirectChannelWaitTerminal, None);

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].reason, Reason::IceChecksInProgress);
        assert_eq!(events[0].count, 1);
        let pair = events[0].pair.as_ref().unwrap();
        assert_eq!(pair.pairs_total, 5);
        assert_eq!(pair.pairs_in_progress, 3);
        assert!(pair.local_host && pair.remote_host && pair.local_srflx && pair.remote_srflx);
        assert_eq!(events[1].reason, Reason::IceStatsUnavailable);
        assert!(events[1].pair.is_none());

        let json = serde_json::to_string(&*events).unwrap();
        for private in ["candidate:", "192.0.2.1", "sdp", "username", "password", "pair-id"] {
            assert!(!json.contains(private));
        }
    }
    #[test]
    fn checklist_cutoff_emits_once_per_attempt_despite_retransmitted_decisions() {
        let (d, events) = capture();
        let scope = d.scope(Path::Direct);
        let checklist = Checklist {
            status: ChecklistStatus::StillInProgress,
            pc: LinkState::Connecting,
            ice: LinkState::Checking,
            dc: LinkState::Connecting,
            pair: None,
        };
        // Mirrors the 2026-10-27 field log: the receiver emitted 17
        // direct_ice_cutoff snapshots for one unchanged checklist because
        // each retransmitted Decision scheduled a fresh snapshot.
        for _ in 0..17 {
            scope.checklist(Stage::DirectIceCutoff, checklist.clone());
        }
        scope.checklist(Stage::DirectSample, checklist.clone());
        let events = events.lock().unwrap();
        assert_eq!(
            events.iter().filter(|e| e.stage == Stage::DirectIceCutoff).count(),
            1
        );
        assert_eq!(
            events.iter().filter(|e| e.stage == Stage::DirectSample).count(),
            1
        );
    }
    #[tokio::test]
    async fn bounded_timeout_is_distinct_from_operation_failure() {
        let (d, events) = capture();
        assert!(d
            .scope(Path::Turn)
            .bounded::<()>(Stage::OfferSetup, std::future::pending())
            .await
            .is_err());
        assert!(events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.reason == Reason::Timeout));
    }
}

#[cfg(test)]
mod checklist_regressions {
    use super::*;
    #[test] fn classify_checklist_without_conflating_empty_with_failure() {
        use crate::online_diagnostics::RouteSummary;
        let mut p=RouteSummary::default();
        assert_eq!(checklist_status(&p),ChecklistStatus::ZeroPairs);
        p.pairs_total=5;p.pairs_in_progress=4;
        assert_eq!(checklist_status(&p),ChecklistStatus::StillInProgress);
        p.pairs_in_progress=0;p.pairs_failed=5;
        assert_eq!(checklist_status(&p),ChecklistStatus::FailedPairs);
        p.pairs_succeeded_direct=1;
        assert_eq!(checklist_status(&p),ChecklistStatus::SucceededDirect);
        p.pairs_succeeded_direct=0;p.pairs_failed=0;p.pairs_succeeded_relay=1;
        assert_eq!(checklist_status(&p),ChecklistStatus::OtherPairs);
    }
    #[tokio::test] async fn snapshot_finishes_before_real_close() {
        let events=std::sync::Mutex::new(Vec::new());
        observe_before_close(async {tokio::task::yield_now().await;events.lock().unwrap().push("snapshot");},
            ||events.lock().unwrap().push("close")).await;
        assert_eq!(*events.lock().unwrap(),vec!["snapshot","close"]);
    }
}
