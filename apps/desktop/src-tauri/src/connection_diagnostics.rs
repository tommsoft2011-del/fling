//! Attempt-local privacy boundary. No API accepts errors, SDP, candidates or IDs.
use crate::online_route_choice::{Path, Role, Wire};
use serde::Serialize;
use std::{
    collections::HashMap,
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
}
#[derive(Clone, Debug, Serialize)]
pub struct PhaseEvent {
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
pub type Observer = Arc<dyn Fn(PhaseEvent) + Send + Sync>;
pub struct Diagnostics {
    started: Instant,
    role: &'static str,
    observer: Observer,
    counts: Mutex<HashMap<(Path, Stage, Outcome, Reason), u32>>,
    pairs: Mutex<HashMap<Path, crate::online_diagnostics::RouteSummary>>,
}
#[derive(Clone)]
pub struct Scope {
    diagnostics: Arc<Diagnostics>,
    path: Path,
}
impl Diagnostics {
    pub fn new(role: Role, observer: Observer) -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            role: if role == Role::Sender {
                "sender"
            } else {
                "receiver"
            },
            observer,
            counts: Mutex::new(HashMap::new()),
            pairs: Mutex::new(HashMap::new()),
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
            if matches!(stage, Stage::IceValidate | Stage::IceAdd | Stage::IceSend) {
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
            || (matches!(stage, Stage::IceValidate | Stage::IceAdd | Stage::IceSend)
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
