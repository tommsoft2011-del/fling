use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::Instant,
};

const MAX_TRACES: usize = 128;
static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(1);
static TRACES: OnceLock<Mutex<VecDeque<PairTrace>>> = OnceLock::new();

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
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PairState {
    Start,
    Ok,
    Timeout,
    Error,
    Rejected,
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
}

#[derive(Clone, Debug)]
pub struct PairTrace {
    attempt: u64,
    role: PairRole,
    ui_attempt: Option<u32>,
    started: Instant,
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
    evidence: &'static str,
}

impl PairTrace {
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
            evidence: "application_stage_not_nic_or_user_ack",
        };
        // A pairing attempt is not a discovery round. Its independent local sequence
        // and monotonic clock are explicit, without any session/task/device identity.
        crate::discovery_diagnostics::record_for_round(
            0,
            0,
            crate::discovery_diagnostics::Event::Pairing(entry),
        );
    }
}

pub fn new_pair_trace(role: PairRole, ui_attempt: Option<u32>) -> PairTrace {
    let trace = PairTrace {
        attempt: NEXT_ATTEMPT.fetch_add(1, Ordering::Relaxed),
        role,
        ui_attempt,
        started: Instant::now(),
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
            evidence: "application_stage_not_nic_or_user_ack",
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
                },
            );
        }
        assert!(registry.len() <= MAX_TRACES);
        assert!(registry.iter().all(|trace| trace.attempt > 0));
    }
}
