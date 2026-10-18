use serde::Serialize;
use sha2::{Digest, Sha256};
use std::net::{IpAddr, SocketAddr};
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
    #[cfg(test)]
    captured: std::sync::Arc<Mutex<Vec<PairEntry>>>,
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
            candidate: None,
            local_address_class: None,
            reachability: None,
            evidence: "application_stage_not_nic_or_user_ack",
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
                },
            );
        }
        assert!(registry.len() <= MAX_TRACES);
        assert!(registry.iter().all(|trace| trace.attempt > 0));
    }
}
