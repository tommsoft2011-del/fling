use serde::Serialize;
use serde_json::{json, Value};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const CAP: u64 = 1024 * 1024;
const ROTATED: &str = "discovery.log.1";
static LOGGER: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
static IO_LOCK: Mutex<()> = Mutex::new(());
static PROCESS_START: OnceLock<Instant> = OnceLock::new();
static ACTIVE_ROUND: OnceLock<Mutex<(u64, Instant)>> = OnceLock::new();
static NEXT_ROUND: AtomicU64 = AtomicU64::new(1);
static ENTRY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PhaseCounts {
    pub resolved_services: u32,
    pub valid_services: u32,
    pub lan_eligible_peers: u32,
    pub ui_emitted_peers: u32,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    None,
    Overlap,
    StopFailed,
    BrowseFailed,
    StartTimeout,
    BrowseDisconnected,
    ModeStopped,
    WorkerDisconnected,
    ReplyTimeout,
    EmitFailed,
    AlreadyPublished,
    LanDisabled,
    NoEligible,
    ZeroPort,
    Ipv6ListenerUnavailable,
    ProtocolMismatch,
    MissingIdentity,
    SelfService,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Timeout,
    Error,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UiReason {
    Initialized,
    PeerUp,
    PeerDown,
    RefreshCompleted,
}

#[derive(Clone, Debug)]
pub enum Event {
    Pairing(crate::pairing_diagnostics::PairEntry),
    PairingUiButton {
        ui_attempt: u32,
    },
    Environment,
    DaemonInitSucceeded,
    DaemonInitFailed,
    BrowsingStarted,
    BrowseConfirmed,
    BrowseStopped,
    RefreshRequested,
    RefreshSucceeded,
    RefreshFailed,
    RefreshStopFailed,
    RefreshBrowseFailed,
    RefreshStartConfirmationFailed,
    RefreshTimeout,
    RefreshOutcome {
        outcome: Outcome,
        reason: Reason,
        counts: PhaseCounts,
    },
    PhaseCounts(PhaseCounts),
    Resolved,
    Protocol {
        valid: bool,
        reason: Reason,
    },
    AddressEligibility {
        private_ipv4: u32,
        link_local_ipv4: u32,
        ula_ipv6: u32,
        rejected: u32,
        reason: Reason,
    },
    LanUiEvent {
        up: bool,
        success: bool,
    },
    LanUiSuppressed(Reason),
    ListenerInit {
        ipv4_ready: bool,
        ipv6_ready: bool,
    },
    ListenerFailed,
    InterfacePolicy,
    PeerUpCount(u32),
    PeerDownCount(u32),
    UiSnapshot {
        received_up: u32,
        received_down: u32,
        rendered_peers: u32,
        reason: UiReason,
    },
    ComputerName {
        local: bool,
        name: String,
    },
}

pub fn next_round() -> u64 {
    NEXT_ROUND.fetch_add(1, Ordering::Relaxed)
}

pub fn activate_round(round: u64) {
    activate_round_at(round, Instant::now());
}

pub fn activate_round_at(round: u64, started: Instant) {
    let mut context = ACTIVE_ROUND
        .get_or_init(|| Mutex::new((0, Instant::now())))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *context = (round, started);
}

fn context() -> (u64, u64) {
    let context = ACTIVE_ROUND
        .get_or_init(|| Mutex::new((0, Instant::now())))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    (
        context.0,
        context.1.elapsed().as_millis().min(u64::MAX as u128) as u64,
    )
}

fn build_id() -> &'static str {
    let value = option_env!("FLING_BUILD_ID").unwrap_or("unknown");
    if value.len() == 40 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        value
    } else {
        "unknown"
    }
}

fn numeric_version(raw: &str) -> String {
    let value: String = raw
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    if !value.is_empty() && value.len() <= 48 && value.bytes().filter(|b| *b == b'.').count() <= 5 {
        value
    } else {
        "unavailable".to_string()
    }
}

#[cfg(target_os = "windows")]
fn os_version() -> String {
    use std::os::windows::process::CommandExt;
    std::process::Command::new("cmd.exe")
        .args(["/C", "ver"])
        .creation_flags(0x08000000)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| numeric_version(&String::from_utf8_lossy(&o.stdout)))
        .unwrap_or_else(|| "unavailable".to_string())
}
#[cfg(target_os = "linux")]
fn os_version() -> String {
    fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|s| numeric_version(s.split('-').next().unwrap_or("")))
        .unwrap_or_else(|| "unavailable".to_string())
}
#[cfg(not(any(target_os = "windows", target_os = "linux")))]
fn os_version() -> String {
    "unavailable".to_string()
}

fn sanitize_computer_name(name: &str) -> String {
    if name.len() > 64 || name.chars().any(char::is_control) {
        return "[redacted]".to_string();
    }
    let s = name.trim();
    let lower = s.to_ascii_lowercase();
    let embedded_ip = s
        .split(|c: char| !c.is_ascii_digit() && c != '.')
        .any(|part| part.parse::<std::net::Ipv4Addr>().is_ok());
    let embedded_mac = s.as_bytes().windows(17).any(|w| {
        let sep = w[2];
        (sep == b'-' || sep == b':')
            && w.iter().enumerate().all(|(i, b)| {
                if [2, 5, 8, 11, 14].contains(&i) {
                    *b == sep
                } else {
                    b.is_ascii_hexdigit()
                }
            })
    });
    let embedded_id = s
        .as_bytes()
        .windows(32)
        .any(|w| w.iter().all(u8::is_ascii_hexdigit));
    let invalid = s.is_empty()
        || s.len() > 64
        || name.chars().any(char::is_control)
        || s.chars()
            .any(|c| !(c.is_alphanumeric() || " ._-()".contains(c)))
        || lower.contains("..")
        || lower.contains("session")
        || lower.contains("transfer")
        || lower.starts_with("sk-")
        || lower.parse::<std::net::IpAddr>().is_ok()
        || embedded_ip
        || embedded_mac
        || embedded_id
        || (s.len() >= 36
            && s.as_bytes().windows(36).any(|w| {
                w.iter().enumerate().all(|(i, b)| {
                    if [8, 13, 18, 23].contains(&i) {
                        *b == b'-'
                    } else {
                        b.is_ascii_hexdigit()
                    }
                })
            }));
    if invalid {
        "[redacted]".to_string()
    } else {
        s.to_string()
    }
}

fn fields(event: Event) -> (&'static str, Value) {
    match event {
        Event::Pairing(entry) => ("pairing_stage", json!(entry)),
        Event::PairingUiButton { ui_attempt } => (
            "pairing_ui_button",
            json!({"uiAttempt": ui_attempt, "scope": "this_ui_instance", "evidence": "button_report_received_by_native_not_handshake_started"}),
        ),
        Event::Environment => (
            "environment",
            json!({
                "os": std::env::consts::OS, "osVersion": os_version(), "arch": std::env::consts::ARCH,
                "appVersion": env!("CARGO_PKG_VERSION"), "evidence": "application_observation_only_not_nic_packet_proof",
                "resolvedRecordsMayBeCached": true,
            }),
        ),
        Event::DaemonInitSucceeded => ("daemon_init_succeeded", json!({})),
        Event::DaemonInitFailed => ("daemon_init_failed", json!({})),
        Event::BrowsingStarted => ("browse_requested", json!({})),
        Event::BrowseConfirmed | Event::RefreshSucceeded => ("browse_start_confirmed", json!({})),
        Event::BrowseStopped => ("browse_stopped", json!({})),
        Event::RefreshRequested => ("refresh_requested", json!({})),
        Event::RefreshFailed => (
            "refresh_error_observed",
            json!({"reason": Reason::WorkerDisconnected}),
        ),
        Event::RefreshStopFailed => (
            "refresh_error_observed",
            json!({"reason": Reason::StopFailed}),
        ),
        Event::RefreshBrowseFailed => (
            "refresh_error_observed",
            json!({"reason": Reason::BrowseFailed}),
        ),
        Event::RefreshStartConfirmationFailed => (
            "refresh_error_observed",
            json!({"reason": Reason::BrowseDisconnected}),
        ),
        Event::RefreshTimeout => ("refresh_timeout_observed", json!({})),
        Event::RefreshOutcome {
            outcome,
            reason,
            counts,
        } => (
            "refresh_outcome",
            json!({"outcome": outcome, "reason": reason, "counts": counts}),
        ),
        Event::PhaseCounts(counts) => ("phase_counts", json!({"counts": counts})),
        Event::Resolved => (
            "service_resolved",
            json!({"source": "mdns_library_live_or_cache"}),
        ),
        Event::Protocol { valid, reason } => (
            "protocol_validation",
            json!({"valid": valid, "reason": reason}),
        ),
        Event::AddressEligibility {
            private_ipv4,
            link_local_ipv4,
            ula_ipv6,
            rejected,
            reason,
        } => (
            "address_eligibility",
            json!({
                "privateIpv4": private_ipv4, "linkLocalIpv4": link_local_ipv4, "ulaIpv6": ula_ipv6,
                "rejected": rejected, "reason": reason,
            }),
        ),
        Event::LanUiEvent { up, success } => (
            if up {
                "lan_peer_up_emit"
            } else {
                "lan_peer_down_emit"
            },
            json!({
                "success": success, "delivery": "tauri_emit_acceptance_not_ui_ack", "reason": if success { Reason::None } else { Reason::EmitFailed },
            }),
        ),
        Event::LanUiSuppressed(reason) => ("lan_peer_up_suppressed", json!({"reason": reason})),
        Event::ListenerInit {
            ipv4_ready,
            ipv6_ready,
        } => (
            "listener_init",
            json!({"ipv4Ready": ipv4_ready, "ipv6Ready": ipv6_ready}),
        ),
        Event::ListenerFailed => ("listener_init_failed", json!({})),
        Event::InterfacePolicy => (
            "interface_selection_policy",
            json!({"policy": "mdns_library_automatic", "selectionVisibility": "library_owned_not_nic_verified"}),
        ),
        Event::PeerUpCount(n) => (
            "legacy_known_peer_count",
            json!({"count": n, "meaning": "protocol_valid_not_ui_visible"}),
        ),
        Event::PeerDownCount(n) => ("legacy_known_peer_count_after_removal", json!({"count": n})),
        Event::UiSnapshot {
            received_up,
            received_down,
            rendered_peers,
            reason,
        } => (
            "ui_snapshot",
            json!({
                "receivedUp": received_up, "receivedDown": received_down, "renderedPeers": rendered_peers,
                "reason": reason, "countsScope": "received_event_counts_and_current_selectable_dom_options_not_screen_paint",
            }),
        ),
        Event::ComputerName { local, name } => (
            "computer_name",
            json!({"role": if local { "local" } else { "peer" }, "name": sanitize_computer_name(&name)}),
        ),
    }
}

fn append_with_context(
    path: &Path,
    event: Event,
    round: u64,
    round_elapsed_ms: u64,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let (label, data) = fields(event);
    let line = format!(
        "{}\n",
        json!({
            "schema": 2, "entrySequence": ENTRY_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            "timestampMs": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
            "processElapsedMs": PROCESS_START.get_or_init(Instant::now).elapsed().as_millis(),
            "buildId": build_id(), "round": round, "roundElapsedMs": round_elapsed_ms,
            "event": label, "data": data,
        })
    );
    if line.len() as u64 > CAP {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "diagnostic entry exceeds size cap",
        ));
    }
    if path
        .metadata()
        .map(|m| m.len().saturating_add(line.len() as u64) > CAP)
        .unwrap_or(false)
    {
        let old = path.with_file_name(ROTATED);
        let _ = fs::remove_file(&old);
        fs::rename(path, old)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(line.as_bytes())
}

pub fn initialize(app_data: &Path) {
    PROCESS_START.get_or_init(Instant::now);
    let path = app_data.join("discovery-diagnostics").join("discovery.log");
    *LOGGER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some(path);
    record(Event::Environment);
}

pub fn record(event: Event) {
    let (round, elapsed) = context();
    record_for_round(round, elapsed, event);
}

pub fn record_for_round(round: u64, round_elapsed_ms: u64, event: Event) {
    let _guard = IO_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(lock) = LOGGER.get() {
        if let Some(path) = lock.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            let _ = append_with_context(path, event, round, round_elapsed_ms);
        }
    }
}

pub fn export_text(app_data: &Path) -> std::io::Result<String> {
    let _guard = IO_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = app_data.join("discovery-diagnostics");
    let mut out = String::new();
    for name in [ROTATED, "discovery.log"] {
        let path = dir.join(name);
        if path.exists() {
            let mut bytes = Vec::new();
            fs::File::open(path)?.take(CAP).read_to_end(&mut bytes)?;
            // Only valid UTF-8 written by this logger is exported; malformed files cannot expand the cap.
            if let Ok(text) = String::from_utf8(bytes) {
                out.push_str(&text);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp() -> PathBuf {
        std::env::temp_dir().join(format!("fling-diagnostics-test-{}", rand::random::<u64>()))
    }
    #[test]
    fn candidate_diagnostic_export_never_contains_raw_network_or_identifier_sentinels() {
        use crate::pairing_diagnostics::{
            new_pair_trace, CandidateDiagnostic, PairReason, PairRole, PairStage, PairState,
            SourceFingerprint,
        };
        let root = temp();
        let path = root.join("discovery-diagnostics").join("discovery.log");
        let sentinels = [
            "SERVICE_ID_SENTINEL._fling._tcp.local.",
            "DEVICE_ID_SENTINEL",
            "SESSION_ID_SENTINEL",
            "TRANSFER_ID_SENTINEL",
            "aa:bb:cc:dd:ee:ff",
            "C:\\secret\\PRIVATE_FILE_SENTINEL.txt",
            "SHORT_CODE_SENTINEL",
            "KEY_SECRET_SENTINEL",
            "PAYLOAD_CONTENT_SENTINEL",
        ];
        let source = SourceFingerprint::from_fullname(&sentinels.join("/"));
        let trace = new_pair_trace(PairRole::Sender, Some(19));
        for (index, socket) in ["192.168.246.19:49157", "[fd12:3456:789a::42]:49157"]
            .into_iter()
            .enumerate()
        {
            let candidate = CandidateDiagnostic::new(
                index + 1,
                socket.parse().unwrap(),
                true,
                vec![source.clone()],
            );
            for (stage, state, reason) in [
                (PairStage::Endpoints, PairState::Ok, PairReason::None),
                (PairStage::TcpConnect, PairState::Start, PairReason::None),
                (
                    PairStage::TcpConnect,
                    PairState::Timeout,
                    PairReason::ConnectTimeout,
                ),
            ] {
                trace.record_candidate(stage, state, reason, &candidate, None, Some(2));
            }
        }
        for entry in trace.captured_entries() {
            append_with_context(&path, Event::Pairing(entry), 0, 0).unwrap();
        }
        let text = export_text(&root).unwrap();
        for raw in sentinels.into_iter().chain([
            "192.168.246.19",
            "fd12:3456:789a::42",
            ":49157",
            "\"port\":49157",
        ]) {
            assert!(
                !text.contains(raw),
                "raw sentinel escaped allowlisted export"
            );
        }
        let rows = text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 6);
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(row["event"], "pairing_stage");
            assert_eq!(row["data"]["attempt"], trace.id());
            assert_eq!(row["data"]["candidate"]["index"], index / 3 + 1);
            assert_eq!(row["data"]["candidate"]["portNonzero"], true);
            assert!(row["data"]["candidate"].get("port").is_none());
            assert!(row["data"]["candidate"].get("address").is_none());
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn post_prompt_all_events_are_bounded_correlated_and_private_in_json_and_manual_export() {
        use crate::pairing_diagnostics::{
            new_pair_trace, CandidateCategory, CandidateDiagnostic, CandidateFamily,
            CandidateRejection, CandidateTransport, FunnelEvent, FunnelTrace, PairReason, PairRole,
            PairStage, PairState, SourceFingerprint,
        };
        let events = [
            FunnelEvent::DecisionLocalSubmittedApproved,
            FunnelEvent::DecisionLocalSubmittedRejected,
            FunnelEvent::DecisionLocalUnavailable,
            FunnelEvent::UiModalMounted,
            FunnelEvent::UiModalVisible,
            FunnelEvent::UiDecisionConfirmed,
            FunnelEvent::UiDecisionRejected,
            FunnelEvent::UiDecisionCancelled,
            FunnelEvent::DecisionLocalApprovedMatch,
            FunnelEvent::DecisionLocalApprovedMismatch,
            FunnelEvent::DecisionLocalRejected,
            FunnelEvent::PeerDecisionConfirmed,
            FunnelEvent::PeerDecisionRejected,
            FunnelEvent::PeerDecisionInvalid,
            FunnelEvent::AuthConfirmed,
            FunnelEvent::PeerConnectionStart,
            FunnelEvent::PeerConnectionOk,
            FunnelEvent::PeerConnectionError,
            FunnelEvent::OfferGenerated,
            FunnelEvent::OfferError,
            FunnelEvent::OfferQueued,
            FunnelEvent::OfferSent,
            FunnelEvent::OfferWriteStart,
            FunnelEvent::OfferWriteError,
            FunnelEvent::OfferWriteTimeout,
            FunnelEvent::OfferReceived,
            FunnelEvent::AnswerGenerated,
            FunnelEvent::AnswerError,
            FunnelEvent::AnswerQueued,
            FunnelEvent::AnswerSent,
            FunnelEvent::AnswerWriteStart,
            FunnelEvent::AnswerWriteError,
            FunnelEvent::AnswerWriteTimeout,
            FunnelEvent::AnswerReceived,
            FunnelEvent::IceGatheringNew,
            FunnelEvent::IceGatheringGathering,
            FunnelEvent::IceGatheringComplete,
            FunnelEvent::IceConnectionNew,
            FunnelEvent::IceConnectionChecking,
            FunnelEvent::IceConnectionConnected,
            FunnelEvent::IceConnectionCompleted,
            FunnelEvent::IceConnectionFailed,
            FunnelEvent::IceConnectionDisconnected,
            FunnelEvent::IceConnectionClosed,
            FunnelEvent::IceConnectionUnknown,
            FunnelEvent::DataChannelCreated,
            FunnelEvent::DataChannelReceived,
            FunnelEvent::DataChannelOpen,
            FunnelEvent::DataChannelClose,
            FunnelEvent::DataChannelError,
            FunnelEvent::ManifestQueued,
            FunnelEvent::ManifestSent,
            FunnelEvent::ManifestWriteStart,
            FunnelEvent::ManifestWriteError,
            FunnelEvent::ManifestWriteTimeout,
            FunnelEvent::ManifestReceived,
            FunnelEvent::ManifestAccepted,
            FunnelEvent::ManifestDenied,
            FunnelEvent::ManifestPeerAccepted,
            FunnelEvent::FirstFrameSendAttempt,
            FunnelEvent::FirstFrameNativeAccepted,
            FunnelEvent::FirstFrameSendError,
            FunnelEvent::FirstFrameReceiverArrival,
            FunnelEvent::FirstFrameAppendOk,
            FunnelEvent::FirstFrameAppendError,
        ];
        let root = temp();
        let path = root.join("discovery-diagnostics").join("discovery.log");
        let sentinels = [
            "192.168.246.19",
            "fd12:3456:789a::42",
            "49157",
            "aa:bb:cc:dd:ee:ff",
            "candidate:CANDIDATE_SENTINEL 1 udp 2122260223 192.168.246.19 49157 typ host",
            "v=0\r\na=SDP_SENTINEL",
            "SHORT_CODE_SENTINEL",
            "KEY_SECRET_SENTINEL",
            "SESSION_ID_SENTINEL",
            "TASK_ID_SENTINEL",
            "TRANSFER_ID_SENTINEL",
            "DEVICE_ID_SENTINEL",
            "SERVICE_ID_SENTINEL",
            "PAYLOAD_SENTINEL",
            "C:\\private\\FILE_NAME_SENTINEL.txt",
        ];
        let mut expected = 0;
        for role in [PairRole::Sender, PairRole::Receiver] {
            let trace = new_pair_trace(role, None);
            let diagnostic = std::sync::Arc::new(FunnelTrace::new(trace.clone()));
            // Runtime-only raw values can enter provenance hashing, not the post-prompt schema.
            let candidate = CandidateDiagnostic::new(
                1,
                "192.168.246.19:49157".parse().unwrap(),
                true,
                vec![SourceFingerprint::from_fullname(&sentinels.join("/"))],
            );
            trace.record_candidate(
                PairStage::Endpoints,
                PairState::Ok,
                PairReason::None,
                &candidate,
                None,
                None,
            );
            let handles = (0..4)
                .map(|_| {
                    let diagnostic = diagnostic.clone();
                    std::thread::spawn(move || {
                        for _ in 0..250 {
                            for event in events {
                                diagnostic.record(event);
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            for handle in handles {
                handle.join().unwrap();
            }
            for _ in 0..1000 {
                diagnostic.candidate(
                    CandidateFamily::Ipv4,
                    CandidateTransport::Udp,
                    CandidateCategory::Host,
                    CandidateRejection::None,
                );
            }
            diagnostic.flush_candidates();
            diagnostic.flush_candidates();
            let entries = trace.captured_entries();
            assert_eq!(entries.len(), events.len() + 2);
            let json = serde_json::to_value(&entries).unwrap();
            for entry in json.as_array().unwrap() {
                assert_eq!(entry["attempt"], trace.id());
                for key in entry.as_object().unwrap().keys() {
                    assert!(
                        [
                            "attempt",
                            "role",
                            "uiAttempt",
                            "attemptElapsedMs",
                            "stage",
                            "state",
                            "reason",
                            "endpointCount",
                            "candidate",
                            "localAddressClass",
                            "reachability",
                            "evidence",
                            "funnelEvent",
                            "codeMatch",
                            "candidateCounts",
                            "approved"
                        ]
                        .contains(&key.as_str()),
                        "unexpected key {key}"
                    );
                }
            }
            for raw in &sentinels {
                assert!(!json.to_string().contains(raw));
            }
            expected += entries.len();
            for entry in entries {
                append_with_context(&path, Event::Pairing(entry), 0, 0).unwrap();
            }
        }
        let exported = export_text(&root).unwrap();
        assert_eq!(exported.lines().count(), expected);
        for raw in &sentinels {
            assert!(
                !exported.contains(raw),
                "private sentinel escaped manual export"
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rotation_is_bounded_and_export_is_allowlisted() {
        let root = temp();
        let dir = root.join("discovery-diagnostics");
        let path = dir.join("discovery.log");
        fs::create_dir_all(&dir).unwrap();
        fs::write(&path, " ".repeat(CAP as usize)).unwrap();
        append_with_context(
            &path,
            Event::RefreshOutcome {
                outcome: Outcome::Completed,
                reason: Reason::None,
                counts: PhaseCounts::default(),
            },
            1,
            3000,
        )
        .unwrap();
        assert!(path.metadata().unwrap().len() <= CAP);
        assert!(path.with_file_name(ROTATED).metadata().unwrap().len() <= CAP);
        assert!(export_text(&root).unwrap().len() <= (2 * CAP) as usize);
        fs::write(&path, " ".repeat(CAP as usize)).unwrap();
        append_with_context(&path, Event::RefreshTimeout, 2, 5000).unwrap();
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 2);
        assert!(export_text(&root).unwrap().len() <= (2 * CAP) as usize);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn timeline_has_order_round_elapsed_build_and_distinct_outcome_counts() {
        let root = temp();
        let path = root.join("discovery.log");
        let counts = PhaseCounts {
            resolved_services: 3,
            valid_services: 1,
            lan_eligible_peers: 0,
            ui_emitted_peers: 0,
        };
        for (ms, event) in [
            (0, Event::RefreshRequested),
            (40, Event::BrowseConfirmed),
            (
                3040,
                Event::RefreshOutcome {
                    outcome: Outcome::Completed,
                    reason: Reason::None,
                    counts,
                },
            ),
        ] {
            append_with_context(&path, event, 7, ms).unwrap();
        }
        let text = fs::read_to_string(&path).unwrap();
        let entries: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(entries[0]["entrySequence"].as_u64() < entries[2]["entrySequence"].as_u64());
        assert_eq!(entries[1]["event"], "browse_start_confirmed");
        assert_eq!(entries[2]["event"], "refresh_outcome");
        assert_eq!(entries[2]["round"], 7);
        assert_eq!(entries[2]["roundElapsedMs"], 3040);
        assert_eq!(entries[2]["data"]["counts"]["validServices"], 1);
        assert_eq!(entries[2]["data"]["counts"]["uiEmittedPeers"], 0);
        assert!(entries[2]["buildId"].is_string());
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn computer_name_filter_rejects_identifiers_paths_addresses_and_injection() {
        assert_eq!(sanitize_computer_name("Den-PC"), "Den-PC");
        assert_eq!(sanitize_computer_name("书房电脑"), "书房电脑");
        for bad in [
            "192.168.1.2",
            "PC-192.168.1.2",
            "PC192.168.1.2Host",
            "aa:bb:cc:dd:ee:ff",
            "PC-aa-bb-cc-dd-ee-ff",
            "C:\\Users\\name",
            "0123456789abcdef0123456789abcdef01234567",
            "550e8400-e29b-41d4-a716-446655440000",
            "../x",
            "\nInjected",
            "key\u{202e}spoof",
            "sk-secret",
        ] {
            assert_eq!(
                sanitize_computer_name(bad),
                "[redacted]",
                "unsafe name was accepted"
            );
        }
        assert_eq!(sanitize_computer_name(&"a".repeat(65)), "[redacted]");
    }
    #[test]
    fn fixed_fields_do_not_contain_network_addresses_or_raw_errors() {
        let (_, data) = fields(Event::AddressEligibility {
            private_ipv4: 1,
            link_local_ipv4: 0,
            ula_ipv6: 0,
            rejected: 2,
            reason: Reason::NoEligible,
        });
        assert_eq!(data["privateIpv4"], 1);
        assert_eq!(data["reason"], "no_eligible");
        let (_, name) = fields(Event::ComputerName {
            local: false,
            name: "Desk-PC".to_string(),
        });
        assert_eq!(name["name"], "Desk-PC");
        let (_, iface) = fields(Event::InterfacePolicy);
        assert_eq!(
            iface["selectionVisibility"],
            "library_owned_not_nic_verified"
        );
        assert!(!data.to_string().contains("192.168"));
    }
    #[test]
    fn oversized_name_cannot_create_an_oversized_log_line() {
        let root = temp();
        let path = root.join("discovery.log");
        append_with_context(
            &path,
            Event::ComputerName {
                local: false,
                name: "z".repeat((CAP * 2) as usize),
            },
            0,
            0,
        )
        .unwrap();
        assert!(path.metadata().unwrap().len() < CAP);
        let entry: Value = serde_json::from_str(fs::read_to_string(path).unwrap().trim()).unwrap();
        assert_eq!(entry["data"]["name"], "[redacted]");
        fs::remove_dir_all(root).unwrap();
    }
}
