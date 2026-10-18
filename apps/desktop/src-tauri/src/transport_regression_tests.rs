//! These exercise production auth/transport handlers with native libwebrtc.
//! They are deliberately NOT claimed as Tauri command/router or cross-Windows E2E.
use super::super::{
    process_control_frame,
    tests::{observed_decide, observed_pair},
    ControlOutcome,
};
use super::*;
use crate::pairing_diagnostics::{FunnelTrace, PairTrace};
use std::net::SocketAddr;

fn authenticated_pair() -> (
    NativeLanSession,
    NativeLanSession,
    PairTrace,
    PairTrace,
    async_mpsc::Receiver<crate::native_lan::EncryptedControlFrame>,
    async_mpsc::Receiver<crate::native_lan::EncryptedControlFrame>,
) {
    let (mut a, mut b, at, bt, code) = observed_pair();
    let af = observed_decide(&a, true, &code);
    let bf = observed_decide(&b, true, &code);
    assert!(matches!(
        process_control_frame(&a, &bf).unwrap(),
        ControlOutcome::PairingConfirmed
    ));
    assert!(matches!(
        process_control_frame(&b, &af).unwrap(),
        ControlOutcome::PairingConfirmed
    ));
    let (tx, ar) = async_mpsc::channel(32);
    a.control_tx = tx;
    let (tx, br) = async_mpsc::channel(32);
    b.control_tx = tx;
    (a, b, at, bt, ar, br)
}
fn entries(trace: &PairTrace) -> serde_json::Value {
    serde_json::to_value(trace.captured_entries()).unwrap()
}
fn has(trace: &PairTrace, phase: &str, outcome: &str, reason: &str) -> bool {
    entries(trace).as_array().unwrap().iter().any(|row| {
        row["transport"]["phase"] == phase
            && row["transport"]["outcome"] == outcome
            && row["transport"]["reason"] == reason
    })
}
fn ice_payload(line: &str) -> String {
    serde_json::to_string(&IceCandidateInit {
        candidate: line.into(),
        sdp_mid: Some("0".into()),
        sdp_mline_index: Some(0),
        username_fragment: None,
    })
    .unwrap()
}
const LOCAL: &str = "candidate:MARKER_CANDIDATE 1 udp 2122260223 127.0.0.1 49157 typ host";

#[tokio::test]
async fn authenticated_native_peers_route_offer_answer_pending_and_delayed_ice() {
    let (mut a, b, at, bt, mut ar, mut br) = authenticated_pair();
    let (dc_tx, mut dc_rx) = async_mpsc::unbounded_channel();
    a.data_channel_tx = dc_tx;
    let (_af, ap) = initialize_peer(&a, |_| {}).unwrap().unwrap();
    let (_bf, _bp) = initialize_peer(&b, |_| {}).unwrap().unwrap();
    let runtime = NativeLanRuntime::new();
    // Production receiver queues a validated early candidate until remote SDP succeeds.
    route_transport_control(
        &runtime,
        &b,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(LOCAL)),
        },
    )
    .await
    .unwrap();
    assert_eq!(b.transfer.lock().unwrap().pending_remote_ice.len(), 1);
    begin_offer(&a, &ap).await.unwrap();
    let channel = dc_rx.recv().await.unwrap();
    observe_channel_state(&a.diagnostic, channel.state(), true);
    let frame = ar.recv().await.unwrap();
    a.observe_control_write_start(frame.sequence);
    a.observe_control_written(frame.sequence);
    let ControlOutcome::Message(offer) = process_control_frame(&b, &frame).unwrap() else {
        panic!("offer")
    };
    route_transport_control(&runtime, &b, offer).await.unwrap();
    assert!(has(&bt, "offer_set_remote", "ok", "none"));
    assert!(has(&bt, "pending_ice_apply", "ok", "none"));
    assert!(b.transfer.lock().unwrap().pending_remote_ice.is_empty());
    let frame = br.recv().await.unwrap();
    b.observe_control_written(frame.sequence);
    let ControlOutcome::Message(answer) = process_control_frame(&a, &frame).unwrap() else {
        panic!("answer")
    };
    let NativeControlMessage::P2pAnswer { sdp } = &answer else {
        panic!("answer")
    };
    let answer_sdp = sdp.clone();
    route_transport_control(&runtime, &a, answer).await.unwrap();
    for phase in [
        "answer_validate",
        "answer_peer_lookup",
        "answer_parse",
        "answer_set_remote",
    ] {
        assert!(has(&at, phase, "start", "none"), "{phase}");
        assert!(has(&at, phase, "ok", "none"), "{phase}");
    }
    assert!(a.transfer.lock().unwrap().remote_description_set);
    // Delayed candidates go straight through the same production add handler.
    route_transport_control(
        &runtime,
        &a,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(LOCAL)),
        },
    )
    .await
    .unwrap();
    // The same valid answer on a fresh PC without a local offer parses but is
    // rejected by native set_remote_description, not mislabeled parse-success.
    let (c, _, ct, _, _cr, _dr) = authenticated_pair();
    initialize_peer(&c, |_| {}).unwrap().unwrap();
    assert!(route_transport_control(
        &runtime,
        &c,
        NativeControlMessage::P2pAnswer { sdp: answer_sdp }
    )
    .await
    .is_err());
    assert!(has(&ct, "answer_parse", "ok", "none"));
    assert!(has(&ct, "answer_set_remote", "error", "set_remote"));
    close(&c);
    close(&a);
    close(&b);
    channel.close();
}

#[tokio::test]
async fn answer_policy_parse_and_pre_initialization_are_separate_failures() {
    let runtime = NativeLanRuntime::new();
    for (sdp, init, phase, reason) in [
        (
            "SDP_MARKER candidate:RAW_MARKER",
            false,
            "answer_validate",
            "sdp_policy",
        ),
        (
            "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nSDP_MARKER",
            false,
            "answer_peer_lookup",
            "peer_not_ready",
        ),
        (
            "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\nSDP_MARKER",
            true,
            "answer_parse",
            "sdp_parse",
        ),
    ] {
        let (a, b, at, _, _ar, _br) = authenticated_pair();
        if init {
            initialize_peer(&a, |_| {}).unwrap().unwrap();
        }
        assert!(route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pAnswer { sdp: sdp.into() }
        )
        .await
        .is_err());
        assert!(
            has(&at, phase, "error", reason),
            "expected {phase}/{reason}; observations={}",
            entries(&at)
        );
        let json = entries(&at).to_string();
        assert!(!json.contains("SDP_MARKER") && !json.contains("RAW_MARKER"));
        close(&a);
        close(&b);
    }
}

#[tokio::test]
async fn early_candidate_before_pc_install_is_observed_not_silently_called_fixed() {
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    let runtime = NativeLanRuntime::new();
    route_transport_control(
        &runtime,
        &a,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(LOCAL)),
        },
    )
    .await
    .unwrap();
    assert_eq!(a.transfer.lock().unwrap().pending_remote_ice.len(), 1);
    initialize_peer(&a, |_| {}).unwrap().unwrap();
    // Existing behavior is intentionally retained. This injected ordering is NOT
    // proof that the Windows production reader/command race caused the incident.
    assert!(a.transfer.lock().unwrap().pending_remote_ice.is_empty());
    let rows = entries(&at);
    assert!(rows
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["transport"]["phase"] == "peer_install" && r["transport"]["count"] == 1));
    close(&a);
    close(&b);
}

#[tokio::test]
async fn candidate_validation_has_one_rejection_category_per_received_payload() {
    let runtime = NativeLanRuntime::new();
    let cases = [
        ("x".repeat(4097), "candidate_payload_bound"),
        ("CANDIDATE_MARKER".into(), "candidate_payload_malformed"),
        (
            ice_payload("candidate:CANDIDATE_MARKER 1 udp 9 192.168.246.19 49157 typ srflx"),
            "candidate_non_host",
        ),
        (
            ice_payload("candidate:CANDIDATE_MARKER 1 udp 9 203.0.113.42 49157 typ host"),
            "candidate_non_local",
        ),
    ];
    let cases = cases.into_iter().chain([
        (ice_payload("CANDIDATE_MARKER"), "candidate_malformed_line"),
        (
            ice_payload("candidate:CANDIDATE_MARKER 1 udp 9 HOST_MARKER 49157 typ host"),
            "candidate_non_numeric",
        ),
    ]);
    for (payload, reason) in cases {
        let (a, b, at, _, _ar, _br) = authenticated_pair();
        assert!(route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pIce {
                candidate: Some(payload)
            }
        )
        .await
        .is_err());
        a.diagnostic.snapshot();
        let rows = entries(&at);
        let summary = rows
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .find(|r| r["transport"]["phase"] == "candidate_summary")
            .unwrap();
        assert_eq!(summary["transport"]["candidateSteps"]["received"], 1);
        assert_eq!(summary["transport"]["candidateSteps"]["rejected"], 1);
        assert_eq!(summary["transport"]["candidateRejections"][reason], 1);
        assert_eq!(
            summary["transport"]["candidateRejections"]
                .as_object()
                .unwrap()
                .len(),
            1
        );
        for marker in [
            "CANDIDATE_MARKER",
            "192.168.246.19",
            "203.0.113.42",
            "49157",
        ] {
            assert!(!rows.to_string().contains(marker));
        }
        close(&a);
        close(&b);
    }
}

#[test]
fn state_revisits_and_first_trigger_survive_cleanup_and_close() {
    let trace = crate::pairing_diagnostics::new_pair_trace(
        crate::pairing_diagnostics::PairRole::Sender,
        None,
    );
    let d = FunnelTrace::new(trace.clone());
    d.transition(Tp::IceConnection, To::Checking);
    d.transition(Tp::IceConnection, To::Connected);
    d.transition(Tp::IceConnection, To::Checking);
    d.transition(Tp::IceConnection, To::Failed);
    d.cleanup(Tr::SessionRemoval);
    d.local_close_requested();
    observe_channel_state(&d, DataChannelState::Closed, false);
    d.transition(Tp::IceConnection, To::Closed);
    assert!(!d.terminal(Tr::ControlRead));
    let rows = entries(&trace);
    assert_eq!(
        rows.as_array()
            .unwrap()
            .iter()
            .filter(|r| r["transport"]["phase"] == "ice_connection"
                && r["transport"]["outcome"] == "checking")
            .count(),
        2
    );
    assert_eq!(
        rows.as_array()
            .unwrap()
            .iter()
            .filter(|r| r["transport"]["phase"] == "first_terminal")
            .count(),
        1
    );
    assert!(has(&trace, "first_terminal", "error", "ice_failed"));
    let close = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["transport"]["phase"] == "data_channel_transition")
        .unwrap();
    assert_eq!(close["transport"]["closeOrigin"], "unknown");
    assert_eq!(close["transport"]["localCloseRequested"], true);
    assert_eq!(close["transport"]["closeObserved"], true);
}

#[test]
fn channel_closed_first_is_unknown_not_false_success_or_invented_remote_reason() {
    let trace = crate::pairing_diagnostics::new_pair_trace(
        crate::pairing_diagnostics::PairRole::Receiver,
        None,
    );
    let d = FunnelTrace::new(trace.clone());
    observe_channel_state(&d, DataChannelState::Connecting, true);
    observe_channel_state(&d, DataChannelState::Open, false);
    observe_channel_state(&d, DataChannelState::Closed, false);
    d.record(FunnelEvent::DataChannelClose);
    d.cleanup(Tr::SessionRemoval);
    let rows = entries(&trace);
    assert!(has(
        &trace,
        "first_terminal",
        "error",
        "data_channel_closed"
    ));
    let close = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["transport"]["outcome"] == "closed")
        .unwrap();
    assert_eq!(close["transport"]["closeOrigin"], "unknown");
    let legacy = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["funnelEvent"] == "data_channel_close")
        .unwrap();
    assert_eq!(legacy["state"], "closed");
    assert_eq!(legacy["reason"], "not_reported");
}

#[tokio::test]
async fn production_channel_wait_reports_open_close_timeout_and_observer_loss() {
    use crate::pairing_diagnostics::{new_pair_trace, PairRole};
    for state in [
        DataChannelState::Open,
        DataChannelState::Closed,
        DataChannelState::Connecting,
    ] {
        let trace = new_pair_trace(PairRole::Sender, None);
        let d = FunnelTrace::new(trace.clone());
        let (_tx, mut rx) = watch::channel(state);
        observe_channel_state(&d, state, true);
        let result = wait_channel_open(&d, &mut rx, Duration::from_millis(2)).await;
        if state == DataChannelState::Open {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
        if state == DataChannelState::Connecting {
            assert!(has(
                &trace,
                "first_terminal",
                "error",
                "data_channel_timeout"
            ));
        }
    }
    let trace = new_pair_trace(PairRole::Receiver, None);
    let d = FunnelTrace::new(trace.clone());
    let (tx, mut rx) = watch::channel(DataChannelState::Connecting);
    drop(tx);
    assert!(wait_channel_open(&d, &mut rx, Duration::from_secs(1))
        .await
        .is_err());
    assert!(has(
        &trace,
        "first_terminal",
        "error",
        "data_channel_observer"
    ));
}

#[tokio::test]
async fn production_reader_eof_then_registry_cleanup_preserves_single_first_reason() {
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    let runtime = NativeLanRuntime::new();
    let a = Arc::new(a);
    runtime.insert_session(a.clone()).unwrap();
    let (mut reader, writer) = tokio::io::duplex(32);
    drop(writer);
    assert!(
        super::super::read_control_observed(&mut reader, &a.diagnostic)
            .await
            .is_err()
    );
    runtime.remove_session(&a.session_id);
    a.diagnostic.transition(Tp::IceConnection, To::Closed);
    let rows = entries(&at);
    assert!(has(&at, "first_terminal", "error", "control_read"));
    assert_eq!(
        rows.as_array()
            .unwrap()
            .iter()
            .filter(|r| r["transport"]["phase"] == "first_terminal")
            .count(),
        1
    );
    assert!(has(&at, "cleanup", "start", "session_removal"));
    close(&b);
}

#[tokio::test]
async fn pending_bound_and_add_metadata_failures_are_uniquely_classified() {
    #[cfg(target_os = "windows")]
    std::env::set_var("RUST_BACKTRACE", "full");
    let runtime = NativeLanRuntime::new();
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    a.transfer.lock().unwrap().pending_remote_ice = vec![ice_payload(LOCAL); MAX_PENDING_ICE];
    assert!(receive_ice_candidate(&runtime, &a, ice_payload(LOCAL))
        .await
        .is_err());
    assert!(has(
        &at,
        "first_terminal",
        "error",
        "candidate_pending_bound"
    ));
    close(&a);
    close(&b);
    for (payload, reason) in [
        (
            r#"{"candidate":"CANDIDATE_MARKER","sdpMLineIndex":0}"#,
            "candidate_missing_mid",
        ),
        (
            r#"{"candidate":"CANDIDATE_MARKER","sdpMid":"0"}"#,
            "candidate_missing_mline",
        ),
        (
            r#"{"candidate":"CANDIDATE_MARKER","sdpMid":"0","sdpMLineIndex":0}"#,
            "candidate_parse",
        ),
    ] {
        let (a, b, at, _, _ar, _br) = authenticated_pair();
        assert!(add_remote_ice(&a, payload).await.is_err());
        assert!(has(&at, "first_terminal", "error", reason));
        close(&a);
        close(&b);
    }
}

#[tokio::test]
async fn candidate_queue_write_failure_and_terminal_flush_counts_are_bounded() {
    let (a, b, at, _, mut ar, _br) = authenticated_pair();
    a.diagnostic.count(Cs::Generated, 1);
    send_authenticated_control(
        &a,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(LOCAL)),
        },
    )
    .await
    .unwrap();
    let frame = ar.recv().await.unwrap();
    a.observe_control_write_start(frame.sequence);
    a.observe_control_written(frame.sequence);
    let ControlOutcome::Message(message) = process_control_frame(&b, &frame).unwrap() else {
        panic!("candidate")
    };
    route_transport_control(&NativeLanRuntime::new(), &b, message)
        .await
        .unwrap();
    a.diagnostic.snapshot(); // gathering-time snapshot is not a permanent freeze
    send_authenticated_control(
        &a,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(LOCAL)),
        },
    )
    .await
    .unwrap();
    let failed = ar.recv().await.unwrap();
    a.observe_control_write_failed(failed.sequence, true);
    drop(ar);
    assert!(send_authenticated_control(
        &a,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(LOCAL))
        }
    )
    .await
    .is_err());
    a.diagnostic.terminal(Tr::ControlWriteTimeout);
    close(&a);
    close(&b);
    let rows = entries(&at);
    let last = rows
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|r| r["transport"]["phase"] == "candidate_summary")
        .unwrap();
    assert_eq!(last["transport"]["candidateSteps"]["generated"], 1);
    assert_eq!(last["transport"]["candidateSteps"]["queued"], 2);
    assert_eq!(last["transport"]["candidateSteps"]["written"], 1);
    assert_eq!(last["transport"]["candidateSteps"]["write_failed"], 1);
    assert_eq!(last["transport"]["candidateSteps"]["queue_failed"], 1);
    assert!(a.diagnostic_control_writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn candidate_state_lock_failure_has_one_safe_rejection() {
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    let a = Arc::new(a);
    let thread_a = a.clone();
    let _ = std::thread::spawn(move || {
        let _lock = thread_a.transfer.lock().unwrap();
        panic!("OS_ERROR_MARKER SESSION_ID_MARKER KEY_MARKER PATH_MARKER");
    })
    .join();
    assert!(
        receive_ice_candidate(&NativeLanRuntime::new(), &a, ice_payload(LOCAL))
            .await
            .is_err()
    );
    assert!(has(&at, "first_terminal", "error", "state_lock"));
    let rows = entries(&at);
    assert!(!rows.to_string().contains("MARKER"));
    let last = rows
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|r| r["transport"]["phase"] == "candidate_summary")
        .unwrap();
    assert_eq!(last["transport"]["candidateSteps"]["rejected"], 1);
    assert_eq!(last["transport"]["candidateRejections"]["state_lock"], 1);
    close(&a);
    close(&b);
}

#[test]
fn simultaneous_terminal_observers_never_overwrite_first_reason() {
    use crate::pairing_diagnostics::{new_pair_trace, PairRole};
    let trace = new_pair_trace(PairRole::Sender, None);
    let d = FunnelTrace::new(trace.clone());
    let handles = [
        Tr::SdpParse,
        Tr::SetRemote,
        Tr::ControlRead,
        Tr::DataChannelClosed,
    ]
    .into_iter()
    .map(|r| {
        let d = d.clone();
        std::thread::spawn(move || {
            d.terminal(r);
            d.cleanup(Tr::SessionRemoval);
        })
    })
    .collect::<Vec<_>>();
    for h in handles {
        h.join().unwrap();
    }
    let rows = entries(&trace);
    let terminals = rows
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["transport"]["phase"] == "first_terminal")
        .collect::<Vec<_>>();
    assert_eq!(terminals.len(), 1);
    let first = &terminals[0]["transport"]["firstTerminal"];
    let cleanup = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["transport"]["phase"] == "cleanup")
        .unwrap();
    assert_eq!(&cleanup["transport"]["firstTerminal"], first);
}

#[test]
fn resolved_endpoint_projection_and_exact_candidate_policy_are_not_nic_ownership_proof() {
    use super::super::{candidate_matches_discovered_endpoints, eligible_lan_endpoints};
    let a: IpAddr = "192.168.246.19".parse().unwrap();
    let b: IpAddr = "192.168.247.23".parse().unwrap();
    let same_subnet_other: IpAddr = "192.168.246.20".parse().unwrap();
    let info = mdns_sd::ServiceInfo::new(
        "_fling._tcp.local.",
        "SERVICE_ID_MARKER",
        "HOST_MARKER.local.",
        "192.168.246.19",
        49157,
        None,
    )
    .unwrap()
    .enable_addr_auto();
    let endpoints = eligible_lan_endpoints(info.get_addresses(), info.get_port(), false);
    assert!(candidate_matches_discovered_endpoints(
        endpoints.iter().map(SocketAddr::ip),
        a
    ));
    assert!(!candidate_matches_discovered_endpoints(
        endpoints.iter().map(SocketAddr::ip),
        b
    ));
    assert!(!candidate_matches_discovered_endpoints(
        endpoints.iter().map(SocketAddr::ip),
        same_subnet_other
    ));
    assert!(candidate_matches_discovered_endpoints([a, b], b));
    assert!(!candidate_matches_discovered_endpoints([], b));
    // Fixture B may model another NIC, but this cannot prove the incident's B belongs to the peer.
    let mapped = IpAddr::V6("::ffff:192.168.246.19".parse().unwrap());
    assert!(candidate_matches_discovered_endpoints([a], mapped));
    assert!(candidate_matches_discovered_endpoints([mapped], a));
}
#[test]
fn all_current_fullnames_are_aggregated_but_foreign_peers_and_removed_addresses_are_not() {
    let runtime = NativeLanRuntime::new();
    let a: SocketAddr = "192.168.246.19:49157".parse().unwrap();
    let b: SocketAddr = "192.168.247.23:49157".parse().unwrap();
    runtime.upsert_peer(
        "SERVICE_ID_MARKER_A",
        "DEVICE_ID_MARKER",
        "NAME_MARKER",
        vec![a],
    );
    runtime.upsert_peer(
        "SERVICE_ID_MARKER_FOREIGN",
        "FOREIGN_DEVICE_ID_MARKER",
        "NAME_MARKER",
        vec![b],
    );
    assert!(!runtime.peer_matches_endpoint("DEVICE_ID_MARKER", b.ip()));
    runtime.upsert_peer(
        "SERVICE_ID_MARKER_B",
        "DEVICE_ID_MARKER",
        "NAME_MARKER",
        vec![b],
    );
    assert!(runtime.peer_matches_endpoint("DEVICE_ID_MARKER", b.ip()));
    runtime.remove_peer("SERVICE_ID_MARKER_B");
    assert!(!runtime.peer_matches_endpoint("DEVICE_ID_MARKER", b.ip()));
    runtime.upsert_peer(
        "SERVICE_ID_MARKER_A",
        "DEVICE_ID_MARKER",
        "NAME_MARKER",
        vec![],
    );
    assert!(!runtime.peer_matches_endpoint("DEVICE_ID_MARKER", a.ip()));
}
#[test]
fn rejected_candidate_family_and_lan_class_are_allowlisted_and_never_contain_input() {
    use crate::transport_diagnostics::{CandidateAddressFamily as F, CandidateLanClass as A};
    for (address, family, class) in [
        ("192.168.246.19", F::Ipv4, A::PrivateIpv4),
        ("169.254.246.19", F::Ipv4, A::LinkLocalIpv4),
        ("fd12:3456:789a::42", F::Ipv6, A::UlaIpv6),
        ("fe80::42", F::Ipv6, A::LinkLocalIpv6),
        ("::ffff:192.168.246.19", F::Ipv6, A::Ipv4MappedIpv6),
        ("127.0.0.1", F::Ipv4, A::Loopback),
        ("0.0.0.0", F::Ipv4, A::Unspecified),
        ("224.0.0.1", F::Ipv4, A::Multicast),
        ("203.0.113.42", F::Ipv4, A::Other),
        ("HOST_MARKER", F::Unknown, A::Unknown),
    ] {
        let input = format!("candidate:CANDIDATE_MARKER 1 udp 9 {address} 49157 typ host SHORT_CODE_MARKER KEY_MARKER SESSION_ID_MARKER C:/PATH_MARKER");
        let context = rejected_candidate_context(&input, Some(false));
        assert_eq!(context.family, family);
        assert_eq!(context.lan_class, class);
        assert_eq!(context.endpoint_matched, Some(false));
        let json = serde_json::to_string(&context).unwrap();
        for marker in [address, "49157", "MARKER", "candidate:"] {
            assert!(!json.contains(marker));
        }
    }
}
#[test]
fn classification_order_remains_host_then_lan_then_exact_endpoint_match() {
    let runtime = NativeLanRuntime::new();
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    runtime.upsert_peer(
        "SERVICE_ID_MARKER",
        &a.peer_device_id,
        "NAME_MARKER",
        vec!["192.168.246.19:49157".parse().unwrap()],
    );
    for (line, reason) in [
        (
            "candidate:MARKER 1 udp 9 203.0.113.42 49157 typ srflx",
            Tr::CandidateNonHost,
        ),
        (
            "candidate:MARKER 1 udp 9 203.0.113.42 49157 typ host",
            Tr::CandidateNonLocal,
        ),
        (
            "candidate:MARKER 1 udp 9 192.168.247.23 49157 typ host",
            Tr::CandidatePeerMismatch,
        ),
    ] {
        assert_eq!(
            classify_peer_candidate(&runtime, &a, line).unwrap_err().0,
            reason
        );
    }
    assert!(classify_peer_candidate(
        &runtime,
        &a,
        "candidate:MARKER 1 udp 9 192.168.246.19 49157 typ host"
    )
    .is_ok());
    a.diagnostic.snapshot();
    let value = entries(&at);
    // First rejection is non-host, before endpoint lookup; do not invent a false match result.
    let last = value.as_array().unwrap().last().unwrap();
    assert!(last["transport"]["firstRejectedCandidate"]
        .get("endpointMatched")
        .is_none());
    close(&a);
    close(&b);
}
#[test]
fn first_rejected_candidate_context_is_fixed_and_never_emits_per_candidate_logs() {
    let trace = crate::pairing_diagnostics::new_pair_trace(
        crate::pairing_diagnostics::PairRole::Sender,
        None,
    );
    let diagnostic = FunnelTrace::new(trace.clone());
    let first = rejected_candidate_context(
        "candidate:MARKER 1 udp 9 192.168.246.19 49157 typ host",
        Some(false),
    );
    let later = rejected_candidate_context(
        "candidate:OTHER_MARKER 1 udp 9 fd12:3456:789a::42 49157 typ host",
        Some(true),
    );
    let before = trace.captured_entries().len();
    diagnostic.rejected_candidate_context(first);
    for _ in 0..10000 {
        diagnostic.rejected_candidate_context(later);
    }
    assert_eq!(trace.captured_entries().len(), before);
    diagnostic.snapshot();
    assert_eq!(trace.captured_entries().len(), before + 1);
    let value = entries(&trace);
    assert_eq!(
        value.as_array().unwrap().last().unwrap()["transport"]["firstRejectedCandidate"],
        serde_json::to_value(first).unwrap()
    );
}
#[tokio::test]
async fn authenticated_reader_skips_link_local_mismatch_then_adds_exact_private_endpoint() {
    let (mut a, b, at, _, mut ar, mut br) = authenticated_pair();
    let (dc_tx, mut dc_rx) = async_mpsc::unbounded_channel();
    a.data_channel_tx = dc_tx;
    let (_af, ap) = initialize_peer(&a, |_| {}).unwrap().unwrap();
    let (_bf, _bp) = initialize_peer(&b, |_| {}).unwrap().unwrap();
    let runtime = NativeLanRuntime::new();
    runtime.upsert_peer(
        "SERVICE_ID_MARKER",
        &a.peer_device_id,
        "NAME_MARKER",
        vec!["192.168.246.19:49157".parse().unwrap()],
    );
    begin_offer(&a, &ap).await.unwrap();
    let channel = dc_rx.recv().await.unwrap();
    let ControlOutcome::Message(offer) =
        process_control_frame(&b, &ar.recv().await.unwrap()).unwrap()
    else {
        panic!("offer")
    };
    route_transport_control(&runtime, &b, offer).await.unwrap();
    let ControlOutcome::Message(answer) =
        process_control_frame(&a, &br.recv().await.unwrap()).unwrap()
    else {
        panic!("answer")
    };
    route_transport_control(&runtime, &a, answer).await.unwrap();
    assert!(has(&at, "answer_set_remote", "ok", "none"));
    let a = Arc::new(a);
    runtime.insert_session(a.clone()).unwrap();
    let mismatch = NativeControlMessage::P2pIce {
        candidate: Some(ice_payload(
            "candidate:CANDIDATE_MARKER 1 udp 9 169.254.247.23 49157 typ host",
        )),
    };
    send_authenticated_control(&b, mismatch).await.unwrap();
    let frame = br.recv().await.unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(8192);
    native_lan::write_control_frame(&mut writer, &frame)
        .await
        .unwrap();
    let frame = super::super::read_control_candidate_bounded(&mut reader, &a)
        .await
        .unwrap();
    let ControlOutcome::Message(message) = process_control_frame(&a, &frame).unwrap() else {
        panic!("authenticated ICE")
    };
    assert!(route_transport_control(&runtime, &a, message).await.is_ok());
    assert!(runtime.session(&a.session_id).is_ok());
    assert!(a.transfer.lock().unwrap().pending_remote_ice.is_empty());
    assert!(!entries(&at)
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["transport"]["phase"] == "first_terminal"));
    assert!(candidate_mismatch_deadline(&a).unwrap().is_some());
    send_authenticated_control(
        &b,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(
                "candidate:MATCHED_MARKER 1 udp 9 192.168.246.19 49157 typ host",
            )),
        },
    )
    .await
    .unwrap();
    native_lan::write_control_frame(&mut writer, &br.recv().await.unwrap())
        .await
        .unwrap();
    let frame = super::super::read_control_candidate_bounded(&mut reader, &a)
        .await
        .unwrap();
    let ControlOutcome::Message(message) = process_control_frame(&a, &frame).unwrap() else {
        panic!("authenticated ICE")
    };
    route_transport_control(&runtime, &a, message)
        .await
        .unwrap();
    assert!(candidate_mismatch_deadline(&a).unwrap().is_none());
    a.diagnostic.snapshot();
    let value = entries(&at);
    let last = value.as_array().unwrap().last().unwrap();
    assert_eq!(last["transport"]["candidateSteps"]["added"], 1);
    assert_eq!(last["transport"]["candidateSteps"]["received"], 2);
    assert!(last["transport"].get("firstTerminal").is_none());
    // Same authenticated stream, native remote SDP already installed. Matched
    // duplicate candidates still consume the total input budget, not just pending.
    for _ in 2..MAX_REMOTE_ICE_INPUTS {
        send_authenticated_control(
            &b,
            NativeControlMessage::P2pIce {
                candidate: Some(ice_payload(
                    "candidate:MATCHED_MARKER 1 udp 9 192.168.246.19 49157 typ host",
                )),
            },
        )
        .await
        .unwrap();
        native_lan::write_control_frame(&mut writer, &br.recv().await.unwrap())
            .await
            .unwrap();
        let frame = super::super::read_control_candidate_bounded(&mut reader, &a)
            .await
            .unwrap();
        let ControlOutcome::Message(message) = process_control_frame(&a, &frame).unwrap() else {
            panic!("ICE")
        };
        route_transport_control(&runtime, &a, message)
            .await
            .unwrap();
    }
    assert_eq!(
        a.transfer.lock().unwrap().remote_ice_inputs,
        MAX_REMOTE_ICE_INPUTS
    );
    send_authenticated_control(
        &b,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(
                "candidate:MATCHED_MARKER 1 udp 9 192.168.246.19 49157 typ host",
            )),
        },
    )
    .await
    .unwrap();
    native_lan::write_control_frame(&mut writer, &br.recv().await.unwrap())
        .await
        .unwrap();
    let frame = super::super::read_control_candidate_bounded(&mut reader, &a)
        .await
        .unwrap();
    let ControlOutcome::Message(message) = process_control_frame(&a, &frame).unwrap() else {
        panic!("ICE")
    };
    assert!(route_transport_control(&runtime, &a, message)
        .await
        .is_err());
    assert_eq!(
        a.transfer.lock().unwrap().remote_ice_inputs,
        MAX_REMOTE_ICE_INPUTS
    );
    assert!(has(
        &at,
        "first_terminal",
        "error",
        "candidate_input_budget"
    ));
    a.diagnostic.snapshot();
    let bounded = entries(&at);
    let bounded = bounded.as_array().unwrap().last().unwrap();
    assert_eq!(
        bounded["transport"]["candidateSteps"]["added"],
        MAX_REMOTE_ICE_INPUTS - 1
    );
    assert_eq!(
        bounded["transport"]["candidateSteps"]["received"],
        MAX_REMOTE_ICE_INPUTS
    );
    runtime.remove_session(&a.session_id);
    assert!(has(&at, "cleanup", "start", "session_removal"));
    let value = entries(&at);
    let summary = value
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["transport"]["firstRejectedCandidate"].is_object())
        .unwrap();
    let context = &summary["transport"]["firstRejectedCandidate"];
    assert_eq!(
        context,
        &serde_json::json!({"family":"ipv4","lanClass":"link_local_ipv4","endpointMatched":false})
    );
    assert_eq!(summary["transport"]["candidateSteps"]["added"], 1);
    assert_eq!(summary["transport"]["candidateSteps"]["rejected"], 1);
    assert_eq!(
        value
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["transport"]["phase"] == "first_terminal")
            .count(),
        1
    );
    for marker in [
        "192.168.246.19",
        "169.254.247.23",
        "49157",
        "CANDIDATE_MARKER",
        "MATCHED_MARKER",
        "SERVICE_ID_MARKER",
    ] {
        assert!(!value.to_string().contains(marker));
    }
    close(&b);
    channel.close();
}

const UNADVERTISED: &str = "candidate:MISMATCH_MARKER 1 udp 9 169.254.247.23 49157 typ host";

#[tokio::test]
async fn all_mismatches_fail_at_fixed_budget_without_adding_and_cleanup_closes_auth() {
    let (a, b, at, _, _ar, mut br) = authenticated_pair();
    let runtime = NativeLanRuntime::new();
    let a = Arc::new(a);
    let _reader_cancel = a.cancel_tx.subscribe();
    runtime.insert_session(a.clone()).unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(8192);
    let mut first_deadline = None;
    for n in 1..=MAX_CANDIDATE_MISMATCHES {
        send_authenticated_control(
            &b,
            NativeControlMessage::P2pIce {
                candidate: Some(ice_payload(UNADVERTISED)),
            },
        )
        .await
        .unwrap();
        native_lan::write_control_frame(&mut writer, &br.recv().await.unwrap())
            .await
            .unwrap();
        let frame = super::super::read_control_candidate_bounded(&mut reader, &a)
            .await
            .unwrap();
        let ControlOutcome::Message(message) = process_control_frame(&a, &frame).unwrap() else {
            panic!("ICE")
        };
        let result = route_transport_control(&runtime, &a, message).await;
        if n < MAX_CANDIDATE_MISMATCHES {
            assert!(result.is_ok());
        } else {
            assert!(result.is_err());
        }
        let deadline = candidate_mismatch_deadline(&a).unwrap();
        if n == 1 {
            first_deadline = deadline;
        }
        assert_eq!(deadline, first_deadline, "mismatch must not renew deadline");
        assert!(a.transfer.lock().unwrap().pending_remote_ice.is_empty());
    }
    assert!(has(
        &at,
        "first_terminal",
        "error",
        "candidate_mismatch_budget"
    ));
    assert!(!a.completed.load(std::sync::atomic::Ordering::Acquire));
    runtime.remove_session(&a.session_id);
    assert!(runtime.session(&a.session_id).is_err());
    assert!(*a.cancel_tx.borrow());
    assert!(matches!(
        *a.auth.lock().unwrap(),
        super::super::LocalAuthState::Closed
    ));
    let value = entries(&at);
    let summary = value
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|row| row["transport"]["phase"] == "candidate_summary")
        .unwrap();
    assert_eq!(summary["transport"]["candidateSteps"]["added"], 0);
    assert_eq!(
        summary["transport"]["candidateSteps"]["rejected"],
        MAX_CANDIDATE_MISMATCHES
    );
    assert!(has(&at, "cleanup", "start", "session_removal"));
    for marker in ["MISMATCH_MARKER", "169.254.247.23", "49157"] {
        assert!(!value.to_string().contains(marker));
    }
    close(&b);
}

#[tokio::test]
async fn mismatch_deadline_bounds_silent_partial_and_busy_control_streams() {
    for mode in ["silent", "partial", "ready"] {
        let (a, b, at, _, _ar, mut br) = authenticated_pair();
        let runtime = NativeLanRuntime::new();
        let a = Arc::new(a);
        runtime.insert_session(a.clone()).unwrap();
        route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pIce {
                candidate: Some(ice_payload(UNADVERTISED)),
            },
        )
        .await
        .unwrap();
        let original = candidate_mismatch_deadline(&a).unwrap().unwrap();
        assert!(original <= tokio::time::Instant::now() + CANDIDATE_MISMATCH_TIMEOUT);
        let (mut writer, mut reader) = tokio::io::duplex(8192);
        if mode == "partial" {
            use tokio::io::AsyncWriteExt;
            writer.write_all(&[0]).await.unwrap();
        } else if mode == "ready" {
            send_authenticated_control(&b, NativeControlMessage::P2pIce { candidate: None })
                .await
                .unwrap();
            native_lan::write_control_frame(&mut writer, &br.recv().await.unwrap())
                .await
                .unwrap();
        }
        // Accelerate the same production absolute deadline, not a per-read timeout.
        a.transfer.lock().unwrap().candidate_mismatch.deadline = Some(if mode == "ready" {
            tokio::time::Instant::now() - Duration::from_millis(1)
        } else {
            tokio::time::Instant::now() + Duration::from_millis(20)
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            super::super::read_control_candidate_bounded(&mut reader, &a),
        )
        .await
        .unwrap();
        assert!(result.is_err(), "{mode}");
        assert!(has(
            &at,
            "first_terminal",
            "error",
            "candidate_mismatch_timeout"
        ));
        runtime.remove_session(&a.session_id);
        assert!(has(&at, "cleanup", "start", "session_removal"));
        assert!(a.transfer.lock().unwrap().pending_remote_ice.is_empty());
        assert!(!a.completed.load(std::sync::atomic::Ordering::Acquire));
        close(&b);
    }
}

#[test]
fn mismatch_budget_is_constant_space_saturating_and_not_renewed() {
    let mut budget = CandidateMismatchBudget::default();
    assert!(budget.reject().is_ok());
    let deadline = budget.deadline;
    for _ in 1..MAX_CANDIDATE_MISMATCHES - 1 {
        assert!(budget.reject().is_ok());
    }
    for _ in 0..10000 {
        assert!(budget.reject().is_err());
    }
    assert_eq!(budget.rejected, MAX_CANDIDATE_MISMATCHES);
    assert_eq!(budget.deadline, deadline);
    budget.added();
    assert!(budget.deadline.is_none());
    assert!(
        budget.reject().is_err(),
        "adding never replenishes rejection budget"
    );
    assert!(budget.deadline.is_none());
}

#[tokio::test]
async fn mismatch_tolerance_does_not_soften_malformed_or_unauthenticated_candidates() {
    let runtime = NativeLanRuntime::new();
    let mut missing_mid: IceCandidateInit =
        serde_json::from_str(&ice_payload(UNADVERTISED)).unwrap();
    missing_mid.sdp_mid = None;
    let mut missing_mline: IceCandidateInit =
        serde_json::from_str(&ice_payload(UNADVERTISED)).unwrap();
    missing_mline.sdp_mline_index = None;
    for (payload, reason) in [
        (
            serde_json::to_string(&missing_mid).unwrap(),
            "candidate_missing_mid",
        ),
        (
            serde_json::to_string(&missing_mline).unwrap(),
            "candidate_missing_mline",
        ),
        (
            ice_payload("candidate:MARKER 1 udp 9 169.254.247.23 BAD_PORT typ host"),
            "candidate_parse",
        ),
        (
            ice_payload("candidate:MARKER 1 udp 9 169.254.247.23 49157 typ srflx"),
            "candidate_non_host",
        ),
        (
            ice_payload("candidate:MARKER 1 udp 9 203.0.113.42 49157 typ host"),
            "candidate_non_local",
        ),
        ("x".repeat(4097), "candidate_payload_bound"),
        ("INVALID_MARKER".into(), "candidate_payload_malformed"),
    ] {
        let (a, b, at, _, _ar, _br) = authenticated_pair();
        route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pIce {
                candidate: Some(ice_payload(UNADVERTISED)),
            },
        )
        .await
        .unwrap();
        assert!(route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pIce {
                candidate: Some(payload)
            }
        )
        .await
        .is_err());
        assert!(has(&at, "first_terminal", "error", reason), "{reason}");
        assert_eq!(a.transfer.lock().unwrap().candidate_mismatch.rejected, 1);
        close(&a);
        close(&b);
    }
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    *a.auth.lock().unwrap() = super::super::LocalAuthState::Closed;
    assert!(route_transport_control(
        &runtime,
        &a,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(UNADVERTISED))
        }
    )
    .await
    .is_err());
    assert!(has(&at, "first_terminal", "error", "control_decode"));
    assert_eq!(a.transfer.lock().unwrap().candidate_mismatch.rejected, 0);
    close(&a);
    close(&b);
}

#[tokio::test]
async fn each_post_mismatch_candidate_uses_authenticated_device_and_current_endpoint_set() {
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    let runtime = NativeLanRuntime::new();
    let candidate = "candidate:MARKER 1 udp 9 192.168.246.19 49157 typ host";
    let endpoint: SocketAddr = "192.168.246.19:49157".parse().unwrap();
    runtime.upsert_peer("FOREIGN_SERVICE", "FOREIGN_DEVICE", "NAME", vec![endpoint]);
    for action in ["foreign_only", "matching", "removed", "mapped_matching"] {
        if action == "matching" || action == "mapped_matching" {
            let advertised = if action == "mapped_matching" {
                "[::ffff:192.168.246.19]:49157".parse().unwrap()
            } else {
                endpoint
            };
            runtime.upsert_peer("SERVICE", &a.peer_device_id, "NAME", vec![advertised]);
        } else if action == "removed" {
            runtime.remove_peer("SERVICE");
        }
        // Preserve the existing mapped-address comparison compatibility without
        // broadening the earlier LAN address eligibility gate for IPv6 candidates.
        let line = candidate;
        route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pIce {
                candidate: Some(ice_payload(line)),
            },
        )
        .await
        .unwrap();
    }
    let state = a.transfer.lock().unwrap();
    assert_eq!(state.candidate_mismatch.rejected, 2);
    assert_eq!(state.pending_remote_ice.len(), 2);
    drop(state);
    assert!(!entries(&at)
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["transport"]["phase"] == "first_terminal"));
    close(&a);
    close(&b);
}

#[tokio::test]
async fn other_handler_errors_after_tolerated_mismatch_keep_terminal_and_cleanup() {
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    let runtime = NativeLanRuntime::new();
    let a = Arc::new(a);
    runtime.insert_session(a.clone()).unwrap();
    route_transport_control(
        &runtime,
        &a,
        NativeControlMessage::P2pIce {
            candidate: Some(ice_payload(UNADVERTISED)),
        },
    )
    .await
    .unwrap();
    assert!(route_transport_control(
        &runtime,
        &a,
        NativeControlMessage::P2pAnswer {
            sdp: "INVALID_SDP_MARKER".into()
        }
    )
    .await
    .is_err());
    assert!(has(&at, "first_terminal", "error", "sdp_policy"));
    runtime.remove_session(&a.session_id);
    assert!(has(&at, "cleanup", "start", "session_removal"));
    assert!(runtime.session(&a.session_id).is_err());
    close(&b);
}

#[tokio::test]
async fn mismatch_frames_cannot_bypass_pending_auth_identity_or_decryption() {
    for mode in ["pending", "foreign_identity", "ciphertext", "closed"] {
        let (a, b, _at, _, _ar, mut br) = authenticated_pair();
        send_authenticated_control(
            &b,
            NativeControlMessage::P2pIce {
                candidate: Some(ice_payload(UNADVERTISED)),
            },
        )
        .await
        .unwrap();
        let mut frame = br.recv().await.unwrap();
        let (pending, other_pending, _, _, _) = observed_pair();
        let target = if mode == "pending" { &pending } else { &a };
        if mode == "foreign_identity" {
            frame.from_device_id = "FOREIGN_ID_MARKER".into();
        }
        if mode == "ciphertext" {
            frame.ct = "INVALID_CIPHERTEXT_MARKER".into();
        }
        if mode == "closed" {
            *a.auth.lock().unwrap() = super::super::LocalAuthState::Closed;
        }
        assert!(process_control_frame(target, &frame).is_err(), "{mode}");
        assert_eq!(
            target.transfer.lock().unwrap().candidate_mismatch.rejected,
            0
        );
        assert!(target
            .transfer
            .lock()
            .unwrap()
            .pending_remote_ice
            .is_empty());
        close(&a);
        close(&b);
        close(&pending);
        close(&other_pending);
    }
}

#[tokio::test]
async fn remote_ice_total_budget_counts_empty_frames_and_never_grows_or_resets() {
    let (a, b, at, _, _ar, _br) = authenticated_pair();
    let runtime = NativeLanRuntime::new();
    let a = Arc::new(a);
    runtime.insert_session(a.clone()).unwrap();
    for n in 1..=MAX_REMOTE_ICE_INPUTS {
        route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pIce { candidate: None },
        )
        .await
        .unwrap();
        assert_eq!(a.transfer.lock().unwrap().remote_ice_inputs, n);
        if n == MAX_REMOTE_ICE_INPUTS / 2 {
            a.transfer.lock().unwrap().remote_description_set = true;
            a.transfer.lock().unwrap().candidate_mismatch.added();
        }
    }
    assert!(route_transport_control(
        &runtime,
        &a,
        NativeControlMessage::P2pIce { candidate: None }
    )
    .await
    .is_err());
    let log_count = at.captured_entries().len();
    for _ in 0..10000 {
        assert!(route_transport_control(
            &runtime,
            &a,
            NativeControlMessage::P2pIce {
                candidate: Some(ice_payload(LOCAL))
            }
        )
        .await
        .is_err());
    }
    assert_eq!(
        a.transfer.lock().unwrap().remote_ice_inputs,
        MAX_REMOTE_ICE_INPUTS
    );
    assert!(a.transfer.lock().unwrap().pending_remote_ice.is_empty());
    assert_eq!(at.captured_entries().len(), log_count);
    assert!(has(
        &at,
        "first_terminal",
        "error",
        "candidate_input_budget"
    ));
    runtime.remove_session(&a.session_id);
    assert!(has(&at, "cleanup", "start", "session_removal"));
    assert!(!a.completed.load(std::sync::atomic::Ordering::Acquire));
    close(&b);
}
