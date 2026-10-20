//! Bound untrusted ICE syntax before entering the native WebRTC parser.
//! In the pinned WebRTC bindings, malformed parse errors can abort Windows
//! debug builds inside the upstream unsafe error conversion. Do not exercise
//! that FFI error path for inputs that can be rejected safely in Rust.
use std::net::IpAddr;
fn address(value: &str) -> bool {
    value.parse::<IpAddr>().is_ok()
        || (value.len() <= 253
            && value.split('.').all(|part| {
                !part.is_empty()
                    && part.len() <= 63
                    && !part.starts_with('-')
                    && !part.ends_with('-')
                    && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            }))
}
pub fn validate(candidate: &str) -> Result<(), String> {
    let invalid = || "invalid ICE candidate syntax".to_string();
    if candidate.len() > 4096
        || candidate
            .bytes()
            .any(|b| b == 0 || b == b'\r' || b == b'\n')
    {
        return Err(invalid());
    }
    let fields: Vec<_> = candidate.split_ascii_whitespace().collect();
    if fields.len() < 8 || (fields.len() - 8) % 2 != 0 {
        return Err(invalid());
    }
    let foundation = fields[0].strip_prefix("candidate:").ok_or_else(invalid)?;
    if foundation.is_empty()
        || foundation.len() > 32
        || !foundation
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'_' | b'-'))
    {
        return Err(invalid());
    }
    let component = fields[1].parse::<u16>().map_err(|_| invalid())?;
    if !(1..=256).contains(&component)
        || !matches!(fields[2].to_ascii_lowercase().as_str(), "udp" | "tcp")
        || fields[3].parse::<u32>().is_err()
        || !address(fields[4])
        || fields[5].parse::<u16>().is_err()
        || fields[6] != "typ"
        || !matches!(fields[7], "host" | "srflx" | "prflx" | "relay")
    {
        return Err(invalid());
    }
    for pair in fields[8..].chunks_exact(2) {
        let valid = match pair[0] {
            "raddr" => address(pair[1]),
            "rport" => pair[1].parse::<u16>().is_ok(),
            "tcptype" => matches!(pair[1], "active" | "passive" | "so"),
            "generation" | "network-id" | "network-cost" => pair[1].parse::<u32>().is_ok(),
            _ => pair
                .iter()
                .all(|value| value.bytes().all(|b| b.is_ascii_graphic())),
        };
        if !valid {
            return Err(invalid());
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn valid_webrtc_candidates_include_ipv4_ipv6_tcp_and_relay() {
        for candidate in [
            // WebRTC accepts this non-RFC foundation used by our native-handler regressions.
            "candidate:MARKER_CANDIDATE 1 udp 2122260223 127.0.0.1 49157 typ host",
            "candidate:1 1 udp 2122260223 192.168.1.2 5000 typ host generation 0 ufrag abc network-id 1 network-cost 10",
            "candidate:2 1 UDP 2122260223 fd12::1 5000 typ host",
            "candidate:3 1 tcp 1518280447 192.168.1.2 9 typ host tcptype active",
            "candidate:4 1 udp 16777215 203.0.113.1 5000 typ relay raddr 192.168.1.2 rport 4000",
            "candidate:5 1 udp 2122260223 example.local 5000 typ host",
        ] { assert!(validate(candidate).is_ok(), "{candidate}"); }
    }
    #[test]
    fn malformed_candidates_never_need_native_parse_error_conversion() {
        for candidate in [
            "CANDIDATE_MARKER",
            "",
            "candidate: 1 udp 1 127.0.0.1 9 typ host",
            "candidate:1 nope udp 1 127.0.0.1 9 typ host",
            "candidate:1 1 udp 4294967296 127.0.0.1 9 typ host",
            "candidate:1 1 udp 1 127.0.0.1 65536 typ host",
            "candidate:1 1 udp 1 127.0.0.1 9 typ host tcptype invalid",
            "candidate:1 1 udp 1 127.0.0.1 9 typ host\r\na=bad",
        ] {
            assert!(validate(candidate).is_err(), "{candidate}");
        }
    }
}
