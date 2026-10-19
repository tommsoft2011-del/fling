//! End-to-end encryption for Fling online rooms.
//!
//! Rust port of `packages/core/src/crypto.ts` (and `normalizeCode` /
//! `roomNameForCode` from `packages/core/src/pairing.ts`).
//!
//! The pairing code is the shared secret: PBKDF2-SHA256 (100k iterations) ->
//! AES-GCM-256. The LiveKit server only ever sees ciphertext.

use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use aes_gcm::aead::Aead;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PBKDF2_ITERATIONS: u32 = 100_000;

/// Normalize a pairing code: trim, uppercase, strip whitespace and hyphens.
/// Must match `normalizeCode` in `packages/core/src/pairing.ts` exactly —
/// both sides derive the same key from it.
pub fn normalize_code(input: &str) -> String {
    input
        .trim()
        .to_uppercase()
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .collect()
}

/// Derive the 256-bit room encryption key from the pairing code.
///
/// `context` selects the PBKDF2 salt domain ('fling-v1-key' for the
/// historical CLI-compatible default, 'fling-v2-room' / 'fling-v2-lobby'
/// for v2 callers). Must match `deriveRoomKey` in `crypto.ts`.
pub fn derive_room_key(code: &str, context: &str) -> [u8; 32] {
    let mut key = [0u8; 32];
    pbkdf2_hmac::<Sha256>(
        normalize_code(code).as_bytes(),
        context.as_bytes(),
        PBKDF2_ITERATIONS,
        &mut key,
    );
    key
}

/// Both sides derive the same LiveKit room name from the pairing code.
/// Must match `roomNameForCode` in `packages/core/src/pairing.ts`:
/// `'fling-' + hex(sha256('fling-v1-room:' + normalized)).slice(0, 16)`.
pub fn room_name_for_code(code: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"fling-v1-room:");
    hasher.update(normalize_code(code).as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    format!("fling-{hex}")
}

#[derive(Serialize, Deserialize)]
struct SealedEnvelope {
    v: u8,
    iv: String,
    ct: String,
}

/// Encrypt a plaintext message into the sealed JSON string that goes on the
/// wire. Format: `{"v":1,"iv":base64(12B),"ct":base64(aes-gcm)}`.
/// Must match `sealText` in `crypto.ts`.
pub fn seal_text(key: &[u8; 32], plaintext: &str) -> String {
    let mut iv = [0u8; 12];
    rand::rng().fill_bytes(&mut iv);
    let cipher = Aes256Gcm::new_from_slice(key).expect("AES-256 key length");
    let ct = cipher
        .encrypt(Nonce::from_slice(&iv), plaintext.as_bytes())
        .expect("AES-GCM encryption");
    let env = SealedEnvelope {
        v: 1,
        iv: B64.encode(iv),
        ct: B64.encode(ct),
    };
    serde_json::to_string(&env).expect("sealed envelope JSON")
}

/// Decrypt a sealed JSON string. Errors on wrong key / tampered data.
/// Must match `openText` in `crypto.ts`.
pub fn open_text(key: &[u8; 32], sealed: &str) -> Result<String, String> {
    let env: SealedEnvelope =
        serde_json::from_str(sealed).map_err(|e| format!("bad envelope: {e}"))?;
    if env.v != 1 || env.iv.is_empty() || env.ct.is_empty() {
        return Err("bad envelope".to_string());
    }
    let iv = B64
        .decode(&env.iv)
        .map_err(|e| format!("bad envelope iv: {e}"))?;
    let ct = B64
        .decode(&env.ct)
        .map_err(|e| format!("bad envelope ct: {e}"))?;
    if iv.len() != 12 {
        return Err("bad envelope iv length".to_string());
    }
    let cipher = Aes256Gcm::new_from_slice(key).expect("AES-256 key length");
    let pt = cipher
        .decrypt(Nonce::from_slice(&iv), ct.as_ref())
        .map_err(|_| "AES-GCM decrypt failed".to_string())?;
    String::from_utf8(pt).map_err(|e| format!("decrypted text is not UTF-8: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_code_matches_ts() {
        assert_eq!(normalize_code("  ab-12 cd "), "AB12CD");
        assert_eq!(normalize_code("x1"), "X1");
    }

    #[test]
    fn seal_open_roundtrip() {
        let key = derive_room_key("123456", "fling-v1-key");
        let sealed = seal_text(&key, "hello fling");
        assert_eq!(open_text(&key, &sealed).unwrap(), "hello fling");
        // Wrong key must fail.
        let wrong = derive_room_key("654321", "fling-v1-key");
        assert!(open_text(&wrong, &sealed).is_err());
    }

    #[test]
    fn room_name_is_stable() {
        let a = room_name_for_code("123-456");
        let b = room_name_for_code("123456");
        assert_eq!(a, b);
        assert!(a.starts_with("fling-"));
        assert_eq!(a.len(), "fling-".len() + 16);
    }
}
