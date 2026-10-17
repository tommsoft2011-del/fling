//! Offline-only native LAN transport primitives.
//!
//! TCP is a bounded control/signaling channel for pairing, SDP/ICE, manifests,
//! cumulative ACKs and low-rate progress only. `NativeControlMessage` has no
//! file-payload variant. File bytes must use FLNG frames over native WebRTC
//! DataChannels (DTLS/SCTP). No LiveKit or cloud fallback is part of this mode.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use hkdf::Hkdf;
use pbkdf2::pbkdf2_hmac;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::{Zeroize, Zeroizing};

pub const MAX_SIGNAL_PACKET_BYTES: usize = 256 * 1024;
pub const MAX_PAIRING_HELLO_BYTES: usize = 8 * 1024;
pub const MAX_CONTROL_PLAINTEXT_BYTES: usize = 180 * 1024;
pub const DIRECT_FILE_FRAME_HEADER_SIZE: usize = 26;
pub const DIRECT_FILE_BATCH_FRAME_HEADER_SIZE: usize = 30;
pub const DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES: usize = 128 * 1024;
const DIRECT_FILE_MAGIC: &[u8; 4] = b"FLNG";
const DIRECT_FILE_VERSION: u8 = 1;
const DIRECT_FILE_CHUNK: u8 = 1;
const DIRECT_FILE_CHUNK_BATCH: u8 = 2;
const ROOM_KEY_PBKDF2_ITERATIONS: u32 = 100_000;
const PAIRING_DOMAIN: &[u8] = b"FLNG-LAN-PAIR-v1\0";
const CONTROL_DOMAIN: &[u8] = b"FLNG-LAN-CONTROL-v1\0";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PairingRole {
    Initiator,
    Responder,
}

/// Exact envelope shape used by the existing online TypeScript sealText/openText.
/// Offline LAN mode uses `EncryptedControlFrame` below instead, with sequence
/// nonces and transcript-bound AAD.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedEnvelope {
    pub v: u8,
    pub iv: String,
    pub ct: String,
}

/// Pairing hello is the only unauthenticated TCP message. It contains public
/// ephemeral material and display labels only; the SAS comparison authenticates
/// the full transcript before any WebRTC offer or transfer manifest is allowed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PairingHello {
    pub v: u8,
    pub role: PairingRole,
    pub session_id: String,
    pub task_id: String,
    pub from_device_id: String,
    pub to_device_id: String,
    pub from_device_label: String,
    pub ephemeral_public_key: String,
    pub nonce: String,
}

/// Bounded authenticated control frame. Every routing field, task/session id,
/// and monotonic sequence number is included in AES-GCM associated data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EncryptedControlFrame {
    pub v: u8,
    pub session_id: String,
    pub task_id: String,
    pub from_device_id: String,
    pub to_device_id: String,
    pub sequence: u64,
    pub nonce: String,
    pub ct: String,
}

/// Control-plane messages allowed on LAN TCP. There is intentionally no
/// `file-chunk`, file path, OS handle, Blob, or byte-array variant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum NativeControlMessage {
    #[serde(rename = "pair-confirm")]
    PairConfirm {
        #[serde(rename = "transcriptHash")]
        transcript_hash: String,
    },
    #[serde(rename = "pair-reject")]
    PairReject {
        #[serde(rename = "transcriptHash")]
        transcript_hash: String,
    },
    #[serde(rename = "p2p-offer")]
    P2pOffer { sdp: String },
    #[serde(rename = "p2p-answer")]
    P2pAnswer { sdp: String },
    #[serde(rename = "p2p-ice")]
    P2pIce { candidate: Option<String> },
    #[serde(rename = "file-manifest")]
    FileManifest { manifest: NativeFileManifest },
    #[serde(rename = "file-manifest-ack")]
    FileManifestAck {
        id: String,
        #[serde(rename = "chunkAck", skip_serializing_if = "Option::is_none")]
        chunk_ack: Option<bool>,
        #[serde(rename = "binaryChunks", skip_serializing_if = "Option::is_none")]
        binary_chunks: Option<bool>,
        #[serde(
            rename = "directFramePayloadBytes",
            skip_serializing_if = "Option::is_none"
        )]
        direct_frame_payload_bytes: Option<u32>,
    },
    #[serde(rename = "file-chunk-ack")]
    FileChunkAck {
        id: String,
        #[serde(rename = "nextSeq")]
        next_seq: u32,
        #[serde(rename = "receivedBytes")]
        received_bytes: u64,
        #[serde(rename = "savedBytes", skip_serializing_if = "Option::is_none")]
        saved_bytes: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        complete: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failed: Option<bool>,
    },
    #[serde(rename = "task-progress")]
    TaskProgress {
        #[serde(rename = "taskId")]
        task_id: String,
        #[serde(rename = "doneBytes")]
        done_bytes: u64,
        #[serde(rename = "receivedBytes", skip_serializing_if = "Option::is_none")]
        received_bytes: Option<u64>,
        #[serde(rename = "savedBytes", skip_serializing_if = "Option::is_none")]
        saved_bytes: Option<u64>,
        #[serde(rename = "totalBytes")]
        total_bytes: u64,
    },
    #[serde(rename = "task-ack")]
    TaskAck {
        #[serde(rename = "taskId")]
        task_id: String,
    },
    #[serde(rename = "task-cancel")]
    TaskCancel {
        #[serde(rename = "taskId")]
        task_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeFileManifest {
    pub id: String,
    pub transfer_nonce: String,
    pub name: String,
    pub size: u64,
    pub sha256: String,
    pub chunk_size: u32,
    pub total_chunks: u32,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectFileChunkFrame {
    pub id: String,
    pub seq: u32,
    pub payload: Vec<u8>,
    pub chunk_count: u16,
    pub chunk_size: Option<u16>,
}

/// X25519 private material is ephemeral and intentionally has no Debug impl.
pub struct EphemeralKeyPair {
    secret: StaticSecret,
}

/// Established local session. The verification code is intentionally omitted
/// from Debug formatting; callers may send it to the pairing prompt UI only.
pub struct SecureLocalSession {
    session_id: String,
    task_id: String,
    peer_device_id: String,
    peer_device_label: String,
    verification_code: String,
    transcript_hash: String,
    cipher: LocalSessionCipher,
}

impl fmt::Debug for SecureLocalSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecureLocalSession")
            .field("session_id", &"[REDACTED]")
            .field("task_id", &"[REDACTED]")
            .field("peer_device_id", &self.peer_device_id)
            .field("peer_device_label", &self.peer_device_label)
            .field("verification_code", &"[REDACTED]")
            .field("cipher", &self.cipher)
            .finish()
    }
}

impl SecureLocalSession {
    pub fn abort(&mut self) {
        self.verification_code.zeroize();
        self.transcript_hash.zeroize();
        self.session_id.zeroize();
        self.task_id.zeroize();
        self.peer_device_id.zeroize();
        self.peer_device_label.clear();
        self.cipher.abort();
    }
}

impl Drop for SecureLocalSession {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Directional keys are separate, so identical sequence counters in opposite
/// directions cannot reuse a key/nonce pair. Each new X25519 session gets fresh
/// keys; within it the 64-bit sequence counter never wraps and is strictly
/// checked on receive.
pub struct LocalSessionCipher {
    session_id: String,
    task_id: String,
    local_device_id: String,
    peer_device_id: String,
    send_key: [u8; 32],
    receive_key: [u8; 32],
    send_nonce_prefix: [u8; 4],
    receive_nonce_prefix: [u8; 4],
    send_sequence: u64,
    receive_sequence: u64,
    poisoned: bool,
}

impl fmt::Debug for LocalSessionCipher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalSessionCipher")
            .field("session_id", &"[REDACTED]")
            .field("task_id", &"[REDACTED]")
            .field("local_device_id", &self.local_device_id)
            .field("peer_device_id", &self.peer_device_id)
            .field("send_sequence", &self.send_sequence)
            .field("receive_sequence", &self.receive_sequence)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl Drop for LocalSessionCipher {
    fn drop(&mut self) {
        self.abort();
    }
}

impl LocalSessionCipher {
    pub fn abort(&mut self) {
        self.poisoned = true;
        self.send_key.zeroize();
        self.receive_key.zeroize();
        self.send_nonce_prefix.zeroize();
        self.receive_nonce_prefix.zeroize();
        self.session_id.zeroize();
        self.task_id.zeroize();
        self.local_device_id.zeroize();
        self.peer_device_id.zeroize();
        self.send_sequence = 0;
        self.receive_sequence = 0;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairingDecision {
    AwaitingLocalApproval,
    AwaitingPeerApproval,
    Confirmed,
    Rejected,
}

struct PairingApproval {
    transcript_hash: String,
    verification_code: String,
    local_approved: bool,
    peer_approved: bool,
    rejected: bool,
}

impl PairingApproval {
    fn new(transcript_hash: String, verification_code: String) -> Self {
        Self {
            transcript_hash,
            verification_code,
            local_approved: false,
            peer_approved: false,
            rejected: false,
        }
    }

    /// The UI must show its locally derived code and require the user to enter
    /// the code read from the other device. The code itself is never sent over
    /// TCP. A mismatch or decline is terminal and clears approval state.
    fn decide_locally(
        &mut self,
        approved: bool,
        peer_code_input: &str,
    ) -> Result<NativeControlMessage, String> {
        if self.rejected {
            return Err("pairing is already rejected".to_string());
        }
        if self.local_approved {
            return Err("local pairing decision is already recorded".to_string());
        }
        if !approved {
            let rejection = NativeControlMessage::PairReject {
                transcript_hash: self.transcript_hash.clone(),
            };
            self.rejected = true;
            self.clear_approval_material();
            return Ok(rejection);
        }
        let entered = normalize_short_code(peer_code_input).unwrap_or_default();
        if entered != self.verification_code {
            let rejection = NativeControlMessage::PairReject {
                transcript_hash: self.transcript_hash.clone(),
            };
            self.rejected = true;
            self.clear_approval_material();
            return Ok(rejection);
        }
        self.local_approved = true;
        self.verification_code.zeroize();
        Ok(NativeControlMessage::PairConfirm {
            transcript_hash: self.transcript_hash.clone(),
        })
    }

    /// Only an authenticated, transcript-matching remote confirmation counts.
    fn accept_peer_message(&mut self, message: &NativeControlMessage) -> Result<(), String> {
        if self.rejected {
            return Err("pairing is already rejected".to_string());
        }
        match message {
            NativeControlMessage::PairConfirm { transcript_hash }
                if transcript_hash == &self.transcript_hash =>
            {
                if self.peer_approved {
                    self.rejected = true;
                    self.clear_approval_material();
                    return Err("duplicate peer pairing confirmation".to_string());
                }
                self.peer_approved = true;
                Ok(())
            }
            NativeControlMessage::PairReject { transcript_hash }
                if transcript_hash == &self.transcript_hash =>
            {
                self.rejected = true;
                self.clear_approval_material();
                Ok(())
            }
            NativeControlMessage::PairConfirm { .. } | NativeControlMessage::PairReject { .. } => {
                self.rejected = true;
                self.clear_approval_material();
                Err("pairing transcript mismatch".to_string())
            }
            _ => {
                self.rejected = true;
                self.clear_approval_material();
                Err("expected pairing confirmation".to_string())
            }
        }
    }

    fn decision(&self) -> PairingDecision {
        if self.rejected {
            PairingDecision::Rejected
        } else if self.local_approved && self.peer_approved {
            PairingDecision::Confirmed
        } else if self.local_approved {
            PairingDecision::AwaitingPeerApproval
        } else {
            PairingDecision::AwaitingLocalApproval
        }
    }

    fn may_start_webrtc(&self) -> bool {
        self.decision() == PairingDecision::Confirmed
    }

    fn clear_approval_material(&mut self) {
        self.verification_code.zeroize();
        self.transcript_hash.zeroize();
        self.local_approved = false;
        self.peer_approved = false;
    }
}

impl Drop for PairingApproval {
    fn drop(&mut self) {
        self.verification_code.zeroize();
        self.transcript_hash.zeroize();
        self.local_approved = false;
        self.peer_approved = false;
        self.rejected = true;
    }
}

/// A local pair that has not yet passed the two-sided short-code gate.
/// Callers cannot access its cipher or create a peer connection directly.
pub struct PendingLocalPair {
    session: Option<SecureLocalSession>,
    approval: PairingApproval,
}

/// Unforgeable capability returned only after both devices confirm the same
/// transcript. Native WebRTC creation must accept this type, never a pending pair.
pub struct AuthenticatedLocalSession {
    session: SecureLocalSession,
}

impl PendingLocalPair {
    pub fn new(session: SecureLocalSession) -> Self {
        let approval = PairingApproval::new(
            session.transcript_hash.clone(),
            session.verification_code.clone(),
        );
        Self {
            session: Some(session),
            approval,
        }
    }

    pub fn verification_code(&self) -> Option<&str> {
        self.session
            .as_ref()
            .map(|session| session.verification_code.as_str())
    }

    pub fn peer_device_label(&self) -> Option<&str> {
        self.session
            .as_ref()
            .map(|session| session.peer_device_label.as_str())
    }

    pub fn session_id(&self) -> Option<&str> {
        self.session
            .as_ref()
            .map(|session| session.session_id.as_str())
    }

    pub fn task_id(&self) -> Option<&str> {
        self.session
            .as_ref()
            .map(|session| session.task_id.as_str())
    }

    pub fn decision(&self) -> PairingDecision {
        self.approval.decision()
    }

    pub fn decide_locally(
        &mut self,
        approved: bool,
        peer_code_input: &str,
    ) -> Result<EncryptedControlFrame, String> {
        let message = match self.approval.decide_locally(approved, peer_code_input) {
            Ok(message) => message,
            Err(error) => {
                self.abort();
                return Err(error);
            }
        };
        let rejected = matches!(message, NativeControlMessage::PairReject { .. });
        let frame_result = match self.session.as_mut() {
            Some(session) => session.cipher.encrypt_control(&message),
            None => return Err("pairing session was cleared".to_string()),
        };
        let frame = match frame_result {
            Ok(frame) => frame,
            Err(error) => {
                self.abort();
                return Err(error);
            }
        };
        if rejected {
            self.abort();
        }
        Ok(frame)
    }

    pub fn accept_peer_frame(
        &mut self,
        frame: &EncryptedControlFrame,
    ) -> Result<PairingDecision, String> {
        let message_result = match self.session.as_mut() {
            Some(session) => session.cipher.decrypt_control(frame),
            None => return Err("pairing session was cleared".to_string()),
        };
        let message = match message_result {
            Ok(message) => message,
            Err(error) => {
                self.abort();
                return Err(error);
            }
        };
        if let Err(error) = self.approval.accept_peer_message(&message) {
            self.abort();
            return Err(error);
        }
        let decision = self.approval.decision();
        if decision == PairingDecision::Rejected {
            self.abort();
        }
        Ok(decision)
    }

    pub fn into_authenticated(mut self) -> Result<AuthenticatedLocalSession, String> {
        if !self.approval.may_start_webrtc() {
            self.abort();
            return Err("both devices must compare and confirm the pairing code".to_string());
        }
        let session = self
            .session
            .take()
            .ok_or_else(|| "pairing session was cleared".to_string())?;
        Ok(AuthenticatedLocalSession { session })
    }

    pub fn abort(&mut self) {
        self.approval.rejected = true;
        self.approval.clear_approval_material();
        if let Some(session) = self.session.as_mut() {
            session.abort();
        }
        self.session.take();
    }
}

impl Drop for PendingLocalPair {
    fn drop(&mut self) {
        self.abort();
    }
}

impl AuthenticatedLocalSession {
    pub fn session_id(&self) -> &str {
        &self.session.session_id
    }

    pub fn task_id(&self) -> &str {
        &self.session.task_id
    }

    pub fn peer_device_id(&self) -> &str {
        &self.session.peer_device_id
    }

    pub fn peer_device_label(&self) -> &str {
        &self.session.peer_device_label
    }

    pub fn encrypt_control(
        &mut self,
        message: &NativeControlMessage,
    ) -> Result<EncryptedControlFrame, String> {
        self.session.cipher.encrypt_control(message)
    }

    pub fn decrypt_control(
        &mut self,
        frame: &EncryptedControlFrame,
    ) -> Result<NativeControlMessage, String> {
        self.session.cipher.decrypt_control(frame)
    }

    pub fn abort(&mut self) {
        self.session.abort();
    }
}

/// Same normalization used by `packages/core/src/pairing.ts` for its ASCII code alphabet.
pub fn normalize_pairing_code(input: &str) -> String {
    input
        .trim()
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '-')
        .collect::<String>()
        .to_ascii_uppercase()
}

fn normalize_short_code(input: &str) -> Option<String> {
    let mut digits = String::with_capacity(8);
    for ch in input.trim().chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else if ch.is_whitespace() || ch == '-' {
            continue;
        } else {
            return None;
        }
    }
    if digits.len() == 8 {
        Some(digits)
    } else {
        None
    }
}

/// Existing online RoomKey derivation, retained for LiveKit compatibility.
pub fn derive_room_key(code: &str, context: &str) -> [u8; 32] {
    let normalized = normalize_pairing_code(code);
    let mut key = [0u8; 32];
    pbkdf2_hmac::<Sha256>(
        normalized.as_bytes(),
        context.as_bytes(),
        ROOM_KEY_PBKDF2_ITERATIONS,
        &mut key,
    );
    key
}

/// Existing online `{v:1,iv,ct}` AES-GCM envelope. LAN-only mode does not use
/// this random-nonce envelope; it uses `LocalSessionCipher` instead.
pub fn seal_text(key: &[u8; 32], plaintext: &str) -> Result<String, String> {
    let iv: [u8; 12] = rand::random();
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| format!("invalid AES key: {e}"))?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&iv), plaintext.as_bytes())
        .map_err(|_| "AES-GCM encrypt failed".to_string())?;
    let envelope = SealedEnvelope {
        v: 1,
        iv: BASE64.encode(iv),
        ct: BASE64.encode(ciphertext),
    };
    serde_json::to_string(&envelope).map_err(|e| format!("serialize envelope: {e}"))
}

pub fn open_text(key: &[u8; 32], sealed: &str) -> Result<String, String> {
    let envelope: SealedEnvelope =
        serde_json::from_str(sealed).map_err(|e| format!("parse envelope: {e}"))?;
    if envelope.v != 1 {
        return Err("unsupported sealed envelope version".to_string());
    }
    let iv = BASE64
        .decode(envelope.iv)
        .map_err(|e| format!("decode IV: {e}"))?;
    if iv.len() != 12 {
        return Err("invalid AES-GCM IV length".to_string());
    }
    let ciphertext = BASE64
        .decode(envelope.ct)
        .map_err(|e| format!("decode ciphertext: {e}"))?;
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| format!("invalid AES key: {e}"))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&iv), ciphertext.as_ref())
        .map_err(|_| "AES-GCM authentication failed".to_string())?;
    String::from_utf8(plaintext).map_err(|e| format!("control message is not UTF-8: {e}"))
}

/// Initiator entry point. The random 128-bit session id is never reused by this call.
pub fn start_pairing(
    task_id: &str,
    local_device_id: &str,
    local_device_label: &str,
    peer_device_id: &str,
) -> Result<(EphemeralKeyPair, PairingHello), String> {
    let random: [u8; 16] = rand::random();
    let session_id = hex(&random);
    create_pairing_hello(
        &session_id,
        task_id,
        local_device_id,
        local_device_label,
        peer_device_id,
        PairingRole::Initiator,
    )
}

/// Create the responder hello after validating the incoming invitation's target.
pub fn respond_to_pairing(
    invitation: &PairingHello,
    local_device_id: &str,
    local_device_label: &str,
    expected_peer_device_id: &str,
) -> Result<(EphemeralKeyPair, PairingHello), String> {
    validate_hello(invitation)?;
    if invitation.to_device_id != local_device_id
        || invitation.from_device_id != expected_peer_device_id
    {
        return Err("pairing invitation is not addressed to this device".to_string());
    }
    create_pairing_hello(
        &invitation.session_id,
        &invitation.task_id,
        local_device_id,
        local_device_label,
        expected_peer_device_id,
        PairingRole::Responder,
    )
}

fn create_pairing_hello(
    session_id: &str,
    task_id: &str,
    local_device_id: &str,
    local_device_label: &str,
    peer_device_id: &str,
    role: PairingRole,
) -> Result<(EphemeralKeyPair, PairingHello), String> {
    let secret_bytes: [u8; 32] = rand::random();
    let nonce: [u8; 16] = rand::random();
    create_pairing_hello_from_material(
        session_id,
        task_id,
        local_device_id,
        local_device_label,
        peer_device_id,
        role,
        secret_bytes,
        nonce,
    )
}

fn create_pairing_hello_from_material(
    session_id: &str,
    task_id: &str,
    local_device_id: &str,
    local_device_label: &str,
    peer_device_id: &str,
    role: PairingRole,
    secret_bytes: [u8; 32],
    nonce: [u8; 16],
) -> Result<(EphemeralKeyPair, PairingHello), String> {
    validate_hex_id(session_id, 32, "session id")?;
    validate_identifier(task_id, 128, "task id")?;
    validate_identifier(local_device_id, 128, "local device id")?;
    validate_identifier(peer_device_id, 128, "peer device id")?;
    validate_label(local_device_label)?;
    if local_device_id == peer_device_id {
        return Err("cannot pair a device with itself".to_string());
    }
    let secret_bytes = Zeroizing::new(secret_bytes);
    let secret = StaticSecret::from(*secret_bytes);
    let public = PublicKey::from(&secret);
    let hello = PairingHello {
        v: 1,
        role,
        session_id: session_id.to_ascii_lowercase(),
        task_id: task_id.to_string(),
        from_device_id: local_device_id.to_string(),
        to_device_id: peer_device_id.to_string(),
        from_device_label: local_device_label.to_string(),
        ephemeral_public_key: BASE64.encode(public.as_bytes()),
        nonce: BASE64.encode(nonce),
    };
    Ok((EphemeralKeyPair { secret }, hello))
}

/// Finish X25519 and transcript binding. The returned 8-digit SAS is for UI
/// comparison only; it is not logged, persisted, included in task details, or
/// sent in plaintext. User confirmation and encrypted peer confirmation are
/// required before DataChannel creation.
pub fn establish_local_session(
    ephemeral: EphemeralKeyPair,
    local_hello: &PairingHello,
    peer_hello: &PairingHello,
) -> Result<SecureLocalSession, String> {
    validate_hello_pair(local_hello, peer_hello)?;
    let peer_public_bytes = BASE64
        .decode(&peer_hello.ephemeral_public_key)
        .map_err(|e| format!("invalid peer public key: {e}"))?;
    let peer_public_array: [u8; 32] = peer_public_bytes
        .try_into()
        .map_err(|_| "invalid X25519 public key length".to_string())?;
    let peer_public = PublicKey::from(peer_public_array);
    let shared = ephemeral.secret.diffie_hellman(&peer_public);
    if shared.as_bytes().iter().all(|byte| *byte == 0) {
        return Err("invalid all-zero X25519 shared secret".to_string());
    }

    let transcript = pairing_transcript(local_hello, peer_hello)?;
    let transcript_hash = Sha256::digest(&transcript);
    let mut transcript_hash_bytes = [0u8; 32];
    transcript_hash_bytes.copy_from_slice(&transcript_hash);
    let transcript_hash_bytes = Zeroizing::new(transcript_hash_bytes);
    let transcript_hash_hex = hex(&transcript_hash_bytes[..]);
    let shared_material = Zeroizing::new(*shared.as_bytes());
    let hkdf = Hkdf::<Sha256>::new(Some(&transcript_hash_bytes[..]), &shared_material[..]);

    let local_id = &local_hello.from_device_id;
    let peer_id = &peer_hello.from_device_id;
    let (send_key, send_nonce_prefix) = derive_direction(&hkdf, local_id, peer_id)?;
    let (receive_key, receive_nonce_prefix) = derive_direction(&hkdf, peer_id, local_id)?;
    let mut sas_bytes = [0u8; 4];
    hkdf.expand(b"fling-lan-v1/short-authentication-string", &mut sas_bytes)
        .map_err(|_| "failed to derive pairing verification code".to_string())?;
    let code_value = u32::from_be_bytes(sas_bytes) % 100_000_000;
    let verification_code = format!("{code_value:08}");

    Ok(SecureLocalSession {
        session_id: local_hello.session_id.clone(),
        task_id: local_hello.task_id.clone(),
        peer_device_id: peer_hello.from_device_id.clone(),
        peer_device_label: peer_hello.from_device_label.clone(),
        verification_code,
        transcript_hash: transcript_hash_hex,
        cipher: LocalSessionCipher {
            session_id: local_hello.session_id.clone(),
            task_id: local_hello.task_id.clone(),
            local_device_id: local_id.clone(),
            peer_device_id: peer_id.clone(),
            send_key,
            receive_key,
            send_nonce_prefix,
            receive_nonce_prefix,
            send_sequence: 0,
            receive_sequence: 0,
            poisoned: false,
        },
    })
}

fn derive_direction(
    hkdf: &Hkdf<Sha256>,
    from_device_id: &str,
    to_device_id: &str,
) -> Result<([u8; 32], [u8; 4]), String> {
    let mut key_info = b"fling-lan-v1/directional-control-key\0".to_vec();
    append_field(&mut key_info, from_device_id.as_bytes());
    append_field(&mut key_info, to_device_id.as_bytes());
    let mut nonce_info = b"fling-lan-v1/directional-nonce-prefix\0".to_vec();
    append_field(&mut nonce_info, from_device_id.as_bytes());
    append_field(&mut nonce_info, to_device_id.as_bytes());
    let mut key = [0u8; 32];
    let mut nonce_prefix = [0u8; 4];
    hkdf.expand(&key_info, &mut key)
        .map_err(|_| "failed to derive directional session key".to_string())?;
    hkdf.expand(&nonce_info, &mut nonce_prefix)
        .map_err(|_| "failed to derive directional nonce prefix".to_string())?;
    Ok((key, nonce_prefix))
}

fn validate_hello(hello: &PairingHello) -> Result<(), String> {
    if hello.v != 1 {
        return Err("unsupported LAN pairing version".to_string());
    }
    validate_hex_id(&hello.session_id, 32, "session id")?;
    validate_identifier(&hello.task_id, 128, "task id")?;
    validate_identifier(&hello.from_device_id, 128, "sender device id")?;
    validate_identifier(&hello.to_device_id, 128, "recipient device id")?;
    validate_label(&hello.from_device_label)?;
    decode_canonical_base64(&hello.ephemeral_public_key, 32, "X25519 public key")?;
    decode_canonical_base64(&hello.nonce, 16, "pairing nonce")?;
    Ok(())
}

fn validate_hello_pair(local: &PairingHello, peer: &PairingHello) -> Result<(), String> {
    validate_hello(local)?;
    validate_hello(peer)?;
    if local.session_id != peer.session_id || local.task_id != peer.task_id {
        return Err("pairing session/task mismatch".to_string());
    }
    if local.role == peer.role {
        return Err("pairing role mismatch".to_string());
    }
    if local.from_device_id == peer.from_device_id
        || local.to_device_id != peer.from_device_id
        || peer.to_device_id != local.from_device_id
    {
        return Err("pairing identity route mismatch".to_string());
    }
    Ok(())
}

fn pairing_transcript(local: &PairingHello, peer: &PairingHello) -> Result<Vec<u8>, String> {
    validate_hello_pair(local, peer)?;
    let mut participants = [local, peer];
    participants.sort_by_key(|hello| match hello.role {
        PairingRole::Initiator => 0u8,
        PairingRole::Responder => 1u8,
    });
    let mut transcript = PAIRING_DOMAIN.to_vec();
    transcript.push(local.v);
    append_field(&mut transcript, local.session_id.as_bytes());
    append_field(&mut transcript, local.task_id.as_bytes());
    for participant in participants {
        let role = match participant.role {
            PairingRole::Initiator => b"initiator".as_slice(),
            PairingRole::Responder => b"responder".as_slice(),
        };
        append_field(&mut transcript, role);
        append_field(&mut transcript, participant.from_device_id.as_bytes());
        append_field(&mut transcript, participant.to_device_id.as_bytes());
        append_field(&mut transcript, participant.from_device_label.as_bytes());
        let public_key =
            decode_canonical_base64(&participant.ephemeral_public_key, 32, "X25519 public key")?;
        let hello_nonce = decode_canonical_base64(&participant.nonce, 16, "pairing nonce")?;
        append_field(&mut transcript, &public_key);
        append_field(&mut transcript, &hello_nonce);
    }
    Ok(transcript)
}

fn append_field(out: &mut Vec<u8>, field: &[u8]) {
    out.extend_from_slice(&(field.len() as u32).to_be_bytes());
    out.extend_from_slice(field);
}

fn authenticated_header(frame: &EncryptedControlFrame) -> Vec<u8> {
    let mut aad = CONTROL_DOMAIN.to_vec();
    aad.push(frame.v);
    append_field(&mut aad, frame.session_id.as_bytes());
    append_field(&mut aad, frame.task_id.as_bytes());
    append_field(&mut aad, frame.from_device_id.as_bytes());
    append_field(&mut aad, frame.to_device_id.as_bytes());
    aad.extend_from_slice(&frame.sequence.to_be_bytes());
    aad
}

fn nonce_for(prefix: &[u8; 4], sequence: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[..4].copy_from_slice(prefix);
    nonce[4..].copy_from_slice(&sequence.to_be_bytes());
    nonce
}

impl LocalSessionCipher {
    pub fn encrypt_control(
        &mut self,
        message: &NativeControlMessage,
    ) -> Result<EncryptedControlFrame, String> {
        if self.poisoned {
            return Err("LAN session cipher is closed".to_string());
        }
        if self.send_sequence == u64::MAX {
            self.abort();
            return Err("LAN send sequence exhausted; create a new session".to_string());
        }
        let plaintext = serde_json::to_vec(message)
            .map_err(|e| format!("serialize LAN control message: {e}"))?;
        if plaintext.len() > MAX_CONTROL_PLAINTEXT_BYTES {
            return Err("LAN control message exceeds plaintext bound".to_string());
        }
        let sequence = self.send_sequence;
        // Consume the nonce before attempting encryption; even an unexpected
        // crypto failure can never cause this sequence nonce to be reused.
        self.send_sequence += 1;
        let mut frame = EncryptedControlFrame {
            v: 1,
            session_id: self.session_id.clone(),
            task_id: self.task_id.clone(),
            from_device_id: self.local_device_id.clone(),
            to_device_id: self.peer_device_id.clone(),
            sequence,
            nonce: String::new(),
            ct: String::new(),
        };
        let nonce = nonce_for(&self.send_nonce_prefix, sequence);
        let aad = authenticated_header(&frame);
        let cipher = match Aes256Gcm::new_from_slice(&self.send_key) {
            Ok(cipher) => cipher,
            Err(_) => {
                self.abort();
                return Err("invalid LAN session key".to_string());
            }
        };
        let ciphertext = match cipher.encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &aad,
            },
        ) {
            Ok(ciphertext) => ciphertext,
            Err(_) => {
                self.abort();
                return Err("LAN control encryption failed".to_string());
            }
        };
        frame.nonce = BASE64.encode(nonce);
        frame.ct = BASE64.encode(ciphertext);
        let encoded_len = match serde_json::to_vec(&frame) {
            Ok(encoded) => encoded.len(),
            Err(_) => {
                self.abort();
                return Err("serialize encrypted control frame failed".to_string());
            }
        };
        if encoded_len > MAX_SIGNAL_PACKET_BYTES {
            self.abort();
            return Err("encrypted LAN control frame exceeds bound".to_string());
        }
        Ok(frame)
    }

    pub fn decrypt_control(
        &mut self,
        frame: &EncryptedControlFrame,
    ) -> Result<NativeControlMessage, String> {
        if self.poisoned {
            return Err("LAN session cipher is closed".to_string());
        }
        if frame.v != 1
            || frame.session_id != self.session_id
            || frame.task_id != self.task_id
            || frame.from_device_id != self.peer_device_id
            || frame.to_device_id != self.local_device_id
        {
            self.abort();
            return Err("LAN control frame route/session/task authentication mismatch".to_string());
        }
        if frame.sequence != self.receive_sequence || self.receive_sequence == u64::MAX {
            self.abort();
            return Err("LAN control replay or out-of-order sequence".to_string());
        }
        let nonce = match decode_canonical_base64(&frame.nonce, 12, "LAN control nonce") {
            Ok(nonce) => nonce,
            Err(_) => {
                self.abort();
                return Err("invalid LAN control nonce".to_string());
            }
        };
        let expected_nonce = nonce_for(&self.receive_nonce_prefix, frame.sequence);
        if nonce.as_slice() != expected_nonce.as_slice() {
            self.abort();
            return Err("LAN control nonce does not match sequence".to_string());
        }
        let ciphertext = match BASE64.decode(&frame.ct) {
            Ok(ciphertext)
                if ciphertext.len() >= 16
                    && ciphertext.len() <= MAX_CONTROL_PLAINTEXT_BYTES + 16
                    && BASE64.encode(&ciphertext) == frame.ct =>
            {
                ciphertext
            }
            _ => {
                self.abort();
                return Err("LAN control ciphertext exceeds bound".to_string());
            }
        };
        let aad = authenticated_header(frame);
        let cipher = match Aes256Gcm::new_from_slice(&self.receive_key) {
            Ok(cipher) => cipher,
            Err(_) => {
                self.abort();
                return Err("invalid LAN session key".to_string());
            }
        };
        let plaintext = match cipher.decrypt(
            Nonce::from_slice(&expected_nonce),
            Payload {
                msg: &ciphertext,
                aad: &aad,
            },
        ) {
            Ok(plaintext) => plaintext,
            Err(_) => {
                self.abort();
                return Err("LAN control authentication failed".to_string());
            }
        };
        if plaintext.len() > MAX_CONTROL_PLAINTEXT_BYTES {
            self.abort();
            return Err("LAN control plaintext exceeds bound".to_string());
        }
        let message: NativeControlMessage = match serde_json::from_slice(&plaintext) {
            Ok(message) => message,
            Err(_) => {
                self.abort();
                return Err("invalid LAN control message".to_string());
            }
        };
        self.receive_sequence = match self.receive_sequence.checked_add(1) {
            Some(next) => next,
            None => {
                self.abort();
                return Err("LAN receive sequence exhausted".to_string());
            }
        };
        Ok(message)
    }
}

/// In-process anti-duplicate guard for incoming invitation prompts. The session
/// key and counters still provide cryptographic replay protection after pairing.
#[derive(Debug)]
pub struct SessionReplayGuard {
    capacity: usize,
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl SessionReplayGuard {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            seen: HashSet::new(),
        }
    }

    pub fn reserve(&mut self, session_id: &str) -> Result<(), String> {
        validate_hex_id(session_id, 32, "session id")?;
        if !self.seen.insert(session_id.to_string()) {
            return Err("duplicate LAN pairing session id".to_string());
        }
        self.order.push_back(session_id.to_string());
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.seen.remove(&oldest);
            }
        }
        Ok(())
    }
}

fn validate_identifier(value: &str, max_len: usize, label: &str) -> Result<(), String> {
    if value.trim().is_empty()
        || value.len() > max_len
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(format!("invalid {label}"));
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), String> {
    if label.trim().is_empty() || label.chars().count() > 64 || label.chars().any(char::is_control)
    {
        return Err("invalid device label".to_string());
    }
    Ok(())
}

fn validate_hex_id(value: &str, expected_len: usize, label: &str) -> Result<(), String> {
    if value.len() != expected_len
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("invalid {label}"));
    }
    Ok(())
}

fn decode_canonical_base64(
    value: &str,
    expected_len: usize,
    label: &str,
) -> Result<Vec<u8>, String> {
    let decoded = BASE64
        .decode(value)
        .map_err(|_| format!("invalid {label} encoding"))?;
    if decoded.len() != expected_len || BASE64.encode(&decoded) != value {
        return Err(format!("non-canonical or invalid {label}"));
    }
    Ok(decoded)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Read/write helpers for bounded JSON records over the local TCP control lane.
/// They never accept or expose the DataChannel file payload.
pub async fn write_pairing_hello<W>(writer: &mut W, hello: &PairingHello) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_json_frame(writer, hello, MAX_PAIRING_HELLO_BYTES).await
}

pub async fn read_pairing_hello<R>(reader: &mut R) -> io::Result<PairingHello>
where
    R: AsyncRead + Unpin,
{
    let hello: PairingHello = read_json_frame(reader, MAX_PAIRING_HELLO_BYTES).await?;
    validate_hello(&hello).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(hello)
}

pub async fn write_control_frame<W>(writer: &mut W, frame: &EncryptedControlFrame) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_json_frame(writer, frame, MAX_SIGNAL_PACKET_BYTES).await
}

pub async fn read_control_frame<R>(reader: &mut R) -> io::Result<EncryptedControlFrame>
where
    R: AsyncRead + Unpin,
{
    read_json_frame(reader, MAX_SIGNAL_PACKET_BYTES).await
}

async fn write_json_frame<W, T>(writer: &mut W, value: &T, max_bytes: usize) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serialize LAN frame: {e}"),
        )
    })?;
    if body.is_empty() || body.len() > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "LAN frame exceeds size bound",
        ));
    }
    let len = u32::try_from(body.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "LAN frame length overflow"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await
}

async fn read_json_frame<R, T>(reader: &mut R, max_bytes: usize) -> io::Result<T>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut prefix = [0u8; 4];
    reader.read_exact(&mut prefix).await?;
    let len = u32::from_be_bytes(prefix) as usize;
    if len == 0 || len > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid LAN frame length",
        ));
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    serde_json::from_slice(&body)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("parse LAN frame: {e}")))
}

/// Encode the existing v1 FLNG single or coalesced binary DataChannel frame.
pub fn encode_direct_file_frame(
    id: &str,
    seq: u32,
    payload: &[u8],
    chunk_size: u16,
    chunk_count: u16,
) -> Result<Vec<u8>, String> {
    if id.len() != 24 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid transfer id".to_string());
    }
    if chunk_count == 0 || (chunk_count > 1 && chunk_size == 0) {
        return Err("invalid direct chunk metadata".to_string());
    }
    if seq > u32::MAX - (u32::from(chunk_count) - 1) {
        return Err("direct chunk sequence range overflow".to_string());
    }
    if chunk_count > 1 {
        let min_len = usize::from(chunk_count - 1) * usize::from(chunk_size);
        let max_len = usize::from(chunk_count) * usize::from(chunk_size);
        if payload.len() <= min_len || payload.len() > max_len {
            return Err("invalid coalesced direct chunk payload length".to_string());
        }
    }
    if payload.len() > DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES {
        return Err("direct file frame exceeds payload ceiling".to_string());
    }
    let batch = chunk_count > 1;
    let header_size = if batch {
        DIRECT_FILE_BATCH_FRAME_HEADER_SIZE
    } else {
        DIRECT_FILE_FRAME_HEADER_SIZE
    };
    let len =
        u32::try_from(payload.len()).map_err(|_| "direct frame length overflow".to_string())?;
    let mut frame = vec![0u8; header_size + payload.len()];
    frame[0..4].copy_from_slice(DIRECT_FILE_MAGIC);
    frame[4] = DIRECT_FILE_VERSION;
    frame[5] = if batch {
        DIRECT_FILE_CHUNK_BATCH
    } else {
        DIRECT_FILE_CHUNK
    };
    for (i, pair) in id.as_bytes().chunks_exact(2).enumerate() {
        let hex_pair =
            std::str::from_utf8(pair).map_err(|e| format!("invalid transfer id: {e}"))?;
        frame[6 + i] =
            u8::from_str_radix(hex_pair, 16).map_err(|e| format!("invalid transfer id: {e}"))?;
    }
    frame[18..22].copy_from_slice(&seq.to_be_bytes());
    frame[22..26].copy_from_slice(&len.to_be_bytes());
    if batch {
        frame[26..28].copy_from_slice(&chunk_size.to_be_bytes());
        frame[28..30].copy_from_slice(&chunk_count.to_be_bytes());
    }
    frame[header_size..].copy_from_slice(payload);
    Ok(frame)
}

/// Return `Ok(None)` for non-FLNG payloads, matching the current TypeScript parser.
pub fn decode_direct_file_frame(data: &[u8]) -> Result<Option<DirectFileChunkFrame>, String> {
    if data.len() < DIRECT_FILE_MAGIC.len() || &data[..4] != DIRECT_FILE_MAGIC {
        return Ok(None);
    }
    if data.len() < DIRECT_FILE_FRAME_HEADER_SIZE {
        return Err("truncated direct file frame".to_string());
    }
    if data[4] != DIRECT_FILE_VERSION
        || (data[5] != DIRECT_FILE_CHUNK && data[5] != DIRECT_FILE_CHUNK_BATCH)
    {
        return Err("unsupported direct file frame".to_string());
    }
    let batch = data[5] == DIRECT_FILE_CHUNK_BATCH;
    let header_size = if batch {
        DIRECT_FILE_BATCH_FRAME_HEADER_SIZE
    } else {
        DIRECT_FILE_FRAME_HEADER_SIZE
    };
    if data.len() < header_size {
        return Err("truncated direct file batch frame".to_string());
    }
    let seq = u32::from_be_bytes(data[18..22].try_into().unwrap());
    let payload_len = u32::from_be_bytes(data[22..26].try_into().unwrap()) as usize;
    if payload_len != data.len() - header_size
        || payload_len > DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES
    {
        return Err("invalid direct file frame length".to_string());
    }
    let (chunk_count, chunk_size) = if batch {
        let size = u16::from_be_bytes(data[26..28].try_into().unwrap());
        let count = u16::from_be_bytes(data[28..30].try_into().unwrap());
        let min_len = usize::from(count.saturating_sub(1)) * usize::from(size);
        let max_len = usize::from(count) * usize::from(size);
        if size == 0
            || count < 2
            || seq > u32::MAX - (u32::from(count) - 1)
            || payload_len <= min_len
            || payload_len > max_len
        {
            return Err("invalid coalesced direct chunk metadata".to_string());
        }
        (count, Some(size))
    } else {
        (1, None)
    };
    let id = data[6..18]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(Some(DirectFileChunkFrame {
        id,
        seq,
        payload: data[header_size..].to_vec(),
        chunk_count,
        chunk_size,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn two_secure_sessions() -> (SecureLocalSession, SecureLocalSession) {
        let session_id = "0123456789abcdef0123456789abcdef";
        let (alice_key, alice_hello) = create_pairing_hello(
            session_id,
            "task-001",
            "device-alice",
            "Alice PC",
            "device-bob",
            PairingRole::Initiator,
        )
        .unwrap();
        let (bob_key, bob_hello) = create_pairing_hello(
            session_id,
            "task-001",
            "device-bob",
            "Bob PC",
            "device-alice",
            PairingRole::Responder,
        )
        .unwrap();
        let alice = establish_local_session(alice_key, &alice_hello, &bob_hello).unwrap();
        let bob = establish_local_session(bob_key, &bob_hello, &alice_hello).unwrap();
        (alice, bob)
    }

    fn fixed_pairing_sessions() -> (SecureLocalSession, SecureLocalSession) {
        let session_id = "00112233445566778899aabbccddeeff";
        let (alice_key, alice_hello) = create_pairing_hello_from_material(
            session_id,
            "task-vector-01",
            "device-a",
            "Desk A",
            "device-b",
            PairingRole::Initiator,
            [0x11; 32],
            [0x33; 16],
        )
        .unwrap();
        let (bob_key, bob_hello) = create_pairing_hello_from_material(
            session_id,
            "task-vector-01",
            "device-b",
            "Desk B",
            "device-a",
            PairingRole::Responder,
            [0x22; 32],
            [0x44; 16],
        )
        .unwrap();
        (
            establish_local_session(alice_key, &alice_hello, &bob_hello).unwrap(),
            establish_local_session(bob_key, &bob_hello, &alice_hello).unwrap(),
        )
    }

    fn substituted_key_sessions() -> (SecureLocalSession, SecureLocalSession) {
        let session_id = "00112233445566778899aabbccddeeff";
        let (alice_key, alice_hello) = create_pairing_hello_from_material(
            session_id,
            "task-vector-01",
            "device-a",
            "Desk A",
            "device-b",
            PairingRole::Initiator,
            [0x11; 32],
            [0x33; 16],
        )
        .unwrap();
        let (bob_real_key, bob_real_hello) = create_pairing_hello_from_material(
            session_id,
            "task-vector-01",
            "device-b",
            "Desk B",
            "device-a",
            PairingRole::Responder,
            [0x22; 32],
            [0x44; 16],
        )
        .unwrap();
        let (_bob_replacement_key, bob_replacement_hello) = create_pairing_hello_from_material(
            session_id,
            "task-vector-01",
            "device-b",
            "Desk B",
            "device-a",
            PairingRole::Responder,
            [0x23; 32],
            [0x45; 16],
        )
        .unwrap();
        (
            establish_local_session(alice_key, &alice_hello, &bob_replacement_hello).unwrap(),
            establish_local_session(bob_real_key, &bob_real_hello, &alice_hello).unwrap(),
        )
    }

    #[test]
    fn pairing_transcript_and_sas_match_fixed_vector() {
        let (alice, bob) = fixed_pairing_sessions();
        assert_eq!(
            alice.transcript_hash,
            "2ae2920ce2113b3fa5d04de87128de5027a8641b4d4ef895f6d16bba9f1e5ffa"
        );
        assert_eq!(alice.verification_code, "01684434");
        assert_eq!(alice.transcript_hash, bob.transcript_hash);
        assert_eq!(alice.verification_code, bob.verification_code);
    }

    #[test]
    fn pairing_hello_and_control_messages_have_only_public_or_transcript_fields() {
        let (_key, hello) = create_pairing_hello_from_material(
            "00112233445566778899aabbccddeeff",
            "task-vector-01",
            "device-a",
            "Desk A",
            "device-b",
            PairingRole::Initiator,
            [0x11; 32],
            [0x33; 16],
        )
        .unwrap();
        let hello_value = serde_json::to_value(hello).unwrap();
        let hello_fields = hello_value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        assert_eq!(
            hello_fields,
            [
                "v",
                "role",
                "sessionId",
                "taskId",
                "fromDeviceId",
                "toDeviceId",
                "fromDeviceLabel",
                "ephemeralPublicKey",
                "nonce"
            ]
            .into_iter()
            .collect()
        );
        assert!(!hello_value.to_string().contains("verificationCode"));
        assert!(!hello_value.to_string().contains("peerCode"));

        for (message, expected_kind) in [
            (
                NativeControlMessage::PairConfirm {
                    transcript_hash: "fixed-transcript".into(),
                },
                "pair-confirm",
            ),
            (
                NativeControlMessage::PairReject {
                    transcript_hash: "fixed-transcript".into(),
                },
                "pair-reject",
            ),
        ] {
            let value = serde_json::to_value(message).unwrap();
            let object = value.as_object().unwrap();
            assert_eq!(object.len(), 2);
            assert_eq!(
                object.get("kind").and_then(serde_json::Value::as_str),
                Some(expected_kind)
            );
            assert!(object.contains_key("transcriptHash"));
            for forbidden in ["shortCode", "peerCode", "verificationCode"] {
                assert!(!object.contains_key(forbidden));
            }
        }
    }

    #[test]
    fn online_room_key_matches_existing_pbkdf2_sha256_parameters() {
        let key = derive_room_key(" k7q2-m9x4 ", "fling-v2-room");
        assert_eq!(
            to_hex(&key),
            "927f36c1dcc0601a0bceec2d6d2a736e0875d2fbc016cf57aa1196600d1e1c9a"
        );
    }

    #[test]
    fn explicit_sas_binds_both_identities_labels_task_and_ephemeral_keys() {
        let (alice, bob) = two_secure_sessions();
        assert_eq!(alice.verification_code, bob.verification_code);
        assert_eq!(alice.transcript_hash, bob.transcript_hash);
        assert_eq!(alice.peer_device_label, "Bob PC");
        let debug = format!("{alice:?}");
        assert!(!debug.contains(&alice.verification_code));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn code_mismatch_or_cancel_is_terminal_and_cannot_start_webrtc() {
        let (alice_session, bob_session) = substituted_key_sessions();
        let mut alice = PendingLocalPair::new(alice_session);
        let bob = PendingLocalPair::new(bob_session);
        let alice_code = alice.verification_code().unwrap().to_string();
        let bob_code = bob.verification_code().unwrap().to_string();
        assert_ne!(alice_code, bob_code);
        let _rejection_frame = alice.decide_locally(true, &bob_code).unwrap();
        assert_eq!(alice.decision(), PairingDecision::Rejected);
        assert!(alice.verification_code().is_none());
        assert!(alice.into_authenticated().is_err());

        let (cancelled_session, _) = fixed_pairing_sessions();
        let mut cancelled = PendingLocalPair::new(cancelled_session);
        assert!(cancelled.decide_locally(false, "").is_ok());
        assert_eq!(cancelled.decision(), PairingDecision::Rejected);
        assert!(cancelled.verification_code().is_none());
        assert!(cancelled.into_authenticated().is_err());
    }

    #[test]
    fn incorrect_local_code_rejects_without_reflecting_input_in_frame_or_error() {
        let (local_session, remote_session) = fixed_pairing_sessions();
        let mut local = PendingLocalPair::new(local_session);
        let mut remote = PendingLocalPair::new(remote_session);
        let input = "87654321";
        let frame = local.decide_locally(true, input).unwrap();
        let decoded = remote
            .session
            .as_mut()
            .unwrap()
            .cipher
            .decrypt_control(&frame)
            .unwrap();
        assert!(matches!(decoded, NativeControlMessage::PairReject { .. }));
        let control = serde_json::to_string(&decoded).unwrap();
        assert!(!control.contains(input));
        assert!(!control.contains("peerCode"));
        assert!(!control.contains("verificationCode"));
        assert_eq!(local.decision(), PairingDecision::Rejected);
        assert!(local.into_authenticated().is_err());
    }

    #[test]
    fn session_confirm_requires_both_local_and_remote_approval() {
        let (alice_session, bob_session) = two_secure_sessions();
        let mut alice = PendingLocalPair::new(alice_session);
        let mut bob = PendingLocalPair::new(bob_session);
        let alice_code = alice.verification_code().unwrap().to_string();
        let bob_code = bob.verification_code().unwrap().to_string();
        assert_eq!(alice_code, bob_code);
        assert_eq!(alice.decision(), PairingDecision::AwaitingLocalApproval);
        let (premature_session, _) = two_secure_sessions();
        assert!(PendingLocalPair::new(premature_session)
            .into_authenticated()
            .is_err());

        let alice_frame = alice.decide_locally(true, &bob_code).unwrap();
        let decoded_confirm = bob
            .session
            .as_mut()
            .unwrap()
            .cipher
            .decrypt_control(&alice_frame)
            .unwrap();
        let confirm_json = serde_json::to_value(&decoded_confirm).unwrap();
        assert_eq!(confirm_json.as_object().unwrap().len(), 2);
        for forbidden in ["shortCode", "peerCode", "verificationCode"] {
            assert!(!confirm_json.as_object().unwrap().contains_key(forbidden));
        }
        assert_eq!(alice.decision(), PairingDecision::AwaitingPeerApproval);
        bob.approval.accept_peer_message(&decoded_confirm).unwrap();
        assert_eq!(bob.decision(), PairingDecision::AwaitingLocalApproval);
        let bob_frame = bob.decide_locally(true, &alice_code).unwrap();
        assert_eq!(
            alice.accept_peer_frame(&bob_frame).unwrap(),
            PairingDecision::Confirmed
        );
        assert_eq!(bob.decision(), PairingDecision::Confirmed);

        let control = NativeControlMessage::PairConfirm {
            transcript_hash: alice.session.as_ref().unwrap().transcript_hash.clone(),
        };
        let value = serde_json::to_value(control).unwrap();
        assert_eq!(value.as_object().unwrap().len(), 2);
        assert!(!value.to_string().contains("verificationCode"));

        let alice_authenticated = alice.into_authenticated().unwrap();
        let bob_authenticated = bob.into_authenticated().unwrap();
        assert_eq!(alice_authenticated.peer_device_id(), "device-bob");
        assert_eq!(bob_authenticated.peer_device_id(), "device-alice");
    }

    #[test]
    fn directional_sequence_nonces_are_unique_and_replay_is_rejected() {
        let (mut alice, mut bob) = two_secure_sessions();
        let message = NativeControlMessage::TaskCancel {
            task_id: "task-001".to_string(),
        };
        let first = alice.cipher.encrypt_control(&message).unwrap();
        let second = alice.cipher.encrypt_control(&message).unwrap();
        assert_eq!(first.sequence, 0);
        assert_eq!(second.sequence, 1);
        assert_ne!(first.nonce, second.nonce);
        assert_eq!(bob.cipher.decrypt_control(&first).unwrap(), message);
        assert_eq!(bob.cipher.decrypt_control(&second).unwrap(), message);
        assert!(bob.cipher.decrypt_control(&first).is_err());
        assert!(bob.cipher.encrypt_control(&message).is_err());
    }

    #[test]
    fn wrong_sequence_and_reused_nonce_are_rejected() {
        let (mut alice, mut bob) = two_secure_sessions();
        let message = NativeControlMessage::TaskCancel {
            task_id: "task-001".to_string(),
        };
        let first = alice.cipher.encrypt_control(&message).unwrap();
        let second = alice.cipher.encrypt_control(&message).unwrap();
        assert_eq!(first.sequence, 0);
        assert_eq!(second.sequence, 1);
        assert!(bob.cipher.decrypt_control(&second).is_err());

        let (mut alice, mut bob) = two_secure_sessions();
        let first = alice.cipher.encrypt_control(&message).unwrap();
        let mut second = alice.cipher.encrypt_control(&message).unwrap();
        bob.cipher.decrypt_control(&first).unwrap();
        second.nonce = first.nonce;
        assert!(bob.cipher.decrypt_control(&second).is_err());
    }

    #[test]
    fn replaced_public_key_changes_sas_and_rejects_peer_confirmation() {
        let (mut alice, mut bob) = substituted_key_sessions();
        assert_ne!(alice.transcript_hash, bob.transcript_hash);
        assert_ne!(alice.verification_code, bob.verification_code);

        let peer_confirm = bob
            .cipher
            .encrypt_control(&NativeControlMessage::PairConfirm {
                transcript_hash: bob.transcript_hash.clone(),
            })
            .unwrap();
        assert!(alice.cipher.decrypt_control(&peer_confirm).is_err());
    }

    #[test]
    fn session_and_role_mismatch_are_rejected_before_authentication() {
        let (alice_key, alice_hello) = create_pairing_hello(
            "0123456789abcdef0123456789abcdef",
            "task-001",
            "device-alice",
            "Alice",
            "device-bob",
            PairingRole::Initiator,
        )
        .unwrap();
        let (bob_key, mut bob_hello) = create_pairing_hello(
            "1123456789abcdef0123456789abcdef",
            "task-001",
            "device-bob",
            "Bob",
            "device-alice",
            PairingRole::Responder,
        )
        .unwrap();
        assert!(establish_local_session(alice_key, &alice_hello, &bob_hello).is_err());
        drop(bob_key);

        let (alice_key, alice_hello) = create_pairing_hello(
            "0123456789abcdef0123456789abcdef",
            "task-001",
            "device-alice",
            "Alice",
            "device-bob",
            PairingRole::Initiator,
        )
        .unwrap();
        let (bob_key, bob_hello) = create_pairing_hello(
            "0123456789abcdef0123456789abcdef",
            "task-001",
            "device-bob",
            "Bob",
            "device-alice",
            PairingRole::Initiator,
        )
        .unwrap();
        assert!(establish_local_session(alice_key, &alice_hello, &bob_hello).is_err());
        drop(bob_key);
    }

    #[test]
    fn control_aad_binds_session_task_both_identities_and_ciphertext() {
        let (mut alice, mut bob) = two_secure_sessions();
        let mut frame = alice
            .cipher
            .encrypt_control(&NativeControlMessage::P2pOffer {
                sdp: "v=0\r\n".to_string(),
            })
            .unwrap();
        frame.task_id = "task-other".to_string();
        assert!(bob.cipher.decrypt_control(&frame).is_err());

        let (mut alice, mut bob) = two_secure_sessions();
        let mut frame = alice
            .cipher
            .encrypt_control(&NativeControlMessage::P2pOffer {
                sdp: "v=0\r\n".to_string(),
            })
            .unwrap();
        frame.ct.push('A');
        assert!(bob.cipher.decrypt_control(&frame).is_err());
    }

    #[test]
    fn pairing_rejects_mismatched_task_and_wrong_recipient() {
        let (alice_key, alice_hello) = create_pairing_hello(
            "0123456789abcdef0123456789abcdef",
            "task-001",
            "device-alice",
            "Alice",
            "device-bob",
            PairingRole::Initiator,
        )
        .unwrap();
        let (bob_key, mut bob_hello) = create_pairing_hello(
            "0123456789abcdef0123456789abcdef",
            "task-002",
            "device-bob",
            "Bob",
            "device-alice",
            PairingRole::Responder,
        )
        .unwrap();
        assert!(establish_local_session(alice_key, &alice_hello, &bob_hello).is_err());
        bob_hello.task_id = "task-001".to_string();
        bob_hello.to_device_id = "device-eve".to_string();
        let (alice_key, _) = create_pairing_hello(
            "0123456789abcdef0123456789abcdef",
            "task-001",
            "device-alice",
            "Alice",
            "device-bob",
            PairingRole::Initiator,
        )
        .unwrap();
        assert!(establish_local_session(alice_key, &alice_hello, &bob_hello).is_err());
        drop(bob_key);
    }

    #[test]
    fn pairing_invitation_guard_rejects_duplicate_session() {
        let mut guard = SessionReplayGuard::new(2);
        let session = "0123456789abcdef0123456789abcdef";
        assert!(guard.reserve(session).is_ok());
        assert!(guard.reserve(session).is_err());
    }

    #[test]
    fn control_schema_rejects_file_bytes_and_file_paths() {
        let chunk =
            r#"{"kind":"file-chunk","id":"00112233445566778899aabb","seq":0,"data":"AA=="}"#;
        assert!(serde_json::from_str::<NativeControlMessage>(chunk).is_err());

        let manifest_with_path = r#"{"kind":"file-manifest","manifest":{"id":"00112233445566778899aabb","name":"x.bin","size":1,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","chunkSize":1,"totalChunks":1,"kind":"file","path":"C:\\secret"}}"#;
        assert!(serde_json::from_str::<NativeControlMessage>(manifest_with_path).is_err());
    }

    #[test]
    fn direct_frame_matches_typescript_golden_layout() {
        let frame = encode_direct_file_frame(
            "00112233445566778899aabb",
            0x0102_0304,
            &[0xde, 0xad, 0xbe, 0xef],
            4,
            1,
        )
        .unwrap();
        assert_eq!(
            to_hex(&frame),
            "464c4e47010100112233445566778899aabb0102030400000004deadbeef"
        );
        let decoded = decode_direct_file_frame(&frame).unwrap().unwrap();
        assert_eq!(decoded.id, "00112233445566778899aabb");
        assert_eq!(decoded.seq, 0x0102_0304);
        assert_eq!(decoded.payload, [0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(decoded.chunk_count, 1);
        assert_eq!(decoded.chunk_size, None);
    }

    #[test]
    fn direct_batch_frame_round_trips_with_existing_header_extension() {
        let payload: Vec<u8> = (0..65_537).map(|index| (index % 251) as u8).collect();
        let frame =
            encode_direct_file_frame("00112233445566778899aabb", 9, &payload, 32_768, 3).unwrap();
        let decoded = decode_direct_file_frame(&frame).unwrap().unwrap();
        assert_eq!(decoded.payload, payload);
        assert_eq!(decoded.chunk_count, 3);
        assert_eq!(decoded.chunk_size, Some(32_768));
    }

    #[tokio::test]
    async fn bounded_pairing_and_control_framing_round_trip() {
        let (mut alice, mut bob) = two_secure_sessions();
        let local = alice
            .cipher
            .encrypt_control(&NativeControlMessage::P2pIce {
                candidate: Some(
                    "candidate:1 1 udp 2122260223 192.168.1.2 50000 typ host".to_string(),
                ),
            })
            .unwrap();
        let (mut writer, mut reader) = duplex(8192);
        write_control_frame(&mut writer, &local).await.unwrap();
        let received = read_control_frame(&mut reader).await.unwrap();
        assert_eq!(
            bob.cipher.decrypt_control(&received).unwrap(),
            NativeControlMessage::P2pIce {
                candidate: Some(
                    "candidate:1 1 udp 2122260223 192.168.1.2 50000 typ host".to_string()
                ),
            }
        );
    }

    #[tokio::test]
    async fn native_datachannel_round_trips_one_mib_in_both_directions() {
        use livekit::webrtc::{
            data_channel::{DataChannel, DataChannelInit, DataChannelState},
            peer_connection::{AnswerOptions, IceConnectionState, IceGatheringState, OfferOptions},
            peer_connection_factory::{
                ContinualGatheringPolicy, IceTransportsType, PeerConnectionFactory,
                RtcConfiguration,
            },
        };
        use tokio::sync::mpsc;
        use tokio::time::{timeout, Duration};

        async fn transfer_one_mib(
            sender: &DataChannel,
            receiver: &mut mpsc::UnboundedReceiver<Vec<u8>>,
            transfer_id: &str,
            seed: u8,
        ) {
            const TOTAL: usize = 1024 * 1024;
            const CHUNK: usize = 32_768;
            let expected: Vec<u8> = (0..TOTAL)
                .map(|i| ((i.wrapping_mul(131) + usize::from(seed)) % 251) as u8)
                .collect();
            let expected_hash = Sha256::digest(&expected);
            let mut actual_hasher = Sha256::new();

            for (seq, payload) in expected.chunks(CHUNK).enumerate() {
                let frame =
                    encode_direct_file_frame(transfer_id, seq as u32, payload, CHUNK as u16, 1)
                        .unwrap();
                sender.send(&frame, true).unwrap();
                let received = timeout(Duration::from_secs(10), receiver.recv())
                    .await
                    .expect("DataChannel message timeout")
                    .expect("DataChannel closed before message");
                let decoded = decode_direct_file_frame(&received).unwrap().unwrap();
                assert_eq!(decoded.id, transfer_id);
                assert_eq!(decoded.seq, seq as u32);
                assert_eq!(decoded.payload.as_slice(), payload);
                actual_hasher.update(&decoded.payload);
            }
            assert_eq!(
                actual_hasher.finalize().as_slice(),
                expected_hash.as_slice()
            );
        }

        let factory = PeerConnectionFactory::default();
        let mut config = RtcConfiguration::default();
        config.ice_servers.clear();
        config.continual_gathering_policy = ContinualGatheringPolicy::GatherOnce;
        config.ice_transport_type = IceTransportsType::All;
        let a = factory.create_peer_connection(config.clone()).unwrap();
        let b = factory.create_peer_connection(config).unwrap();

        let (a_ice_tx, mut a_ice_rx) = mpsc::unbounded_channel();
        let (b_ice_tx, mut b_ice_rx) = mpsc::unbounded_channel();
        let (b_dc_tx, mut b_dc_rx) = mpsc::unbounded_channel();
        a.on_ice_candidate(Some(Box::new(move |candidate| {
            let _ = a_ice_tx.send(candidate);
        })));
        b.on_ice_candidate(Some(Box::new(move |candidate| {
            let _ = b_ice_tx.send(candidate);
        })));
        b.on_data_channel(Some(Box::new(move |channel| {
            let _ = b_dc_tx.send(channel);
        })));

        let a_dc = a
            .create_data_channel("fling-file", DataChannelInit::default())
            .unwrap();
        let offer = a.create_offer(OfferOptions::default()).await.unwrap();
        a.set_local_description(offer.clone()).await.unwrap();
        b.set_remote_description(offer).await.unwrap();
        let answer = b.create_answer(AnswerOptions::default()).await.unwrap();
        b.set_local_description(answer.clone()).await.unwrap();
        a.set_remote_description(answer).await.unwrap();

        timeout(Duration::from_secs(15), async {
            loop {
                if a.ice_gathering_state() == IceGatheringState::Complete
                    && b.ice_gathering_state() == IceGatheringState::Complete
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("host ICE gathering did not complete");
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut a_candidates = Vec::new();
        let mut b_candidates = Vec::new();
        while let Ok(candidate) = a_ice_rx.try_recv() {
            a_candidates.push(candidate);
        }
        while let Ok(candidate) = b_ice_rx.try_recv() {
            b_candidates.push(candidate);
        }
        assert!(!a_candidates.is_empty(), "A gathered no host candidates");
        assert!(!b_candidates.is_empty(), "B gathered no host candidates");
        for candidate in b_candidates {
            a.add_ice_candidate(candidate).await.unwrap();
        }
        for candidate in a_candidates {
            b.add_ice_candidate(candidate).await.unwrap();
        }

        timeout(Duration::from_secs(20), async {
            loop {
                let a_connected = matches!(
                    a.ice_connection_state(),
                    IceConnectionState::Connected | IceConnectionState::Completed
                );
                let b_connected = matches!(
                    b.ice_connection_state(),
                    IceConnectionState::Connected | IceConnectionState::Completed
                );
                if a_connected && b_connected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("host ICE candidates did not connect");
        let b_dc = timeout(Duration::from_secs(15), b_dc_rx.recv())
            .await
            .expect("remote DataChannel creation timeout")
            .expect("DataChannel callback closed");
        assert_eq!(a_dc.label(), "fling-file");
        assert_eq!(b_dc.label(), "fling-file");

        let (to_a_tx, mut to_a_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (to_b_tx, mut to_b_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        a_dc.on_message(Some(Box::new(move |message| {
            if message.binary {
                let _ = to_a_tx.send(message.data.to_vec());
            }
        })));
        b_dc.on_message(Some(Box::new(move |message| {
            if message.binary {
                let _ = to_b_tx.send(message.data.to_vec());
            }
        })));

        timeout(Duration::from_secs(20), async {
            loop {
                if a_dc.state() == DataChannelState::Open && b_dc.state() == DataChannelState::Open
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("DataChannel did not open");

        transfer_one_mib(&a_dc, &mut to_b_rx, "00112233445566778899aabb", 7).await;
        transfer_one_mib(&b_dc, &mut to_a_rx, "ffeeddccbbaa998877665544", 19).await;
        a.close();
        b.close();
    }

    #[tokio::test]
    async fn oversized_control_length_is_rejected_before_allocation() {
        let (mut writer, mut reader) = duplex(16);
        writer
            .write_all(&((MAX_SIGNAL_PACKET_BYTES as u32) + 1).to_be_bytes())
            .await
            .unwrap();
        let error = read_control_frame::<_>(&mut reader).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
