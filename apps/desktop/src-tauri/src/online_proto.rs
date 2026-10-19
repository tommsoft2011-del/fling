//! Online (LiveKit) room client in Rust.
//!
//! Replaces the browser `livekit-client` + `FlingRoom` TypeScript path for
//! online transfers. The WebView keeps only the UI; signaling and the data
//! plane both live here.
//!
//! Wire compatibility: every message below must match
//! `packages/core/src/protocol.ts` exactly (JSON shape, field names, the
//! `fling-v2` topic, and the sealed-envelope encryption from
//! `online_crypto`), because the remote peer may run the TypeScript or CLI
//! implementation.

use serde::{Deserialize, Serialize};

pub const FLING_TOPIC: &str = "fling-v2";
pub const PROTOCOL_VERSION: u32 = 2;

/// File item kinds that can be transferred.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum FileKind {
    File,
    #[serde(rename = "clipboard-image")]
    ClipboardImage,
}

/// Metadata for one file in a transfer. Mirrors `FileManifest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileManifest {
    pub id: String,
    pub name: String,
    pub size: u64,
    /// hex sha256 of the whole file
    pub sha256: String,
    pub chunk_size: u32,
    pub total_chunks: u32,
    pub kind: FileKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OfferItem {
    pub name: String,
    pub size: u64,
    pub kind: String, // 'text' | 'file' | 'clipboard-image'
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// All Fling wire messages. The `kind` tag values must match the TypeScript
/// `FlingMessage` union in `packages/core/src/protocol.ts` exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum FlingMessage {
    #[serde(rename = "hello")]
    Hello { device: String },
    #[serde(rename = "text")]
    Text { id: String, text: String },
    #[serde(rename = "bye")]
    Bye { device: String },
    /// NOTE: the manifest is nested because FileManifest itself carries a
    /// `kind` field; a flat spread would clobber the discriminator.
    #[serde(rename = "file-manifest")]
    FileManifest { manifest: FileManifest },
    /// Relay path chunk: base64 data.
    #[serde(rename = "file-chunk")]
    FileChunk { id: String, seq: u32, data: String },
    #[serde(rename = "file-manifest-ack")]
    FileManifestAck {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        chunk_ack: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        binary_chunks: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        direct_frame_payload_bytes: Option<u32>,
    },
    /// Application-level cumulative ACK for direct P2P windows. Stays on the
    /// encrypted LiveKit relay by design.
    #[serde(rename = "file-chunk-ack")]
    FileChunkAck {
        id: String,
        /// Next contiguous chunk the receiver still needs.
        next_seq: u32,
        /// Aggregate bytes in the receiver's reassembly buffer.
        received_bytes: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        complete: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        failed: Option<bool>,
    },
    #[serde(rename = "offer")]
    Offer {
        task_id: String,
        from_device: String,
        from_device_id: String,
        task_room: String,
        task_key: String,
        items: Vec<OfferItem>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total_items: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total_file_items: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total_bytes: Option<u64>,
    },
    #[serde(rename = "offer-withdraw")]
    OfferWithdraw { task_id: String },
    #[serde(rename = "offer-rejected")]
    OfferRejected { task_id: String },
    #[serde(rename = "task-done")]
    TaskDone { task_id: String },
    #[serde(rename = "task-ack")]
    TaskAck { task_id: String },
    #[serde(rename = "task-cancel")]
    TaskCancel { task_id: String },
    #[serde(rename = "task-failed")]
    TaskFailed {
        task_id: String,
        failures: Vec<TaskFailure>,
    },
    #[serde(rename = "task-progress")]
    TaskProgress {
        task_id: String,
        done_bytes: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        received_bytes: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        saved_bytes: Option<u64>,
        total_bytes: u64,
    },
    /// P2P signaling: SDP / ICE go through the encrypted LiveKit relay;
    /// file bytes go over the direct RTCDataChannel.
    #[serde(rename = "p2p-offer")]
    P2pOffer { sdp: String },
    #[serde(rename = "p2p-answer")]
    P2pAnswer { sdp: String },
    #[serde(rename = "p2p-ice")]
    P2pIce { candidate: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFailure {
    pub name: String,
    pub size: u64,
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_json_matches_ts_shapes() {
        // file-manifest-ack with capabilities, as the TS sender emits it.
        let msg = FlingMessage::FileManifestAck {
            id: "abc123".to_string(),
            chunk_ack: Some(true),
            binary_chunks: Some(true),
            direct_frame_payload_bytes: Some(131072),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["kind"], "file-manifest-ack");
        assert_eq!(v["id"], "abc123");
        assert_eq!(v["chunkAck"], true);
        assert_eq!(v["directFramePayloadBytes"], 131072);

        // file-chunk-ack round trip.
        let ack = FlingMessage::FileChunkAck {
            id: "abc123".to_string(),
            next_seq: 42,
            received_bytes: 1376256,
            ack_id: None,
            complete: Some(true),
            failed: None,
        };
        let json = serde_json::to_string(&ack).unwrap();
        let back: FlingMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, FlingMessage::FileChunkAck { next_seq: 42, .. }));

        // p2p-ice with null candidate (end-of-gathering).
        let ice = FlingMessage::P2pIce { candidate: None };
        let json = serde_json::to_string(&ice).unwrap();
        assert!(json.contains("\"candidate\":null"));
    }
}
