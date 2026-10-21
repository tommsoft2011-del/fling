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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
#[serde(tag = "kind")]
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
    FileManifest { manifest: FileManifest, #[serde(default, skip_serializing_if = "Option::is_none")] route: Option<crate::online_direct::Capability> },
    /// Relay path chunk: base64 data.
    #[serde(rename = "file-chunk")]
    FileChunk { id: String, seq: u32, data: String },
    #[serde(rename = "file-manifest-ack", rename_all = "camelCase")]
    FileManifestAck {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        route: Option<crate::online_direct::Capability>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_single_file: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        chunk_ack: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        binary_chunks: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        direct_frame_payload_bytes: Option<u32>,
    },
    /// Application-level cumulative ACK for direct P2P windows. Stays on the
    /// encrypted LiveKit relay by design.
    #[serde(rename = "file-chunk-ack", rename_all = "camelCase")]
    FileChunkAck {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        route_fence: Option<crate::online_route_choice::Fence>,
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
    #[serde(rename = "offer", rename_all = "camelCase")]
    Offer {
        task_id: String,
        from_device: String,
        from_device_id: String,
        task_room: String,
        task_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        native_single_file: Option<bool>,
        items: Vec<OfferItem>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total_items: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total_file_items: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        total_bytes: Option<u64>,
    },
    #[serde(rename = "offer-withdraw", rename_all = "camelCase")]
    OfferWithdraw { task_id: String },
    #[serde(rename = "offer-rejected", rename_all = "camelCase")]
    OfferRejected { task_id: String },
    #[serde(rename = "task-done", rename_all = "camelCase")]
    TaskDone { task_id: String },
    #[serde(rename = "task-ack", rename_all = "camelCase")]
    TaskAck { task_id: String },
    #[serde(rename = "task-cancel", rename_all = "camelCase")]
    TaskCancel { task_id: String },
    #[serde(rename = "task-failed", rename_all = "camelCase")]
    TaskFailed {
        task_id: String,
        failures: Vec<TaskFailure>,
    },
    #[serde(rename = "task-progress", rename_all = "camelCase")]
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
    #[serde(rename = "native-route")]
    NativeRoute { id: String, signal: crate::online_direct::Signal },
    #[serde(rename = "p2p-offer")]
    P2pOffer { sdp: String },
    #[serde(rename = "p2p-answer")]
    P2pAnswer { sdp: String },
    #[serde(rename = "p2p-ice")]
    P2pIce { candidate: Option<String> },
}

impl FlingMessage {
    /// WebView may request legacy high-level task actions, never author native
    /// SDP/ICE, route transactions, file bytes or file ACKs (including non-native sessions).
    pub fn webview_intent(&self) -> bool {
        !matches!(self, Self::NativeRoute {..} | Self::P2pOffer {..} | Self::P2pAnswer {..}
            | Self::P2pIce {..} | Self::FileManifest {..} | Self::FileManifestAck {..}
            | Self::FileChunk {..} | Self::FileChunkAck {..})
    }
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
    fn manifest_duplicate_requires_all_metadata_not_only_id_name_and_size() {
        let original = FileManifest { id: "attempt-file".into(), name: "file.bin".into(),
            size: 4, sha256: "expected-sha".into(), chunk_size: 4, total_chunks: 1,
            kind: FileKind::File, mime_type: None, path: None };
        assert_eq!(original, original.clone());
        let mut changed = original.clone(); changed.sha256 = "different-sha".into();
        assert_ne!(original, changed);
        let mut changed = original.clone(); changed.chunk_size = 8; assert_ne!(original, changed);
        let mut changed = original.clone(); changed.total_chunks = 2; assert_ne!(original, changed);
        let mut changed = original.clone(); changed.path = Some("different/file.bin".into()); assert_ne!(original, changed);
        let mut changed = original.clone(); changed.id = "old-attempt".into(); assert_ne!(original, changed);
    }

    #[test]
    fn message_json_matches_ts_shapes() {
        // file-manifest-ack with capabilities, as the TS sender emits it.
        let msg = FlingMessage::FileManifestAck {
            id: "abc123".to_string(),
            route: None,
            native_single_file: None,
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
            route_fence: None,
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

#[cfg(test)]
mod route_contract_tests {
    use super::*;
    use crate::online_direct::{Capability,Signal};
    use crate::online_route_choice::{Envelope,Fence,Path,Wire};
    #[test]
    fn route_capability_and_full_fence_survive_wire_and_cannot_be_ui_authored() {
        let capability=Capability::fresh();let fence=Fence{epoch:capability.epoch,seq:1,path:Path::Turn};
        let signal=FlingMessage::NativeRoute{id:"transfer".into(),signal:Signal::Control(Envelope{fence,message:Wire::StartAck})};
        let value=serde_json::to_value(&signal).unwrap();
        assert_eq!(value["kind"],"native-route");
        let back:FlingMessage=serde_json::from_value(value).unwrap();
        assert!(!back.webview_intent());
        assert!(matches!(back,FlingMessage::NativeRoute{signal:Signal::Control(Envelope{fence:f,message:Wire::StartAck}),..} if f==fence));
        for msg in [FlingMessage::P2pOffer{sdp:String::new()},FlingMessage::P2pAnswer{sdp:String::new()},FlingMessage::P2pIce{candidate:None},
            FlingMessage::FileChunk{id:"file".into(),seq:0,data:"not allowed".into()},
            FlingMessage::FileChunkAck{id:"file".into(),route_fence:Some(fence),next_seq:0,received_bytes:0,ack_id:None,complete:None,failed:None}] {
            assert!(!msg.webview_intent());
        }
        assert!(FlingMessage::TaskCancel{task_id:"task".into()}.webview_intent());
        assert!(FlingMessage::Text{id:"text".into(),text:"hello".into()}.webview_intent());
    }
}
