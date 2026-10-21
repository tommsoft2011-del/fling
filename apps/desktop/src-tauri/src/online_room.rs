//! LiveKit room client for Fling online transfers.
//!
//! Replaces the browser `livekit-client` transport (`FlingTransport`) used by
//! the TypeScript `FlingRoom`. All signaling messages are end-to-end
//! encrypted with the room key before they hit the LiveKit data channel, so
//! the server only ever sees ciphertext.

use livekit::{DataPacket, Room, RoomEvent, RoomOptions};
use tokio::sync::mpsc;

use crate::online_crypto::{open_text, seal_text};
use crate::online_proto::{FlingMessage, FLING_TOPIC};

/// Events from the room, after decryption and JSON parsing.
#[derive(Debug)]
pub enum OnlineRoomEvent {
    /// A decrypted Fling protocol message from a peer.
    Message { from: String, msg: FlingMessage },
    /// A peer joined and is ready for data messages.
    PeerJoined(String),
    /// A peer left.
    PeerLeft(String),
    /// The room connection dropped.
    Disconnected { reason: String },
    /// A message arrived that was not on the Fling topic or failed to
    /// decrypt / parse. Logged, never fatal.
    ProtocolWarning { detail: String },
}

/// A connected LiveKit room with the Fling message bus on top.
pub struct OnlineRoom {
    room: Room,
    room_key: [u8; 32],
}

impl OnlineRoom {
    /// Connect to the LiveKit room and start dispatching events to
    /// `event_tx`. Mirrors `FlingRoom.connect()` + `NodeTransport.connect()`.
    pub async fn connect(
        server_url: &str,
        token: &str,
        room_key: [u8; 32],
        event_tx: mpsc::UnboundedSender<OnlineRoomEvent>,
    ) -> Result<Self, String> {
        let (room, mut event_rx) = Room::connect(server_url, token, RoomOptions::default())
            .await
            .map_err(|e| format!("LiveKit room connect failed: {e}"))?;

        // Later ParticipantConnected events do not replace the initial snapshot.
        // The second native process must see a peer that joined before it did.
        for identity in room.remote_participants().keys() {
            let _ = event_tx.send(OnlineRoomEvent::PeerJoined(identity.to_string()));
        }
        // Dispatch loop: decrypt + parse incoming data, forward peer events.
        let key = room_key;
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    RoomEvent::DataReceived { payload, topic, participant, .. } => {
                        if topic.as_deref() != Some(FLING_TOPIC) {
                            continue;
                        }
                        let from = participant
                            .map(|p| p.identity().to_string())
                            .unwrap_or_else(|| "unknown".to_string());
                        let text = String::from_utf8_lossy(&payload);
                        match open_text(&key, &text) {
                            Ok(plain) => match serde_json::from_str::<FlingMessage>(&plain) {
                                Ok(msg) => {
                                    let _ = event_tx.send(OnlineRoomEvent::Message { from, msg });
                                }
                                Err(e) => {
                                    let _ = event_tx.send(OnlineRoomEvent::ProtocolWarning {
                                        detail: format!("unparseable Fling message from {from}: {e}"),
                                    });
                                }
                            },
                            Err(e) => {
                                let _ = event_tx.send(OnlineRoomEvent::ProtocolWarning {
                                    detail: format!("decrypt failed for message from {from}: {e}"),
                                });
                            }
                        }
                    }
                    RoomEvent::ParticipantConnected(p) => {
                        let _ = event_tx.send(OnlineRoomEvent::PeerJoined(p.identity().to_string()));
                    }
                    RoomEvent::ParticipantDisconnected(p) => {
                        let _ = event_tx.send(OnlineRoomEvent::PeerLeft(p.identity().to_string()));
                    }
                    RoomEvent::Disconnected { reason } => {
                        let _ = event_tx.send(OnlineRoomEvent::Disconnected {
                            reason: format!("{reason:?}"),
                        });
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(Self { room, room_key })
    }

    /// Our own participant identity (valid after connect).
    pub fn local_identity(&self) -> String {
        self.room.local_participant().identity().to_string()
    }

    /// Serialize, encrypt, and publish a Fling message on the `fling-v2`
    /// topic (reliable). Mirrors `FlingRoom.send()`.
    pub async fn send(&self, msg: &FlingMessage) -> Result<(), String> {
        let json = serde_json::to_string(msg).map_err(|e| format!("encode message: {e}"))?;
        let sealed = seal_text(&self.room_key, &json);
        crate::online_setup::bounded_initial_effect(async { self.room
            .local_participant()
            .publish_data(DataPacket {
                payload: sealed.into_bytes(),
                topic: Some(FLING_TOPIC.to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .map_err(|e| format!("publish Fling message: {e}")) }).await
    }

    /// Publish raw bytes (already encrypted by the caller) on the Fling
    /// topic. Used for the relay file-chunk path.
    pub async fn publish_raw(&self, payload: Vec<u8>) -> Result<(), String> {
        crate::online_setup::bounded_initial_effect(async { self.room
            .local_participant()
            .publish_data(DataPacket {
                payload,
                topic: Some(FLING_TOPIC.to_string()),
                reliable: true,
                ..Default::default()
            })
            .await
            .map_err(|e| format!("publish raw payload: {e}")) }).await
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        // Peer departure and the owner can concurrently request the same close.
        if self.room.connection_state()==livekit::ConnectionState::Disconnected {return Ok(());}
        match self.room.close().await {
            Ok(())=>Ok(()),
            Err(_) if self.room.connection_state()==livekit::ConnectionState::Disconnected=>Ok(()),
            Err(error)=>Err(format!("LiveKit disconnect failed: {error}")),
        }
    }
}
