// P2P direct file channel.
//
// Design: LiveKit stays the signaling + NAT hole-punching (ICE) pipe.
// File bytes go over a direct RTCPeerConnection data channel between the
// two peers whenever ICE succeeds; otherwise everything falls back to the
// LiveKit relay transparently (per-chunk routing decision in FlingRoom).
//
// This module is platform-neutral: it only declares the channel interface.
// The actual RTCPeerConnection wiring lives in the app (desktop/src/p2p.ts)
// because Node/Bun targets don't all have WebRTC.

/**
 * A direct, ordered, reliable byte channel to the peer (e.g. RTCDataChannel).
 * The app builds this after a successful P2P handshake and hands it to
 * FlingRoom.setDirectChannel(). FlingRoom routes file-chunk messages through
 * it while it's open; anything else keeps using the LiveKit relay.
 */
export interface DirectChannel {
  /** peer label for logs / event attribution */
  readonly label: string;
  readonly readyState: string;
  readonly bufferedAmount: number;
  /** Negotiated SCTP maximum message size; Infinity/unknown is clamped by the sender. */
  readonly maxMessageSize: number;
  send(data: Uint8Array<ArrayBuffer>): void;
  close(): void;
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null;
  onclose: (() => void) | null;
  /**
   * Resolve once bufferedAmount drops to/below threshold. Reject when the
   * channel closes or the drain watchdog expires.
   * Used for sender-side backpressure on the direct channel.
   */
  waitForDrain(threshold: number): Promise<void>;
}

/** How long to wait for the P2P handshake before giving up to relay. */
export const P2P_CONNECT_TIMEOUT_MS = 12_000;

/**
 * Pause sending when the direct channel buffers more than this.
 * Well under any SCTP send-buffer limit; drained via bufferedamountlow.
 */
export const P2P_DRAIN_THRESHOLD = 256 * 1024;

/** Never wait forever for bufferedamountlow on a wedged SCTP path. */
export const P2P_DRAIN_TIMEOUT_MS = 3_000;

/** Maximum time to wait for the receiver's relay manifest confirmation. */
export const P2P_MANIFEST_ACK_TIMEOUT_MS = 8_000;
