// Fling wire protocol (v2): everything below is encrypted with the room key
// before it hits the LiveKit data channel.
//
// v2 is the transfer-tool generation: one send action = one task = one
// dedicated LiveKit task room. Signaling (offer / withdraw) happens in a
// lobby room (far field, pairing code) or a device inbox room (near field).

export const FLING_TOPIC = 'fling-v2';
export const PROTOCOL_VERSION = 2;

/** Max plaintext bytes per text message (keep well under data-channel limits). */
export const MAX_TEXT_BYTES = 60 * 1024;

/**
 * Raw chunk size for file transfer.
 *
 * WARNING: do NOT raise this without checking livekit-client's per-packet cap
 * (RTCEngine.DEFAULT_MAX_MESSAGE_SIZE = 64_000 bytes). A chunk goes through
 * base64 (x4/3) inside the file-chunk JSON, then the whole message is sealed
 * with AES-GCM into another base64 envelope (x4/3 again) plus protobuf framing.
 * 32 KiB raw  => ~58.5 KiB on the wire (fits with margin).
 * 256 KiB raw => ~466 KiB on the wire => PublishDataError, every chunk fails.
 */
export const FILE_CHUNK_SIZE = 32 * 1024;

/** What the bytes of a transferred item mean to the receiver. */
export type FileKind = 'file' | 'clipboard-image';

export interface FileManifest {
  id: string;
  name: string;
  size: number;
  /** hex sha256 of the whole file */
  sha256: string;
  chunkSize: number;
  totalChunks: number;
  /** 'file' -> save to disk; 'clipboard-image' -> copy into clipboard */
  kind: FileKind;
}

export interface OfferItem {
  name: string;
  size: number;
  kind: 'text' | 'file' | 'clipboard-image';
}

/**
 * Announced in a lobby / inbox room to offer a new transfer task.
 * The receiver then joins `taskRoom` to accept it.
 */
export interface OfferMessage {
  kind: 'offer';
  taskId: string;
  fromDevice: string;
  fromDeviceId: string;
  /** LiveKit room name of the dedicated task room. */
  taskRoom: string;
  /**
   * base64 of 32 random bytes (near field). Empty string for far field —
   * the receiver then derives the task key from the pairing code instead.
   */
  taskKey: string;
  items: OfferItem[];
}

export type FlingMessage =
  | { kind: 'hello'; device: string }
  | { kind: 'text'; id: string; text: string }
  | { kind: 'bye'; device: string }
  // NOTE: the manifest is nested because FileManifest itself carries a
  // `kind` field ('file' | 'clipboard-image'); a flat spread would clobber
  // the message discriminator on the wire.
  | { kind: 'file-manifest'; manifest: FileManifest }
  | { kind: 'file-chunk'; id: string; seq: number; data: string } // base64
  | OfferMessage
  | { kind: 'offer-withdraw'; taskId: string }
  | { kind: 'offer-rejected'; taskId: string }
  | { kind: 'task-done'; taskId: string }
  // P2P 直连信令：经 LiveKit 中继通道（端到端加密）交换 SDP / ICE，
  // 文件数据本身走直连的 RTCDataChannel，不经过 LiveKit SFU。
  | { kind: 'p2p-offer'; sdp: string }
  | { kind: 'p2p-answer'; sdp: string }
  | { kind: 'p2p-ice'; candidate: string | null };

export function isFlingMessage(o: unknown): o is FlingMessage {
  if (typeof o !== 'object' || o === null) return false;
  const k = (o as { kind?: unknown }).kind;
  return (
    k === 'hello' ||
    k === 'text' ||
    k === 'bye' ||
    k === 'file-manifest' ||
    k === 'file-chunk' ||
    k === 'offer' ||
    k === 'offer-withdraw' ||
    k === 'offer-rejected' ||
    k === 'task-done' ||
    k === 'p2p-offer' ||
    k === 'p2p-answer' ||
    k === 'p2p-ice'
  );
}

export function randomId(length = 8): string {
  const buf = new Uint8Array(length);
  crypto.getRandomValues(buf);
  return [...buf].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** 32 random bytes, base64 — the near-field task key carried in an offer. */
export function newTaskKey(): string {
  const buf = new Uint8Array(32);
  crypto.getRandomValues(buf);
  let s = '';
  for (const b of buf) s += String.fromCharCode(b);
  return btoa(s);
}
