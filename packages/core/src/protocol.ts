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

/** P2P frame header: magic(4), version(1), type(1), transfer id(12), seq(4), payload length(4). */
export const DIRECT_FILE_FRAME_HEADER_SIZE = 26;
/** Additional metadata for direct-only frames containing multiple logical chunks. */
export const DIRECT_FILE_BATCH_FRAME_HEADER_SIZE = 30;
/** Hard payload ceiling for direct coalescing; sender also checks SCTP maxMessageSize. */
export const DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES = 128 * 1024;
const DIRECT_FILE_FRAME_MAGIC = [0x46, 0x4c, 0x4e, 0x47] as const; // FLNG
const DIRECT_FILE_FRAME_VERSION = 1;
const DIRECT_FILE_CHUNK_TYPE = 1;
const DIRECT_FILE_CHUNK_BATCH_TYPE = 2;

export interface DirectFileChunkFrame {
  id: string;
  seq: number;
  payload: Uint8Array<ArrayBuffer>;
  /** Number and size of contiguous logical chunks in a coalesced frame. */
  chunkCount?: number;
  chunkSize?: number;
}

/** Encode a raw file chunk for RTCDataChannel; control messages remain JSON. */
export function encodeDirectFileChunkFrame(
  id: string,
  seq: number,
  payload: Uint8Array,
  chunkSize = payload.byteLength,
  chunkCount = 1,
): Uint8Array<ArrayBuffer> {
  if (!/^[0-9a-f]{24}$/i.test(id)) throw new Error('invalid transfer id');
  if (!Number.isInteger(seq) || seq < 0 || seq > 0xffffffff) throw new Error('invalid chunk sequence');
  if (!Number.isInteger(chunkCount) || chunkCount < 1 || chunkCount > 0xffff) throw new Error('invalid direct chunk count');
  if (chunkCount > 1 && (!Number.isInteger(chunkSize) || chunkSize <= 0 || chunkSize > 0xffff)) {
    throw new Error('invalid direct chunk size');
  }
  if (seq > 0xffffffff - (chunkCount - 1)) throw new Error('direct chunk sequence range overflow');
  if (chunkCount > 1 && (
    payload.byteLength <= (chunkCount - 1) * chunkSize ||
    payload.byteLength > chunkCount * chunkSize
  )) throw new Error('invalid coalesced direct chunk payload length');
  const headerSize = chunkCount > 1 ? DIRECT_FILE_BATCH_FRAME_HEADER_SIZE : DIRECT_FILE_FRAME_HEADER_SIZE;
  const frame = new Uint8Array(headerSize + payload.byteLength);
  frame.set(DIRECT_FILE_FRAME_MAGIC, 0);
  frame[4] = DIRECT_FILE_FRAME_VERSION;
  frame[5] = chunkCount > 1 ? DIRECT_FILE_CHUNK_BATCH_TYPE : DIRECT_FILE_CHUNK_TYPE;
  for (let i = 0; i < 12; i++) frame[6 + i] = Number.parseInt(id.slice(i * 2, i * 2 + 2), 16);
  const view = new DataView(frame.buffer);
  view.setUint32(18, seq, false);
  view.setUint32(22, payload.byteLength, false);
  if (chunkCount > 1) {
    view.setUint16(26, chunkSize, false);
    view.setUint16(28, chunkCount, false);
  }
  frame.set(payload, headerSize);
  return frame;
}

/** Return undefined for non-FLING payloads so older JSON direct messages remain accepted. */
export function decodeDirectFileChunkFrame(data: Uint8Array): DirectFileChunkFrame | undefined {
  if (data.byteLength < 4 || DIRECT_FILE_FRAME_MAGIC.some((b, i) => data[i] !== b)) return undefined;
  if (data.byteLength < DIRECT_FILE_FRAME_HEADER_SIZE) throw new Error('truncated direct file frame');
  if (data[4] !== DIRECT_FILE_FRAME_VERSION || (data[5] !== DIRECT_FILE_CHUNK_TYPE && data[5] !== DIRECT_FILE_CHUNK_BATCH_TYPE)) {
    throw new Error('unsupported direct file frame');
  }
  const isBatch = data[5] === DIRECT_FILE_CHUNK_BATCH_TYPE;
  const headerSize = isBatch ? DIRECT_FILE_BATCH_FRAME_HEADER_SIZE : DIRECT_FILE_FRAME_HEADER_SIZE;
  if (data.byteLength < headerSize) throw new Error('truncated direct file batch frame');
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const seq = view.getUint32(18, false);
  const payloadLength = view.getUint32(22, false);
  if (payloadLength !== data.byteLength - headerSize) {
    throw new Error('invalid direct file frame length');
  }
  const chunkSize = isBatch ? view.getUint16(26, false) : undefined;
  const chunkCount = isBatch ? view.getUint16(28, false) : 1;
  if (isBatch && (
    !chunkSize || chunkCount < 2 || seq > 0xffffffff - (chunkCount - 1) ||
    payloadLength <= (chunkCount - 1) * chunkSize || payloadLength > chunkCount * chunkSize
  )) throw new Error('invalid coalesced direct chunk metadata');
  let id = '';
  for (let i = 6; i < 18; i++) id += data[i].toString(16).padStart(2, '0');
  return {
    id,
    seq,
    payload: data.subarray(headerSize) as Uint8Array<ArrayBuffer>,
    chunkCount,
    ...(chunkSize ? { chunkSize } : {}),
  };
}

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
  /** Optional browser-provided MIME type, persisted in transfer history. */
  mimeType?: string;
  /** 文件夹发送时的相对路径，如 "docs/report.pdf"；普通文件为空 */
  path?: string;
}

export interface OfferItem {
  name: string;
  size: number;
  kind: 'text' | 'file' | 'clipboard-image';
  /** 文件夹发送时的相对路径，如 "docs/report.pdf"；普通文件为空 */
  path?: string;
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
  /** 预览条目（为避开 LiveKit 64KB 单包上限，只带前 N 个）；完整列表由 manifest 逐个送达 */
  items: OfferItem[];
  /** 全部条目数（包含 text；用于接收端展示总条目数） */
  totalItems?: number;
  /** 文件/剪贴板图片条目数（不包含 text；用于接收端等待 manifest） */
  totalFileItems?: number;
  /** 全部条目总字节数（items 被截断时预览用） */
  totalBytes?: number;
  /** Rust-owned native disk-file test path (exactly one ordinary file). */
  nativeSingleFile?: boolean;
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
  // Sent over the LiveKit relay after the receiver has accepted a manifest.
  // chunkAck advertises support for receive-confirmed relay windows; it is
  // optional so older peers can still use the legacy relay stream.
  | { kind: 'file-manifest-ack'; id: string; chunkAck?: boolean; binaryChunks?: boolean; directFramePayloadBytes?: number }
  // Application-level receive acknowledgement for direct P2P windows.  This
  // deliberately stays on the encrypted LiveKit relay: an ACK behind the
  // same wedged RTCDataChannel would not be a useful health signal.
  | {
      kind: 'file-chunk-ack';
      id: string;
      /** Next contiguous chunk the receiver still needs. */
      nextSeq: number;
      /** Aggregate bytes present in the receiver's reassembly buffer. */
      receivedBytes: number;
      /** Optional diagnostic correlation ID; older peers may omit it. */
      ackId?: string;
      complete?: boolean;
      /** Receiver could not assemble, verify, or persist the file. */
      failed?: boolean;
    }
  | OfferMessage
  | { kind: 'offer-withdraw'; taskId: string }
  | { kind: 'offer-rejected'; taskId: string }
  | { kind: 'task-done'; taskId: string }
  // 接收方全部收齐并落盘后发送：发送方收到前不得拆除连接，
  // 否则"发送成功"只是数据进了发送缓冲，接收方实际没收完。
  | { kind: 'task-ack'; taskId: string }
  // 接收方中途取消：发送方收到后不得再标"成功"，应标"已取消"。
  | { kind: 'task-cancel'; taskId: string }
  | { kind: 'task-failed'; taskId: string; failures: { name: string; size: number; error: string }[] }
  // 接收端上报已收到和已写入临时文件的字节数。doneBytes 保留为兼容旧版，
  // 新版同时发送 receivedBytes 和 savedBytes，发送端不能以本机 send() 计数代替。
  | { kind: 'task-progress'; taskId: string; doneBytes: number; receivedBytes?: number; savedBytes?: number; totalBytes: number }
  // P2P 直连信令：经 LiveKit 中继通道（端到端加密）交换 SDP / ICE，
  // 文件数据本身走直连的 RTCDataChannel，不经过 LiveKit SFU。
  | { kind: 'p2p-offer'; sdp: string }
  | { kind: 'p2p-answer'; sdp: string }
  | { kind: 'p2p-ice'; candidate: string | null };

export function isFlingMessage(o: unknown): o is FlingMessage {
  if (typeof o !== 'object' || o === null) return false;
  const k = (o as { kind?: unknown }).kind;
  if (k === 'task-progress') {
    const msg = o as { taskId?: unknown; doneBytes?: unknown; receivedBytes?: unknown; savedBytes?: unknown; totalBytes?: unknown };
    const isByteCount = (value: unknown): value is number =>
      typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
    const receivedBytes = msg.receivedBytes ?? msg.doneBytes;
    return typeof msg.taskId === 'string' &&
      isByteCount(receivedBytes) && isByteCount(msg.totalBytes) &&
      (msg.savedBytes === undefined || (isByteCount(msg.savedBytes) && msg.savedBytes <= receivedBytes));
  }
  return (
    k === 'hello' ||
    k === 'text' ||
    k === 'bye' ||
    k === 'file-manifest' ||
    k === 'file-chunk' ||
    k === 'file-manifest-ack' ||
    k === 'file-chunk-ack' ||
    k === 'offer' ||
    k === 'offer-withdraw' ||
    k === 'offer-rejected' ||
    k === 'task-done' ||
    k === 'task-ack' ||
    k === 'task-cancel' ||
    k === 'task-failed' ||
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
