// Fling wire protocol (v1): everything below is encrypted with the room key
// before it hits the LiveKit data channel.

export const FLING_TOPIC = 'fling-v1';
export const PROTOCOL_VERSION = 1;

/** Max plaintext bytes per text message (keep well under data-channel limits). */
export const MAX_TEXT_BYTES = 60 * 1024;

/** Raw chunk size for file transfer; base64 + encryption overhead still fits. */
export const FILE_CHUNK_SIZE = 32 * 1024;

export interface FileManifest {
  id: string;
  name: string;
  size: number;
  /** hex sha256 of the whole file */
  sha256: string;
  chunkSize: number;
  totalChunks: number;
}

export type FlingMessage =
  | { kind: 'hello'; device: string }
  | { kind: 'text'; id: string; text: string }
  | { kind: 'bye'; device: string }
  | ({ kind: 'file-manifest' } & FileManifest)
  | { kind: 'file-chunk'; id: string; seq: number; data: string }; // base64

export function isFlingMessage(o: unknown): o is FlingMessage {
  if (typeof o !== 'object' || o === null) return false;
  const k = (o as { kind?: unknown }).kind;
  return (
    k === 'hello' ||
    k === 'text' ||
    k === 'bye' ||
    k === 'file-manifest' ||
    k === 'file-chunk'
  );
}

export function randomId(length = 8): string {
  const buf = new Uint8Array(length);
  crypto.getRandomValues(buf);
  return [...buf].map((b) => b.toString(16).padStart(2, '0')).join('');
}
