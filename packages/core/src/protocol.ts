// Fling wire protocol (v1): everything below is encrypted with the room key
// before it hits the LiveKit data channel.

export const FLING_TOPIC = 'fling-v1';
export const PROTOCOL_VERSION = 1;

/** Max plaintext bytes per text message (keep well under data-channel limits). */
export const MAX_TEXT_BYTES = 60 * 1024;

export type FlingMessage =
  | { kind: 'hello'; device: string }
  | { kind: 'text'; id: string; text: string }
  | { kind: 'bye'; device: string };

export function isFlingMessage(o: unknown): o is FlingMessage {
  if (typeof o !== 'object' || o === null) return false;
  const k = (o as { kind?: unknown }).kind;
  return k === 'hello' || k === 'text' || k === 'bye';
}

export function randomId(length = 8): string {
  const buf = new Uint8Array(length);
  crypto.getRandomValues(buf);
  return [...buf].map((b) => b.toString(16).padStart(2, '0')).join('');
}
