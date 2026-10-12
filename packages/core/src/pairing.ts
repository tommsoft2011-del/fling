// Pairing code generation and room-name derivation.
// Pure WebCrypto — runs in Bun, Node 19+ and browsers (shared core).

import { randomId } from './protocol.js';

const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no 0/O, 1/I/L
export const CODE_LENGTH = 6;

/** Generate a human-friendly pairing code, e.g. "K7Q2M9". */
export function generatePairingCode(length: number = CODE_LENGTH): string {
  const buf = new Uint8Array(length);
  crypto.getRandomValues(buf);
  let out = '';
  for (const b of buf) out += CODE_ALPHABET[b % CODE_ALPHABET.length];
  return out;
}

/** Normalize user-typed code: uppercase, strip spaces/dashes. */
export function normalizeCode(input: string): string {
  return input.trim().toUpperCase().replace(/[\s-]+/g, '');
}

function hex(bytes: Uint8Array): string {
  return [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** Both sides derive the same LiveKit room name from the pairing code. */
export async function roomNameForCode(code: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode('fling-v1-room:' + normalizeCode(code)),
  );
  return 'fling-' + hex(new Uint8Array(digest)).slice(0, 16);
}

/**
 * v2 lobby room: both sides derive the same name from the pairing code.
 * Offers for far-field transfers are announced here.
 */
export async function lobbyNameForCode(code: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode('fling-v2-lobby:' + normalizeCode(code)),
  );
  return 'fling-lobby-' + hex(new Uint8Array(digest)).slice(0, 16);
}

/**
 * Per-device inbox room for near-field offers. The deviceId is sanitized
 * down to [a-z0-9] so the room name is always LiveKit-safe.
 */
export function inboxRoomName(deviceId: string): string {
  const clean = deviceId.toLowerCase().replace(/[^a-z0-9]/g, '');
  if (!clean) throw new Error('inboxRoomName: deviceId must contain at least one [a-z0-9] character');
  return 'fling-inbox-' + clean;
}

/** Random dedicated room name for one transfer task (one send action). */
export function newTaskRoomName(): string {
  return 'fling-task-' + randomId(12);
}
