// End-to-end encryption for Fling messages.
// The pairing code is the shared secret: PBKDF2 -> AES-GCM-256.
// The LiveKit server only ever sees ciphertext.

import { normalizeCode } from './pairing.js';

const PBKDF2_ITERATIONS = 100_000;

function b64encode(bytes: Uint8Array): string {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s);
}

function b64decode(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export interface SealedEnvelope {
  v: 1;
  iv: string; // base64, 12 bytes
  ct: string; // base64
}

/**
 * Derive the room encryption key from the pairing code.
 *
 * `context` selects the PBKDF2 salt domain. It defaults to the historical
 * 'fling-v1-key' salt so existing single-argument callers keep deriving the
 * exact same key. New v2 callers pass 'fling-v2-room' / 'fling-v2-lobby'.
 */
export async function deriveRoomKey(code: string, context: string = 'fling-v1-key'): Promise<CryptoKey> {
  const enc = new TextEncoder();
  const base = await crypto.subtle.importKey(
    'raw',
    enc.encode(normalizeCode(code)),
    'PBKDF2',
    false,
    ['deriveKey'],
  );
  return crypto.subtle.deriveKey(
    {
      name: 'PBKDF2',
      salt: enc.encode(context),
      iterations: PBKDF2_ITERATIONS,
      hash: 'SHA-256',
    },
    base,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt'],
  );
}

/** Encrypt a plaintext message into a JSON string ready for the data channel. */
export async function sealText(key: CryptoKey, plaintext: string): Promise<string> {
  const iv = new Uint8Array(12);
  crypto.getRandomValues(iv);
  const ct = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv: iv as BufferSource },
    key,
    new TextEncoder().encode(plaintext),
  );
  const env: SealedEnvelope = { v: 1, iv: b64encode(iv), ct: b64encode(new Uint8Array(ct)) };
  return JSON.stringify(env);
}

/** Decrypt a sealed JSON string. Throws on wrong key / tampered data. */
export async function openText(key: CryptoKey, sealed: string): Promise<string> {
  const env = JSON.parse(sealed) as SealedEnvelope;
  if (env.v !== 1 || !env.iv || !env.ct) throw new Error('bad envelope');
  const pt = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv: b64decode(env.iv) as BufferSource },
    key,
    b64decode(env.ct) as BufferSource,
  );
  return new TextDecoder().decode(pt);
}
