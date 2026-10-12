// Pairing code generation and room-name derivation.
// Pure WebCrypto — runs in Bun, Node 19+ and browsers (shared core).

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
