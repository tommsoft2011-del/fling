// File transfer over the Fling data channel.
// Platform-neutral: works on anything that can provide a FileLike
// (browser File, node fs, ...). Hashing streams via @noble/hashes
// so multi-GB files never need to sit fully in memory.

import { sha256 } from '@noble/hashes/sha2';
import { bytesToHex } from '@noble/hashes/utils';
import { FILE_CHUNK_SIZE, randomId, type FileManifest } from './protocol.js';

/** Minimal file abstraction shared by browser File, node fs, etc. */
export interface FileLike {
  readonly name: string;
  readonly size: number;
  slice(start: number, end: number): Promise<Uint8Array>;
}

/** Wrap a browser File/Blob. */
export function wrapWebFile(f: Blob & { name?: string }, name?: string): FileLike {
  return {
    name: name ?? f.name ?? 'file',
    size: f.size,
    slice: async (s, e) => new Uint8Array(await f.slice(s, e).arrayBuffer()),
  };
}

function b64encode(bytes: Uint8Array): string {
  let s = '';
  const CH = 0x8000;
  for (let i = 0; i < bytes.length; i += CH) {
    s += String.fromCharCode(...bytes.subarray(i, i + CH));
  }
  return btoa(s);
}

function b64decode(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/** Stream-hash a file (two reads max, constant memory). */
export async function hashFile(file: FileLike, chunkSize = 256 * 1024): Promise<string> {
  const h = sha256.create();
  for (let off = 0; off < file.size; off += chunkSize) {
    h.update(await file.slice(off, Math.min(off + chunkSize, file.size)));
  }
  return bytesToHex(h.digest());
}

export async function buildManifest(file: FileLike): Promise<FileManifest> {
  const chunkSize = FILE_CHUNK_SIZE;
  return {
    id: randomId(12),
    name: file.name,
    size: file.size,
    sha256: await hashFile(file),
    chunkSize,
    totalChunks: Math.max(1, Math.ceil(file.size / chunkSize)),
  };
}

/** Receiver-side reassembly with progress + sha256 verification. */
export class FileReceiver {
  private chunks: (Uint8Array | undefined)[];
  private receivedBytes = 0;

  constructor(readonly manifest: FileManifest) {
    // NOTE: must .fill() — a bare new Array(n) has holes which .every() skips,
    // making isComplete() vacuously true.
    this.chunks = new Array<Uint8Array | undefined>(manifest.totalChunks).fill(undefined);
  }

  get totalBytes(): number {
    return this.manifest.size;
  }

  /** Add a chunk; returns true when the file is complete. */
  addChunk(seq: number, dataB64: string): boolean {
    if (seq < 0 || seq >= this.manifest.totalChunks) return false;
    if (this.chunks[seq]) return this.isComplete(); // duplicate
    const bytes = b64decode(dataB64);
    this.chunks[seq] = bytes;
    this.receivedBytes += bytes.length;
    return this.isComplete();
  }

  get progress(): number {
    return this.manifest.size === 0 ? 1 : this.receivedBytes / this.manifest.size;
  }

  isComplete(): boolean {
    return this.chunks.every((c) => c !== undefined);
  }

  /** Concatenate and verify sha256. Throws on mismatch. */
  assemble(): Uint8Array {
    const out = new Uint8Array(this.manifest.size);
    let off = 0;
    for (const c of this.chunks) {
      if (!c) throw new Error('missing chunks');
      out.set(c, off);
      off += c.length;
    }
    const digest = bytesToHex(sha256(out));
    if (digest !== this.manifest.sha256) {
      throw new Error(`sha256 mismatch (got ${digest.slice(0, 12)}…, want ${this.manifest.sha256.slice(0, 12)}…)`);
    }
    return out;
  }

  static encodeChunk(bytes: Uint8Array): string {
    return b64encode(bytes);
  }
}
