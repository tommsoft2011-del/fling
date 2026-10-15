// File transfer over the Fling data channel.
// Platform-neutral: works on anything that can provide a FileLike
// (browser File, node fs, ...). Hashing streams via @noble/hashes
// so multi-GB files never need to sit fully in memory.

import { sha256 } from '@noble/hashes/sha2';
import { bytesToHex } from '@noble/hashes/utils';
import { FILE_CHUNK_SIZE, randomId, type FileKind, type FileManifest } from './protocol.js';

/** Minimal file abstraction shared by browser File, node fs, etc. */
export interface FileLike {
  readonly name: string;
  readonly size: number;
  readonly type?: string;
  slice(start: number, end: number): Promise<Uint8Array>;
  /** 文件夹发送时的相对路径，如 "docs/report.pdf"；普通文件为空 */
  readonly path?: string;
}

/** Wrap a browser File/Blob. */
export function wrapWebFile(f: Blob & { name?: string }, name?: string, path?: string): FileLike {
  return {
    name: name ?? f.name ?? 'file',
    size: f.size,
    type: f.type,
    slice: async (s, e) => new Uint8Array(await f.slice(s, e).arrayBuffer()),
    path,
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

export async function buildManifest(file: FileLike, kind: FileKind = 'file'): Promise<FileManifest> {
  const chunkSize = FILE_CHUNK_SIZE;
  return {
    id: randomId(12),
    name: file.name,
    size: file.size,
    sha256: await hashFile(file),
    chunkSize,
    totalChunks: Math.max(1, Math.ceil(file.size / chunkSize)),
    kind,
    mimeType: file.type || undefined,
    path: file.path,
  };
}

/** Receiver-side reassembly with progress + sha256 verification. */
export class FileReceiver {
  private chunks: (Uint8Array | undefined)[];
  private receivedBytes = 0;
  /** The first chunk not yet present in the contiguous prefix. */
  private nextMissingSeq = 0;

  constructor(readonly manifest: FileManifest) {
    // NOTE: must .fill() — a bare new Array(n) has holes which .every() skips,
    // making isComplete() vacuously true.
    this.chunks = new Array<Uint8Array | undefined>(manifest.totalChunks).fill(undefined);
  }

  get totalBytes(): number {
    return this.manifest.size;
  }

  /** Aggregate bytes already present, including chunks after a hole. */
  get receivedByteCount(): number {
    return this.receivedBytes;
  }

  /** The next contiguous chunk needed by a sender doing windowed replay. */
  get nextSeq(): number {
    if (this.manifest.size === 0) return this.manifest.totalChunks;
    return this.nextMissingSeq;
  }

  /** Add a chunk; returns true when the file is complete. */
  addChunk(seq: number, dataB64: string): boolean {
    if (!Number.isInteger(seq) || seq < 0 || seq >= this.manifest.totalChunks) {
      throw new Error('invalid chunk sequence');
    }
    if (this.chunks[seq]) return this.isComplete();
    return this.addChunkBytes(seq, b64decode(dataB64));
  }

  /** Store the received binary view directly; the DataChannel ArrayBuffer remains alive through this view. */
  addChunkBytes(seq: number, bytes: Uint8Array): boolean {
    if (!Number.isInteger(seq) || seq < 0 || seq >= this.manifest.totalChunks) {
      throw new Error('invalid chunk sequence');
    }
    if (this.chunks[seq]) return this.isComplete();
    const expectedBytes = Math.min(
      this.manifest.chunkSize,
      Math.max(0, this.manifest.size - seq * this.manifest.chunkSize),
    );
    if (bytes.byteLength !== expectedBytes) {
      throw new Error(`invalid chunk size (got ${bytes.byteLength}, want ${expectedBytes})`);
    }
    this.chunks[seq] = bytes;
    this.receivedBytes += bytes.byteLength;
    // Direct delivery is ordered today, but relay replay and a future
    // unordered channel are allowed to fill holes. Keep this incremental so
    // an ACK does not scan the whole file on every chunk.
    while (this.nextMissingSeq < this.chunks.length && this.chunks[this.nextMissingSeq]) {
      this.nextMissingSeq++;
    }
    return this.isComplete();
  }

  get progress(): number {
    return this.manifest.size === 0 ? 1 : this.receivedBytes / this.manifest.size;
  }

  isComplete(): boolean {
    return this.manifest.size === 0 || this.chunks.every((c) => c !== undefined);
  }

  /** Concatenate and verify in bounded turns so a large file cannot starve network callbacks. */
  async assembleAsync(): Promise<Uint8Array> {
    const out = new Uint8Array(this.manifest.size);
    const hash = sha256.create();
    let off = 0;
    let nextYieldAt = 4 * 1024 * 1024;
    for (let i = 0; i < this.chunks.length; i++) {
      if (this.manifest.size === 0) break;
      const c = this.chunks[i];
      if (!c) throw new Error('missing chunks');
      out.set(c, off);
      hash.update(c);
      off += c.length;
      // The completed file is now represented by `out`; release each chunk
      // reference as soon as it has been copied and hashed.
      this.chunks[i] = undefined;
      if (off >= nextYieldAt) {
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        nextYieldAt = off + 4 * 1024 * 1024;
      }
    }
    const digest = bytesToHex(hash.digest());
    if (digest !== this.manifest.sha256) {
      throw new Error(`sha256 mismatch (got ${digest.slice(0, 12)}…, want ${this.manifest.sha256.slice(0, 12)}…)`);
    }
    return out;
  }

  static encodeChunk(bytes: Uint8Array): string {
    return b64encode(bytes);
  }
}
