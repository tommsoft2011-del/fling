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

/** Hard upper bound for one desktop write IPC payload. */
export const FILE_RECEIVE_BATCH_SIZE = 4 * 1024 * 1024;

/** Receiver-side chunk store with bounded streaming and a legacy array path. */
export class FileReceiver {
  private chunks: (Uint8Array | undefined)[];
  /** Compact presence map survives after chunk payloads are released. */
  private present: Uint8Array;
  private receivedBytes = 0;
  private receivedChunks = 0;
  private consuming = false;
  private consumed = false;
  /** The first chunk not yet present in the contiguous prefix. */
  private nextMissingSeq = 0;

  constructor(readonly manifest: FileManifest) {
    // A presence bitmap avoids scanning payload references after streaming.
    this.chunks = new Array<Uint8Array | undefined>(manifest.totalChunks).fill(undefined);
    this.present = new Uint8Array(manifest.totalChunks);
  }

  get totalBytes(): number {
    return this.manifest.size;
  }

  /** Aggregate bytes already present, including chunks after a hole. */
  get receivedByteCount(): number {
    return this.receivedBytes;
  }

  /** Payload bytes still retained in memory (released progressively by streamTo). */
  get retainedChunkBytes(): number {
    let bytes = 0;
    for (const chunk of this.chunks) bytes += chunk?.byteLength ?? 0;
    return bytes;
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
    if (this.present[seq]) return this.isComplete();
    return this.addChunkBytes(seq, b64decode(dataB64));
  }

  /** Store the received binary view directly; the DataChannel ArrayBuffer remains alive through this view. */
  addChunkBytes(seq: number, bytes: Uint8Array): boolean {
    if (!Number.isInteger(seq) || seq < 0 || seq >= this.manifest.totalChunks) {
      throw new Error('invalid chunk sequence');
    }
    if (this.present[seq]) return this.isComplete();
    if (this.consuming || this.consumed) throw new Error('file finalization already started');
    const expectedBytes = Math.min(
      this.manifest.chunkSize,
      Math.max(0, this.manifest.size - seq * this.manifest.chunkSize),
    );
    if (bytes.byteLength !== expectedBytes) {
      throw new Error(`invalid chunk size (got ${bytes.byteLength}, want ${expectedBytes})`);
    }
    this.chunks[seq] = bytes;
    this.present[seq] = 1;
    this.receivedBytes += bytes.byteLength;
    this.receivedChunks++;
    // Direct delivery is ordered today, but relay replay and a future
    // unordered channel are allowed to fill holes. Keep this incremental so
    // an ACK does not scan the whole file on every chunk.
    while (this.nextMissingSeq < this.chunks.length && this.present[this.nextMissingSeq]) {
      this.nextMissingSeq++;
    }
    return this.isComplete();
  }

  get progress(): number {
    return this.manifest.size === 0 ? 1 : this.receivedBytes / this.manifest.size;
  }

  isComplete(): boolean {
    return this.manifest.size === 0 || this.receivedChunks === this.manifest.totalChunks;
  }

  /**
   * Consume a complete file in bounded 4 MiB batches. The callback is awaited before
   * the next batch is allocated, providing backpressure to disk/IPC sinks.
   * The sink is responsible for validating the manifest hash before commit.
   */
  async streamTo(
    writeBatch: (bytes: Uint8Array, writtenBytes: number) => Promise<void>,
    batchSize = FILE_RECEIVE_BATCH_SIZE,
  ): Promise<void> {
    if (!this.isComplete()) throw new Error('missing chunks');
    if (this.consuming || this.consumed) throw new Error('file finalization already started');
    if (!Number.isInteger(batchSize) || batchSize <= 0 || batchSize > FILE_RECEIVE_BATCH_SIZE) {
      throw new Error(`invalid stream batch size (max ${FILE_RECEIVE_BATCH_SIZE})`);
    }

    this.consuming = true;
    let seq = 0;
    let chunkOffset = 0;
    let written = 0;
    try {
      const batchBuffer = new Uint8Array(batchSize);
      while (written < this.manifest.size) {
        const length = Math.min(batchSize, this.manifest.size - written);
        const batch = batchBuffer.subarray(0, length);
        let batchOffset = 0;
        while (batchOffset < length) {
          const chunk = this.chunks[seq];
          if (!chunk) throw new Error('missing chunks');
          const take = Math.min(chunk.byteLength - chunkOffset, length - batchOffset);
          batch.set(chunk.subarray(chunkOffset, chunkOffset + take), batchOffset);
          batchOffset += take;
          chunkOffset += take;
          if (chunkOffset === chunk.byteLength) {
            this.chunks[seq] = undefined;
            seq++;
            chunkOffset = 0;
          }
        }
        await writeBatch(batch, written + length);
        written += length;
        // Give network/UI callbacks a turn even when the sink resolves quickly.
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
      }
      if (written !== this.manifest.size) throw new Error('streamed size mismatch');
    } finally {
      // On success or failure, release all references; duplicate frames remain
      // recognizable through `present` without retaining their payload.
      this.chunks.fill(undefined);
      this.consuming = false;
      this.consumed = true;
    }
  }

  /** Concatenate and verify for legacy non-desktop consumers such as the CLI. */
  async assembleAsync(): Promise<Uint8Array> {
    if (!this.isComplete()) throw new Error('missing chunks');
    if (this.consuming || this.consumed) throw new Error('file finalization already started');
    this.consuming = true;
    const hash = sha256.create();
    let off = 0;
    let nextYieldAt = 4 * 1024 * 1024;
    try {
      const out = new Uint8Array(this.manifest.size);
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
    } finally {
      this.chunks.fill(undefined);
      this.consuming = false;
      this.consumed = true;
    }
  }

  static encodeChunk(bytes: Uint8Array): string {
    return b64encode(bytes);
  }
}
