import { sha256 } from '@noble/hashes/sha2';
import { bytesToHex } from '@noble/hashes/utils';
import { FILE_CHUNK_SIZE, type FileManifest } from '../src/protocol.js';
import { FileReceiver, FILE_RECEIVE_BATCH_SIZE, hashFile, type FileLike } from '../src/files.js';

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`FAIL: ${message}`);
}

function patternFile(size: number): FileLike {
  return {
    name: 'large-stream-test.bin',
    size,
    slice: async (start, end) => {
      const bytes = new Uint8Array(end - start);
      for (let i = 0; i < bytes.length; i++) bytes[i] = ((start + i) % 251) & 0xff;
      return bytes;
    },
  };
}

async function testLargeBoundedBackpressureStream(): Promise<void> {
  const size = 122_891_016;
  const source = patternFile(size);
  const manifest: FileManifest = {
    id: 'stream-large-test',
    name: source.name,
    size,
    sha256: await hashFile(source),
    chunkSize: FILE_CHUNK_SIZE,
    totalChunks: Math.max(1, Math.ceil(size / FILE_CHUNK_SIZE)),
    kind: 'file',
  };
  const receiver = new FileReceiver(manifest);
  for (let seq = 0; seq < manifest.totalChunks; seq++) {
    const start = seq * FILE_CHUNK_SIZE;
    const end = Math.min(start + FILE_CHUNK_SIZE, size);
    const chunk = await source.slice(start, end);
    const complete = receiver.addChunkBytes(seq, chunk);
    if (seq === 0 || seq % 257 === 0) {
      const before = receiver.receivedByteCount;
      receiver.addChunkBytes(seq, chunk);
      check(receiver.receivedByteCount === before, 'duplicate chunks do not increase received byte count');
    }
    check(complete === (seq === manifest.totalChunks - 1), 'completion becomes true only after all chunks arrive');
  }
  check(receiver.isComplete(), 'all 123 MB of chunks are present');
  check(Number(receiver.retainedChunkBytes) === size, 'receive payload memory matches the declared file bytes before streaming');

  const receivedHash = sha256.create();
  let batches = 0;
  let maxBatchBytes = 0;
  let batchBuffer: ArrayBufferLike | undefined;
  let reusesSingleBatchBuffer = true;
  let activeWrites = 0;
  let maxConcurrentWrites = 0;
  let lastWritten = 0;
  let uiTicks = 0;
  const uiTimer = setInterval(() => { uiTicks++; }, 10);
  const started = Date.now();
  try {
    await receiver.streamTo(async (batch, writtenBytes) => {
      activeWrites++;
      maxConcurrentWrites = Math.max(maxConcurrentWrites, activeWrites);
      batches++;
      maxBatchBytes = Math.max(maxBatchBytes, batch.byteLength);
      if (batchBuffer === undefined) batchBuffer = batch.buffer;
      else if (batch.buffer !== batchBuffer) reusesSingleBatchBuffer = false;
      check(batch.byteLength <= FILE_RECEIVE_BATCH_SIZE, 'IPC batch stays within the 4 MiB bound');
      check(writtenBytes > lastWritten && writtenBytes <= size, 'stream progress is monotonic');
      // Emulate a finite-speed Rust/IPC sink: the next batch must wait for this one.
      await new Promise<void>((resolve) => setTimeout(resolve, 1));
      receivedHash.update(batch);
      lastWritten = writtenBytes;
      activeWrites--;
    });
  } finally {
    clearInterval(uiTimer);
  }
  const elapsed = Date.now() - started;
  check(lastWritten === size, 'stream writes the exact manifest size');
  check(bytesToHex(receivedHash.digest()) === manifest.sha256, 'streamed bytes match the manifest SHA-256');
  check(maxBatchBytes <= FILE_RECEIVE_BATCH_SIZE, 'peak transient batch is bounded');
  check(reusesSingleBatchBuffer, 'one reusable batch buffer bounds transient allocations');
  check(maxConcurrentWrites === 1, 'awaited sink provides strict backpressure');
  check(batches === Math.ceil(size / FILE_RECEIVE_BATCH_SIZE), 'sink receives the expected number of batches');
  check(uiTicks > 0, 'event loop remains responsive while streaming');
  check(elapsed > 0, 'stream path yields between batches');
  check(receiver.retainedChunkBytes === 0, 'stream completion releases all received payload buffers');
  check(receiver.receivedByteCount === size && receiver.isComplete(), 'consuming chunks preserves receive accounting');
  // A late replay is still deduplicated although its original payload reference was released.
  receiver.addChunkBytes(0, new Uint8Array(FILE_CHUNK_SIZE));
  check(receiver.receivedByteCount === size, 'late duplicate after streaming remains deduplicated');
  console.log(`PASS 123 MB bounded stream: ${batches} awaited batches, max=${maxBatchBytes} bytes, one reusable buffer, payload bound <=${size + maxBatchBytes} bytes, ${elapsed} ms, UI ticks=${uiTicks}`);
}

async function testInterruptedStreamReleasesReceiver(): Promise<void> {
  const source = patternFile(FILE_CHUNK_SIZE * 2);
  const manifest: FileManifest = {
    id: 'stream-interrupted-test',
    name: source.name,
    size: source.size,
    sha256: await hashFile(source),
    chunkSize: FILE_CHUNK_SIZE,
    totalChunks: 2,
    kind: 'file',
  };
  const receiver = new FileReceiver(manifest);
  receiver.addChunkBytes(0, await source.slice(0, FILE_CHUNK_SIZE));
  receiver.addChunkBytes(1, await source.slice(FILE_CHUNK_SIZE, source.size));
  let calls = 0;
  let failed = false;
  try {
    await receiver.streamTo(async () => {
      calls++;
      throw new Error('simulated interrupted sink');
    });
  } catch (error) {
    failed = error instanceof Error && error.message === 'simulated interrupted sink';
  }
  check(failed && calls === 1, 'sink interruption propagates immediately');
  check(receiver.retainedChunkBytes === 0, 'interrupted stream releases all retained payload buffers');
  check(receiver.receivedByteCount === source.size && receiver.isComplete(), 'interrupted stream retains correct receive accounting');
  let retryRejected = false;
  try {
    await receiver.streamTo(async () => {});
  } catch {
    retryRejected = true;
  }
  check(retryRejected, 'a consumed/failed receiver cannot silently restart with released chunks');
  console.log('PASS interrupted stream releases chunk references and reports failure');
}

await testLargeBoundedBackpressureStream();
await testInterruptedStreamReleasesReceiver();
console.log('ALL FILE STREAM TESTS PASSED');
