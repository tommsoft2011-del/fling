// P2P direct-channel routing test (no WebRTC, no network).
// Two FlingRooms talk through the in-memory mock transport; a fake
// DirectChannel pair simulates the P2P link. Covers:
//   - file-chunk + file-manifest go through the direct channel while open
//     (same ordered channel, so chunks can never beat the manifest)
//   - text / task-done still use the relay
//   - receiver reassembles a file delivered over the direct channel
//   - chunks arriving before the manifest are buffered and replayed
//   - mid-transfer direct failure falls back to the relay transparently
//   - per-chunk mixed routing (some chunks direct, some relay) reassembles

import { FlingRoom, type FlingRoomEvents } from '../src/room.js';
import { P2P_DRAIN_THRESHOLD, type DirectChannel } from '../src/p2p.js';
import { buildManifest, FileReceiver, type FileLike } from '../src/files.js';
import { decodeDirectFileChunkFrame, encodeDirectFileChunkFrame } from '../src/protocol.js';
import type { DataHandler, FlingTransport } from '../src/transport.js';

let failures = 0;
function check(cond: unknown, msg: string): void {
  if (cond) {
    console.log(`  ok - ${msg}`);
  } else {
    failures++;
    console.error(`  FAIL - ${msg}`);
  }
}

class Hub {
  private peers = new Map<string, MockTransport>();
  add(t: MockTransport): void {
    this.peers.set(t.localIdentity, t);
  }
  publish(from: MockTransport, payload: Uint8Array, topic: string): void {
    for (const peer of this.peers.values()) {
      if (peer !== from) peer.deliver(payload, from.localIdentity, topic);
    }
  }
}

class MockTransport implements FlingTransport {
  isConnected = false;
  publishDelayMs = 0;
  dropNextPublish = false;
  droppedPublishes = 0;
  private dataHandler: DataHandler | null = null;
  constructor(
    readonly localIdentity: string,
    private hub: Hub,
  ) {}
  async connect(): Promise<void> {
    this.isConnected = true;
    this.hub.add(this);
  }
  async disconnect(): Promise<void> {
    this.isConnected = false;
  }
  async publish(payload: Uint8Array, topic: string): Promise<void> {
    if (this.dropNextPublish) {
      this.dropNextPublish = false;
      this.droppedPublishes++;
      return;
    }
    if (this.publishDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.publishDelayMs));
    }
    this.hub.publish(this, payload, topic);
  }
  onData(h: DataHandler): void {
    this.dataHandler = h;
  }
  onPeerJoined(): void {}
  onPeerLeft(): void {}
  deliver(payload: Uint8Array, from: string, topic: string): void {
    this.dataHandler?.(payload, from, topic);
  }
}

/** Fake direct channel: deliver() is wired by the test to the peer's room. */
class FakeDirect implements DirectChannel {
  readonly readyState: string = 'open';
  readonly maxMessageSize = 64 * 1024;
  bufferedAmount = 0;
  drainWaitThresholds: number[] = [];
  maxBufferedAmountObserved = 0;
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null = null;
  onclose: (() => void) | null = null;
  sent: Uint8Array[] = [];
  dead = false;
  /** Simulates a black-holed SCTP path: send() accepts bytes but peer sees nothing. */
  blackhole = false;
  /** Drop sends after this many accepted calls while keeping readyState=open. */
  dropAfter: number | undefined;
  private sendCount = 0;
  onSend: ((count: number) => void) | null = null;
  constructor(readonly label: string) {}
  send(data: Uint8Array<ArrayBuffer>): void {
    if (this.dead) throw new Error('channel closed');
    const count = ++this.sendCount;
    this.bufferedAmount += data.byteLength;
    this.maxBufferedAmountObserved = Math.max(this.maxBufferedAmountObserved, this.bufferedAmount);
    if (!this.blackhole && (this.dropAfter === undefined || count <= this.dropAfter)) {
      this.sent.push(data);
    }
    this.onSend?.(count);
  }
  close(): void {
    this.dead = true;
    this.onclose?.();
  }
  waitForDrain(threshold: number): Promise<void> {
    this.drainWaitThresholds.push(threshold);
    if (this.dead) return Promise.reject(new Error('channel closed'));
    // Browsers only signal bufferedamountlow at the fixed threshold. A
    // request for a smaller threshold after crossing it would never resolve.
    if (threshold < P2P_DRAIN_THRESHOLD && this.bufferedAmount <= P2P_DRAIN_THRESHOLD && this.bufferedAmount > threshold) {
      return Promise.reject(new Error('simulated missed bufferedamountlow event'));
    }
    this.bufferedAmount = Math.min(this.bufferedAmount, threshold);
    return Promise.resolve();
  }
  resetStats(): void {
    this.sent = [];
    this.sendCount = 0;
    this.bufferedAmount = 0;
    this.maxBufferedAmountObserved = 0;
    this.drainWaitThresholds = [];
  }
}

const enc = new TextEncoder();
const dec = new TextDecoder();

function fileLike(name: string, size: number): FileLike {
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) bytes[i] = i & 0xff;
  return {
    name,
    size,
    slice: async (s: number, e: number) => bytes.slice(s, e),
  };
}

async function main(): Promise<void> {
  const hub = new Hub();
  const tA = new MockTransport('A', hub);
  const tB = new MockTransport('B', hub);
  const roomA = new FlingRoom(tA, 'devA');
  const roomB = new FlingRoom(tB, 'devB');

  const directA = new FakeDirect('A->B');
  const directB = new FakeDirect('B->A');
  // wire the pair: A's sends arrive at B's room and vice versa
  const wire = () => {
    const fromA = directA.sent;
    directA.sent = [];
    directA.bufferedAmount = 0;
    fromA.forEach((d) => directB.onmessage?.(d as Uint8Array<ArrayBuffer>));
    const fromB = directB.sent;
    directB.sent = [];
    directB.bufferedAmount = 0;
    fromB.forEach((d) => directA.onmessage?.(d as Uint8Array<ArrayBuffer>));
  };

  let relayPublishesA = 0;
  const origPublishA = tA.publish.bind(tA);
  tA.publish = async (p: Uint8Array, topic: string) => {
    relayPublishesA++;
    return origPublishA(p, topic);
  };

  function freshBox(): { cur: { id: string; bytes: Uint8Array } | null } {
    return { cur: null };
  }
  let done = freshBox();
  const eventsB: FlingRoomEvents = {
    onFileManifest: (manifest) => {
      if (manifest.name === 'hello.bin') {
        // Simulate SCTP data queued from before the first raw chunk. This is
        // between bufferedamountlow (256 KiB) and the old per-frame target.
        directA.bufferedAmount = P2P_DRAIN_THRESHOLD - 16 * 1024;
      }
    },
    onFileComplete: (id, _m, bytes) => {
      done.cur = { id, bytes };
    },
  };
  const eventsA: FlingRoomEvents = {};
  const directWindowLogs: string[] = [];
  eventsA.onLog = (_tag, message) => directWindowLogs.push(message);
  await roomA.connect('x', 'tok', 'code123', eventsA);
  await roomB.connect('x', 'tok', 'code123', eventsB);

  // --- 1. direct open: file chunks bypass the relay -------------------------
  roomA.setDirectChannel(directA);
  roomB.setDirectChannel(directB);
  check(roomA.directActive && roomB.directActive, 'directActive true while open');

  const relayBefore = relayPublishesA;
  const drainChecksBefore = directA.drainWaitThresholds.length;
  const file = fileLike('hello.bin', 100_000); // 100KB -> 4 chunks of 32KB
  const p1 = roomA.sendFile(file, undefined, 'file');
  // pump: deliver direct messages as they are produced
  for (let i = 0; i < 50 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await p1;
  wire();
  await new Promise((r) => setTimeout(r, 20));
  check(!!done.cur, 'file completed over direct channel');
  const got1 = done.cur;
  check(
    got1 !== null &&
      got1.bytes.length === 100_000 &&
      got1.bytes[99999] === (99999 & 0xff),
    'reassembled bytes identical over direct channel',
  );
  // manifest + chunks all bypass the relay while direct is open
  const relayDuringDirect = relayPublishesA - relayBefore;
  check(relayDuringDirect === 0, `manifest+chunks bypassed relay (relay publishes: ${relayDuringDirect}, want 0)`);
  const drainTargets = directA.drainWaitThresholds.slice(drainChecksBefore);
  check(
    drainTargets.length > 0 && drainTargets.every((threshold) => threshold === P2P_DRAIN_THRESHOLD),
    'drain wait target matches bufferedamountlow threshold in the 223–256 KiB window',
  );

  // --- 1a. adaptive bounded 1/4/8 MiB windows and send-buffer cap -----------
  done = freshBox();
  directA.resetStats();
  directB.resetStats();
  const adaptiveLogStart = directWindowLogs.length;
  const adaptiveSend = roomA.sendFile(fileLike('adaptive-8m.bin', 8 * 1024 * 1024));
  for (let i = 0; i < 2_000 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 1));
  }
  await adaptiveSend;
  const adaptiveLogs = directWindowLogs.slice(adaptiveLogStart);
  const caps = adaptiveLogs
    .filter((message) => message.startsWith('direct window start '))
    .map((message) => Number(/cap=(\d+)/.exec(message)?.[1] ?? 0));
  check(done.cur?.bytes.length === 8 * 1024 * 1024, '8 MiB direct file is fully received and saved');
  check(caps.includes(1 * 1024 * 1024) && caps.includes(4 * 1024 * 1024) && caps.includes(8 * 1024 * 1024), `adaptive windows grow 1 -> 4 -> 8 MiB (${caps.join(',')})`);
  check(
    directA.maxBufferedAmountObserved <= P2P_DRAIN_THRESHOLD + 32 * 1024 + 26,
    `sender bufferedAmount stays within threshold + one frame (${directA.maxBufferedAmountObserved} bytes)`,
  );

  // --- 1aa. slow ACK is tolerated and the final saved-file ACK still wins ---
  done = freshBox();
  tB.publishDelayMs = 250;
  const delayedStartedAt = Date.now();
  const delayedSend = roomA.sendFile(fileLike('delayed-ack.bin', 100_000));
  for (let i = 0; i < 200 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await delayedSend;
  const delayedElapsedMs = Date.now() - delayedStartedAt;
  tB.publishDelayMs = 0;
  check(done.cur?.bytes.length === 100_000, 'delayed receiver ACK does not trigger a false P2P failure');
  check(delayedElapsedMs >= 400, `sendFile waits for delayed manifest/final ACKs (${delayedElapsedMs} ms)`);

  // --- 1ab. one lost progress ACK is repaired by the later saved-file ACK ---
  done = freshBox();
  let lossScheduled = false;
  const droppedBefore = tB.droppedPublishes;
  const lostAckSend = roomA.sendFile(fileLike('lost-progress-ack.bin', 100_000), () => {
    if (!lossScheduled) {
      tB.dropNextPublish = true;
      lossScheduled = true;
    }
  });
  for (let i = 0; i < 200 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await lostAckSend;
  check(lossScheduled && tB.droppedPublishes === droppedBefore + 1, 'simulated receiver progress ACK is lost');
  check(done.cur?.bytes.length === 100_000, 'later final ACK confirms saved file after a lost progress ACK');

  // --- 1b. raw chunks beating the manifest are buffered, not dropped ------
  done = freshBox();
  const fileE = fileLike('early.bin', 100_000); // 4 chunks
  const earlyManifest = await buildManifest(fileE);
  for (let seq = 0; seq < earlyManifest.totalChunks; seq++) {
    const bytes = await fileE.slice(seq * earlyManifest.chunkSize, Math.min((seq + 1) * earlyManifest.chunkSize, fileE.size));
    directB.onmessage?.(encodeDirectFileChunkFrame(earlyManifest.id, seq, bytes));
  }
  directB.onmessage?.(enc.encode(JSON.stringify({ kind: 'file-manifest', manifest: earlyManifest })) as Uint8Array<ArrayBuffer>);
  await new Promise((r) => setTimeout(r, 20));
  const gotE = done.cur;
  check(
    gotE !== null && gotE.bytes.length === 100_000 && gotE.bytes[99999] === (99999 & 0xff),
    'early chunks buffered and replayed once the manifest arrived',
  );
  const probe = encodeDirectFileChunkFrame(earlyManifest.id, 0, new Uint8Array([1, 2, 3]));
  const decodedProbe = decodeDirectFileChunkFrame(probe);
  check(decodedProbe?.id === earlyManifest.id && decodedProbe.payload.length === 3 && decodedProbe.payload[2] === 3,
    'direct file frame contains a small header plus raw bytes, not JSON/Base64');

  // --- 1c. black-hole: open/send-success is not delivery --------------------
  done = freshBox();
  directA.blackhole = true;
  roomA.setDirectChannel(directA);
  const relayBeforeBlackhole = relayPublishesA;
  const blackholeStart = Date.now();
  await roomA.sendFile(fileLike('blackhole.bin', 70_000), undefined, 'file');
  for (let i = 0; i < 100 && !done.cur; i++) await new Promise((r) => setTimeout(r, 5));
  const blackholeElapsed = Date.now() - blackholeStart;
  check(!!done.cur, 'black-holed direct path recovers through relay');
  check(blackholeElapsed < 10_000, `black-hole fallback completes promptly (${blackholeElapsed}ms)`);
  check(relayPublishesA > relayBeforeBlackhole, 'black-hole retry publishes manifest/chunks through relay');
  check(!roomA.directActive, 'black-hole fallback detaches direct channel');

  // --- 1d. partial silent loss -> replay the same manifest through relay ---
  done = freshBox();
  directA.resetStats();
  directA.blackhole = false;
  directA.dropAfter = 2; // manifest + first chunk arrive, later sends vanish
  directA.onSend = (count) => {
    if (count === 3) roomA.fallbackToRelay('test partial silent loss');
  };
  roomA.setDirectChannel(directA);
  const partial = roomA.sendFile(fileLike('partial.bin', 70_000), undefined, 'file');
  for (let i = 0; i < 200 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await partial;
  for (let i = 0; i < 100 && !done.cur; i++) await new Promise((r) => setTimeout(r, 5));
  check(!!done.cur, 'partial silent loss is repaired by same-file relay replay');
  check(done.cur?.bytes.length === 70_000, 'partial replay bytes intact');
  directA.dropAfter = undefined;
  directA.onSend = null;

  // --- 2. direct dies mid-transfer -> transparent relay fallback ------------
  done = freshBox();
  directA.dead = true; // next send throws -> room must fall back
  directA.blackhole = false;
  roomA.setDirectChannel(directA); // re-attach the (now dead) channel
  const file2 = fileLike('world.bin', 70_000);
  const p2 = roomA.sendFile(file2, undefined, 'file');
  for (let i = 0; i < 50 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await p2;
  // relay delivery to B happens via hub automatically (async decrypt)
  for (let i = 0; i < 50 && !done.cur; i++) {
    await new Promise((r) => setTimeout(r, 5));
  }
  check(!!done.cur, 'file completed after direct failure (relay fallback)');
  const got2 = done.cur;
  check(
    got2 !== null && got2.bytes.length === 70_000,
    'fallback bytes intact',
  );
  check(!roomA.directActive, 'directActive false after failure');

  // --- 3. asynchronous whole-file validation yields to network callbacks ---
  const largeFile = fileLike('large.bin', 8 * 1024 * 1024);
  const largeManifest = await buildManifest(largeFile);
  const receiver = new FileReceiver(largeManifest);
  for (let seq = 0; seq < largeManifest.totalChunks; seq++) {
    const start = seq * largeManifest.chunkSize;
    receiver.addChunkBytes(seq, await largeFile.slice(start, Math.min(start + largeManifest.chunkSize, largeFile.size)));
  }
  let eventLoopTicked = false;
  setTimeout(() => { eventLoopTicked = true; }, 0);
  const assembled = await receiver.assembleAsync();
  await new Promise((r) => setTimeout(r, 0));
  check(eventLoopTicked, 'large-file assembly/hash yields to the event loop');
  check(assembled.length === largeFile.size && assembled[123456] === (123456 & 0xff), 'async hash preserves file bytes');

  await roomA.close();
  await roomB.close();

  if (failures > 0) {
    throw new Error(`${failures} FAILURES`);
  }
  console.log('\nALL PASS');
}

main().catch((e) => {
  console.error('FATAL', e);
  throw e;
});
