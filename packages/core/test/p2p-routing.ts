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
import { buildManifest, FILE_HASH_CHUNK_SIZE, FileReceiver, type FileLike, wrapWebFile } from '../src/files.js';
import { decodeDirectFileChunkFrame, encodeDirectFileChunkFrame, FILE_CHUNK_SIZE } from '../src/protocol.js';
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
  readonly maxMessageSize: number;
  bufferedAmount = 0;
  drainWaitThresholds: number[] = [];
  maxBufferedAmountObserved = 0;
  coalescedFrameCount = 0;
  maxFrameBytesObserved = 0;
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
  constructor(readonly label: string, maxMessageSize = 64 * 1024) {
    this.maxMessageSize = maxMessageSize;
  }
  send(data: Uint8Array<ArrayBuffer>): void {
    if (this.dead) throw new Error('channel closed');
    if (data.byteLength > this.maxMessageSize) throw new Error(`message exceeds maxMessageSize (${data.byteLength} > ${this.maxMessageSize})`);
    const frame = decodeDirectFileChunkFrame(data);
    if ((frame?.chunkCount ?? 1) > 1) this.coalescedFrameCount++;
    this.maxFrameBytesObserved = Math.max(this.maxFrameBytesObserved, data.byteLength);
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
    this.coalescedFrameCount = 0;
    this.maxFrameBytesObserved = 0;
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

const v062SevenFileSizes = [
  122_891_016,
  63_116_252,
  120_586_279,
  96_543_107,
  115_626_483,
  131_358_558,
  102_373_972,
] as const;

async function runSevenFileWindowAB(maxWindowBytes: number): Promise<void> {
  const hub = new Hub();
  const senderTransport = new MockTransport(`sender-${maxWindowBytes}`, hub);
  const receiverTransport = new MockTransport(`receiver-${maxWindowBytes}`, hub);
  const senderLogs: string[] = [];
  const received = new Map<string, { size: number; lastByte: number }>();
  const sender = new FlingRoom(senderTransport, 'Sender', P2P_DRAIN_THRESHOLD, maxWindowBytes);
  const receiver = new FlingRoom(receiverTransport, 'Receiver');
  await sender.connect('x', 'tok', `window-${maxWindowBytes}`, { onLog: (_tag, message) => senderLogs.push(message) });
  await receiver.connect('x', 'tok', `window-${maxWindowBytes}`, {
    onFileComplete: (_id, manifest, bytes) => {
      received.set(manifest.name, { size: bytes.byteLength, lastByte: bytes.at(-1) ?? 0 });
    },
  });

  const senderDirect = new FakeDirect(`sender-${maxWindowBytes}`);
  const receiverDirect = new FakeDirect(`receiver-${maxWindowBytes}`);
  sender.setDirectChannel(senderDirect);
  receiver.setDirectChannel(receiverDirect);
  const wire = () => {
    const outbound = senderDirect.sent;
    senderDirect.sent = [];
    senderDirect.bufferedAmount = 0;
    for (const frame of outbound) receiverDirect.onmessage?.(frame as Uint8Array<ArrayBuffer>);
    const inbound = receiverDirect.sent;
    receiverDirect.sent = [];
    receiverDirect.bufferedAmount = 0;
    for (const frame of inbound) senderDirect.onmessage?.(frame as Uint8Array<ArrayBuffer>);
  };

  const totalBytes = v062SevenFileSizes.reduce((sum, size) => sum + size, 0);
  const startedAt = performance.now();
  for (let index = 0; index < v062SevenFileSizes.length; index++) {
    const size = v062SevenFileSizes[index]!;
    const name = `v0.6.2-size-matched-${index + 1}.mp4`;
    const pending = sender.sendFile(fileLike(name, size), undefined, 'file');
    let settled = false;
    let sendError: unknown;
    void pending.then(() => { settled = true; }, (error) => { sendError = error; settled = true; });
    for (let tick = 0; tick < 180_000 && !settled; tick++) {
      wire();
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    if (!settled) throw new Error(`size-matched ${maxWindowBytes}-byte window timed out on file ${index + 1}`);
    await pending;
    if (sendError) throw sendError;
    const result = received.get(name);
    if (!result || result.size !== size || result.lastByte !== ((size - 1) & 0xff)) {
      throw new Error(`size-matched file ${index + 1} failed final hash/byte-size verification`);
    }
  }
  const elapsedMs = performance.now() - startedAt;
  const sevenFileCount = received.size;
  const sevenFileLogs = [...senderLogs];
  const peakBufferedBytes = senderDirect.maxBufferedAmountObserved;
  const maxDataChannelFrameBytes = senderDirect.maxFrameBytesObserved;
  let recovery: Record<string, number | string | boolean> | undefined;
  if (maxWindowBytes === 16 * 1024 * 1024) {
    senderDirect.resetStats();
    // Manifest + 1 MiB + 4 MiB + 8 MiB reach the 16 MiB stage; accept ten
    // frames there and silently drop the rest to verify cumulative replay.
    senderDirect.dropAfter = 1 + 32 + 128 + 256 + 10;
    const recoveryBytes = 20 * 1024 * 1024;
    const recoveryName = 'window16-partial-loss-recovery.bin';
    const recoveryStart = performance.now();
    const pending = sender.sendFile(fileLike(recoveryName, recoveryBytes), undefined, 'file');
    let settled = false;
    let recoveryError: unknown;
    void pending.then(() => { settled = true; }, (error) => { recoveryError = error; settled = true; });
    for (let tick = 0; tick < 180_000 && !settled; tick++) {
      wire();
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    if (!settled) throw new Error('16 MiB partial-loss recovery timed out');
    try {
      await pending;
    } catch (error) {
      console.error('16 MiB partial-loss sender log tail:\n' + senderLogs.slice(-60).join('\n'));
      throw error;
    }
    if (recoveryError) throw recoveryError;
    const recoveryLog = senderLogs.slice(sevenFileLogs.length);
    const recoveryMetric = [...recoveryLog].reverse().find((message) => message.startsWith('direct metrics ')) ?? '';
    const relayWindowAckTimeoutCount = recoveryLog.filter((message) => message.startsWith('relay chunk ACK timeout ')).length;
    const recoveryCap = Math.max(0, ...recoveryLog
      .filter((message) => message.startsWith('direct window start '))
      .map((message) => Number(/cap=(\d+)/.exec(message)?.[1] ?? 0)));
    const repairBytes = Number(/relayRepairBytes=(\d+)/.exec(recoveryMetric)?.[1] ?? 0);
    const finalRoute = /finalRoute=(direct|relay)/.exec(recoveryMetric)?.[1] ?? 'unknown';
    const recovered = received.get(recoveryName);
    check(recoveryCap === 16 * 1024 * 1024, 'silent-loss recovery actually exercised the bounded 16 MiB application window');
    check(finalRoute === 'relay' && repairBytes > 0 && repairBytes <= recoveryBytes, '16 MiB partial loss is repaired through relay without unbounded replay');
    check(relayWindowAckTimeoutCount === 0, 'bounded relay repair receives the progress ACK heartbeat without window retries');
    check(recovered?.size === recoveryBytes && recovered.lastByte === ((recoveryBytes - 1) & 0xff), '16 MiB-window relay repair passes final hash/size verification');
    recovery = {
      directWindowCapBytes: recoveryCap,
      repairBytes,
      finalRoute,
      relayWindowAckTimeoutCount,
      completedAfterMs: Math.round(performance.now() - recoveryStart),
      finalHashAndSizeVerified: recovered?.size === recoveryBytes,
    };
  }

  const windowStarts = sevenFileLogs.filter((message) => message.startsWith('direct window start '));
  const windowSends = sevenFileLogs.filter((message) => message.startsWith('direct window sent '));
  const windowAcks = sevenFileLogs.filter((message) => message.startsWith('direct window ACK '));
  const fileMetrics = sevenFileLogs.filter((message) => message.startsWith('direct metrics '));
  const directOnlyFileCount = fileMetrics.filter((message) => message.includes('finalRoute=direct') && message.includes('relayRepairBytes=0')).length;
  const maxWindowCap = Math.max(0, ...windowStarts.map((message) => Number(/cap=(\d+)/.exec(message)?.[1] ?? 0)));
  const sendMs = windowSends.reduce((sum, message) => sum + Number(/sendMs=([\d.]+)/.exec(message)?.[1] ?? 0), 0);
  const ackWaitMs = windowAcks.reduce((sum, message) => sum + Number(/ackWaitMs=([\d.]+)/.exec(message)?.[1] ?? 0), 0);
  const waiterUnlockMs = windowAcks.reduce((sum, message) => sum + Number(/waiterUnlockMs=([\d.]+)/.exec(message)?.[1] ?? 0), 0);
  const expectedFrameBound = P2P_DRAIN_THRESHOLD + FILE_CHUNK_SIZE + 26;
  check(sevenFileCount === 7 && totalBytes === 752_495_667, 'A/B input matches the seven v0.6.2 file byte sizes (752,495,667 bytes)');
  check(maxWindowCap <= maxWindowBytes && maxWindowCap > 0, `window cap stays within the ${maxWindowBytes / (1024 * 1024)} MiB experiment (${maxWindowCap} bytes observed)`);
  check(fileMetrics.length === 7 && directOnlyFileCount === 7, 'all seven A/B files complete direct-only with no relay repair');
  check(peakBufferedBytes <= expectedFrameBound, `DataChannel buffered bytes remain within drain threshold plus one 32 KiB frame (${peakBufferedBytes} bytes)`);
  check(senderDirect.coalescedFrameCount === 0, 'window A/B keeps direct frame size fixed so only window cap varies');
  console.log(JSON.stringify({
    kind: 'IN_MEMORY_FlingRoom_AB_NOT_LAN',
    maxWindowBytes,
    matchedFileCount: sevenFileCount,
    totalBytes,
    totalMiB: Number((totalBytes / (1024 * 1024)).toFixed(2)),
    sendFileEndToEndMs: Math.round(elapsedMs),
    completedAckWindowCount: windowAcks.length,
    dataWindowSendMsSum: Math.round(sendMs),
    applicationWindowAckWaitMsSum: Math.round(ackWaitMs),
    waiterUnlockMsSum: Math.round(waiterUnlockMs),
    maxWindowCapBytes: maxWindowCap,
    peakBufferedBytes,
    maxDataChannelFrameBytes,
    payloadMode: 'synthetic-pattern, exact size match only; no disk/fsync or physical network',
    ...(recovery ? { partialLossRecovery: recovery } : {}),
  }));
  await sender.close();
  await receiver.close();
}

async function main(): Promise<void> {
  const hub = new Hub();
  const tA = new MockTransport('A', hub);
  const tB = new MockTransport('B', hub);
  const roomA = new FlingRoom(tA, 'devA');
  const roomB = new FlingRoom(tB, 'devB');

  let directA = new FakeDirect('A->B');
  let directB = new FakeDirect('B->A');
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
  const receiverAckLogs: string[] = [];
  const eventsB: FlingRoomEvents = {
    onLog: (_tag, message) => receiverAckLogs.push(message),
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

  // --- 1a. direct-only coalescing is negotiated against each peer's actual SCTP max ---
  directA = new FakeDirect('A->B 64 KiB coalescing candidate', 96 * 1024);
  directB = new FakeDirect('B->A 64 KiB coalescing candidate', 96 * 1024);
  roomA.setDirectChannel(directA);
  roomB.setDirectChannel(directB);
  done = freshBox();
  const coalesce64 = roomA.sendFile(fileLike('coalesce-64k.bin', 8 * FILE_CHUNK_SIZE), undefined, 'file');
  for (let i = 0; i < 500 && !done.cur; i++) {
    wire();
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  await coalesce64;
  check(done.cur?.bytes.length === 8 * FILE_CHUNK_SIZE, '64 KiB direct coalescing reassembles the unchanged 32 KiB logical chunks');
  check(directA.coalescedFrameCount === 4, `64 KiB payload candidate emits 4 frames for 8 logical chunks (${directA.coalescedFrameCount})`);
  check(directA.maxFrameBytesObserved <= directA.maxMessageSize && directA.maxFrameBytesObserved <= 64 * 1024 + 30, '64 KiB candidate respects the actual 96 KiB DataChannel cap');
  check(directA.maxBufferedAmountObserved <= P2P_DRAIN_THRESHOLD + directA.maxMessageSize, '64 KiB coalescing remains under drain threshold plus one negotiated frame');

  directA = new FakeDirect('A->B 128 KiB coalescing candidate', 256 * 1024);
  directB = new FakeDirect('B->A 128 KiB coalescing candidate', 256 * 1024);
  roomA.setDirectChannel(directA);
  roomB.setDirectChannel(directB);
  done = freshBox();
  const coalesce128 = roomA.sendFile(fileLike('coalesce-128k.bin', 16 * FILE_CHUNK_SIZE), undefined, 'file');
  for (let i = 0; i < 500 && !done.cur; i++) {
    wire();
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  await coalesce128;
  check(done.cur?.bytes.length === 16 * FILE_CHUNK_SIZE, '128 KiB direct coalescing preserves all 32 KiB logical chunks');
  check(directA.coalescedFrameCount === 4, `128 KiB payload candidate emits 4 frames for 16 logical chunks (${directA.coalescedFrameCount})`);
  check(directA.maxFrameBytesObserved <= directA.maxMessageSize && directA.maxFrameBytesObserved <= 128 * 1024 + 30, '128 KiB candidate respects the actual 256 KiB DataChannel cap');
  check(directA.maxBufferedAmountObserved <= P2P_DRAIN_THRESHOLD + directA.maxMessageSize, '128 KiB coalescing remains under drain threshold plus one negotiated frame');

  // Restore the conservative channel limit for existing single-chunk/backpressure assertions.
  directA = new FakeDirect('A->B');
  directB = new FakeDirect('B->A');
  roomA.setDirectChannel(directA);
  roomB.setDirectChannel(directB);

  // A manifest prepared while the previous file is being saved must be reused
  // rather than hashing the next Blob again at the start of sendFile.
  done = freshBox();
  const countedBase = fileLike('prehashed.bin', 100_000);
  const sourceBytes = await countedBase.slice(0, countedBase.size);
  const blobBuffer = new ArrayBuffer(sourceBytes.byteLength);
  new Uint8Array(blobBuffer).set(sourceBytes);
  const immutableBlob = new Blob([blobBuffer]);
  const blobSnapshot = new Uint8Array(await immutableBlob.arrayBuffer());
  const webFile = wrapWebFile(immutableBlob, 'prehashed.bin');
  let sliceReads = 0;
  const countedFile: FileLike = {
    ...webFile,
    slice: async (start, end) => {
      sliceReads++;
      return webFile.slice(start, end);
    },
  };
  const precomputedManifest = await buildManifest(countedFile);
  const manifestSnapshot = JSON.stringify(precomputedManifest);
  const hashReads = sliceReads;
  const prehashedSend = roomA.sendFile(countedFile, undefined, 'file', precomputedManifest);
  for (let i = 0; i < 100 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await prehashedSend;
  check(FILE_HASH_CHUNK_SIZE === 256 * 1024, 'manifest hashing reads at most one 256 KiB chunk at a time');
  check(done.cur?.bytes.length === countedFile.size, 'prehashed next-file manifest still completes and saves');
  check(
    !!done.cur && done.cur.bytes.length === blobSnapshot.length && done.cur.bytes.every((byte, index) => byte === blobSnapshot[index]),
    'immutable Blob contents are unchanged through prehash and send',
  );
  check(JSON.stringify(precomputedManifest) === manifestSnapshot, 'manifest fields remain unchanged during send');
  check(sliceReads - hashReads === precomputedManifest.totalChunks, 'sendFile reuses a prepared manifest without hashing the Blob twice');

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
  const correlatedAckLine = adaptiveLogs.find((message) => message.startsWith('direct window ACK ') && /ackId=[a-f0-9]{16}/.test(message));
  const correlatedAckId = correlatedAckLine?.match(/ackId=([a-f0-9]{16})/)?.[1];
  const correlatedReceiverLogs = correlatedAckId
    ? receiverAckLogs.filter((message) => message.includes(`ackId=${correlatedAckId}`))
    : [];
  check(!!correlatedAckId && correlatedReceiverLogs.some((message) => message.includes('publish start')) && correlatedReceiverLogs.some((message) => message.includes('publish end')), 'receiver publish and sender window ACK share a trace ID');
  check(!!correlatedAckLine && correlatedAckLine.includes('callbackToHandlerMs=') && correlatedAckLine.includes('waiterUnlockMs='), 'sender logs callback handling and waiter-unlock durations');
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
  const coalescedProbePayload = new Uint8Array(2 * FILE_CHUNK_SIZE);
  coalescedProbePayload[FILE_CHUNK_SIZE] = 0x5a;
  const coalescedProbe = decodeDirectFileChunkFrame(
    encodeDirectFileChunkFrame(earlyManifest.id, 4, coalescedProbePayload, FILE_CHUNK_SIZE, 2),
  );
  check(
    coalescedProbe?.chunkCount === 2 && coalescedProbe.chunkSize === FILE_CHUNK_SIZE &&
      coalescedProbe.seq === 4 && coalescedProbe.payload[FILE_CHUNK_SIZE] === 0x5a,
    'direct batch frame preserves contiguous logical sequence and payload metadata',
  );

  // --- 1c. black-hole: open/send-success is not delivery --------------------
  done = freshBox();
  directA = new FakeDirect('A->B 128 KiB black-hole candidate', 256 * 1024);
  directB = new FakeDirect('B->A 128 KiB black-hole candidate', 256 * 1024);
  roomA.setDirectChannel(directA);
  roomB.setDirectChannel(directB);
  // The direct manifest must arrive so the negotiated coalesced frame is the
  // only silently lost part of this scenario.
  directA.dropAfter = 1;
  const relayBeforeBlackhole = relayPublishesA;
  const blackholeStart = Date.now();
  const coalescedBlackholeSend = roomA.sendFile(fileLike('blackhole.bin', 70_000), undefined, 'file');
  for (let i = 0; i < 2_000 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await coalescedBlackholeSend;
  const blackholeElapsed = Date.now() - blackholeStart;
  check(!!done.cur, 'black-holed direct path recovers through relay');
  check(blackholeElapsed < 10_000, `black-hole fallback completes promptly (${blackholeElapsed}ms)`);
  check(relayPublishesA > relayBeforeBlackhole, 'black-hole retry publishes manifest/chunks through relay');
  check(directA.coalescedFrameCount > 0, 'black-hole scenario exercised a coalesced direct frame before relay repair');
  check(!roomA.directActive, 'black-hole fallback detaches direct channel');

  // --- 1d. partial silent loss -> replay the same manifest through relay ---
  done = freshBox();
  directA = new FakeDirect('A->B');
  directB = new FakeDirect('B->A');
  roomA.setDirectChannel(directA);
  roomB.setDirectChannel(directB);
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

  // --- 2a. duplicate/out-of-order cumulative ACKs never move the prefix back ---
  const ackProbeId = 'abcdef0123456789abcdef01';
  const cumulativeAckLogsStart = directWindowLogs.length;
  const sendProbeAck = async (nextSeq: number, receivedBytes: number, ackId?: string) => {
    await roomB.sendSignal({ kind: 'file-chunk-ack', id: ackProbeId, nextSeq, receivedBytes, ...(ackId ? { ackId } : {}) });
    await roomA.flush();
  };
  const ackProgress = (roomA as unknown as {
    chunkAckProgress: Map<string, { nextSeq: number; receivedBytes: number; ackId?: string }>;
  }).chunkAckProgress;
  await sendProbeAck(8, 800, '0000000000000001');
  await sendProbeAck(4, 900, '0000000000000002');
  const afterOlderAck = ackProgress.get(ackProbeId);
  check(afterOlderAck?.nextSeq === 8 && afterOlderAck.receivedBytes === 900 && afterOlderAck.ackId === '0000000000000001', 'older cumulative ACK may add buffered-byte evidence but cannot regress nextSeq or replace its trace ID');
  await sendProbeAck(8, 800, '0000000000000003');
  const afterDuplicateAck = ackProgress.get(ackProbeId);
  check(afterDuplicateAck?.nextSeq === 8 && afterDuplicateAck.receivedBytes === 900, 'duplicate ACK does not overwrite newer cumulative progress');
  await sendProbeAck(12, 1_200, '0000000000000004');
  const afterNewerAck = ackProgress.get(ackProbeId);
  check(afterNewerAck?.nextSeq === 12 && afterNewerAck.receivedBytes === 1_200, 'later cumulative ACK advances the receive prefix normally');
  check(directWindowLogs.slice(cumulativeAckLogsStart).some((message) => message.includes('chunk ACK ignored') && message.includes('ackId=0000000000000003')), 'duplicate/regressing ACK is explicitly diagnosed');
  await sendProbeAck(13, 1_300);
  check(ackProgress.get(ackProbeId)?.nextSeq === 13 && directWindowLogs.slice(cumulativeAckLogsStart).some((message) => message.includes('ackId=legacy-abcdef01-13-progress')), 'ACK from an older peer without a trace ID remains accepted');

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

  // --- 4. event-driven ACK waiters: wake, timeout, race and cancellation -----
  const ackWaitRoom = roomA as unknown as {
    waitForChunkAck: (id: string, targetSeq: number, timeoutMs: number) => Promise<{
      ackId: string; nextSeq: number; waiterUnlockMs: number;
    } | null>;
    waitForFileComplete: (manifest: Awaited<ReturnType<typeof buildManifest>>, timeoutMs: number) => Promise<boolean>;
  };
  const eventWaitId = 'ffffffffffffffffffffffff';
  const eventWaitStartedAt = performance.now();
  const eventWait = ackWaitRoom.waitForChunkAck(eventWaitId, 2, 1_000);
  await new Promise((resolve) => setTimeout(resolve, 5));
  await roomB.sendSignal({ kind: 'file-chunk-ack', id: eventWaitId, nextSeq: 2, receivedBytes: 64 * 1024, ackId: 'event-wake-0001' });
  await roomA.flush();
  const eventWaitResult = await eventWait;
  check(eventWaitResult?.nextSeq === 2, 'an ACK arriving after waiter registration wakes the matching waiter');
  check(
    !!eventWaitResult && eventWaitResult.waiterUnlockMs < 40 && performance.now() - eventWaitStartedAt < 250,
    `ACK waiter resumes from the event without a 50ms poll (${eventWaitResult?.waiterUnlockMs.toFixed(2) ?? 'null'} ms unlock)`,
  );

  const racedId = 'eeeeeeeeeeeeeeeeeeeeeeee';
  await roomB.sendSignal({ kind: 'file-chunk-ack', id: racedId, nextSeq: 1, receivedBytes: 32 * 1024, ackId: 'race-ready-0001' });
  await roomA.flush();
  const raceStartedAt = performance.now();
  const racedResult = await ackWaitRoom.waitForChunkAck(racedId, 1, 500);
  check(!!racedResult && performance.now() - raceStartedAt < 50, 'an ACK received just before subscription is observed immediately');

  const timeoutStartedAt = performance.now();
  const timedOut = await ackWaitRoom.waitForChunkAck('dddddddddddddddddddddddd', 1, 35);
  const timeoutElapsedMs = performance.now() - timeoutStartedAt;
  check(timedOut === null && timeoutElapsedMs >= 25 && timeoutElapsedMs < 250, `ACK silence timeout fires once without polling (${timeoutElapsedMs.toFixed(1)} ms)`);

  const duplicateId = 'cccccccccccccccccccccccc';
  await roomB.sendSignal({ kind: 'file-chunk-ack', id: duplicateId, nextSeq: 1, receivedBytes: 32 * 1024, ackId: 'dup-seed-000001' });
  await roomA.flush();
  const duplicateTimeoutStartedAt = performance.now();
  const duplicateWait = ackWaitRoom.waitForChunkAck(duplicateId, 3, 45);
  for (let i = 0; i < 3; i++) {
    await new Promise((resolve) => setTimeout(resolve, 8));
    await roomB.sendSignal({ kind: 'file-chunk-ack', id: duplicateId, nextSeq: 0, receivedBytes: 32 * 1024, ackId: `dup-old-00000${i}` });
    await roomA.flush();
  }
  const duplicateTimeoutResult = await duplicateWait;
  const duplicateElapsedMs = performance.now() - duplicateTimeoutStartedAt;
  check(duplicateTimeoutResult === null && duplicateElapsedMs < 250, `duplicate/regressing ACKs do not extend the silence deadline (${duplicateElapsedMs.toFixed(1)} ms)`);

  const cancelManifest = await buildManifest(fileLike('cancel-wait.bin', 1));
  const cancelChunkWait = ackWaitRoom.waitForChunkAck(cancelManifest.id, 1, 5_000);
  const cancelFinalizeWait = ackWaitRoom.waitForFileComplete(cancelManifest, 5_000);
  await new Promise((resolve) => setTimeout(resolve, 5));
  const cancelStartedAt = performance.now();

  await roomA.close();
  const [cancelledChunk, cancelledFinalize] = await Promise.all([cancelChunkWait, cancelFinalizeWait]);
  check(cancelledChunk === null && cancelledFinalize === false, 'room close cancels both chunk and durable-finalization ACK waiters');
  check(performance.now() - cancelStartedAt < 500, 'cancelled waiters settle promptly instead of lingering on timers');
  await roomB.close();

  if (failures > 0) {
    throw new Error(`${failures} FAILURES`);
  }
  await runSevenFileWindowAB(8 * 1024 * 1024);
  await runSevenFileWindowAB(16 * 1024 * 1024);
  console.log('\nALL PASS');
}

main().catch((e) => {
  console.error('FATAL', e);
  throw e;
});
