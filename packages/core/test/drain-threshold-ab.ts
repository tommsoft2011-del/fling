// Isolated A/B of the real FlingRoom send/backpressure path over a shaped
// in-memory DirectChannel. This measures the mock channel only, not LAN/WebRTC.
import { FlingRoom, type FlingRoomEvents } from '../src/room.js';
import { P2P_DRAIN_THRESHOLD, type DirectChannel } from '../src/p2p.js';
import { type FileLike } from '../src/files.js';
import type { DataHandler, FlingTransport } from '../src/transport.js';

const RATE_BYTES_PER_SECOND = 7_594_000; // calibrate mock drain rate to the supplied run's weighted receive rate
const ACK_DELAY_MS = 8;
const FRAME_BYTES = 32 * 1024 + 26;
const CANDIDATES = [256 * 1024, 512 * 1024] as const;
const NORMAL_REPETITIONS = 3;

type Scenario = 'normal' | 'lost-progress-ack' | 'blackhole-chunks';

class Hub {
  private peers = new Map<string, MockTransport>();
  add(peer: MockTransport): void { this.peers.set(peer.localIdentity, peer); }
  publish(from: MockTransport, payload: Uint8Array, topic: string): void {
    for (const peer of this.peers.values()) if (peer !== from) peer.deliver(payload, from.localIdentity, topic);
  }
}

class MockTransport implements FlingTransport {
  isConnected = false;
  publishDelayMs = 0;
  dropNextPublish = false;
  droppedPublishes = 0;
  private dataHandler: DataHandler | null = null;
  constructor(readonly localIdentity: string, private hub: Hub) {}
  async connect(): Promise<void> { this.isConnected = true; this.hub.add(this); }
  async disconnect(): Promise<void> { this.isConnected = false; }
  async publish(payload: Uint8Array, topic: string): Promise<void> {
    if (this.dropNextPublish) {
      this.dropNextPublish = false;
      this.droppedPublishes++;
      return;
    }
    if (this.publishDelayMs > 0) await new Promise((resolve) => setTimeout(resolve, this.publishDelayMs));
    this.hub.publish(this, payload, topic);
  }
  onData(handler: DataHandler): void { this.dataHandler = handler; }
  onPeerJoined(): void {}
  onPeerLeft(): void {}
  deliver(payload: Uint8Array, from: string, topic: string): void { this.dataHandler?.(payload, from, topic); }
}

interface DrainWaiter { threshold: number; startedAt: number; resolve: () => void; reject: (error: Error) => void; }
class ShapedDirect implements DirectChannel {
  readonly maxMessageSize = 64 * 1024;
  readyState = 'open';
  bufferedAmount = 0;
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null = null;
  onclose: (() => void) | null = null;
  peer: ShapedDirect | null = null;
  readonly waitThresholds: number[] = [];
  drainWaitMs = 0;
  actualDrainWaits = 0;
  maxBufferedAmountObserved = 0;
  private queue: Uint8Array<ArrayBuffer>[] = [];
  private waiters: DrainWaiter[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  private drainCredit = 0;
  private lastDrainAt = performance.now();

  constructor(readonly label: string, private readonly dropBinaryChunks = false) {}

  send(data: Uint8Array<ArrayBuffer>): void {
    if (this.readyState !== 'open') throw new Error('shaped channel is closed');
    const frame = new Uint8Array(data) as Uint8Array<ArrayBuffer>;
    this.queue.push(frame);
    this.bufferedAmount += frame.byteLength;
    this.maxBufferedAmountObserved = Math.max(this.maxBufferedAmountObserved, this.bufferedAmount);
    this.scheduleDrain();
  }

  waitForDrain(threshold: number): Promise<void> {
    this.waitThresholds.push(threshold);
    if (this.readyState !== 'open') return Promise.reject(new Error('shaped channel is closed'));
    if (this.bufferedAmount <= threshold) return Promise.resolve();
    this.actualDrainWaits++;
    return new Promise((resolve, reject) => {
      this.waiters.push({ threshold, startedAt: performance.now(), resolve, reject });
      this.scheduleDrain();
    });
  }

  close(): void {
    this.readyState = 'closed';
    this.onclose?.();
    for (const waiter of this.waiters.splice(0)) waiter.reject(new Error('shaped channel closed'));
  }

  private scheduleDrain(): void {
    if (this.timer || this.readyState !== 'open') return;
    this.timer = setTimeout(() => {
      this.timer = null;
      const now = performance.now();
      const elapsedMs = Math.max(0, now - this.lastDrainAt);
      this.lastDrainAt = now;
      if (this.queue.length === 0) {
        this.drainCredit = 0;
        this.resolveWaiters();
        return;
      }
      this.drainCredit += RATE_BYTES_PER_SECOND * elapsedMs / 1000;
      while (this.queue.length > 0 && this.queue[0]!.byteLength <= this.drainCredit) {
        const frame = this.queue.shift()!;
        this.drainCredit -= frame.byteLength;
        this.bufferedAmount -= frame.byteLength;
        const isBinaryChunk = frame.byteLength >= 4 && frame[0] === 0x46 && frame[1] === 0x4c && frame[2] === 0x4e && frame[3] === 0x47;
        if (!(this.dropBinaryChunks && isBinaryChunk)) this.peer?.onmessage?.(frame);
      }
      this.resolveWaiters();
      if (this.queue.length > 0) this.scheduleDrain();
      else {
        this.drainCredit = 0;
        this.lastDrainAt = performance.now();
      }
    }, 1);
  }

  private resolveWaiters(): void {
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      const waiter = this.waiters[i]!;
      if (this.bufferedAmount <= waiter.threshold) {
        this.waiters.splice(i, 1);
        this.drainWaitMs += performance.now() - waiter.startedAt;
        waiter.resolve();
      }
    }
  }
}

function fileLike(name: string, size: number): FileLike {
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) bytes[i] = i & 0xff;
  return { name, size, slice: async (start, end) => bytes.slice(start, end) };
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)]!;
}

interface Sample {
  thresholdKiB: number;
  scenario: Scenario;
  repeat: number;
  bytes: number;
  elapsedMs: number;
  throughputMBps: number;
  drainWaits: number;
  drainWaitMs: number;
  bufferedAmountPeak: number;
  sendPathHighWater: number;
  ackP50Ms: number;
  ackP90Ms: number;
  ackMaxMs: number;
  repairBytes: number;
  repairs: number;
  retries: number;
  fallbacks: number;
  droppedAcks: number;
  errors: number;
  finalRoute: string;
}

async function runSample(thresholdBytes: number, scenario: Scenario, repeat = 1): Promise<Sample> {
  const hub = new Hub();
  const senderTransport = new MockTransport(`sender-${thresholdBytes}-${scenario}`, hub);
  const receiverTransport = new MockTransport(`receiver-${thresholdBytes}-${scenario}`, hub);
  receiverTransport.publishDelayMs = ACK_DELAY_MS;
  const sender = new FlingRoom(senderTransport, 'Sender', thresholdBytes);
  const receiver = new FlingRoom(receiverTransport, 'Receiver');
  const logs: string[] = [];
  let receivedBytes = 0;
  let lossArmed = false;
  const senderEvents: FlingRoomEvents = { onLog: (_tag, message) => logs.push(message) };
  const receiverEvents: FlingRoomEvents = {
    onFileComplete: (_id, _manifest, bytes) => { receivedBytes = bytes.byteLength; },
  };
  await sender.connect('mock://local', 'sender-token', 'DRAINAB1', senderEvents);
  await receiver.connect('mock://local', 'receiver-token', 'DRAINAB1', receiverEvents);

  const senderDirect = new ShapedDirect(`sender-${thresholdBytes}`, scenario === 'blackhole-chunks');
  const receiverDirect = new ShapedDirect(`receiver-${thresholdBytes}`, false);
  senderDirect.peer = receiverDirect;
  receiverDirect.peer = senderDirect;
  sender.setDirectChannel(senderDirect);
  receiver.setDirectChannel(receiverDirect);

  const size = scenario === 'normal' ? 8 * 1024 * 1024 : scenario === 'lost-progress-ack' ? 100_000 : 70_000;
  const file = fileLike(`${scenario}-${thresholdBytes}.bin`, size);
  const startedAt = performance.now();
  let error: unknown;
  try {
    await sender.sendFile(file, () => {
      if (scenario === 'lost-progress-ack' && !lossArmed) {
        receiverTransport.dropNextPublish = true;
        lossArmed = true;
      }
    });
  } catch (caught) {
    error = caught;
  }
  const elapsedMs = performance.now() - startedAt;
  await receiver.flush();

  const metric = [...logs].reverse().find((message) => message.startsWith('direct metrics ')) ?? '';
  const number = (key: string): number => Number(new RegExp(`${key}=([\\d.]+)`).exec(metric)?.[1] ?? 0);
  const ackWaits = logs.flatMap((message) => [...message.matchAll(/ackWaitMs=(\d+)/g)].map((match) => Number(match[1])));
  const repairs = logs.filter((message) => message.includes('direct chunk ACK timeout') || message.includes('direct send failure repair')).length;
  const retries = logs.filter((message) => message.includes('relay chunk ACK timeout')).length;
  const fallbacks = logs.filter((message) => message.includes('fallback')).length;
  const finalRoute = /finalRoute=(direct|relay)/.exec(metric)?.[1] ?? 'unknown';
  const sample: Sample = {
    thresholdKiB: thresholdBytes / 1024,
    scenario,
    repeat,
    bytes: size,
    elapsedMs: Math.round(elapsedMs),
    throughputMBps: Number((size / (elapsedMs / 1000) / 1_000_000).toFixed(3)),
    drainWaits: number('drainWaits'),
    drainWaitMs: number('drainWaitMs'),
    bufferedAmountPeak: senderDirect.maxBufferedAmountObserved,
    sendPathHighWater: number('bufferedHighWater'),
    ackP50Ms: percentile(ackWaits, 0.5),
    ackP90Ms: percentile(ackWaits, 0.9),
    ackMaxMs: Math.max(0, ...ackWaits),
    repairBytes: number('relayRepairBytes'),
    repairs,
    retries,
    fallbacks,
    droppedAcks: receiverTransport.droppedPublishes,
    errors: error ? 1 : 0,
    finalRoute,
  };

  const expectedBytes = scenario === 'normal' ? size : size;
  if (sample.errors !== 0) throw new Error(`${scenario}/${sample.thresholdKiB} KiB failed: ${String(error)}`);
  if (sample.bufferedAmountPeak > thresholdBytes + FRAME_BYTES) {
    throw new Error(`bufferedAmount bound exceeded at ${sample.thresholdKiB} KiB: ${sample.bufferedAmountPeak}`);
  }
  if (scenario === 'blackhole-chunks') {
    if (receivedBytes !== expectedBytes || sample.finalRoute !== 'relay' || sample.repairBytes !== size || sample.repairs < 1) {
      throw new Error(`black-hole recovery failed at ${sample.thresholdKiB} KiB: ${JSON.stringify(sample)}, received=${receivedBytes}`);
    }
  } else {
    if (receivedBytes !== expectedBytes || sample.finalRoute !== 'direct' || sample.repairBytes !== 0 || sample.repairs !== 0 || sample.retries !== 0) {
      throw new Error(`${scenario} stability regression at ${sample.thresholdKiB} KiB: ${JSON.stringify(sample)}, received=${receivedBytes}`);
    }
  }
  if (scenario === 'lost-progress-ack' && sample.droppedAcks !== 1) {
    throw new Error(`expected exactly one lost progress ACK at ${sample.thresholdKiB} KiB, got ${sample.droppedAcks}`);
  }
  if (scenario !== 'lost-progress-ack' && sample.droppedAcks !== 0) {
    throw new Error(`unexpected lost ACK at ${sample.thresholdKiB} KiB`);
  }
  await sender.close();
  await receiver.close();
  return sample;
}

async function main(): Promise<void> {
  if (P2P_DRAIN_THRESHOLD !== 256 * 1024) throw new Error('production drain threshold changed unexpectedly');
  const samples: Sample[] = [];
  for (const threshold of CANDIDATES) {
    for (const scenario of ['normal', 'lost-progress-ack', 'blackhole-chunks'] as const) {
      const repetitions = scenario === 'normal' ? NORMAL_REPETITIONS : 1;
      for (let repeat = 1; repeat <= repetitions; repeat++) {
        samples.push(await runSample(threshold, scenario, repeat));
      }
    }
  }
  for (const sample of samples) console.log(JSON.stringify(sample));
  const median = (values: number[]): number => {
    const sorted = [...values].sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length / 2)] ?? 0;
  };
  for (const thresholdKiB of [256, 512]) {
    const normalRuns = samples.filter((sample) => sample.thresholdKiB === thresholdKiB && sample.scenario === 'normal');
    console.log(JSON.stringify({
      thresholdKiB,
      normalRepeatCount: normalRuns.length,
      throughputMedianMBps: median(normalRuns.map((sample) => sample.throughputMBps)),
      drainWaitsMedian: median(normalRuns.map((sample) => sample.drainWaits)),
      drainWaitMsMedian: median(normalRuns.map((sample) => sample.drainWaitMs)),
      bufferedAmountPeakMax: Math.max(...normalRuns.map((sample) => sample.bufferedAmountPeak)),
      ackP50MedianMs: median(normalRuns.map((sample) => sample.ackP50Ms)),
      ackP90MedianMs: median(normalRuns.map((sample) => sample.ackP90Ms)),
    }));
  }
  const baseline = samples.filter((sample) => sample.thresholdKiB === 256);
  const candidate = samples.filter((sample) => sample.thresholdKiB === 512);
  const normalBase = median(baseline.filter((sample) => sample.scenario === 'normal').map((sample) => sample.throughputMBps));
  const normalCandidate = median(candidate.filter((sample) => sample.scenario === 'normal').map((sample) => sample.throughputMBps));
  const improvement = (normalCandidate / normalBase - 1) * 100;
  console.log(`A/B normal throughput delta 512 vs 256 KiB: ${improvement.toFixed(2)}%`);
  console.log('Isolated shaped-channel result only; no physical LAN/WebRTC claim.');
  console.log('A/B PASS: identical delivery/recovery outcomes, production default remains 256 KiB pending real-device evidence.');
}

main().catch((error) => {
  console.error('A/B FAIL', error);
  throw error;
});
