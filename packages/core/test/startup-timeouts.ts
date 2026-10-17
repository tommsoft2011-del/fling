import { FlingRoom } from '../src/room.js';
import { P2P_DRAIN_THRESHOLD } from '../src/p2p.js';
import {
  DIRECT_FIRST_FRAME_STARTUP_TIMEOUT_MS,
  FILE_STARTUP_PREPARATION_TIMEOUT_MS,
  peerProgressTimeoutExpired,
  startPeerProgressWatchdogAfterFrameAccepted,
  StageCancelledError,
  StageTimeoutError,
  runWithStageTimeout,
  type StageTimeoutClock,
  type WatchdogIntervalClock,
} from '../src/stage-timeout.js';
import type { DataHandler, FlingTransport } from '../src/transport.js';

let failures = 0;
function check(condition: unknown, message: string): void {
  if (condition) console.log(`ok - ${message}`);
  else {
    failures++;
    console.error(`FAIL - ${message}`);
  }
}

class ManualClock implements StageTimeoutClock {
  now = 0;
  private nextId = 1;
  private readonly timers = new Map<number, { dueAt: number; callback: () => void }>();

  setTimeout(callback: () => void, delayMs: number): unknown {
    const id = this.nextId++;
    this.timers.set(id, { dueAt: this.now + delayMs, callback });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.timers.delete(Number(handle));
  }

  advanceBy(ms: number): void {
    const target = this.now + ms;
    while (true) {
      const next = [...this.timers.entries()]
        .filter(([, timer]) => timer.dueAt <= target)
        .sort((a, b) => a[1].dueAt - b[1].dueAt || a[0] - b[0])[0];
      if (!next) break;
      const [id, timer] = next;
      this.timers.delete(id);
      this.now = timer.dueAt;
      timer.callback();
    }
    this.now = target;
  }

  get pendingTimers(): number {
    return this.timers.size;
  }
}

async function flushMicrotasks(): Promise<void> {
  for (let i = 0; i < 12; i++) await Promise.resolve();
}

class SoloTransport implements FlingTransport {
  isConnected = false;
  readonly localIdentity = 'solo-test';
  private handler: DataHandler | null = null;
  async connect(): Promise<void> { this.isConnected = true; }
  async disconnect(): Promise<void> { this.isConnected = false; }
  async publish(): Promise<void> {}
  onData(handler: DataHandler): void { this.handler = handler; }
  onPeerJoined(): void {}
  onPeerLeft(): void {}
}

async function main(): Promise<void> {
  const prepClock = new ManualClock();
  let prepResolved = false;
  const prep = runWithStageTimeout(
    'manifest_preparation',
    FILE_STARTUP_PREPARATION_TIMEOUT_MS,
    () => new Promise<void>((resolve) => {
      prepClock.setTimeout(() => { prepResolved = true; resolve(); }, 12_394);
    }),
    undefined,
    prepClock,
  );
  await flushMicrotasks();
  prepClock.advanceBy(12_394);
  await flushMicrotasks();
  await prep;
  check(prepResolved && prepClock.now === 12_394 && prepClock.pendingTimers === 0,
    '12,394ms manifest preparation (longer than 8s) completes under the 120s total startup deadline without wall-clock waiting');

  const firstFrameClock = new ManualClock();
  let firstFrameError: unknown;
  const firstFrame = runWithStageTimeout(
    'first_direct_frame_startup',
    DIRECT_FIRST_FRAME_STARTUP_TIMEOUT_MS,
    () => new Promise<never>(() => {}),
    undefined,
    firstFrameClock,
  ).catch((error: unknown) => { firstFrameError = error; });
  await flushMicrotasks();
  firstFrameClock.advanceBy(DIRECT_FIRST_FRAME_STARTUP_TIMEOUT_MS - 1);
  await flushMicrotasks();
  check(firstFrameError === undefined, 'first-frame startup does not fire one millisecond before its 30s boundary');
  firstFrameClock.advanceBy(1);
  await flushMicrotasks();
  await firstFrame;
  check(firstFrameError instanceof StageTimeoutError && firstFrameError.stage === 'first_direct_frame_startup',
    'the first-frame boundary reports the exact startup timer stage');

  const progressClock = new ManualClock();
  const firstProgressAt = progressClock.now;
  progressClock.advanceBy(7_999);
  check(!peerProgressTimeoutExpired(firstProgressAt, progressClock.now, 8_000),
    'peer-progress watchdog remains quiet one millisecond before its 8s timeout');
  progressClock.advanceBy(1);
  check(peerProgressTimeoutExpired(firstProgressAt, progressClock.now, 8_000),
    'peer-progress watchdog expires at exactly 8s after the last progress timestamp');

  let intervalArms = 0;
  let intervalTicks = 0;
  const intervalClock: WatchdogIntervalClock = {
    setInterval: (callback, intervalMs) => {
      intervalArms++;
      check(intervalMs === 1_000, 'watchdog polling interval remains one second');
      return callback;
    },
  };
  const beforeAcceptedFrame = startPeerProgressWatchdogAfterFrameAccepted(
    false, 1_000, () => { intervalTicks++; }, intervalClock,
  );
  check(beforeAcceptedFrame === null && intervalArms === 0,
    'peer-progress watchdog is not armed before DataChannel accepts the first data frame');
  const afterAcceptedFrame = startPeerProgressWatchdogAfterFrameAccepted(
    true, 1_000, () => { intervalTicks++; }, intervalClock,
  );
  if (typeof afterAcceptedFrame === 'function') (afterAcceptedFrame as () => void)();
  check(intervalArms === 1 && intervalTicks === 1,
    'the watchdog timer is armed and ticks only after the first frame is accepted');

  const cancelClock = new ManualClock();
  const abort = new AbortController();
  let cancelError: unknown;
  const cancellable = runWithStageTimeout(
    'file_source_read',
    FILE_STARTUP_PREPARATION_TIMEOUT_MS,
    () => new Promise<never>(() => {}),
    abort.signal,
    cancelClock,
  ).catch((error: unknown) => { cancelError = error; });
  await flushMicrotasks();
  abort.abort();
  await flushMicrotasks();
  await cancellable;
  check(cancelError instanceof StageCancelledError && cancelClock.pendingTimers === 0,
    'cancellation interrupts a pending startup stage and clears its timer immediately');

  const logs: string[] = [];
  const timeoutRoom = new FlingRoom(
    new SoloTransport(),
    'timeout-test',
    P2P_DRAIN_THRESHOLD,
    8 * 1024 * 1024,
    { manifestPreparationMs: 25 },
  );
  await timeoutRoom.connect('mock://local', 'token', 'TIMEOUT1', {
    onLog: (_tag, message) => logs.push(message),
  });
  let manifestError: unknown;
  try {
    await timeoutRoom.sendFile({
      name: 'never-ready.bin',
      size: 1,
      slice: () => new Promise<Uint8Array>(() => {}),
    }, undefined, 'file', undefined, 0, Date.now() + 1_000);
  } catch (error) {
    manifestError = error;
  }
  check(manifestError instanceof StageTimeoutError && manifestError.stage === 'manifest_preparation',
    'FlingRoom enforces the manifest-preparation stage deadline on a stalled source');
  check(logs.some((message) => message.includes('timer stage fired: stage=manifest_preparation')),
    'the runtime log identifies which startup timer fired');
  await timeoutRoom.close();

  if (failures > 0) throw new Error(`${failures} startup timeout test(s) failed`);
  console.log('\nStartup timeout tests passed.');
}

await main();
