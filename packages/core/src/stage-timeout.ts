/** Named finite deadlines for file-transfer startup and transport stages. */
export const FILE_STARTUP_PREPARATION_TIMEOUT_MS = 120_000;
export const FILE_SOURCE_READ_TIMEOUT_MS = 120_000;
export const MANIFEST_PREPARATION_TIMEOUT_MS = 120_000;
export const FILE_CHUNK_PREPARATION_TIMEOUT_MS = 120_000;
export const MANIFEST_PUBLISH_TIMEOUT_MS = 8_000;
export const DIRECT_FIRST_FRAME_STARTUP_TIMEOUT_MS = 30_000;
export const DIRECT_PEER_PROGRESS_TIMEOUT_MS = 8_000;
export const RELAY_PUBLISH_TIMEOUT_MS = 15_000;

export class StageTimeoutError extends Error {
  readonly name = 'StageTimeoutError';

  constructor(readonly stage: string, readonly timeoutMs: number) {
    super(`transfer timer stage fired: ${stage} after ${timeoutMs}ms`);
  }
}

export class StageCancelledError extends Error {
  readonly name = 'AbortError';

  constructor(readonly stage: string) {
    super(`transfer stage cancelled: ${stage}`);
  }
}

export interface StageTimeoutClock {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

const systemClock: StageTimeoutClock = {
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export interface WatchdogIntervalClock {
  setInterval(callback: () => void, intervalMs: number): unknown;
}

const systemIntervalClock: WatchdogIntervalClock = {
  setInterval: (callback, intervalMs) => setInterval(callback, intervalMs),
};

/** Do not arm peer-progress polling until a direct data frame is locally accepted. */
export function startPeerProgressWatchdogAfterFrameAccepted(
  frameAccepted: boolean,
  intervalMs: number,
  onTick: () => void,
  clock: WatchdogIntervalClock = systemIntervalClock,
): unknown | null {
  if (!frameAccepted) return null;
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
    throw new RangeError(`invalid watchdog interval: ${intervalMs}`);
  }
  return clock.setInterval(onTick, intervalMs);
}

export function peerProgressTimeoutExpired(lastProgressAt: number, now: number, timeoutMs: number): boolean {
  return now - lastProgressAt >= timeoutMs;
}

/**
 * Run an asynchronous stage with a finite deadline and optional task cancellation.
 * The operation is started only after the timer and abort listener are installed.
 */
export function runWithStageTimeout<T>(
  stage: string,
  timeoutMs: number,
  operation: () => T | PromiseLike<T>,
  signal?: AbortSignal,
  clock: StageTimeoutClock = systemClock,
): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return Promise.reject(new RangeError(`invalid timeout for ${stage}: ${timeoutMs}`));
  }
  if (signal?.aborted) return Promise.reject(new StageCancelledError(stage));

  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      clock.clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    };
    const finish = (callback: (value: never) => void, value: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      callback(value as never);
    };
    const onAbort = () => finish(reject, new StageCancelledError(stage));
    const timer = clock.setTimeout(
      () => finish(reject, new StageTimeoutError(stage, timeoutMs)),
      timeoutMs,
    );
    signal?.addEventListener('abort', onAbort, { once: true });
    Promise.resolve()
      .then(operation)
      .then((value) => finish(resolve, value))
      .catch((error: unknown) => finish(reject, error));
  });
}
