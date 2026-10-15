import {
  AdaptiveDirectWindow,
  DIRECT_WINDOW_GROW_BELOW_MS,
  DIRECT_WINDOW_SHRINK_ABOVE_MS,
  DIRECT_WINDOW_STEPS_BYTES,
} from '../src/direct-window.js';

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`FAIL: ${message}`);
}

const window = new AdaptiveDirectWindow();
check(window.byteLimit === 1 * 1024 * 1024, 'starts with a 1 MiB ACK window for black-hole detection');
check(window.chunksFor(32 * 1024) === 32, '1 MiB window holds 32 standard chunks');
check(window.chunksFor(256 * 1024) * 256 * 1024 <= window.byteLimit, 'large chunks never exceed the byte cap');
check(window.onAcknowledged(DIRECT_WINDOW_GROW_BELOW_MS - 1), 'fast ACK grows the window');
check(window.byteLimit === 4 * 1024 * 1024 && window.chunksFor(32 * 1024) === 128, 'first growth step is exactly 4 MiB');
check(window.onAcknowledged(250), 'second fast ACK grows again');
check(window.byteLimit === 8 * 1024 * 1024 && window.chunksFor(32 * 1024) === 256, 'maximum in-flight window is exactly 8 MiB');
check(!window.onAcknowledged(250), 'fast ACK at cap does not grow without bound');
check(window.byteLimit === DIRECT_WINDOW_STEPS_BYTES[2], 'cap remains 8 MiB');
check(!window.onAcknowledged(4_000), 'mid-latency ACK holds the current window');
check(window.onAcknowledged(DIRECT_WINDOW_SHRINK_ABOVE_MS + 1), 'slow progress reduces the window');
check(window.byteLimit === 4 * 1024 * 1024, 'slow 8 MiB window backs off to 4 MiB');
check(window.onAcknowledged(6_000), 'another slow window backs off again');
check(window.byteLimit === 1 * 1024 * 1024, 'backoff reaches the conservative 1 MiB floor');
check(!window.onAcknowledged(Number.NaN), 'invalid timing samples cannot change the controller');
check(!window.onAcknowledged(-1), 'negative timing samples cannot change the controller');
let oversizedChunkRejected = false;
try { window.chunksFor(3 * 1024 * 1024); } catch { oversizedChunkRejected = true; }
check(oversizedChunkRejected, 'a chunk larger than the window cap is rejected');
let badChunkRejected = false;
try { window.chunksFor(0); } catch { badChunkRejected = true; }
check(badChunkRejected, 'invalid chunk sizes are rejected');

interface BenchResult { elapsedSeconds: number; mibPerSecond: number; windows: number; }
function simulate(bytes: number, linkMiBps: number, ackDelaySeconds: number, adaptive: boolean): BenchResult {
  const controller = new AdaptiveDirectWindow();
  let remaining = bytes;
  let elapsedSeconds = 0;
  let windows = 0;
  while (remaining > 0) {
    const limitBytes = adaptive ? controller.byteLimit : DIRECT_WINDOW_STEPS_BYTES[0];
    const windowBytes = Math.min(remaining, limitBytes);
    const transferSeconds = windowBytes / (linkMiBps * 1024 * 1024);
    const elapsedMs = (transferSeconds + ackDelaySeconds) * 1_000;
    elapsedSeconds += elapsedMs / 1_000;
    remaining -= windowBytes;
    windows++;
    if (adaptive) controller.onAcknowledged(elapsedMs);
  }
  return { elapsedSeconds, mibPerSecond: bytes / (1024 * 1024) / elapsedSeconds, windows };
}

// Deterministic illustrative model: 717.64 MiB transfer, 50 MiB/s link,
// 323 ms per application ACK. This is not a claim about physical LAN speed;
// it isolates ACK-window amortization with every other assumption fixed.
const bytes = 717.64 * 1024 * 1024;
const ackDelaySeconds = 0.323;
const linkMiBps = 50;
const baseline = simulate(bytes, linkMiBps, ackDelaySeconds, false);
const adaptive = simulate(bytes, linkMiBps, ackDelaySeconds, true);
const speedup = adaptive.mibPerSecond / baseline.mibPerSecond;
// A second fit-only scenario chooses an effective payload rate so the fixed
// 1 MiB model matches the measured 1.972 MiB/s transfer baseline. This is a
// calibration, not an independent prediction or physical-network benchmark.
const observedBaselineMiBps = 1.972;
const calibratedLinkMiBps = 1 / (1 / observedBaselineMiBps - ackDelaySeconds);
const calibratedBaseline = simulate(bytes, calibratedLinkMiBps, ackDelaySeconds, false);
const calibratedAdaptive = simulate(bytes, calibratedLinkMiBps, ackDelaySeconds, true);
const calibratedSpeedup = calibratedAdaptive.mibPerSecond / calibratedBaseline.mibPerSecond;
console.log(`PASS adaptive window steps: 1 -> 4 -> 8 MiB, capped at ${DIRECT_WINDOW_STEPS_BYTES[2]} bytes`);
console.log(`BENCH model: 717.64 MiB, link=${linkMiBps} MiB/s, ACK delay=${ackDelaySeconds * 1000} ms`);
console.log(`A/B fixed-1MiB: windows=${baseline.windows}, elapsed=${baseline.elapsedSeconds.toFixed(2)}s, throughput=${baseline.mibPerSecond.toFixed(2)} MiB/s`);
console.log(`A/B adaptive-1/4/8MiB: windows=${adaptive.windows}, elapsed=${adaptive.elapsedSeconds.toFixed(2)}s, throughput=${adaptive.mibPerSecond.toFixed(2)} MiB/s, speedup=${speedup.toFixed(2)}x`);
console.log(`CALIBRATED A/B: fixed=${calibratedBaseline.mibPerSecond.toFixed(2)} MiB/s, adaptive=${calibratedAdaptive.mibPerSecond.toFixed(2)} MiB/s, link-fit=${calibratedLinkMiBps.toFixed(2)} MiB/s, speedup=${calibratedSpeedup.toFixed(2)}x (fit-only)`);
console.log('NOTE modeled result only; user LAN/NIC/Wi-Fi/LiveKit behavior must be measured on actual devices.');
console.log('ALL DIRECT WINDOW TESTS PASSED');
