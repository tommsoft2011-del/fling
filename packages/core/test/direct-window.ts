import {
  AdaptiveDirectWindow,
  DIRECT_WINDOW_DEFAULT_MAX_BYTES,
  DIRECT_WINDOW_GROW_BELOW_MS,
  DIRECT_WINDOW_SHRINK_ABOVE_MS,
  DIRECT_WINDOW_STEPS_BYTES,
} from '../src/direct-window.js';

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`FAIL: ${message}`);
}

const mib = 1024 * 1024;
const chunk = 32 * 1024;
check(DIRECT_WINDOW_DEFAULT_MAX_BYTES === 8 * mib, 'production maximum remains 8 MiB');
check(DIRECT_WINDOW_STEPS_BYTES[3] === 16 * mib, '16 MiB is available only as a bounded experiment step');

const production = new AdaptiveDirectWindow();
check(production.byteLimit === 1 * mib && production.chunksFor(chunk) === 32, 'starts at 1 MiB for conservative black-hole detection');
check(production.onAcknowledged(DIRECT_WINDOW_GROW_BELOW_MS - 1), 'fast application ACK grows the window');
check(production.byteLimit === 4 * mib && production.chunksFor(chunk) === 128, 'first growth step is 4 MiB');
check(production.onAcknowledged(250), 'another fast application ACK grows the window');
check(production.byteLimit === 8 * mib && production.chunksFor(chunk) === 256, 'production default reaches but does not exceed 8 MiB');
check(!production.onAcknowledged(250) && production.byteLimit === 8 * mib, 'production controller stays capped at 8 MiB');
check(!production.onAcknowledged(4_000), 'mid-latency application ACK holds its current stage');
check(production.onAcknowledged(DIRECT_WINDOW_SHRINK_ABOVE_MS + 1), 'slow progress reduces the production stage');
check(production.byteLimit === 4 * mib, 'slow 8 MiB window backs off to 4 MiB');
check(production.onAcknowledged(6_000) && production.byteLimit === 1 * mib, 'repeated slow progress backs off to the conservative 1 MiB floor');
check(!production.onAcknowledged(Number.NaN) && !production.onAcknowledged(-1), 'invalid timing samples cannot change the controller');

const experimental = new AdaptiveDirectWindow(16 * mib);
check(experimental.onAcknowledged(250), 'experimental bounded controller grows 1 -> 4 MiB');
check(experimental.onAcknowledged(250), 'experimental bounded controller grows 4 -> 8 MiB');
check(experimental.onAcknowledged(250), 'experimental bounded controller grows 8 -> 16 MiB');
check(experimental.byteLimit === 16 * mib && experimental.chunksFor(chunk) === 512, 'experimental cap is exactly 16 MiB / 512 logical chunks');
check(experimental.byteLimit <= 16 * mib, 'experimental in-flight application window has a hard 16 MiB cap');
check(experimental.onAcknowledged(6_000) && experimental.byteLimit === 8 * mib, 'slow application ACK backs the 16 MiB experiment off to 8 MiB');

let oversizedChunkRejected = false;
try { production.chunksFor(9 * mib); } catch { oversizedChunkRejected = true; }
check(oversizedChunkRejected, 'a logical chunk larger than the production window is rejected');
let badChunkRejected = false;
try { production.chunksFor(0); } catch { badChunkRejected = true; }
check(badChunkRejected, 'invalid chunk sizes are rejected');
let invalidCapRejected = false;
try { new AdaptiveDirectWindow(12 * mib); } catch { invalidCapRejected = true; }
check(invalidCapRejected, 'unsupported intermediate caps cannot bypass the tested bounded steps');

console.log('PASS window controller: production remains 1/4/8 MiB; 16 MiB is available only through an explicit bounded experiment seam.');
console.log('NOTE no LAN throughput model is inferred from ICE RTT or per-MiB progress intervals; end-to-end mock measurements are reported separately.');
console.log('ALL DIRECT WINDOW TESTS PASSED');
