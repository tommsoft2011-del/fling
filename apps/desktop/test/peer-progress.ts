import { estimateRemainingSeconds, mergePeerProgress } from '../src/peer-progress.js';

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`FAIL: ${message}`);
  console.log(`ok - ${message}`);
}

const firstWindow = mergePeerProgress(
  { receivedBytes: 0, savedBytes: 0 },
  0,
  1_000,
  512,
  false,
);
check(firstWindow.receivedBytes === 512, 'receiver ACK advances task-level received bytes');
check(firstWindow.savedBytes === 512, 'cumulative ACK reflects bytes appended to the temp file');

const laterFile = mergePeerProgress(
  { receivedBytes: 1_000, savedBytes: 1_000 },
  1_000,
  1_000,
  200,
  false,
);
check(laterFile.receivedBytes === 1_200, 'per-file ACK is offset by completed earlier files');
check(laterFile.savedBytes === 1_200, 'later-file ACK advances append progress independently of final completion');

const staleAck = mergePeerProgress({ receivedBytes: 1_500, savedBytes: 1_500 }, 1_000, 1_000, 200, false);
check(staleAck.receivedBytes === 1_500 && staleAck.savedBytes === 1_500,
  'a stale ACK cannot move monotonic task-level counters backwards');

const committed = mergePeerProgress(
  { receivedBytes: 1_500, savedBytes: 1_500 },
  1_000,
  1_000,
  1_000,
  true,
);
check(committed.receivedBytes === 2_000 && committed.savedBytes === 2_000,
  'complete ACK advances both counters after receiver file completion');

const incompleteCommit = mergePeerProgress({ receivedBytes: 0, savedBytes: 0 }, 0, 1_000, 500, true);
check(incompleteCommit.receivedBytes === 500 && incompleteCommit.savedBytes === 500,
  'a malformed early complete flag cannot mark unreceived bytes complete');

const clamped = mergePeerProgress(
  { receivedBytes: 0, savedBytes: 0 },
  0,
  1_000,
  2_000,
  false,
);
check(clamped.receivedBytes === 1_000 && clamped.savedBytes === 1_000,
  'peer byte counts are clamped to manifest size');

check(estimateRemainingSeconds(1_000, 250, 250) === 3,
  'ETA uses the same peer-progress speed source as its byte counter');
check(estimateRemainingSeconds(1_000, 0, 0) === null,
  'no peer ACK speed means no countdown');
check(estimateRemainingSeconds(1_000, 500, -1) === null,
  'invalid speed cannot produce an ETA');
check(estimateRemainingSeconds(1_000, 1_000, 250) === null,
  'completed progress has no remaining-time estimate');
