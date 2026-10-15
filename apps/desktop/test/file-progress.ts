import { distributeSequentialProgress, progressForItem, progressPercent } from '../src/file-progress.js';

function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`FAIL: ${message}`);
  console.log(`ok - ${message}`);
}

const files = [{ totalBytes: 100 }, { totalBytes: 40 }];
const duringSave = distributeSequentialProgress(files, 140, 0);
check(duringSave[0]?.receivedBytes === 100 && duringSave[0]?.savedBytes === 0,
  'a fully received file remains at 100% received while saved progress starts at zero');
check(duringSave[1]?.receivedBytes === 40 && duringSave[1]?.savedBytes === 0,
  'each file keeps separate receive and save counters');

const secondFileReceiving = distributeSequentialProgress(files, 125, 100);
check(secondFileReceiving[0]?.receivedBytes === 100 && secondFileReceiving[0]?.savedBytes === 100,
  'completed earlier file retains both confirmed counters');
check(secondFileReceiving[1]?.receivedBytes === 25 && secondFileReceiving[1]?.savedBytes === 0,
  'current file can be partially received before any of it is written');

const secondFileSaving = distributeSequentialProgress(files, 140, 125);
check(secondFileSaving[1]?.receivedBytes === 40 && secondFileSaving[1]?.savedBytes === 25,
  'receive progress stays complete while the second file is being written');

const complete = distributeSequentialProgress(files, 140, 140);
check(complete[1]?.receivedBytes === 40 && complete[1]?.savedBytes === 40,
  'both counters reach completion independently after successful persistence');

const receiverSaving = progressForItem(100, 100, 0);
check(receiverSaving.receivedBytes === 100 && receiverSaving.savedBytes === 0,
  'receiver row does not reset its received progress when save progress begins');
check(progressPercent(100, receiverSaving.receivedBytes) === 100 && progressPercent(100, receiverSaving.savedBytes) === 0,
  'the separate receiver bars render 100% received and 0% written during save');
const bounded = progressForItem(100, 60, 80);
check(bounded.receivedBytes === 60 && bounded.savedBytes === 60,
  'saved progress is clamped to the received byte count');
