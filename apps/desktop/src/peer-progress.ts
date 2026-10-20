export interface PeerProgressCounters {
  receivedBytes: number;
  savedBytes: number;
}

/** Map one sequential file's receiver ACK to task-level counters. Ordinary
 * cumulative ACKs confirm append to the receiver's temp file; `complete`
 * separately marks hash verification and final commit. */
export function mergePeerProgress(
  current: PeerProgressCounters,
  completedBeforeFile: number,
  fileSize: number,
  fileReceivedBytes: number,
  complete: boolean,
): PeerProgressCounters {
  const base = Math.max(0, Math.floor(completedBeforeFile));
  const size = Math.max(0, Math.floor(fileSize));
  const received = Math.max(0, Math.min(size, Math.floor(fileReceivedBytes)));
  const target = base + size;
  const appended = base + received;
  const fileComplete = complete && received >= size;
  return {
    receivedBytes: Math.max(current.receivedBytes, appended),
    savedBytes: Math.max(current.savedBytes, fileComplete ? target : appended),
  };
}

/** ETA is meaningful only when its speed measures the same progress counter. */
export function estimateRemainingSeconds(totalBytes: number, progressedBytes: number, speedBps: number): number | null {
  if (!Number.isFinite(totalBytes) || !Number.isFinite(progressedBytes) || !Number.isFinite(speedBps)) return null;
  if (totalBytes <= 0 || progressedBytes < 0 || speedBps <= 0 || progressedBytes >= totalBytes) return null;
  return Math.ceil((totalBytes - progressedBytes) / speedBps);
}
