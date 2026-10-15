export interface FileByteProgress {
  receivedBytes: number;
  savedBytes: number;
}

export interface ProgressItem {
  totalBytes: number;
}

function byteCount(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

/** Clamp independently reported counters without letting saved bytes exceed received bytes. */
export function progressForItem(totalBytes: number, receivedBytes: number, savedBytes: number): FileByteProgress {
  const total = byteCount(totalBytes);
  const received = Math.min(total, byteCount(receivedBytes));
  return {
    receivedBytes: received,
    savedBytes: Math.min(received, byteCount(savedBytes)),
  };
}

export function progressPercent(totalBytes: number, progressBytes: number, complete = false): number {
  if (complete) return 100;
  const total = byteCount(totalBytes);
  return total > 0 ? Math.min(100, Math.round((byteCount(progressBytes) / total) * 100)) : 0;
}

/**
 * Map cumulative task counters to sequential items. Both counters are distributed
 * independently so receiving can stay complete while the current item is still
 * being written to its temporary file.
 */
export function distributeSequentialProgress(
  items: readonly ProgressItem[],
  receivedTotal: number,
  savedTotal: number,
): FileByteProgress[] {
  let remainingReceived = byteCount(receivedTotal);
  let remainingSaved = Math.min(remainingReceived, byteCount(savedTotal));

  return items.map((item) => {
    const total = byteCount(item.totalBytes);
    const receivedBytes = Math.min(total, remainingReceived);
    const savedBytes = Math.min(receivedBytes, remainingSaved);
    remainingReceived -= receivedBytes;
    remainingSaved -= savedBytes;
    return { receivedBytes, savedBytes };
  });
}
