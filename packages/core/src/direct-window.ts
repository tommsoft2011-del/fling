/**
 * Adaptive, bounded application-level direct-channel window.
 *
 * The initial 1 MiB stage preserves today's fast black-hole detection. Once a
 * receiver ACK arrives promptly, the window grows to 4 MiB and then 8 MiB to
 * amortize application ACK latency. Slow but progressing paths reduce the stage;
 * the ACK timeout and relay repair remain the final safety net.
 */
export const DIRECT_WINDOW_STEPS_BYTES = [
  1 * 1024 * 1024,
  4 * 1024 * 1024,
  8 * 1024 * 1024,
  16 * 1024 * 1024,
] as const;

/** Production remains capped at 8 MiB until real-device evidence justifies more. */
export const DIRECT_WINDOW_DEFAULT_MAX_BYTES = DIRECT_WINDOW_STEPS_BYTES[2];

export const DIRECT_WINDOW_GROW_BELOW_MS = 3_000;
export const DIRECT_WINDOW_SHRINK_ABOVE_MS = 5_000;

export class AdaptiveDirectWindow {
  private step = 0;
  private readonly maxStep: number;

  constructor(maxBytes = DIRECT_WINDOW_DEFAULT_MAX_BYTES) {
    const maxStep = DIRECT_WINDOW_STEPS_BYTES.indexOf(maxBytes as (typeof DIRECT_WINDOW_STEPS_BYTES)[number]);
    if (maxStep < 0) throw new Error(`invalid direct window maximum (${maxBytes})`);
    this.maxStep = maxStep;
  }

  get byteLimit(): number {
    return DIRECT_WINDOW_STEPS_BYTES[Math.min(this.step, this.maxStep)]!;
  }

  get chunkLimit(): number {
    return this.byteLimit;
  }

  /**
   * Record a window's elapsed time from first send through its ACK. Return
   * whether the configured bound changed. ACKs are never disabled; this only
   * tunes how many chunks may be sent before waiting for the next ACK.
   */
  onAcknowledged(elapsedMs: number): boolean {
    const before = this.step;
    if (!Number.isFinite(elapsedMs) || elapsedMs < 0) return false;

    if (elapsedMs < DIRECT_WINDOW_GROW_BELOW_MS) {
      this.step = Math.min(this.step + 1, this.maxStep);
    } else if (elapsedMs > DIRECT_WINDOW_SHRINK_ABOVE_MS) {
      this.step = Math.max(this.step - 1, 0);
    }

    return before !== this.step;
  }

  /** Convert the byte cap to a whole-chunk cap without exceeding it. */
  chunksFor(chunkSize: number): number {
    if (!Number.isSafeInteger(chunkSize) || chunkSize <= 0) {
      throw new Error(`invalid chunk size (${chunkSize})`);
    }
    if (chunkSize > this.byteLimit) {
      throw new Error(`chunk size exceeds direct window cap (${chunkSize} > ${this.byteLimit})`);
    }
    return Math.max(1, Math.floor(this.byteLimit / chunkSize));
  }
}
