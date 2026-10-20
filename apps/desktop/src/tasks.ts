// 传输引擎：发送任务 / 接收任务 / 全局管理器
// 一次发送动作 = 一个任务 = 独立 LiveKit 房间，任务之间互不阻塞。
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { writeText as writeClipboardText } from '@tauri-apps/plugin-clipboard-manager';
import {
  buildManifest,
  wrapWebFile,
  type FileReceiver,
  type FileManifest,
  type FileLike,
  type FlingMessage,
  type OfferItem,
  type OfferMessage,
  DIRECT_PEER_PROGRESS_TIMEOUT_MS,
  FILE_STARTUP_PREPARATION_TIMEOUT_MS,
  MANIFEST_PREPARATION_TIMEOUT_MS,
  peerProgressTimeoutExpired,
  runWithStageTimeout,
  startPeerProgressWatchdogAfterFrameAccepted,
  StageTimeoutError,
} from '@fling/core';
import {
  connectFlingRoom,
  inboxSpec,
  inboxRoomName,
  lobbySpec,
  nearbyTaskSpecWithRoom,
  remoteTaskSpec,
  remoteTaskSpecWithRoom,
  newTaskRoomName,
  newTaskKey,
  normalizeCode,
  getDeviceId,
  getDeviceName,
  KEY_CTX,
} from './net.js';
import { saveHistoryEntry, HIST_IMAGE_LIMIT, settings, type HistoryEntry } from './store.js';
import { b64encode, toast, uid, utf8bytes } from './ui.js';
import { P2PLink, type P2PState } from './p2p.js';
import { log, flushLogs, type LogTag } from './log.js';
import { notifyTaskDone } from './notify.js';
import type { RustFlingRoom } from './online-rust.js';

export type ItemKind = 'text' | 'file' | 'clipboard-image';

/**
 * 统一错误信息提取：Tauri invoke 失败时 reject 的是字符串而非 Error，
 * 直接取 .message 会吞掉真实信息（曾导致"保存失败"掩盖真实原因）。
 */
export function errText(e: unknown): string {
  if (typeof e === 'string') return e;
  if (e instanceof Error && e.message) return e.message;
  try {
    const s = JSON.stringify(e);
    if (s && s !== '{}') return s;
  } catch {
    /* ignore */
  }
  return '未知错误';
}

function errorType(e: unknown): string {
  return e instanceof Error ? e.name : typeof e;
}

interface SavedReceivedFileResult {
  path: string;
  writeAllMs: number;
  syncAllMs: number;
  sha256VerifyMs: number;
  commitMs: number;
}

interface AppendedReceivedFileBatchResult {
  bytes: number;
  cumulativeBytes: number;
  writeElapsedMs: number;
}

async function saveReceivedFile(
  taskId: string,
  transferId: string,
  dir: string,
  name: string,
  receiver: FileReceiver,
  relPath: string | null,
  onBatch?: (bytes: Uint8Array) => void,
  onProgress?: (writtenBytes: number) => void,
): Promise<string> {
  const xfer = transferId.slice(0, 8);
  const size = receiver.totalBytes;
  const startedAt = Date.now();
  let sessionId: string | undefined;
  let lastProgressLog = 0;
  let batchCount = 0;
  let batchesSinceLog = 0;
  let invokeMsSinceLog = 0;
  let invokeMsMaxSinceLog = 0;
  let rustWriteMsSinceLog = 0;
  let lastProgressAt = performance.now();
  let failurePhase = 'begin';
  const beginStartedAt = performance.now();
  log('xfer', 'receiver temp file begin requested', { taskId, xfer, size });
  try {
    const currentSessionId = await invoke<string>('begin_received_file', {
      dir,
      name,
      relPath,
      expectedSize: size,
      expectedSha256: receiver.manifest.sha256,
    });
    sessionId = currentSessionId;
    log('xfer', 'receiver temp file begin complete', {
      taskId, xfer, size, status: 'ok', elapsedMs: Math.round(performance.now() - beginStartedAt),
    });
    failurePhase = 'append';
    await receiver.streamTo(async (bytes, writtenBytes) => {
      const invokeStartedAt = performance.now();
      const result = await invoke<AppendedReceivedFileBatchResult>(
        'append_received_file_chunk',
        bytes,
        { headers: { 'x-session-id': currentSessionId } },
      );
      const invokeElapsedMs = performance.now() - invokeStartedAt;
      if (result.bytes !== bytes.byteLength || result.cumulativeBytes !== writtenBytes) {
        throw new Error('received file append accounting mismatch');
      }
      // streamTo awaits every append, so there are no queued batches behind this one.
      batchCount++;
      batchesSinceLog++;
      invokeMsSinceLog += invokeElapsedMs;
      invokeMsMaxSinceLog = Math.max(invokeMsMaxSinceLog, invokeElapsedMs);
      rustWriteMsSinceLog += result.writeElapsedMs;
      const now = performance.now();
      if (batchCount === 1) {
        log('xfer', 'first receiver append completed', {
          taskId, xfer, batchBytes: result.bytes, cumulativeBytes: result.cumulativeBytes,
          invokeMs: Math.round(invokeElapsedMs), rustWriteMs: Math.round(result.writeElapsedMs),
        });
      }
      onBatch?.(bytes);
      onProgress?.(writtenBytes);
      if (
        writtenBytes - lastProgressLog >= 32 * 1024 * 1024 ||
        now - lastProgressAt >= 10_000 ||
        writtenBytes === size
      ) {
        log('xfer', 'receiver append aggregate', {
          taskId, xfer, size, cumulativeBytes: writtenBytes, batches: batchesSinceLog,
          invokeTotalMs: Math.round(invokeMsSinceLog), invokeMaxMs: Math.round(invokeMsMaxSinceLog),
          rustWriteTotalMs: Math.round(rustWriteMsSinceLog),
        });
        lastProgressLog = writtenBytes;
        lastProgressAt = now;
        batchesSinceLog = 0;
        invokeMsSinceLog = 0;
        invokeMsMaxSinceLog = 0;
        rustWriteMsSinceLog = 0;
      }
    });
    failurePhase = 'verify-sync-commit';
    const result = await invoke<SavedReceivedFileResult>('finish_received_file', { sessionId });
    log('xfer', 'Rust SHA-256 verify stage', {
      taskId, xfer, size, status: 'ok', elapsedMs: Math.round(result.sha256VerifyMs),
    });
    log('xfer', 'Rust write_all stage', {
      taskId, xfer, size, status: 'ok', elapsedMs: result.writeAllMs,
    });
    log('xfer', 'Rust sync_all stage', {
      taskId, xfer, size, status: 'ok', elapsedMs: result.syncAllMs,
    });
    log('xfer', 'Rust atomic commit stage', {
      taskId, xfer, size, status: 'ok', elapsedMs: Math.round(result.commitMs),
    });
    log('xfer', 'Tauri save IPC end', {
      taskId, xfer, size, status: 'ok', elapsedMs: Date.now() - startedAt,
    });
    return result.path;
  } catch (e) {
    if (sessionId) {
      try {
        const cleanupStartedAt = performance.now();
        await invoke('abort_received_file', { sessionId });
        log('xfer', 'receiver temp cleanup completed', {
          taskId, xfer, reason: 'save_error',
          elapsedMs: Math.round(performance.now() - cleanupStartedAt),
        });
      } catch (cleanupError) {
        log('xfer', 'receiver temp cleanup failed', {
          taskId, xfer, reason: 'save_error', errorType: errorType(cleanupError),
        });
        // The backend also drops unfinished temp files when a session fails.
      }
    }
    log('xfer', 'Tauri save IPC end', {
      taskId, xfer, size, status: 'error', elapsedMs: Date.now() - startedAt,
      failurePhase, errorType: errorType(e),
    });
    throw e;
  }
}

/** 发送框里的一个条目（快照） */
export interface ComposeItem {
  uid: string;
  kind: ItemKind;
  name: string;
  size: number;
  blob?: Blob;
  text?: string;
  previewUrl?: string;
  /** 文件夹发送时的相对路径，如 "docs/report.pdf" */
  relPath?: string;
  /** 所属顶层文件夹名（用于发送框折叠显示；单文件为空） */
  folder?: string;
  /** Tauri 下文件的绝对路径（懒加载：发送时才读文件内容，避免大文件夹提前占内存） */
  filePath?: string;
}

export type SendTarget =
  | { type: 'nearby'; deviceId: string; deviceName: string }
  | { type: 'remote'; code: string };

export interface ItemState {
  uid: string;
  kind: ItemKind;
  name: string;
  size: number;
  status: 'waiting' | 'active' | 'done' | 'error';
  doneBytes: number;
  totalBytes: number;
  error?: string;
  /** 接收端落盘后的完整路径（历史记录用） */
  savedPath?: string;
  /** 临时文件中已写入的字节；只有 durable commit 成功后 status 才会变为 done。 */
  saveBytes?: number;
  saving?: boolean;
  /** 文件 sha256（用于完整性核对） */
  sha256?: string;
  mimeType?: string;
  transport?: 'p2p' | 'relay' | 'mixed';
  relPath?: string;
}

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
const TEXT_CHUNK_CHARS = 15000;
const OFFER_REANNOUNCE_MS = 5000;
const WAIT_PEER_TIMEOUT_MS = 5 * 60 * 1000;
/** Receiver reports are emitted once per second; allow relay ACK jitter without
 * masking a genuinely silent direct path. */
const DIRECT_HEALTH_CHECK_MS = 1000;
const PEER_PROGRESS_WATCHDOG_STAGE = 'peer_progress_after_first_frame';
/** Progress is an aggregate, throttled lower-bound; replay an overlap to cover interior holes. */
const DIRECT_REPLAY_OVERLAP_CHUNKS = 64;
/** Relay throughput is variable; fail only after it stops making progress for this long. */
const RELAY_PEER_PROGRESS_SILENCE_TIMEOUT_MS = 30_000;

function toOfferItem(c: ComposeItem): OfferItem {
  return {
    name: c.kind === 'text' ? (c.text ?? '').slice(0, 24) || '文本' : c.name,
    size: c.size,
    kind: c.kind,
    path: c.relPath,
  };
}

function kindLabel(k: ItemKind): string {
  return k === 'text' ? '文本' : k === 'clipboard-image' ? '剪贴板' : '文件';
}

function mergeTransport(current: ItemState['transport'], route: 'p2p' | 'relay'): ItemState['transport'] {
  return current && current !== route ? 'mixed' : route;
}

function summarizeTransport(items: ItemState[]): 'p2p' | 'relay' | 'mixed' | undefined {
  const routes = new Set(items.map((item) => item.transport).filter(Boolean));
  if (routes.size === 0) return undefined;
  return routes.size === 1 ? [...routes][0] : 'mixed';
}

// ================================================================== 发送任务
export type SendPhase = 'preparing' | 'waiting' | 'sending' | 'done' | 'error' | 'cancelled' | 'rejected';

export class SendTask {
  readonly taskId = uid('task');
  phase: SendPhase = 'preparing';
  items: ItemState[];
  startedAt = Date.now();
  endedAt?: number;
  speedBps = 0;
  error?: string;
  onChange: () => void = () => {};
  /** P2P 直连状态：idle=未尝试 connecting=握手中 open=直连传输中 failed=已回退中继 */
  p2p: P2PState = 'idle';

  private room?: RustFlingRoom;
  private p2pLink: P2PLink | null = null;
  /** P2P 就绪（open/failed），文件发送前等待，避免前半走慢中继 */
  private p2pReady: Promise<'open' | 'failed'> | null = null;
  /** 接收方上报的真实接收进度（task-progress），UI 显示"对方已接收" */
  peerDoneBytes = 0;
  /** 接收端 Rust append 已确认的字节数；最终完成仍以提交后的 ACK 为准。 */
  peerSavedBytes = 0;
  peerTotalBytes = 0;
  /** 等待接收方 task-ack 的 resolver（收到确认/对方离开/超时后置空） */
  private ackResolve: ((ok: boolean) => void) | null = null;
  private peerFailures: { name: string; size: number; error: string }[] = [];
  private currentSendingItem: ItemState | null = null;
  private currentPeerProgressTarget: number | null = null;
  private currentFileFinalizing = false;
  /** At most one loaded next Blob is hashed while the current file is durably saved. */
  private preparedManifest: { index: number; deadlineAt: number; promise: Promise<FileManifest | null> } | null = null;
  private lobbyLeave?: () => void;
  private announceTimer?: ReturnType<typeof setInterval>;
  private waitTimer?: ReturnType<typeof setTimeout>;
  private peerJoined = false;
  private cancelled = false;
  private readonly stageAbortController = new AbortController();
  /** 接收方中途取消（收到 task-cancel）：不再标成功 */
  private peerCancelled = false;
  private lastBytes = 0;
  private lastT = 0;
  private doneNotified = false;
  private directHealthTimer: ReturnType<typeof setInterval> | null = null;
  private lastPeerProgressAt = 0;
  private watchdogStartedAt = 0;
  private lastWatchdogArmedAt = 0;
  private watchdogPauseReason: string | null = null;
  private currentFileStartedAt = 0;
  private currentFileFirstFrameAccepted = false;
  private currentFileStartupDeadlineAt = 0;
  private lastPeerProgressLogAt = 0;
  private peerDoneBytesAtLastLog = 0;
  private peerSavedBytesAtLastLog = 0;

  constructor(
    readonly source: ComposeItem[],
    readonly target: SendTarget,
  ) {
    this.items = source.map((c) => ({
      uid: c.uid,
      kind: c.kind,
      name: c.kind === 'text' ? (c.text ?? '').slice(0, 24) || '文本' : (c.relPath ?? c.name),
      size: c.size,
      status: 'waiting',
      doneBytes: 0,
      totalBytes: c.size,
    }));
  }

  private runStartupStage<T>(
    stage: string,
    timeoutMs: number,
    operation: () => T | PromiseLike<T>,
    deadlineAt = this.currentFileStartupDeadlineAt,
  ): Promise<T> {
    const remainingMs = deadlineAt > 0 ? deadlineAt - Date.now() : timeoutMs;
    const effectiveTimeoutMs = Math.min(timeoutMs, remainingMs);
    if (effectiveTimeoutMs <= 0) {
      const error = new StageTimeoutError('source_manifest_startup', FILE_STARTUP_PREPARATION_TIMEOUT_MS);
      log('xfer', 'startup timer stage fired', {
        taskId: this.taskId,
        timerStage: error.stage,
        timeoutMs: error.timeoutMs,
        attemptedStage: stage,
        firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
      });
      return Promise.reject(error);
    }
    return runWithStageTimeout(stage, effectiveTimeoutMs, operation, this.stageAbortController.signal).catch((error: unknown) => {
      if (error instanceof StageTimeoutError) {
        log('xfer', 'startup timer stage fired', {
          taskId: this.taskId,
          timerStage: error.stage,
          timeoutMs: error.timeoutMs,
          firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
        });
      }
      throw error;
    });
  }

  /**
   * Overlap only the next already-loaded Blob's streaming hash with receiver
   * save/fsync. Hashing reads 256 KiB slices, so extra byte-buffer memory is
   * independent of Blob size. Lazy path-backed files are never read early.
   */
  private prepareNextBlobManifest(): void {
    if (this.cancelled || !this.currentSendingItem) return;
    const currentIndex = this.items.indexOf(this.currentSendingItem);
    let nextIndex = currentIndex + 1;
    while (nextIndex < this.source.length && this.source[nextIndex]?.kind === 'text') nextIndex++;
    const next = this.source[nextIndex];
    if (!next || next.kind === 'text') return;
    if (!next.blob) {
      log('xfer', 'next manifest prehash skipped for lazy path-backed file', {
        taskId: this.taskId,
        size: next.size,
        source: 'native_path',
      });
      return;
    }
    const nextKind = next.kind;
    if (this.preparedManifest?.index === nextIndex) return;

    const file = wrapWebFile(next.blob, next.name, next.relPath);
    const startedAt = performance.now();
    const deadlineAt = Date.now() + FILE_STARTUP_PREPARATION_TIMEOUT_MS;
    log('xfer', 'next manifest prehash started', {
      taskId: this.taskId,
      size: next.size,
      source: 'in_memory_blob',
      overlapsCurrentItemSize: this.currentSendingItem.size,
    });
    const promise = this.runStartupStage(
      'manifest_preparation',
      MANIFEST_PREPARATION_TIMEOUT_MS,
      () => buildManifest(file, nextKind),
      deadlineAt,
    ).then((manifest) => {
      log('xfer', 'next manifest prehash complete', {
        taskId: this.taskId,
        size: next.size,
        status: 'ok',
        elapsedMs: Math.round(performance.now() - startedAt),
      });
      return manifest;
    }).catch((error: unknown) => {
      log('xfer', 'next manifest prehash failed', {
        taskId: this.taskId,
        size: next.size,
        elapsedMs: Math.round(performance.now() - startedAt),
        errorType: errorType(error),
      });
      if (error instanceof StageTimeoutError) throw error;
      return null;
    });
    void promise.catch(() => undefined);
    this.preparedManifest = { index: nextIndex, deadlineAt, promise };
  }

  get peerLabel(): string {
    return this.target.type === 'nearby' ? this.target.deviceName : `配对码 ${this.target.code}`;
  }
  get waitLabel(): string {
    return this.target.type === 'nearby' ? '连接设备' : '等待对方加入';
  }
  get finalizing(): boolean {
    return this.currentFileFinalizing;
  }
  get totalBytes(): number {
    return this.items.reduce((a, i) => a + i.totalBytes, 0);
  }
  get doneBytes(): number {
    return this.items.reduce((a, i) => a + i.doneBytes, 0);
  }
  get active(): boolean {
    return this.phase === 'preparing' || this.phase === 'waiting' || this.phase === 'sending';
  }

  private lastNotify = 0;
  private notifyTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * UI 刷新节流：分块到来时每块都调 notify 会高频重绘，
   * 在慢机器上拖慢接收管线。300ms 内合并，尾调用保证最终状态刷出。
   */
  private notify(): void {
    const now = Date.now();
    const elapsed = now - this.lastNotify;
    if (elapsed >= 300) {
      this.lastNotify = now;
      this.onChange();
      return;
    }
    if (!this.notifyTimer) {
      this.notifyTimer = setTimeout(() => {
        this.notifyTimer = null;
        this.lastNotify = Date.now();
        this.onChange();
      }, 300 - elapsed);
    }
  }

  private trackSpeed(doneBytes: number): void {
    const now = Date.now();
    const dt = (now - this.lastT) / 1000;
    if (dt >= 0.4 && this.lastT > 0) {
      const inst = (doneBytes - this.lastBytes) / dt;
      this.speedBps = this.speedBps === 0 ? inst : this.speedBps * 0.7 + inst * 0.3;
      this.lastBytes = doneBytes;
      this.lastT = now;
    } else if (this.lastT === 0) {
      this.lastT = now;
      this.lastBytes = doneBytes;
    }
  }

  async run(): Promise<void> {
    try {
      const taskRoom = newTaskRoomName();
      const taskKey = newTaskKey();
      log('task', 'send start', {
        taskId: this.taskId,
        itemCount: this.items.length,
        totalBytes: this.items.reduce((sum, item) => sum + item.size, 0),
        items: this.items.map((i) => ({ kind: i.kind, size: i.size })),
      });
      const offer: OfferMessage = {
        kind: 'offer',
        taskId: this.taskId,
        fromDevice: getDeviceName(),
        fromDeviceId: getDeviceId(),
        taskRoom,
        taskKey,
        // LiveKit 单包 64KB 上限：文件多了 items 必须截断（1232 个文件时曾报
        // "cannot publish data packet larger than 64000 bytes"），只保留前 30 个做预览；
        // 缺的条目由 manifest 到达时补建，totalFileItems 单独统计文件 manifest 数。
        items: this.source.slice(0, 30).map(toOfferItem),
        totalItems: this.source.length,
        totalFileItems: this.source.filter((item) => item.kind !== 'text').length,
        totalBytes: this.source.reduce((a, c) => a + c.size, 0),
      };

      // 1. 建任务房间（发送方先进）
      const spec =
        this.target.type === 'nearby'
          ? nearbyTaskSpecWithRoom(taskRoom, taskKey)
          : { roomName: taskRoom, keyMaterial: normalizeCode(this.target.code), keyContext: KEY_CTX.room };
      this.room = await connectFlingRoom(spec, {
        onPeerJoined: () => this.onPeer(),
        onPeerLeft: () => {
          // 接收方离开：ack 可能还在路上，给 3 秒宽限期，超时才判失败
          // （接收方发完 ack 会立刻关房间，直接判失败会误报"对方未确认接收"）
          // Promise resolve 多次调用只有第一次有效，所以宽限期和 onTaskAck 不会冲突
          if (this.ackResolve) {
            const resolve = this.ackResolve;
            this.ackResolve = null;
            setTimeout(() => resolve(false), 3000);
            this.ackResolve = (ok: boolean) => resolve(ok);
          }
        },
        onP2PSignal: (sig) => {
          void this.p2pLink?.handleSignal(sig);
        },
        onTaskAck: (taskId) => {
          if (taskId === this.taskId) {
            log('task', 'task-ack received', { taskId });
            this.peerDoneBytes = Math.max(this.peerDoneBytes, this.totalBytes);
            this.peerSavedBytes = Math.max(this.peerSavedBytes, this.totalBytes);
            this.peerTotalBytes = Math.max(this.peerTotalBytes, this.totalBytes);
            this.currentFileFinalizing = false;
            this.notify();
            this.ackResolve?.(true);
          }
        },
        onTaskCancel: (taskId) => {
          if (taskId === this.taskId) {
            log('task', 'task-cancel received', { taskId });
            this.peerCancelled = true;
            this.stageAbortController.abort();
            this.ackResolve?.(false);
          }
        },
        onTaskFailed: (taskId, failures) => {
          if (taskId !== this.taskId) return;
          this.peerFailures = failures;
          for (const failure of failures) {
            const index = this.items.findIndex((it, i) =>
              (it.name === failure.name || this.source[i]?.name === failure.name) && it.totalBytes === failure.size,
            );
            const item = index >= 0 ? this.items[index] : undefined;
            if (item) {
              item.status = 'error';
              item.error = failure.error;
            }
          }
          log('task', 'task-failed received', { taskId, failureCount: failures.length });
          this.ackResolve?.(false);
          this.notify();
        },
        onDirectFallback: (reason) => this.onDirectFallback(reason),
        onFileFinalizing: (id, size) => {
          this.currentFileFinalizing = true;
          log('p2p', 'receiver finalizing; pause byte-stall watchdog', {
            taskId: this.taskId,
            xfer: id.slice(0, 8),
            size,
          });
          this.prepareNextBlobManifest();
          this.notify();
        },
        onTaskProgress: (taskId, receivedBytes, totalBytes, _from, savedBytes = 0) => {
          if (taskId === this.taskId) {
            const previousPeerDone = this.peerDoneBytes;
            const progressAt = Date.now();
            const watchdogSilenceBeforeMs = this.lastPeerProgressAt > 0
              ? Math.max(0, progressAt - this.lastPeerProgressAt)
              : undefined;
            const received = Math.max(0, Math.floor(receivedBytes));
            const saved = Math.max(0, Math.floor(savedBytes));
            const progressAdvanced = received > previousPeerDone;
            if (this.phase === 'sending' && this.p2p === 'open' && this.room?.directActive && progressAdvanced) {
              this.lastPeerProgressAt = progressAt;
            }
            // 诊断：对方上报的已收到不应超过我已发送的；倒退/超发都说明计数错乱
            const mySent = this.doneBytes;
            if (received > mySent + 65536) {
              log('xfer', 'PEER PROGRESS EXCEEDS SENT', {
                taskId, peerDone: received, mySent, peerTotal: totalBytes,
              });
            }
            if (received < this.peerDoneBytes || saved < this.peerSavedBytes) {
              log('xfer', 'PEER PROGRESS BACKWARDS', {
                taskId, receivedWas: this.peerDoneBytes, receivedNow: received,
                savedWas: this.peerSavedBytes, savedNow: saved,
              });
            }
            // Keep the two monotonic counters independent: a newer write ACK
            // must not be lost behind an older received-byte report.
            this.peerDoneBytes = Math.max(this.peerDoneBytes, received);
            this.peerSavedBytes = Math.max(this.peerSavedBytes, Math.min(saved, received));
            this.peerTotalBytes = Math.max(this.peerTotalBytes, totalBytes);
            const progressLogDue = this.lastPeerProgressLogAt === 0 ||
              progressAt - this.lastPeerProgressLogAt >= 5_000 ||
              this.peerDoneBytes - this.peerDoneBytesAtLastLog >= 32 * 1024 * 1024 ||
              this.peerSavedBytes - this.peerSavedBytesAtLastLog >= 32 * 1024 * 1024;
            if (progressLogDue) {
              log('xfer', 'peer progress aggregate', {
                taskId,
                receivedBytes: this.peerDoneBytes,
                receivedDeltaSinceLog: this.peerDoneBytes - this.peerDoneBytesAtLastLog,
                immediateReceivedDelta: this.peerDoneBytes - previousPeerDone,
                savedBytes: this.peerSavedBytes,
                savedDeltaSinceLog: this.peerSavedBytes - this.peerSavedBytesAtLastLog,
                totalBytes: this.peerTotalBytes,
                aggregateIntervalMs: this.lastPeerProgressLogAt ? progressAt - this.lastPeerProgressLogAt : undefined,
                direct: this.room?.directActive ?? false,
                watchdogSilenceBeforeMs: this.p2p === 'open' && this.room?.directActive
                  ? watchdogSilenceBeforeMs
                  : undefined,
                watchdogReset: progressAdvanced && this.p2p === 'open' && this.room?.directActive,
                firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
              });
              this.lastPeerProgressLogAt = progressAt;
              this.peerDoneBytesAtLastLog = this.peerDoneBytes;
              this.peerSavedBytesAtLastLog = this.peerSavedBytes;
            }
            this.notify();
          }
        },
        onFileRoute: (route, id) => {
          if (this.currentSendingItem) {
            this.currentSendingItem.transport = mergeTransport(this.currentSendingItem.transport, route);
            if (route === 'p2p' && !this.currentFileFirstFrameAccepted) {
              const acceptedAt = Date.now();
              this.currentFileFirstFrameAccepted = true;
              log('p2p', 'first direct frame locally accepted by DataChannel send()', {
                taskId: this.taskId,
                xfer: id.slice(0, 8),
                acceptedAt,
                elapsedFromFileStartMs: this.currentFileStartedAt ? Math.max(0, acceptedAt - this.currentFileStartedAt) : undefined,
                elapsedFromWatchdogArmMs: this.lastWatchdogArmedAt ? Math.max(0, acceptedAt - this.lastWatchdogArmedAt) : undefined,
                watchdogStartedBeforeFirstAcceptedFrame: this.lastWatchdogArmedAt > 0 && this.lastWatchdogArmedAt < acceptedAt,
                watchdogTimeoutMs: DIRECT_PEER_PROGRESS_TIMEOUT_MS,
                peerProgressAlreadySeen: this.peerDoneBytes > 0,
              });
              this.startDirectHealthAfterFirstFrame();
            }
          }
        },
        onLog: (tag, msg) => log(tag as LogTag, `[room] ${msg}`),
      });
      if (this.cancelled) return;

      // 2. 投递 offer
      this.phase = 'waiting';
      this.notify();
      if (this.target.type === 'nearby') {
        await this.deliverOfferNearby(offer, this.target.deviceId);
      } else {
        await this.announceInLobby(offer, this.target.code);
      }
      if (this.cancelled) return;

      // 3. 等对方加入（5 分钟超时）
      await new Promise<void>((resolve, reject) => {
        if (this.peerJoined) return resolve();
        const check = setInterval(() => {
          if (this.peerJoined || this.cancelled) {
            clearInterval(check);
            clearTimeout(this.waitTimer);
            resolve();
          }
        }, 300);
        this.waitTimer = setTimeout(() => {
          clearInterval(check);
          reject(new Error('对方无响应（5 分钟未加入），已取消'));
        }, WAIT_PEER_TIMEOUT_MS);
      });
      if (this.cancelled) return;
      this.stopAnnounce();

      // 3.5 尝试 P2P 直连：SDP/ICE 走加密中继做信令，文件走直连；
      // 握手失败或超时则无缝回退中继，不阻塞发送。
      this.startP2P();

      // 4. 逐项发送。文本先行（小，走中继即可）；文件等 P2P 结论
      // （open 走直连 / failed 走中继）再发，避免前半部分走慢速中继。
      this.phase = 'sending';
      this.notify();
      let okCount = 0;
      let p2pAwaited = false;
      for (let i = 0; i < this.source.length; i++) {
        if (this.cancelled) break;
        const src = this.source[i];
        const st = this.items[i];
        this.currentSendingItem = src.kind === 'text' ? null : st;
        this.currentFileFinalizing = false;
        this.currentPeerProgressTarget = src.kind === 'text'
          ? null
          : this.items.slice(0, i + 1).reduce((sum, item) => sum + item.totalBytes, 0);
        if (src.kind !== 'text' && !p2pAwaited) {
          p2pAwaited = true;
          const ready = (await this.p2pReady?.catch(() => 'failed' as const)) ?? 'failed';
          log('p2p', ready === 'open' ? 'ready, files go direct' : 'not ready, files go relay', {
            taskId: this.taskId,
          });
        }
        if (src.kind !== 'text') {
          this.currentFileStartedAt = Date.now();
          this.currentFileFirstFrameAccepted = false;
          this.currentFileStartupDeadlineAt = this.currentFileStartedAt + FILE_STARTUP_PREPARATION_TIMEOUT_MS;
          this.markDirectTransferStart();
        }
        st.status = 'active';
        this.notify();
        log('xfer', 'send item start', { taskId: this.taskId, kind: src.kind, size: st.size });
        try {
          if (src.kind === 'text') {
            await this.sendTextItem(src.text ?? '', st);
          } else {
            // Path-backed sources (file picker / drag-drop / folder items):
            // the Rust room opens the file by path itself, so the WebView
            // must NOT materialize the content. The old flow read the whole
            // file through read_file_bytes into a Blob (then spilled it back
            // to a temp file via Array.from) — fine for small files, fatal
            // for hundreds of MB (the read/IPC conversion throws and the
            // send dies ~3s in, before the manifest is even published).
            let file: FileLike;
            if (src.filePath) {
              file = {
                name: src.name,
                size: src.size,
                path: src.relPath,
                slice: async () => {
                  throw new Error('path-backed file: in-memory slice unavailable');
                },
              } as FileLike;
              (file as unknown as { diskPath?: string }).diskPath = src.filePath;
            } else {
              const blob = src.blob;
              if (!blob) throw new Error('文件内容不可用');
              file = wrapWebFile(blob, src.name, src.relPath);
            }
            const directFileTransfer = this.room?.directActive === true;
            const peerTarget = this.currentPeerProgressTarget!;
            let receiverSaved = false;
            const onFileProgress = (sent: number, _total: number, saved = false) => {
              if (saved) {
                receiverSaved = true;
                this.currentFileFinalizing = false;
                this.peerSavedBytes = Math.max(this.peerSavedBytes, peerTarget);
              }
              // A relay replay uses the same manifest and must not make the
              // local progress display jump backwards either.
              st.doneBytes = Math.max(st.doneBytes, sent);
              this.trackSpeed(this.doneBytes);
              this.notify();
            };
            const prepared = this.preparedManifest?.index === i
              ? (await this.preparedManifest.promise) ?? undefined
              : undefined;
            if (this.preparedManifest?.index === i) this.preparedManifest = null;
            // sendFile (Rust online_send_file) blocks until the receiver's
            // complete ACK arrives (SHA verified + file renamed). A successful
            // return means the file is confirmed — no need to wait for the
            // legacy peerDoneBytes which Rust never updates (no task-progress
            // messages in the v0.6.7+ Rust path).
            let confirmed = true;
            try {
              const manifest = await this.room!.sendFile(
                file,
                onFileProgress,
                src.kind,
                prepared,
                0,
                this.currentFileStartupDeadlineAt,
              );
              st.sha256 = manifest.sha256;
              st.mimeType = manifest.mimeType;
              // Mark peer progress as complete so UI and waitForPeerProgress
              // see the confirmation.
              this.peerDoneBytes = Math.max(this.peerDoneBytes, peerTarget);
              this.peerSavedBytes = Math.max(this.peerSavedBytes, peerTarget);
            } catch (e) {
              confirmed = false;
              throw e;
            }
            // Legacy waitForPeerProgress removed: Rust already confirmed via
            // complete ACK. Keep the variable for the replay logic below.
            const directPathStillActive = directFileTransfer && this.room?.directActive === true;
            if (directFileTransfer && !confirmed) {
              const reason = `file progress stalled at ${this.peerDoneBytes}/${peerTarget}`;
              const previousTarget = peerTarget - st.totalBytes;
              const peerFileBytes = Math.min(
                st.totalBytes,
                Math.max(0, this.peerDoneBytes - previousTarget),
              );
              const replayFromSeq = Math.max(
                0,
                Math.floor(peerFileBytes / manifest.chunkSize) - DIRECT_REPLAY_OVERLAP_CHUNKS,
              );
              log('p2p', 'file confirmation stalled, replaying via relay', {
                taskId: this.taskId,
                name: st.name,
                peerDoneBytes: this.peerDoneBytes,
                peerTarget,
                peerFileBytes,
                replayFromSeq,
                direct: this.room?.directActive ?? false,
              });
              this.room?.fallbackToRelay(reason);
              await this.room!.sendFile(file, onFileProgress, src.kind, manifest, replayFromSeq);
              confirmed = await this.waitForPeerProgress(
                peerTarget,
                RELAY_PEER_PROGRESS_SILENCE_TIMEOUT_MS,
                true,
              );
              // A throttled progress report can still be stale, or the first
              // overlap may not cover an unusual interior hole. Use one full
              // replay as a bounded last resort, but never advance to the next
              // file while the receiver still has not confirmed this one.
              if (!confirmed && replayFromSeq > 0) {
                log('p2p', 'selective relay replay not confirmed, retrying full file', {
                  taskId: this.taskId,
                  name: st.name,
                  replayFromSeq,
                });
                await this.room!.sendFile(file, onFileProgress, src.kind, manifest, 0);
                confirmed = await this.waitForPeerProgress(
                  peerTarget,
                  RELAY_PEER_PROGRESS_SILENCE_TIMEOUT_MS,
                  true,
                );
              }
            }
            if (!confirmed) {
              throw new Error(
                directFileTransfer ? '中继重传后对方仍未确认文件进度' : '中继传输后对方进度停止',
              );
            }
          }
          st.status = 'done';
          st.doneBytes = st.totalBytes;
          okCount++;
          log('xfer', 'send item done', { taskId: this.taskId, name: st.name, size: st.size });
        } catch (e) {
          st.status = 'error';
          st.error = (e as Error).message || '发送失败';
          log('xfer', 'send item error', { taskId: this.taskId, size: st.size, status: 'error', errMsg: String((e as Error)?.message ?? e) });
        }
        this.notify();
        this.currentSendingItem = null;
        this.currentPeerProgressTarget = null;
        this.currentFileFinalizing = false;
        // Do not enqueue later files after one item has lost end-to-end
        // confirmation. Continuing here used to turn a single relay stall
        // into an even larger LiveKit queue and made the UI show e.g. 3/5
        // while item 2 was already failed.
        if (st.status === 'error') {
          log('xfer', 'stopping after item failure', { taskId: this.taskId, size: st.size });
          break;
        }
      }

      // 5. 收尾：等接收方确认全部落盘（task-ack）再拆连接。
      // 以前是 task-done 后固定等 2s 就关——"发送成功"只是数据进了发送缓冲，
      // 接收方可能还没收完，这是"发送端成功但接收端失败"的根因。
      try {
        await this.room!.sendTaskDone(this.taskId);
        log('task', 'task-done sent', { taskId: this.taskId });
      } catch { /* 尽力 */ }
      // A partial task cannot become successful by waiting for the receiver's
      // final ACK. Waiting 30 seconds here used to hide the real item-level
      // relay failure and keep the broken room alive; close promptly after a
      // failed item, while preserving the full end-to-end wait for success.
      const acked = okCount === this.items.length
      ? await this.waitForAck(30_000)
      : false;
      this.stopDirectHealth(acked ? 'task_ack_received' : 'task_finished_without_task_ack');
      log('task', acked ? 'ack ok, closing' : 'ack timeout, closing anyway', {
        taskId: this.taskId,
        peerCancelled: this.peerCancelled,
      });
      // 成功判定必须以接收方确认为准：对方取消 / 未确认（超时）都不算成功。
      // 以前 acked 被完全忽略，"字节发出去了"就标成功——这就是"接收方点了取消，
      // 发送方还显示成功"的根因。
      if (this.cancelled || this.peerCancelled) {
        this.phase = 'cancelled';
      } else if (!acked) {
        this.phase = 'error';
        this.error = this.peerFailures[0]?.error ?? '对方未确认接收';
      } else if (okCount === this.items.length) {
        this.phase = 'done';
      } else {
        this.phase = 'error';
        this.error = '部分项目发送失败';
      }
      this.endedAt = Date.now();
      this.notify();
      // 系统通知（纯加法，不影响传输逻辑）
      notifyTaskDone('send', this.phase === 'done',
        `${this.peerLabel} · ${this.items.length} 项` + (this.error ? ` · ${this.error}` : ''));
      this.closeP2P();
      log('task', 'send end', {
        taskId: this.taskId,
        phase: this.phase,
        ok: `${okCount}/${this.items.length}`,
        p2p: this.p2p,
        acked,
      });
      void flushLogs();
      await this.archive();
      this.closeRooms();
    } catch (e) {
      const errMsg = typeof e === 'string' ? e : (e as Error)?.message || String(e);
      log('task', 'send failed', { taskId: this.taskId, error: errMsg });
      if (this.cancelled) {
        this.phase = 'cancelled';
      } else {
        this.phase = 'error';
        this.error = errMsg || '发送失败';
        for (const it of this.items) {
          if (it.status !== 'done') {
            it.status = 'error';
            it.error = it.error ?? this.error;
          }
        }
      }
      this.endedAt = Date.now();
      this.notify();
      this.closeP2P();
      await this.archive();
      this.closeRooms();
    }
  }

  private async sendTextItem(text: string, st: ItemState): Promise<void> {
    const total = utf8bytes(text);
    st.totalBytes = total;
    let sentChars = 0;
    for (let i = 0; i < text.length; i += TEXT_CHUNK_CHARS) {
      if (this.cancelled) throw new Error('已取消');
      await this.room!.sendText(text.slice(i, i + TEXT_CHUNK_CHARS));
      sentChars += Math.min(TEXT_CHUNK_CHARS, text.length - i);
      st.doneBytes = Math.round((sentChars / text.length) * total);
      this.trackSpeed(this.doneBytes);
      this.notify();
    }
  }

  private onPeer(): void {
    if (!this.peerJoined) {
      this.peerJoined = true;
      log('task', 'peer joined', { taskId: this.taskId });
      this.stopAnnounce();
    }
  }

  /** P2P is handled in Rust (online_p2p.rs). This is a no-op stub. */
  private startP2P(): void {
    // The Rust session starts P2P automatically in send_file().
    // P2P state arrives via fling://online/event { kind: 'p2p' }.
    this.p2pReady = Promise.resolve('open' as const);
  }

  private closeP2P(): void {
    this.stopDirectHealth('p2p_link_owner_closed');
    // Rust owns the P2P link; nothing to close here.
  }

  /** Keep old-file silence from leaking into a new file's source/manifest startup. */
  private markDirectTransferStart(): void {
    this.stopDirectHealth('waiting_for_first_direct_frame');
    this.lastWatchdogArmedAt = 0;
    this.lastPeerProgressAt = 0;
    if (this.p2p === 'open' && this.room?.directActive) {
      log('p2p', 'peer progress watchdog deferred until first accepted frame', {
        taskId: this.taskId,
        timerStage: 'source_manifest_first_frame_startup',
        currentFileStartedAt: this.currentFileStartedAt,
        currentFileSize: this.currentSendingItem?.size,
        firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
        timeoutMs: DIRECT_PEER_PROGRESS_TIMEOUT_MS,
      });
    }
  }

  /** Start the peer-progress watchdog only after DataChannel.send() accepts a data frame. */
  private startDirectHealthAfterFirstFrame(): void {
    if (this.p2p !== 'open' || !this.room?.directActive || !this.currentFileFirstFrameAccepted) return;
    const now = Date.now();
    this.lastWatchdogArmedAt = now;
    this.lastPeerProgressAt = now;
    this.watchdogStartedAt = now;
    this.watchdogPauseReason = null;
    log('p2p', 'peer progress watchdog started', {
      taskId: this.taskId,
      timerStage: PEER_PROGRESS_WATCHDOG_STAGE,
      timeoutMs: DIRECT_PEER_PROGRESS_TIMEOUT_MS,
      startedAt: now,
      armedAt: now,
      currentFileStartedAt: this.currentFileStartedAt,
      currentFileSize: this.currentSendingItem?.size,
      currentFilePhase: 'after_first_direct_frame_accepted',
      firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
      sentBytes: this.doneBytes,
      peerDoneBytes: this.peerDoneBytes,
      directChannelOpen: this.room.directActive,
    });
    const timer = startPeerProgressWatchdogAfterFrameAccepted(
      this.currentFileFirstFrameAccepted,
      DIRECT_HEALTH_CHECK_MS,
      () => {
      if (this.phase !== 'sending' || this.p2p !== 'open' || !this.room?.directActive) {
        this.stopDirectHealth('phase_changed_or_direct_path_lost');
        return;
      }
      const silentMs = Date.now() - this.lastPeerProgressAt;
      const pauseReason = this.currentFileFinalizing
        ? 'receiver_finalizing'
        : this.currentPeerProgressTarget !== null && this.peerDoneBytes >= this.currentPeerProgressTarget
          ? 'peer_reached_current_target'
          : null;
      if (pauseReason) {
        if (this.watchdogPauseReason !== pauseReason) {
          log('p2p', 'peer progress watchdog paused', {
            taskId: this.taskId, reason: pauseReason, silentMs,
            peerDoneBytes: this.peerDoneBytes, target: this.currentPeerProgressTarget,
          });
          this.watchdogPauseReason = pauseReason;
        }
        return;
      }
      if (this.watchdogPauseReason) {
        log('p2p', 'peer progress watchdog resumed', {
          taskId: this.taskId, previousReason: this.watchdogPauseReason, silentMs,
          peerDoneBytes: this.peerDoneBytes, target: this.currentPeerProgressTarget,
        });
        this.watchdogPauseReason = null;
      }
      if (!peerProgressTimeoutExpired(this.lastPeerProgressAt, Date.now(), DIRECT_PEER_PROGRESS_TIMEOUT_MS)) return;
      const reason = 'peer_progress_silence_timeout';
      log('p2p', 'peer progress watchdog timed out; relay fallback requested', {
        taskId: this.taskId,
        reason,
        timerStage: PEER_PROGRESS_WATCHDOG_STAGE,
        timeoutMs: DIRECT_PEER_PROGRESS_TIMEOUT_MS,
        silentMs,
        watchdogElapsedMs: Math.max(0, Date.now() - this.watchdogStartedAt),
        lastFileArmAt: this.lastWatchdogArmedAt,
        fileElapsedMs: this.currentFileStartedAt ? Math.max(0, Date.now() - this.currentFileStartedAt) : undefined,
        firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
        firstFrameNotYetAcceptedAtTimeout: !this.currentFileFirstFrameAccepted,
        lastProgressAt: this.lastPeerProgressAt,
        sentBytes: this.doneBytes,
        peerDoneBytes: this.peerDoneBytes,
        peerSavedBytes: this.peerSavedBytes,
        peerProgressTarget: this.currentPeerProgressTarget,
        currentFileFinalizing: this.currentFileFinalizing,
        directChannelOpen: this.room?.directActive ?? false,
        p2pState: this.p2p,
      });
      this.stopDirectHealth('watchdog_timeout');
      this.room?.fallbackToRelay(reason);
      },
    );
    if (timer === null) return;
    this.directHealthTimer = timer as ReturnType<typeof setInterval>;
  }

  private stopDirectHealth(reason = 'not_active'): void {
    if (this.directHealthTimer) {
      const now = Date.now();
      clearInterval(this.directHealthTimer);
      this.directHealthTimer = null;
      log('p2p', 'peer progress watchdog stopped', {
        taskId: this.taskId,
        reason,
        elapsedMs: this.watchdogStartedAt ? Math.max(0, now - this.watchdogStartedAt) : 0,
        timeoutMs: DIRECT_PEER_PROGRESS_TIMEOUT_MS,
        originalStartedAt: this.watchdogStartedAt,
        lastFileArmAt: this.lastWatchdogArmedAt,
        silentMs: this.lastPeerProgressAt ? Math.max(0, now - this.lastPeerProgressAt) : undefined,
        currentFileStartedAt: this.currentFileStartedAt,
        firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
        sentBytes: this.doneBytes,
        peerDoneBytes: this.peerDoneBytes,
        directChannelOpen: this.room?.directActive ?? false,
      });
    }
    this.watchdogPauseReason = null;
  }

  /** Wait for receiver bytes; relay mode uses a no-progress timeout, not a total-size timeout. */
  private async waitForPeerProgress(
    target: number,
    timeoutMs: number,
    timeoutOnSilence = false,
  ): Promise<boolean> {
    const start = Date.now();
    let lastObserved = this.peerDoneBytes;
    let lastAdvanceAt = start;
    while (!this.cancelled && this.peerDoneBytes < target) {
      await delay(200);
      if (this.peerDoneBytes > lastObserved) {
        lastObserved = this.peerDoneBytes;
        lastAdvanceAt = Date.now();
      }
      const elapsed = timeoutOnSilence ? Date.now() - lastAdvanceAt : Date.now() - start;
      if (elapsed >= timeoutMs) break;
    }
    const ok = this.peerDoneBytes >= target;
    if (!ok) {
      log('xfer', 'peer file confirmation timeout', {
        taskId: this.taskId,
        peerDoneBytes: this.peerDoneBytes,
        target,
        timeoutMs,
        timeoutOnSilence,
        direct: this.room?.directActive ?? false,
      });
    }
    return ok;
  }

  /** FlingRoom found a direct send/manifest/drain failure. Close the link owner. */
  private onDirectFallback(reason: string): void {
    log('p2p', 'sender observed relay fallback', {
      taskId: this.taskId,
      reason,
      sentBytes: this.doneBytes,
      peerDoneBytes: this.peerDoneBytes,
      peerSavedBytes: this.peerSavedBytes,
      currentFileStartedAt: this.currentFileStartedAt,
      firstDirectFrameAccepted: this.currentFileFirstFrameAccepted,
      watchdogTimeoutMs: DIRECT_PEER_PROGRESS_TIMEOUT_MS,
      directChannelOpen: this.room?.directActive ?? false,
      p2pState: this.p2p,
    });
    this.stopDirectHealth('room_relay_fallback');
    this.p2pLink?.fallbackToRelay(reason);
  }

  /**
   * 等接收方 task-ack（全部收齐并落盘）。超时/对方离开则返回 false，
   * 调用方照常关闭——旧版本接收方不会发 ack，不能无限等。
   */
  private waitForAck(timeoutMs: number): Promise<boolean> {
    if (this.peerFailures.length > 0) return Promise.resolve(false);
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.ackResolve = null;
        resolve(false);
      }, timeoutMs);
      this.ackResolve = (ok) => {
        clearTimeout(timer);
        this.ackResolve = null;
        resolve(ok);
      };
    });
  }

  /** 接收方拒绝（经 inbox / lobby 回来） */
  onRejected(): void {
    if (!this.active) return;
    log('task', 'sender observed peer rejection', { taskId: this.taskId, phase: this.phase, sentBytes: this.doneBytes, peerDoneBytes: this.peerDoneBytes, firstDirectFrameAccepted: this.currentFileFirstFrameAccepted });
    this.cancelled = true;
    this.stageAbortController.abort();
    this.phase = 'rejected';
    this.error = '对方拒绝了传输';
    for (const it of this.items) {
      if (it.status !== 'done') {
        it.status = 'error';
        it.error = '对方拒绝';
      }
    }
    this.endedAt = Date.now();
    clearTimeout(this.waitTimer);
    this.stopAnnounce();
    this.notify();
    this.closeP2P();
    void this.archive();
    this.closeRooms();
  }

  private async deliverOfferNearby(offer: OfferMessage, peerDeviceId: string): Promise<void> {
    const spec = {
      roomName: inboxRoomName(peerDeviceId),
      keyMaterial: 'inbox:' + peerDeviceId,
      keyContext: KEY_CTX.inbox,
    };
    // v0.6.13: 暂时回退到纯 LiveKit（v0.6.9 的局域网直连可能有问题，先保证基本功能）
    const sendOnce = async () => {
      const r = await connectFlingRoom(spec, {});
      try {
        await r.sendOffer(offer);
        await delay(1200);
      } finally {
        await r.close().catch(() => {});
      }
    };
    await sendOnce();
    // 对方 inbox 若短暂断线，8s 重投一次，直到对方加入或取消
    this.announceTimer = setInterval(() => {
      if (this.peerJoined || this.cancelled) return this.stopAnnounce();
      sendOnce().catch(() => {});
    }, 8000);
  }

  private async announceInLobby(offer: OfferMessage, code: string): Promise<void> {
    this.lobbyLeave = await manager.joinLobby(code);
    const lobby = manager.lobbyRoomFor(code);
    if (!lobby) throw new Error('配对房间连接失败');
    await lobby.sendOffer(offer);
    this.announceTimer = setInterval(() => {
      if (this.peerJoined || this.cancelled) return this.stopAnnounce();
      lobby.sendOffer(offer).catch(() => {});
    }, OFFER_REANNOUNCE_MS);
  }

  private stopAnnounce(): void {
    if (this.announceTimer) {
      clearInterval(this.announceTimer);
      this.announceTimer = undefined;
    }
  }

  cancel(): void {
    if (!this.active) return;
    log('task', 'sender cancellation requested', { taskId: this.taskId, phase: this.phase, sentBytes: this.doneBytes, peerDoneBytes: this.peerDoneBytes, currentFileSize: this.currentSendingItem?.size, firstDirectFrameAccepted: this.currentFileFirstFrameAccepted });
    this.cancelled = true;
    this.stageAbortController.abort();
    clearTimeout(this.waitTimer);
    this.stopAnnounce();
    // 尽力撤回 offer（对方若还没确认，待确认卡会消失）
    const withdraw = async () => {
      try {
        if (this.target.type === 'nearby') {
          const spec = {
            roomName: inboxRoomName(this.target.deviceId),
            keyMaterial: 'inbox:' + this.target.deviceId,
            keyContext: KEY_CTX.inbox,
          };
          const r = await connectFlingRoom(spec, {});
          await r.sendOfferWithdraw(this.taskId);
          await delay(800);
          await r.close().catch(() => {});
        } else {
          const lobby = manager.lobbyRoomFor(this.target.code);
          await lobby?.sendOfferWithdraw(this.taskId);
        }
      } catch { /* 尽力 */ }
    };
    void withdraw().finally(() => {
      this.phase = 'cancelled';
      this.endedAt = Date.now();
      for (const it of this.items) {
        if (it.status !== 'done') {
          it.status = 'error';
          it.error = '已取消';
        }
      }
      this.notify();
      void this.archive();
      this.closeRooms();
      if (this.lobbyLeave) {
        this.lobbyLeave();
        this.lobbyLeave = undefined;
      }
    });
  }

  private closeRooms(): void {
    if (this.lobbyLeave) {
      this.lobbyLeave();
      this.lobbyLeave = undefined;
    }
    const r = this.room;
    this.room = undefined;
    if (r) void r.close().catch(() => {});
  }

  private async archive(): Promise<void> {
    if (this.doneNotified) return;
    this.doneNotified = true;
    const items = await Promise.all(
      this.source.map(async (c, i) => {
        const st = this.items[i];
        const base = {
          name: st.name,
          path: c.relPath,
          size: c.size,
          kind: c.kind,
          status: (st.status === 'done' ? 'ok' : 'fail') as 'ok' | 'fail',
          error: st.error,
          sha256: st.sha256,
          mimeType: st.mimeType,
          transport: st.transport,
        };
        if (c.kind === 'text') return { ...base, text: c.text };
        if (c.kind === 'clipboard-image' && c.blob && c.blob.size < HIST_IMAGE_LIMIT) {
          try {
            const buf = new Uint8Array(await c.blob.arrayBuffer());
            return { ...base, imageB64: b64encode(buf) };
          } catch { /* 忽略 */ }
        }
        return base;
      }),
    );
    const entry: HistoryEntry = {
      taskId: this.taskId,
      dir: 'send',
      peer: this.peerLabel,
      time: this.startedAt,
      endedAt: this.endedAt,
      transport: summarizeTransport(this.items),
      items,
      totalBytes: this.totalBytes,
    };
    saveHistoryEntry(entry);
    manager.recentSources.set(this.taskId, this.source);
  }
}

// ================================================================== 接收任务
export type RecvPhase = 'receiving' | 'done' | 'error' | 'cancelled';

export interface PendingOffer {
  offer: OfferMessage;
  via: 'nearby' | 'remote';
  code?: string;
  at: number;
}

export class RecvTask {
  readonly taskId: string;
  phase: RecvPhase = 'receiving';
  items: ItemState[];
  startedAt = Date.now();
  endedAt?: number;
  speedBps = 0;
  error?: string;
  onChange: () => void = () => {};

  private room?: RustFlingRoom;
  private textParts: string[] = [];
  private textItem?: ItemState;
  private fileQueue: ItemState[] = [];
  private manifestToItem = new Map<string, ItemState>();
  private cancelled = false;
  private finished = false;
  private lastBytes = 0;
  private lastT = 0;
  private peerGoneTimer?: ReturnType<typeof setTimeout>;
  private savedDir = '';
  /** P2P 直连状态：idle=未尝试 connecting=握手中 open=直连传输中 failed=已回退中继 */
  p2p: P2PState = 'idle';
  private p2pLink: P2PLink | null = null;
  /** 正在落盘的 promise（task-done 到达时需等它们完成再发 task-ack） */
  private pendingSaves = new Set<Promise<void>>();
  /** ack 重试定时器（ack 可能丢包，重发直到发送方离开或超时） */
  private ackTimer: ReturnType<typeof setInterval> | null = null;
  /** Progress report trailing edge: a fast burst must still publish its final value. */
  private progressTimer: ReturnType<typeof setTimeout> | null = null;
  /** Serialize progress reports so async encryption cannot publish stale values after newer ones. */
  private progressSendChain: Promise<void> = Promise.resolve();
  /** 发送方已离开（收到 ack 后关闭房间，ack 重试可停） */
  private peerGone = false;
  /** 已收到 manifest 但还没落盘完成的个数（在途分块计数） */
  private pendingFiles = 0;
  private pendingFileIds = new Set<string>();
  /** 期望收到的 manifest 总数（offer 里 file/clipboard-image 的个数） */
  private expectedManifests = 0;
  /** 已收到的 manifest 个数 */
  private manifestsReceived = 0;
  /** 收到的剪贴板图片（供"另存为文件"） */
  clipImages = new Map<string, Uint8Array>();

  constructor(
    readonly offer: OfferMessage,
    readonly via: 'nearby' | 'remote',
    readonly code?: string,
  ) {
    this.taskId = offer.taskId;
    this.items = offer.items.map((o, i) => ({
      uid: `ri-${i}`,
      kind: o.kind,
      name: o.name,
      size: o.size,
      status: 'waiting',
      doneBytes: 0,
      totalBytes: o.size,
    }));
    this.textItem = this.items.find((i) => i.kind === 'text');
    this.fileQueue = this.items.filter((i) => i.kind !== 'text');
    // manifest 只对应文件/图片，不对应 text。新版 offer 明确带
    // totalFileItems；兼容旧版时从未截断的 items 里推导，避免
    // totalItems 把文本也算进去导致接收端平白等待 30 秒后失败。
    this.expectedManifests = offer.totalFileItems ?? (
      offer.totalItems !== undefined && offer.items.length === offer.totalItems
        ? offer.items.filter((item) => item.kind !== 'text').length
        : this.fileQueue.length
    );
  }

  get peerLabel(): string {
    return this.offer.fromDevice;
  }
  get totalBytes(): number {
    return this.items.reduce((a, i) => a + i.totalBytes, 0);
  }
  get doneBytes(): number {
    return this.items.reduce((a, i) => a + i.doneBytes, 0);
  }
  get active(): boolean {
    return this.phase === 'receiving';
  }

  get savingProgress(): { bytes: number; totalBytes: number } | undefined {
    const item = this.items.find((candidate) => candidate.saving);
    return item ? { bytes: item.saveBytes ?? 0, totalBytes: item.totalBytes } : undefined;
  }

  get savedBytes(): number {
    return this.items.reduce((sum, item) => sum + (
      item.kind === 'text' ? item.doneBytes : Math.min(item.saveBytes ?? 0, item.totalBytes)
    ), 0);
  }

  private lastNotify = 0;
  private notifyTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * UI 刷新节流：分块到来时每块都调 notify 会高频重绘，
   * 在慢机器上拖慢接收管线。300ms 内合并，尾调用保证最终状态刷出。
   */
  private notify(): void {
    const now = Date.now();
    const elapsed = now - this.lastNotify;
    if (elapsed >= 300) {
      this.lastNotify = now;
      this.onChange();
      return;
    }
    if (!this.notifyTimer) {
      this.notifyTimer = setTimeout(() => {
        this.notifyTimer = null;
        this.lastNotify = Date.now();
        this.onChange();
      }, 300 - elapsed);
    }
  }

  private trackSpeed(): void {
    const now = Date.now();
    const done = this.doneBytes;
    const dt = (now - this.lastT) / 1000;
    if (dt >= 0.4 && this.lastT > 0) {
      const inst = (done - this.lastBytes) / dt;
      this.speedBps = this.speedBps === 0 ? inst : this.speedBps * 0.7 + inst * 0.3;
      this.lastBytes = done;
      this.lastT = now;
    } else if (this.lastT === 0) {
      this.lastT = now;
      this.lastBytes = done;
    }
  }

  async run(): Promise<void> {
    try {
      this.savedDir = await manager.getDownloadDir();
      log('task', 'recv start', {
        taskId: this.taskId,
        peer: this.peerLabel,
        via: this.via,
        items: this.items.map((i) => ({ kind: i.kind, name: i.name, size: i.size })),
      });
      const spec =
        this.via === 'nearby'
          ? nearbyTaskSpecWithRoom(this.offer.taskRoom, this.offer.taskKey)
          : remoteTaskSpecWithRoom(this.offer.taskRoom, this.code ?? '');
      this.room = await connectFlingRoom(spec, {
        onText: (t) => this.onTextPart(t),
        onFileManifest: (m) => this.onManifest(m),
        onFileProgress: (id, r, t) => this.onProgress(id, r, t),
        onFileRoute: (route, id) => {
          // Rust reports p2p-open as a session-level event (empty id);
          // attribute the route to all items so the UI shows the ⚡ label.
          if (id) {
            const item = this.manifestToItem.get(id);
            if (item) item.transport = mergeTransport(item.transport, route);
          } else {
            for (const item of this.manifestToItem.values()) {
              item.transport = mergeTransport(item.transport, route);
            }
          }
          this.notify();
        },
        onFileCompleteStream: (id, m, receiver) => {
          // 落盘是异步的：跟踪起来，finish(true) 需要等它们全部完成再发 task-ack
          const p = this.onFileComplete(id, m, receiver);
          this.pendingSaves.add(p);
          void p.finally(() => this.pendingSaves.delete(p)).catch(() => {});
          return p;
        },
        onFileError: (id, err) => this.onFileError(id, err),
        onDirectFallback: (reason) => this.onDirectFallback(reason),
        onP2PSignal: (sig) => this.onP2PSignal(sig),
        onLog: (tag, msg) => log(tag as LogTag, `[room] ${msg}`),
        onTaskDone: (taskId) => {
          if (taskId === this.taskId) void this.finish(true);
        },
        onPeerLeft: () => this.onPeerLeft(),
      });
      this.notify();
    } catch (e) {
      this.phase = 'error';
      this.error = (e as Error).message || '接收失败';
      for (const it of this.items) {
        if (it.status !== 'done') {
          it.status = 'error';
          it.error = this.error;
        }
      }
      this.endedAt = Date.now();
      this.notify();
      this.closeP2P();
      void this.archive();
    }
  }

  private onTextPart(t: string): void {
    if (!this.textItem || this.finished) return;
    this.textParts.push(t);
    this.textItem.status = 'active';
    this.textItem.doneBytes += utf8bytes(t);
    if (this.textItem.doneBytes > this.textItem.totalBytes) {
      this.textItem.totalBytes = this.textItem.doneBytes;
    }
    // 文本收齐就打 ✓（不用等整个任务结束；写剪贴板仍在 finish 里统一做）
    if (this.textItem.doneBytes >= this.textItem.totalBytes) {
      this.textItem.status = 'done';
    }
    this.trackSpeed();
    this.notify();
    this.reportProgress();
  }

  private onManifest(m: FileManifest): void {
    const startedAt = Date.now();
    const xfer = m.id.slice(0, 8);
    log('xfer', 'manifest processing start', { taskId: this.taskId, xfer, size: m.size });
    // 诊断：manifest 重复到达会导致条目错配、doneBytes 虚高
    const dup = this.manifestToItem.has(m.id);
    // 按 offer 顺序匹配（data channel 有序可靠），辅以 文件名+大小 兜底
    let item = this.fileQueue.shift();
    let matchMode = 'queue';
    if (!item) {
      // offer.items 被截断时：manifest 对应的条目不在预览里，按 manifest 现场补建
      item = {
        uid: uid('ri'),
        kind: m.kind,
        name: m.name,
        size: m.size,
        status: 'waiting',
        doneBytes: 0,
        totalBytes: m.size,
      };
      this.items.push(item);
      matchMode = 'created';
    }
    if (item.name !== m.name || item.size !== m.size) {
      const alt = this.fileQueue.find((i) => i.name === m.name && i.size === m.size);
      if (alt) {
        this.fileQueue = this.fileQueue.filter((i) => i !== alt);
        this.fileQueue.unshift(item);
        item = alt;
        matchMode = 'name-size-fallback';
      } else {
        matchMode = 'queue-mismatch!';
      }
    }
    item.status = 'active';
    item.totalBytes = m.size;
    item.mimeType = m.mimeType;
    item.relPath = m.path;
    this.manifestToItem.set(m.id, item);
    if (!this.pendingFileIds.has(m.id)) {
      this.pendingFileIds.add(m.id);
      this.pendingFiles++;
    }
    this.manifestsReceived++;
    log('xfer', 'manifest recv', {
      taskId: this.taskId,
      mid: m.id.slice(0, 8),
      name: m.name,
      size: m.size,
      chunks: m.totalChunks,
      chunkSize: m.chunkSize,
      kind: m.kind,
      duplicate: dup,
      matchMode,
      queueLeft: this.fileQueue.length,
      itemsCount: this.items.length,
    });
    log('xfer', 'manifest processing end', {
      taskId: this.taskId, xfer, size: m.size, status: 'ok', elapsedMs: Date.now() - startedAt,
    });
    if (dup || matchMode === 'queue-mismatch!') {
      log('xfer', 'MANIFEST ANOMALY', {
        taskId: this.taskId, mid: m.id.slice(0, 8), name: m.name,
        duplicate: dup, matchMode,
      });
    }
    this.notify();
  }

  private onProgress(id: string, r: number, t: number): void {
    const item = this.manifestToItem.get(id);
    if (!item) return;
    // transfer-done signal: Rust uses MAX_SAFE_INTEGER to indicate completion
    // (file already persisted by Rust, SHA verified).
    if (r === Number.MAX_SAFE_INTEGER) {
      this.onTransferDone(id);
      return;
    }
    // 诊断：doneBytes 倒退说明条目错配或重复计数
    if (r < item.doneBytes) {
      log('xfer', 'PROGRESS BACKWARDS', {
        taskId: this.taskId, mid: id.slice(0, 8), name: item.name,
        was: item.doneBytes, now: r, total: t,
      });
    }
    item.doneBytes = r;
    item.totalBytes = t;
    this.trackSpeed();
    this.notify();
    this.reportProgress();
  }

  /** Rust transfer-done: file already persisted by Rust (SHA verified).
   * Mark the item done so UI leaves 0%. */
  private onTransferDone(transferId: string): void {
    const item = this.manifestToItem.get(transferId);
    if (!item) {
      log('xfer', 'transfer-done for unknown item', { taskId: this.taskId, transferId: transferId.slice(0, 8) });
      return;
    }
    item.doneBytes = item.totalBytes;
    item.saveBytes = item.totalBytes;
    item.status = 'done';
    // Rust path persists the file itself, so the legacy onFileComplete
    // finally-block that settles the pending counter never runs. Settle
    // here, or finish() stalls 30s in waitForPendingFiles.
    this.settlePendingFile(transferId);
    log('xfer', 'transfer-done marked', { taskId: this.taskId, name: item.name, size: item.totalBytes });
    this.notify();
    this.reportProgress(true);
    // Check if all items done, then finish.
    if (this.items.every((it) => it.status === 'done')) {
      void this.finish(true);
    }
  }

  private lastProgressReport = 0;
  /** 向发送方上报真实接收进度（1s 节流），让发送端显示"对方已接收"而非"已发送" */
  private reportProgress(force = false): void {
    const now = Date.now();
    if (this.finished) return;
    const elapsed = now - this.lastProgressReport;
    if (!force && elapsed < 1000) {
      // Direct chunks can arrive in a sub-second burst. Without a trailing
      // report, the sender may only see the first 32KB forever and replay the
      // whole file even though the receiver already has almost all chunks.
      if (!this.progressTimer) {
        this.progressTimer = setTimeout(() => {
          this.progressTimer = null;
          this.reportProgress();
        }, 1000 - elapsed);
      }
      return;
    }
    if (this.progressTimer) {
      clearTimeout(this.progressTimer);
      this.progressTimer = null;
    }
    this.lastProgressReport = now;
    const room = this.room;
    if (room) {
      const receivedBytes = this.doneBytes;
      const savedBytes = this.savedBytes;
      // 诊断：上报值不应超过 totalBytes；记录明细以便核对
      if (receivedBytes > this.totalBytes || savedBytes > receivedBytes) {
        log('xfer', 'REPORT OVERFLOW', {
          taskId: this.taskId, receivedBytes, savedBytes, total: this.totalBytes,
          items: this.items.map((i) => ({ name: i.name, received: i.doneBytes, saved: i.saveBytes ?? 0, total: i.totalBytes })),
        });
      }
      void this.enqueueProgressReport(receivedBytes, this.totalBytes, savedBytes);
    }
  }

  private enqueueProgressReport(receivedBytes: number, totalBytes: number, savedBytes: number): Promise<void> {
    const room = this.room;
    if (!room) return Promise.resolve();
    const next = this.progressSendChain.then(() => room.sendTaskProgress(this.taskId, receivedBytes, totalBytes, savedBytes));
    this.progressSendChain = next.catch(() => {});
    return this.progressSendChain;
  }

  private onFileError(id: string, err: string): void {
    const item = this.manifestToItem.get(id);
    if (!item) return;
    item.status = 'error';
    item.error = err;
    this.settlePendingFile(id);
    log('xfer', 'item failed', { taskId: this.taskId, xfer: id.slice(0, 8), size: item.totalBytes, status: 'error' });
    this.notify();
  }

  private async onFileComplete(id: string, m: FileManifest, receiver: FileReceiver): Promise<void> {
    const item = this.manifestToItem.get(id);
    if (!item) return;
    item.saving = true;
    item.saveBytes = 0;
    this.notify();
    // A small file may finish within the 1s throttle window. Send its exact
    // received byte count immediately so the sender can advance to the next
    // file without relying on the final task-ack.
    this.reportProgress(true);
    // Rust 在增量写入时核对 SHA-256；这里只记录通过保存验证后的 manifest 值供历史核对。
    item.sha256 = m.sha256;
    try {
      const keepImageBytes = m.kind === 'clipboard-image' && m.size < HIST_IMAGE_LIMIT
        ? new Uint8Array(m.size)
        : undefined;
      let imageOffset = 0;
      const onBatch = keepImageBytes
        ? (batch: Uint8Array) => {
            keepImageBytes.set(batch, imageOffset);
            imageOffset += batch.byteLength;
          }
        : undefined;
      if (m.kind === 'clipboard-image') {
        // 剪贴板图片存为 PNG 文件，不直接写入系统剪贴板（避免覆盖用户剪贴板内容）；
        // 用户需要时去历史记录里点"复制到剪贴板"
        const name = /\.png$/i.test(m.name) ? m.name : m.name + '.png';
        const savedPath = await saveReceivedFile(this.taskId, id, this.savedDir, name, receiver, null, onBatch, (writtenBytes) => {
          item.saveBytes = writtenBytes;
          this.notify();
          this.reportProgress();
        });
        log('xfer', 'clipboard image saved', { taskId: this.taskId, xfer: id.slice(0, 8), size: m.size });
        item.savedPath = savedPath;
        if (keepImageBytes) this.clipImages.set(item.uid, keepImageBytes);
      } else {
        const savedPath = await saveReceivedFile(this.taskId, id, this.savedDir, m.name, receiver, m.path ?? null, onBatch, (writtenBytes) => {
          item.saveBytes = writtenBytes;
          this.notify();
          this.reportProgress();
        });
        log('xfer', 'item saved', { taskId: this.taskId, xfer: id.slice(0, 8), size: m.size });
        item.savedPath = savedPath;
      }
      item.saving = false;
      item.saveBytes = item.totalBytes;
      item.status = 'done';
      item.doneBytes = item.totalBytes;
      // This exact report follows hash verification, fsync and atomic commit.
      this.reportProgress(true);
    } catch (e) {
      item.saving = false;
      item.status = 'error';
      item.error = errText(e);
      log('xfer', 'item save failed', { taskId: this.taskId, xfer: id.slice(0, 8), size: m.size, status: 'error' });
      this.notify();
      throw e;
    } finally {
      this.settlePendingFile(id);
    }
    this.notify();
  }

  private settlePendingFile(id: string): void {
    if (!this.pendingFileIds.delete(id)) return;
    this.pendingFiles = Math.max(0, this.pendingFiles - 1);
  }

  /** 等待在途分块收齐并落盘（task-done 可能先于部分分块到达）。超时返回 false。 */
  private async waitForPendingFiles(timeoutMs: number): Promise<boolean> {
    const start = Date.now();
    while (this.pendingFiles > 0 && Date.now() - start < timeoutMs) {
      await new Promise((r) => setTimeout(r, 200));
    }
    return this.pendingFiles === 0;
  }

  /**
   * 等待所有 manifest 到达。task-done 走中继、manifest 可能走直连，
   * 两条通道乱序时 task-done 可能先到——此时不能直接判定结束，
   * 否则还没到的 manifest 对应的文件会被误判失败。
   */
  private async waitForManifests(timeoutMs: number): Promise<boolean> {
    const start = Date.now();
    while (this.manifestsReceived < this.expectedManifests && Date.now() - start < timeoutMs) {
      await new Promise((r) => setTimeout(r, 200));
    }
    const ok = this.manifestsReceived >= this.expectedManifests;
    if (!ok) {
      log('task', 'wait manifests timeout', {
        taskId: this.taskId,
        got: this.manifestsReceived,
        want: this.expectedManifests,
      });
    }
    return ok;
  }

  /** Mirrors SendTask.closeP2P: in the Rust architecture the P2P link is
   * owned by the Rust session, so there is nothing to close here. This
   * method was missing on RecvTask while finish()/run() call it — the
   * resulting TypeError aborted finish() before task-ack was sent. */
  private closeP2P(): void {
    this.p2pLink = null;
  }

  /** Registered in the room events but never defined on RecvTask (only
   * SendTask had it). The Rust room never emits it today; keep a log-only
   * handler so a future emit can't throw. */
  private onDirectFallback(reason: string): void {
    log('p2p', 'receiver observed relay fallback', { taskId: this.taskId, reason });
  }

  private onPeerLeft(): void {
    // ack 已发出、房间保持打开等发送方确认：发送方离开 = 收到了 ack，停重试关房间
    if (this.finished && this.ackTimer) {
      this.peerGone = true;
      if (this.ackTimer) {
        clearInterval(this.ackTimer);
        this.ackTimer = null;
      }
      void this.closeRoom();
      return;
    }
    if (!this.active || this.finished) return;
    log('task', 'peer left', { taskId: this.taskId });
    // 发送方发完 task-done 后 2s 关房间；若 4s 内没收到 task-done，视为中断
    clearTimeout(this.peerGoneTimer);
    this.peerGoneTimer = setTimeout(() => {
      if (this.active && !this.finished) void this.finish(false);
    }, 4000);
  }

  /** offer 之前到达的远端 ICE candidate 缓存（发起方局域网 host candidate 极快，经常跑赢 offer，不能丢） */
  private earlyIce: RTCIceCandidateInit[] = [];

  /** P2P is handled in Rust. This is a no-op stub. */
  private onP2PSignal(_sig: unknown): void {
    // Rust consumes p2p-offer/p2p-answer/p2p-ice internally.
  }

  private async finish(completed: boolean): Promise<void> {
    if (this.finished) return;
    this.finished = true;
    if (this.progressTimer) {
      clearTimeout(this.progressTimer);
      this.progressTimer = null;
    }
    clearTimeout(this.peerGoneTimer);
    if (completed) {
      // task-done 可能先于 manifest/分块到达（中继与直连是两条通道）：
      // 先等齐 manifest，再等在途分块收齐、落盘完成，最后判定。
      // 注意此时不能关 P2P，否则在途分块会丢失。
      await this.waitForManifests(30_000);
      const drained = await this.waitForPendingFiles(30_000);
      if (!drained) {
        log('task', 'wait pending files timeout', { taskId: this.taskId, pending: this.pendingFiles });
      }
      await this.room?.flush().catch(() => {});
      await Promise.allSettled([...this.pendingSaves]);
    }
    this.closeP2P();
    // 文本：拼起来写剪贴板
    if (this.textItem && this.textParts.length > 0) {
      const full = this.textParts.join('');
      this.textItem.status = 'done';
      this.textItem.doneBytes = this.textItem.totalBytes;
      try {
        await writeClipboardText(full);
      } catch { /* 忽略 */ }
    }
    let failed = 0;
    for (const it of this.items) {
      if (it.status !== 'done') {
        it.status = 'error';
        it.error = it.error ?? (completed ? '未收到数据' : '对方断开连接');
        failed++;
      }
    }
    this.phase = failed === 0 ? 'done' : 'error';
    if (failed > 0) this.error = `${failed} 项未完成`;
    this.endedAt = Date.now();
    this.notify();
    // 系统通知（纯加法，不影响传输逻辑）
    notifyTaskDone('recv', this.phase === 'done',
      `${this.peerLabel} · ${this.items.length} 项` + (this.error ? ` · ${this.error}` : ''));
    log('task', 'recv end', {
      taskId: this.taskId,
      completed,
      phase: this.phase,
      failed,
      p2p: this.p2p,
      items: this.items.map((i) => ({ kind: i.kind, size: i.totalBytes, status: i.status })),
    });
    void flushLogs();
    await this.archive();

    // 全部收齐并落盘：告诉发送方可以拆连接了（发送方收到前不会主动断开）
    // ack 走中继但仍可能丢包（接收端发完就关房间）：flush 后每 3s 重发，
    // 直到发送方离开（onPeerLeft）或 30s 超时，保证"接收成功但发送端未确认"不再发生。
    // 注意：房间此时不关，等发送方确认后离开再关。
    if (completed && failed === 0 && this.room) {
      const room = this.room;
      const sendAck = () => {
        room.sendTaskAck(this.taskId).catch(() => {});
        log('task', 'task-ack sent', { taskId: this.taskId });
      };
      try {
        // 先发最终进度（100%），再发 ack：避免发送端停留在过期的百分比上
        await this.enqueueProgressReport(this.doneBytes, this.totalBytes, this.savedBytes);
        sendAck();
        await room.flush().catch(() => {});
      } catch {
        /* 忽略 */
      }
      let retries = 0;
      this.ackTimer = setInterval(() => {
        if (this.peerGone || retries >= 10) {
          if (this.ackTimer) clearInterval(this.ackTimer);
          this.ackTimer = null;
          void this.closeRoom();
          return;
        }
        retries++;
        sendAck();
      }, 3000);
      return;
    }
    if (completed && failed > 0 && this.room) {
      const failures = this.items
        .filter((it) => it.status !== 'done')
        .map((it) => ({ name: it.name, size: it.totalBytes, error: it.error ?? '接收或保存失败' }));
      try {
        await this.room.sendTaskFailed(this.taskId, failures);
        await this.room.flush();
        log('task', 'task-failed sent', { taskId: this.taskId, failureCount: failures.length });
      } catch { /* 尽力通知发送方具体失败原因 */ }
    }
    await this.closeRoom();
  }

  /** 关闭房间（ack 重试结束或发送方已离开时调用） */
  private async closeRoom(): Promise<void> {
    if (this.ackTimer) {
      clearInterval(this.ackTimer);
      this.ackTimer = null;
    }
    const r = this.room;
    this.room = undefined;
    if (r) await r.close().catch(() => {});
  }

  async cancel(): Promise<void> {
    if (!this.active) return;
    log('task', 'receiver cancellation requested', { taskId: this.taskId, phase: this.phase, receivedBytes: this.items.reduce((sum, item) => sum + item.doneBytes, 0), savedBytes: this.items.reduce((sum, item) => sum + (item.saveBytes ?? 0), 0) });
    this.cancelled = true;
    // 先告诉发送方"我取消了"再拆连接；flush 保证取消信令发出去，
    // 否则发送方等 ack 超时（以前还会误标"成功"）。
    try {
      await this.room?.sendTaskCancel(this.taskId);
    } catch {
      /* 忽略 */
    }
    try {
      await this.room?.flush();
    } catch {
      /* 忽略 */
    }
    log('task', 'recv cancelled, task-cancel sent', { taskId: this.taskId });
    void this.finish(false).then(() => {
      log('task', 'receiver cancellation cleanup complete', {
        taskId: this.taskId,
        receivedBytes: this.items.reduce((sum, item) => sum + item.doneBytes, 0),
        savedBytes: this.items.reduce((sum, item) => sum + (item.saveBytes ?? 0), 0),
        pendingSaveCount: this.pendingSaves.size,
      });
      this.phase = 'cancelled';
      this.notify();
    });
  }

  private async archive(): Promise<void> {
    const items = await Promise.all(
      this.items.map(async (it) => {
        const base = {
          name: it.name,
          path: it.relPath,
          size: it.size,
          kind: it.kind,
          status: (it.status === 'done' ? 'ok' : 'fail') as 'ok' | 'fail',
          error: it.error,
          savedPath: it.savedPath,
          sha256: it.sha256,
          mimeType: it.mimeType,
        };
        if (it.kind === 'text') return { ...base, text: this.textParts.join('') };
        if (it.kind === 'clipboard-image') {
          const bytes = this.clipImages.get(it.uid);
          if (bytes && bytes.length < HIST_IMAGE_LIMIT) {
            return { ...base, imageB64: b64encode(bytes) };
          }
        }
        return base;
      }),
    );
    const entry: HistoryEntry = {
      taskId: this.taskId,
      dir: 'recv',
      peer: this.offer.fromDevice,
      time: this.startedAt,
      items,
      totalBytes: this.totalBytes,
    };
    saveHistoryEntry(entry);
  }
}

// ================================================================== 全局管理器
type ChangeFn = () => void;

class TransferManager {
  sendTasks = new Map<string, SendTask>();
  recvTasks = new Map<string, RecvTask>();
  pending = new Map<string, PendingOffer>();
  peers = new Map<string, string>(); // deviceId -> name
  /** 本 session 内发送源（供失败重试） */
  recentSources = new Map<string, ComposeItem[]>();

  private listeners = new Set<ChangeFn>();
  private inboxRoom?: RustFlingRoom;
  private lobbies = new Map<string, { room: RustFlingRoom; refs: number }>();
  private downloadDir = '';
  /**
   * 高频进度回调（每分块一次）会触发全量 UI 重渲染；在慢机器上不加节流会
   * 拖垮事件循环，进而拖垮传输。这里把渲染上限压到 4 次/秒（首尾都触发）。
   */
  private notifyTimer?: ReturnType<typeof setTimeout>;
  private notifyQueued = false;

  onChange(fn: ChangeFn): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }
  notify(): void {
    if (this.notifyTimer) {
      this.notifyQueued = true;
      return;
    }
    this.emitNotify();
    this.notifyTimer = setTimeout(() => {
      this.notifyTimer = undefined;
      if (this.notifyQueued) {
        this.notifyQueued = false;
        this.emitNotify();
      }
    }, 250);
  }
  private emitNotify(): void {
    for (const fn of this.listeners) {
      try {
        fn();
      } catch { /* 忽略 */ }
    }
  }

  async init(): Promise<void> {
    // 常驻 inbox：收附近设备的 offer
    const spec = inboxSpec();
    this.inboxRoom = await connectFlingRoom(spec, {
      onOffer: (offer) => this.onOffer(offer, 'nearby'),
      onOfferWithdraw: (taskId) => {
        if (this.pending.delete(taskId)) {
          toast('对方取消了传输');
          this.notify();
        }
      },
      onOfferRejected: (taskId) => this.routeRejected(taskId),
    });

    await listen<{ id: string; name: string }>('fling://peer-up', (e) => {
      this.peers.set(e.payload.id, e.payload.name);
      this.notify();
    });
    await listen<{ id: string }>('fling://peer-down', (e) => {
      this.peers.delete(e.payload.id);
      this.notify();
    });
    await invoke('discovery_set_browsing', { on: true });
    await invoke('discovery_set_discoverable', { on: settings.discoverable });
    // v0.6.9 曾在此启动 LAN 运行时，但导致初始化失败；回退到按需启动
  }

  async setDiscoverable(on: boolean): Promise<void> {
    await invoke('discovery_set_discoverable', { on });
  }

  async getDownloadDir(): Promise<string> {
    if (!this.downloadDir) {
      this.downloadDir = (await invoke('get_download_dir')) as string;
    }
    return this.downloadDir;
  }
  setDownloadDirCache(dir: string): void {
    this.downloadDir = dir;
  }

  // ---------- lobby（远程配对）引用计数 ----------
  async joinLobby(code: string): Promise<() => void> {
    const c = normalizeCode(code);
    let entry = this.lobbies.get(c);
    if (!entry) {
      const spec = await lobbySpec(c);
      const room = await connectFlingRoom(spec, {
        onOffer: (offer) => this.onOffer(offer, 'remote', c),
        onOfferWithdraw: (taskId) => {
          if (this.pending.delete(taskId)) {
            toast('对方取消了传输');
            this.notify();
          }
        },
        onOfferRejected: (taskId) => this.routeRejected(taskId),
      });
      entry = { room, refs: 0 };
      this.lobbies.set(c, entry);
    }
    entry.refs++;
    let left = false;
    return () => {
      if (left) return;
      left = true;
      const e = this.lobbies.get(c);
      if (!e) return;
      e.refs--;
      if (e.refs <= 0) {
        this.lobbies.delete(c);
        void e.room.close().catch(() => {});
      }
    };
  }

  lobbyRoomFor(code: string): RustFlingRoom | undefined {
    return this.lobbies.get(normalizeCode(code))?.room;
  }

  // ---------- offer ----------
  private onOffer(offer: OfferMessage, via: 'nearby' | 'remote', code?: string): void {
    if (this.pending.has(offer.taskId) || this.recvTasks.has(offer.taskId)) return;
    // 自己发出的 offer 回环（理论上不会，自己不在 inbox 里）
    if (offer.fromDeviceId === getDeviceId()) return;
    this.pending.set(offer.taskId, { offer, via, code, at: Date.now() });
    const n = offer.items.length;
    const summary =
      n === 1 && offer.items[0].kind === 'text'
        ? '文本'
        : `${n} 个项目`;
    toast(`${offer.fromDevice} 发来 ${summary}，等待确认`);
    this.notify();
  }

  private routeRejected(taskId: string): void {
    const t = this.sendTasks.get(taskId);
    if (t) t.onRejected();
  }

  // ---------- 发送 ----------
  startSend(source: ComposeItem[], target: SendTarget): SendTask {
    const task = new SendTask(source, target);
    task.onChange = () => this.notify();
    this.sendTasks.set(task.taskId, task);
    this.notify();
    void task.run().finally(() => this.notify());
    return task;
  }

  /** 重试发送任务里失败的条目（用本 session 缓存的源） */
  retryFailed(taskId: string): void {
    const task = this.sendTasks.get(taskId);
    const sources = this.recentSources.get(taskId);
    if (!task || !sources) {
      toast('源文件已不在缓存中，请重新添加', true);
      return;
    }
    const failedIdx = new Set(
      task.items.filter((i) => i.status === 'error').map((i) => i.uid),
    );
    const retrySources = sources.filter((s) => failedIdx.has(s.uid));
    if (retrySources.length === 0) {
      toast('没有可重试的失败项');
      return;
    }
    this.startSend(retrySources, task.target);
    toast(`开始重试 ${retrySources.length} 个失败项`);
  }

  // ---------- 接收 ----------
  acceptOffer(taskId: string): RecvTask | undefined {
    const p = this.pending.get(taskId);
    if (!p) return undefined;
    this.pending.delete(taskId);
    const task = new RecvTask(p.offer, p.via, p.code);
    task.onChange = () => this.notify();
    this.recvTasks.set(taskId, task);
    this.notify();
    void task.run().finally(() => this.notify());
    return task;
  }

  rejectOffer(taskId: string): void {
    const p = this.pending.get(taskId);
    if (!p) return;
    this.pending.delete(taskId);
    // 通知发送方
    const sendReject = async () => {
      try {
        if (p.via === 'nearby') {
          const spec = {
            roomName: inboxRoomName(p.offer.fromDeviceId),
            keyMaterial: 'inbox:' + p.offer.fromDeviceId,
            keyContext: KEY_CTX.inbox,
          };
          const r = await connectFlingRoom(spec, {});
          await r.sendOfferRejected(taskId);
          await delay(800);
          await r.close().catch(() => {});
        } else if (p.code) {
          const lobby = this.lobbyRoomFor(p.code);
          await lobby?.sendOfferRejected(taskId);
        }
      } catch { /* 尽力 */ }
    };
    void sendReject();
    toast('已拒绝');
    this.notify();
  }

  // ---------- badge 计数 ----------
  get sendActiveCount(): number {
    let n = 0;
    for (const t of this.sendTasks.values()) if (t.active) n++;
    return n;
  }
  get recvBadgeCount(): number {
    let n = this.pending.size;
    for (const t of this.recvTasks.values()) if (t.active) n++;
    return n;
  }

  /** 成功任务展示 5s 后自动收起（UI 层调用） */
  autoDismiss(task: SendTask | RecvTask, remove: (t: SendTask | RecvTask) => void): void {
    setTimeout(() => {
      if ((task.phase === 'done' || task.phase === 'cancelled') && !task.active) {
        remove(task);
      }
    }, 5000);
  }
}

export const manager = new TransferManager();
