// 传输引擎：发送任务 / 接收任务 / 全局管理器
// 一次发送动作 = 一个任务 = 独立 LiveKit 房间，任务之间互不阻塞。
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { writeText as writeClipboardText } from '@tauri-apps/plugin-clipboard-manager';
import {
  wrapWebFile,
  type FileManifest,
  type FlingMessage,
  type OfferItem,
  type OfferMessage,
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
import { b64encode, b64decode, toast, uid, utf8bytes } from './ui.js';
import { P2PLink, type P2PState } from './p2p.js';
import { log, flushLogs, type LogTag } from './log.js';
import { notifyTaskDone } from './notify.js';
import type { FlingRoom } from '@fling/core';

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
  /** 文件 sha256（用于完整性核对） */
  sha256?: string;
}

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));
const TEXT_CHUNK_CHARS = 15000;
const OFFER_REANNOUNCE_MS = 5000;
const WAIT_PEER_TIMEOUT_MS = 5 * 60 * 1000;

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

  private room?: FlingRoom;
  private p2pLink: P2PLink | null = null;
  /** P2P 就绪（open/failed），文件发送前等待，避免前半走慢中继 */
  private p2pReady: Promise<'open' | 'failed'> | null = null;
  /** 接收方上报的真实接收进度（task-progress），UI 显示"对方已接收" */
  peerDoneBytes = 0;
  peerTotalBytes = 0;
  /** 等待接收方 task-ack 的 resolver（收到确认/对方离开/超时后置空） */
  private ackResolve: ((ok: boolean) => void) | null = null;
  private lobbyLeave?: () => void;
  private announceTimer?: ReturnType<typeof setInterval>;
  private waitTimer?: ReturnType<typeof setTimeout>;
  private peerJoined = false;
  private cancelled = false;
  /** 接收方中途取消（收到 task-cancel）：不再标成功 */
  private peerCancelled = false;
  private lastBytes = 0;
  private lastT = 0;
  private doneNotified = false;

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

  get peerLabel(): string {
    return this.target.type === 'nearby' ? this.target.deviceName : `配对码 ${this.target.code}`;
  }
  get waitLabel(): string {
    return this.target.type === 'nearby' ? '连接设备' : '等待对方加入';
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
        peer: this.peerLabel,
        items: this.items.map((i) => ({ kind: i.kind, name: i.name, size: i.size })),
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
        // 接收端用 totalItems 做 expectedManifests，缺的条目由 manifest 到达时补建。
        items: this.source.slice(0, 30).map(toOfferItem),
        totalItems: this.source.length,
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
            this.ackResolve?.(true);
          }
        },
        onTaskCancel: (taskId) => {
          if (taskId === this.taskId) {
            log('task', 'task-cancel received', { taskId });
            this.peerCancelled = true;
            this.ackResolve?.(false);
          }
        },
        onTaskProgress: (taskId, doneBytes, totalBytes) => {
          if (taskId === this.taskId) {
            // 诊断：对方上报的已收到不应超过我已发送的；倒退/超发都说明计数错乱
            const mySent = this.doneBytes;
            if (doneBytes > mySent + 65536) {
              log('xfer', 'PEER PROGRESS EXCEEDS SENT', {
                taskId, peerDone: doneBytes, mySent, peerTotal: totalBytes,
              });
            }
            if (doneBytes < this.peerDoneBytes) {
              log('xfer', 'PEER PROGRESS BACKWARDS', {
                taskId, was: this.peerDoneBytes, now: doneBytes,
              });
            }
            this.peerDoneBytes = doneBytes;
            this.peerTotalBytes = totalBytes;
            this.notify();
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
        if (src.kind !== 'text' && !p2pAwaited) {
          p2pAwaited = true;
          const ready = (await this.p2pReady?.catch(() => 'failed' as const)) ?? 'failed';
          log('p2p', ready === 'open' ? 'ready, files go direct' : 'not ready, files go relay', {
            taskId: this.taskId,
          });
        }
        st.status = 'active';
        this.notify();
        log('xfer', 'send item start', { taskId: this.taskId, kind: src.kind, name: st.name, size: st.size });
        try {
          if (src.kind === 'text') {
            await this.sendTextItem(src.text ?? '', st);
          } else {
            // 文件夹/大文件可能是懒加载：发送时才从磁盘读
            let blob = src.blob;
            if (!blob && src.filePath) {
              const b64 = (await invoke('read_file_bytes', { path: src.filePath })) as string;
              blob = new Blob([b64decode(b64) as BlobPart]);
            }
            if (!blob) throw new Error('文件内容不可用');
            await this.room!.sendFile(wrapWebFile(blob, src.name, src.relPath), (sent) => {
              st.doneBytes = sent;
              this.trackSpeed(this.doneBytes);
              this.notify();
            }, src.kind);
          }
          st.status = 'done';
          st.doneBytes = st.totalBytes;
          okCount++;
          log('xfer', 'send item done', { taskId: this.taskId, name: st.name, size: st.size });
        } catch (e) {
          st.status = 'error';
          st.error = (e as Error).message || '发送失败';
          log('xfer', 'send item error', { taskId: this.taskId, name: st.name, error: st.error });
        }
        this.notify();
      }

      // 5. 收尾：等接收方确认全部落盘（task-ack）再拆连接。
      // 以前是 task-done 后固定等 2s 就关——"发送成功"只是数据进了发送缓冲，
      // 接收方可能还没收完，这是"发送端成功但接收端失败"的根因。
      try {
        await this.room!.sendTaskDone(this.taskId);
        log('task', 'task-done sent', { taskId: this.taskId });
      } catch { /* 尽力 */ }
      const acked = await this.waitForAck(30_000);
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
        this.error = '对方未确认接收';
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
        error: this.error,
      });
      void flushLogs();
      void this.archive();
      this.closeRooms();
    } catch (e) {
      if (this.cancelled) {
        this.phase = 'cancelled';
      } else {
        this.phase = 'error';
        this.error = (e as Error).message || '发送失败';
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
      void this.archive();
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

  /** 发起 P2P 直连（发送方做 initiator）。 */
  private startP2P(): void {
    if (this.p2pLink || !this.room || this.cancelled) return;
    const link = new P2PLink(this.room, 'initiator', this.peerLabel);
    this.p2pLink = link;
    // 就绪 promise：open 或明确失败时 resolve，12s 超时则按失败处理。
    // 文件在 P2P 结论出来前不发送——否则前半部分走慢速中继，永远追不上。
    this.p2pReady = new Promise<'open' | 'failed'>((resolve) => {
      const timer = setTimeout(() => resolve('failed'), 12_000);
      link.onstate = (s) => {
        this.p2p = s;
        this.notify();
        if (s === 'open' || s === 'failed') {
          clearTimeout(timer);
          resolve(s);
        }
      };
    });
    void link.start();
  }

  private closeP2P(): void {
    this.p2pLink?.close();
    this.p2pLink = null;
  }

  /**
   * 等接收方 task-ack（全部收齐并落盘）。超时/对方离开则返回 false，
   * 调用方照常关闭——旧版本接收方不会发 ack，不能无限等。
   */
  private waitForAck(timeoutMs: number): Promise<boolean> {
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
    this.cancelled = true;
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
    this.cancelled = true;
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
          size: c.size,
          kind: c.kind,
          status: (st.status === 'done' ? 'ok' : 'fail') as 'ok' | 'fail',
          error: st.error,
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

  private room?: FlingRoom;
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
  /** 发送方已离开（收到 ack 后关闭房间，ack 重试可停） */
  private peerGone = false;
  /** 已收到 manifest 但还没落盘完成的个数（在途分块计数） */
  private pendingFiles = 0;
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
    // offer.items 可能被截断（64KB 上限），用 totalItems 做真正的期望数；
    // 老版本发送方无 totalItems 时回退 items.length。
    this.expectedManifests = offer.totalItems ?? this.fileQueue.length;
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
        savedDir: this.savedDir,
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
        onFileComplete: (id, m, bytes) => {
          // 落盘是异步的：跟踪起来，finish(true) 需要等它们全部完成再发 task-ack
          const p = this.onFileComplete(id, m, bytes);
          this.pendingSaves.add(p);
          void p.finally(() => this.pendingSaves.delete(p));
        },
        onFileError: (id, err) => this.onFileError(id, err),
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
  }

  private onManifest(m: FileManifest): void {
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
    this.manifestToItem.set(m.id, item);
    this.pendingFiles++;
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

  private lastProgressReport = 0;
  /** 向发送方上报真实接收进度（1s 节流），让发送端显示"对方已接收"而非"已发送" */
  private reportProgress(): void {
    const now = Date.now();
    if (now - this.lastProgressReport < 1000 || this.finished) return;
    this.lastProgressReport = now;
    const room = this.room;
    if (room) {
      const d = this.doneBytes;
      // 诊断：上报值不应超过 totalBytes；记录明细以便核对
      if (d > this.totalBytes) {
        log('xfer', 'REPORT OVERFLOW', {
          taskId: this.taskId, done: d, total: this.totalBytes,
          items: this.items.map((i) => ({ name: i.name, done: i.doneBytes, total: i.totalBytes })),
        });
      }
      void room.sendTaskProgress(this.taskId, d, this.totalBytes).catch(() => {});
    }
  }

  private onFileError(id: string, err: string): void {
    const item = this.manifestToItem.get(id);
    if (!item) return;
    item.status = 'error';
    item.error = err;
    this.pendingFiles = Math.max(0, this.pendingFiles - 1);
    log('xfer', 'item failed', { taskId: this.taskId, name: item.name, error: err });
    this.notify();
  }

  private async onFileComplete(id: string, m: FileManifest, bytes: Uint8Array): Promise<void> {
    const item = this.manifestToItem.get(id);
    if (!item) return;
    // sha256 已在 assemble() 里校验过（不一致会抛错进 catch），这里记录供历史核对
    item.sha256 = m.sha256;
    try {
      if (m.kind === 'clipboard-image') {
        // 剪贴板图片存为 PNG 文件，不直接写入系统剪贴板（避免覆盖用户剪贴板内容）；
        // 用户需要时去历史记录里点"复制到剪贴板"
        const name = /\.png$/i.test(m.name) ? m.name : m.name + '.png';
        const savedPath = (await invoke('save_received_file', {
          dir: this.savedDir,
          name,
          dataB64: b64encode(bytes),
          relPath: null,
        })) as string;
        log('xfer', 'clipboard image saved', { taskId: this.taskId, name, size: bytes.length, path: savedPath });
        item.savedPath = savedPath;
        if (bytes.length < HIST_IMAGE_LIMIT) this.clipImages.set(item.uid, bytes);
      } else {
        const savedPath = (await invoke('save_received_file', {
          dir: this.savedDir,
          name: m.name,
          dataB64: b64encode(bytes),
          relPath: m.path ?? null,
        })) as string;
        log('xfer', 'item saved', { taskId: this.taskId, name: m.name, size: bytes.length, path: savedPath });
        item.savedPath = savedPath;
      }
      item.status = 'done';
      item.doneBytes = item.totalBytes;
    } catch (e) {
      item.status = 'error';
      item.error = errText(e);
      log('xfer', 'item save failed', { taskId: this.taskId, name: item.name, error: item.error });
    } finally {
      this.pendingFiles = Math.max(0, this.pendingFiles - 1);
    }
    this.notify();
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

  /** P2P 信令：发送方发来 offer 则以 responder 身份应答，其余转给已建的链路。 */
  private onP2PSignal(sig: Extract<FlingMessage, { kind: `p2p-${string}` }>): void {
    if (this.finished) return;
    if (sig.kind === 'p2p-offer') {
      if (this.p2pLink || !this.room) return;
      const link = new P2PLink(this.room, 'responder', this.peerLabel);
      this.p2pLink = link;
      link.onstate = (s) => {
        this.p2p = s;
        this.notify();
      };
      // 把 offer 之前到达的 candidate 补上
      const early = this.earlyIce;
      this.earlyIce = [];
      for (const init of early) link.addRemoteIce(init);
      void link.acceptOffer(sig.sdp);
      return;
    }
    if (sig.kind === 'p2p-ice' && !this.p2pLink) {
      // offer 还没到：先缓存，不能丢弃
      if (sig.candidate) {
        try {
          this.earlyIce.push(JSON.parse(sig.candidate) as RTCIceCandidateInit);
        } catch {
          /* ignore */
        }
      }
      return;
    }
    void this.p2pLink?.handleSignal(sig);
  }

  private closeP2P(): void {
    this.p2pLink?.close();
    this.p2pLink = null;
  }

  private async finish(completed: boolean): Promise<void> {
    if (this.finished) return;
    this.finished = true;
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
      items: this.items.map((i) => ({ kind: i.kind, name: i.name, status: i.status, error: i.error })),
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
        await room.sendTaskProgress(this.taskId, this.doneBytes, this.totalBytes).catch(() => {});
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
      this.phase = 'cancelled';
      this.notify();
    });
  }

  private async archive(): Promise<void> {
    const items = await Promise.all(
      this.items.map(async (it) => {
        const base = {
          name: it.name,
          size: it.size,
          kind: it.kind,
          status: (it.status === 'done' ? 'ok' : 'fail') as 'ok' | 'fail',
          error: it.error,
          savedPath: it.savedPath,
          sha256: it.sha256,
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
  private inboxRoom?: FlingRoom;
  private lobbies = new Map<string, { room: FlingRoom; refs: number }>();
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

  lobbyRoomFor(code: string): FlingRoom | undefined {
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
