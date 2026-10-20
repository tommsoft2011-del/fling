// Rust-backed online room: replaces the TypeScript FlingRoom +
// livekit-client + RTCPeerConnection for online transfers.
//
// The WebView only displays; signaling and the data plane both live in
// Rust (online_session.rs). This class implements the subset of the
// FlingRoom interface used by tasks.ts, delegating to Tauri commands.

import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import type {
  FileKind,
  FileManifest,
  FlingRoomEvents,
} from '@fling/core';
import type { FileLike } from '@fling/core';
import { log } from './log.js';
import { KEY_CTX, mintToken } from './net.js';
import type { RoomSpec } from './net.js';

interface OnlineUiEvent {
  session: string;
  event: {
    kind: string;
    [key: string]: unknown;
  };
}

/** Write in-memory file bytes to a temp file so Rust can stream it. */
async function ensureFilePath(file: FileLike): Promise<string> {
  // Path-backed source (drag-drop via Tauri native events / folder items):
  // hand the real disk path straight to Rust. NOTE: `file.path` is NOT a
  // disk path — it is the in-folder relative path; the disk path rides
  // along as `diskPath`.
  const diskPath = (file as unknown as { diskPath?: string }).diskPath;
  if (diskPath) return diskPath;
  // In-memory source (file picker, HTML drag-drop, paste): the blob has no
  // disk path. Spill it to a temp file in 4 MiB chunks over Tauri's native
  // binary IPC (a bare Uint8Array invoke arg is sent as application/octet-stream,
  // no JSON, no base64 on file content): frame = [u32 LE path_len][path UTF-8][chunk].
  const tmp = (await invoke('online_temp_begin', { name: file.name })) as string;
  const pathBytes = new TextEncoder().encode(tmp);
  const header = new Uint8Array(4 + pathBytes.length);
  new DataView(header.buffer).setUint32(0, pathBytes.length, true);
  header.set(pathBytes, 4);
  const CHUNK = 4 * 1024 * 1024;
  try {
    let lastSpillLogFrac = 0;
    for (let off = 0; off < file.size; off += CHUNK) {
      const bytes = await file.slice(off, Math.min(off + CHUNK, file.size));
      const frame = new Uint8Array(header.length + bytes.length);
      frame.set(header);
      frame.set(bytes, header.length);
      await invoke('online_temp_append', frame);
      const frac = (off + bytes.length) / file.size;
      if (frac - lastSpillLogFrac >= 0.25 || frac >= 1) {
        lastSpillLogFrac = frac;
        log('p2p', 'temp spill progress', {
          spilledBytes: off + bytes.length,
          fileSize: file.size,
        });
      }
    }
  } catch (e) {
    log('p2p', 'temp spill failed', { fileSize: file.size, errMsg: String(e) });
    throw e;
  }
  log('p2p', 'temp spill complete', { fileSize: file.size });
  return tmp;
}

export class RustFlingRoom {
  private sessionId: string | null = null;
  private unlisten: (() => void) | null = null;
  private events: FlingRoomEvents;
  /** FlingRoom compat: whether a direct P2P path is active. */
  directActive = false;
  /** Live progress callback of the currently sending file (set BEFORE
   * the blocking invoke, since Rust emits progress DURING the transfer). */
  private activeSendProgress: ((sent: number, total: number) => void) | null = null;

  constructor(events: FlingRoomEvents) {
    this.events = events;
  }

  async connect(spec: RoomSpec, serverUrl: string): Promise<void> {
    const token = await mintToken(spec.roomName);
    this.sessionId = (await invoke('online_connect', {
      serverUrl,
      token,
      pairingCode: spec.keyMaterial,
      keyContext: spec.keyContext,
    })) as string;

    this.unlisten = await listen<OnlineUiEvent>('fling://online/event', (e) => {
      if (e.payload.session !== this.sessionId) return;
      this.dispatch(e.payload.event);
    });
  }

  private dispatch(ev: OnlineUiEvent['event']): void {
    const events = this.events;
    try {
      switch (ev.kind) {
        case 'text':
          events.onText?.(ev.text as string, ev.from as string);
          break;
        case 'peer': {
          const identity = ev.identity as string;
          if (ev.joined) events.onPeerJoined?.(identity);
          else events.onPeerLeft?.(identity);
          break;
        }
        case 'progress':
          // Receiver-side or sender-side progress.
          events.onFileProgress?.(
            ev.transfer_id as string,
            ev.sent_bytes as number,
            ev.total_bytes as number,
          );
          // Sender-side live progress: route to the in-flight sendFile
          // callback (installed BEFORE the blocking invoke starts).
          if (ev.direction === 'sending') {
            this.activeSendProgress?.(
              ev.sent_bytes as number,
              ev.total_bytes as number,
            );
          }
          break;
        case 'file-manifest':
          // Receiver-side: Rust got a manifest, frontend needs to create
          // the transferId -> item mapping for progress display.
          events.onFileManifest?.({
            id: ev.transfer_id as string,
            name: ev.name as string,
            size: ev.size as number,
            kind: ev.file_kind as string,
          } as never);
          break;
        case 'p2p':
          this.directActive = ev.state === 'open';
          if (ev.state === 'open') {
            events.onFileRoute?.('p2p', '');
          } else if (ev.state === 'failed') {
            // So tasks can mark their P2P state as failed (card shows 中转).
            events.onFileRoute?.('relay', '');
          }
          break;
        case 'p2p-log':
          log('p2p', 'rust: ' + (ev.message as string));
          break;
        case 'transfer-done':
          if (ev.ok) {
            // The Rust receiver already persisted the file (SHA verified,
            // renamed). Notify the task layer via onFileProgress with full
            // bytes, then let it mark done. We don't have total_bytes in
            // TransferDone, so use a dedicated handler if available.
            const tid = ev.transfer_id as string;
            // Try the file-complete path first (receiver side).
            if ((events as any).onTransferDone) {
              (events as any).onTransferDone(tid);
            } else {
              // Fallback: mark progress as complete via onFileProgress.
              // The task layer will resolve total from its item mapping.
              events.onFileProgress?.(tid, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER);
            }
          } else {
            events.onFileError?.(ev.transfer_id as string, (ev.error as string) ?? 'transfer failed');
          }
          break;
        case 'disconnected':
          log('online-rust', 'room disconnected', { reason: ev.reason });
          break;
        case 'protocol': {
          // Task-level signaling for the frontend orchestrator.
          const msg = ev.message as { kind: string; [k: string]: unknown };
          const from = ev.from as string;
          switch (msg.kind) {
            case 'offer':
              events.onOffer?.(msg as never, from);
              break;
            case 'offer-withdraw':
              events.onOfferWithdraw?.(msg.taskId as string, from);
              break;
            case 'offer-rejected':
              events.onOfferRejected?.(msg.taskId as string, from);
              break;
            case 'task-done':
              events.onTaskDone?.(msg.taskId as string, from);
              break;
            case 'task-ack':
              events.onTaskAck?.(msg.taskId as string, from);
              break;
            case 'task-cancel':
              events.onTaskCancel?.(msg.taskId as string, from);
              break;
            case 'task-failed':
              events.onTaskFailed?.(
                msg.taskId as string,
                msg.failures as { name: string; size: number; error: string }[],
                from,
              );
              break;
            case 'task-progress':
              events.onTaskProgress?.(
                msg.taskId as string,
                (msg.receivedBytes ?? msg.doneBytes) as number,
                msg.totalBytes as number,
                from,
                msg.savedBytes as number | undefined,
              );
              break;
            case 'hello':
              // Presence is handled via peer events; ignore.
              break;
            case 'bye':
              break;
            default:
              break;
          }
          break;
        }
        default:
          break;
      }
    } catch (e) {
      log('online-rust', 'event dispatch failed', { error: String(e) });
    }
  }

  private requireSession(): string {
    if (!this.sessionId) throw new Error('online session not connected');
    return this.sessionId;
  }

  async sendText(text: string): Promise<void> {
    await invoke('online_send_text', {
      sessionId: this.requireSession(),
      text,
    });
  }

  /**
   * Send a file via Rust. Mirrors FlingRoom.sendFile() signature used by
   * tasks.ts. Returns the manifest (built by Rust).
   */
  async sendFile(
    file: FileLike,
    onProgress?: (sentBytes: number, totalBytes: number, receiverSaved?: boolean) => void,
    kind: FileKind = 'file',
  ): Promise<FileManifest> {
    const path = await ensureFilePath(file);
    // Progress arrives via fling://online/event DURING the transfer, but
    // `online_send_file` only returns after the receiver's complete ACK —
    // so the callback MUST be installed before the invoke starts. (The old
    // code bridged events.onFileProgress after the invoke resolved, which
    // meant the sender UI showed 0 bytes for the whole transfer.)
    this.activeSendProgress = onProgress ? (sent, total) => onProgress(sent, total) : null;
    let transferId: string;
    try {
      transferId = (await invoke('online_send_file', {
        sessionId: this.requireSession(),
        path,
        name: file.name,
        kind,
      })) as string;
    } finally {
      this.activeSendProgress = null;
    }
    // The manifest is not returned by the command; synthesize a minimal
    // one for tasks.ts bookkeeping. Full manifest lives in Rust.
    const manifest: FileManifest = {
      id: transferId,
      name: file.name,
      size: file.size,
      sha256: '',
      chunkSize: 32 * 1024,
      totalChunks: Math.ceil(file.size / (32 * 1024)),
      kind,
    };
    return manifest;
  }

  async sendOffer(offer: unknown): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: { kind: 'offer', ...(offer as object) },
    });
  }

  async sendOfferWithdraw(taskId: string): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: { kind: 'offer-withdraw', taskId },
    });
  }

  async sendOfferRejected(taskId: string): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: { kind: 'offer-rejected', taskId },
    });
  }

  async sendTaskAck(taskId: string): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: { kind: 'task-ack', taskId },
    });
  }

  async sendTaskFailed(
    taskId: string,
    failures: { name: string; size: number; error: string }[],
  ): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: { kind: 'task-failed', taskId, failures },
    });
  }

  async sendTaskProgress(
    taskId: string,
    receivedBytes: number,
    totalBytes: number,
    savedBytes = 0,
  ): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: {
        kind: 'task-progress',
        taskId,
        doneBytes: receivedBytes,
        receivedBytes,
        savedBytes,
        totalBytes,
      },
    });
  }

  async sendTaskDone(taskId: string): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: { kind: 'task-done', taskId },
    });
  }

  async flush(): Promise<void> {
    // Rust publishes are already flushed; no-op for compat.
  }

  async close(): Promise<void> {
    if (this.sessionId) {
      try {
        await invoke('online_disconnect', { sessionId: this.sessionId });
      } catch {
        /* ignore */
      }
      this.sessionId = null;
    }
    this.unlisten?.();
    this.unlisten = null;
  }
}

/**
 * Drop-in replacement for connectFlingRoom() from net.ts.
 * Returns a Rust-backed room; the WebView does no WebRTC.
 */
export async function connectRustRoom(
  spec: RoomSpec,
  events: FlingRoomEvents,
  serverUrl: string,
): Promise<RustFlingRoom> {
  const room = new RustFlingRoom(events);
  await room.connect(spec, serverUrl);
  return room;
}

// Re-export key contexts for callers.
export { KEY_CTX };
