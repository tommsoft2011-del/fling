// Rust-backed online room for online transfers: signaling and the data
// plane live in Rust (online_session.rs); the WebView only renders state.
//
// Native disk-file tasks only pass opaque sourceId + display metadata.
// WebView-owned Blob bytes are staged in Rust memory only (never a disk
// temp file) and streamed from RAM by the Rust sender.
// This class implements the subset of the
// FlingRoom interface used by tasks.ts, delegating to Tauri commands.

import { invoke } from '@tauri-apps/api/core';
import { listen } from './ui-events.js';
import type {
  FileKind,
  FileManifest,
  FlingRoomEvents,
} from '@fling/core';
import type { FileLike } from '@fling/core';
import { log } from './log.js';
import { safeConnectionDiagnostic } from './connection-diagnostics.js';
import { safeRouteDiagnostic, type NativeRoute } from './route-diagnostics.js';
import { KEY_CTX, mintToken } from './net.js';
import type { RoomSpec } from './net.js';

export interface NativeTaskStatus {
  taskId: string; direction: 'sending' | 'receiving'; stage: string;
  name: string; totalBytes: number; localBytes: number; peerBytes: number;
  savedBytes: number; sha256?: string; savedPath?: string; error?: string;
}
export type NativeRoomEvents = FlingRoomEvents & {
  onNativeTask?: (status: NativeTaskStatus) => void;
  onNativeRoute?: (route: NativeRoute | undefined) => void;
};

interface OnlineUiEvent {
  session: string;
  event: {
    kind: string;
    [key: string]: unknown;
  };
}

/** Stage WebView-owned Blob bytes in Rust memory so the Rust sender can
 * stream them. Ordinary disk files must NOT use this: they use sourceId.
 * Bytes cross IPC once, as raw 4 MiB frames, and are held only in RAM;
 * no file is written anywhere (the old fling-online disk spill is gone). */
async function ensureMemorySource(file: FileLike, kind: FileKind): Promise<string> {
  const memoryId = (await invoke('online_memory_begin', { name: file.name, kind })) as string;
  const idBytes = new TextEncoder().encode(memoryId);
  const header = new Uint8Array(4 + idBytes.length);
  new DataView(header.buffer).setUint32(0, idBytes.length, true);
  header.set(idBytes, 4);
  const CHUNK = 4 * 1024 * 1024;
  try {
    let lastLogFrac = 0;
    for (let off = 0; off < file.size; off += CHUNK) {
      const bytes = await file.slice(off, Math.min(off + CHUNK, file.size));
      const frame = new Uint8Array(header.length + bytes.length);
      frame.set(header);
      frame.set(bytes, header.length);
      await invoke('online_memory_append', frame);
      const frac = (off + bytes.length) / file.size;
      if (frac - lastLogFrac >= 0.25 || frac >= 1) {
        lastLogFrac = frac;
        log('p2p', 'memory staging progress', {
          stagedBytes: off + bytes.length,
          fileSize: file.size,
        });
      }
    }
  } catch (e) {
    log('p2p', 'memory staging failed', { fileSize: file.size, errMsg: String(e) });
    throw e;
  }
  log('p2p', 'memory staging complete', { fileSize: file.size });
  return memoryId;
}

export class RustFlingRoom {
  private sessionId: string | null = null;
  private nativeTask = false;
  private unlisten: (() => void) | null = null;
  private events: NativeRoomEvents;
  /** FlingRoom compat: whether a direct P2P path is active. */
  directActive = false;
  /** Live progress callback of the currently sending file (set BEFORE
   * the blocking invoke, since Rust emits progress DURING the transfer). */
  private activeSendProgress: ((sent: number, total: number) => void) | null = null;

  constructor(events: NativeRoomEvents) {
    this.events = events;
  }

  async connect(spec: RoomSpec, serverUrl: string): Promise<void> {
    this.nativeTask = !!spec.nativeTask;
    const token = await mintToken(spec.roomName);
    // Subscribe first, with a client-generated correlation ID: fast peers must
    // not race listener registration after online_connect has already returned.
    this.sessionId = crypto.randomUUID().replaceAll('-', '');
    this.unlisten = await listen<OnlineUiEvent>('fling://online/event', (e) => {
      if (e.payload.session !== this.sessionId) return;
      this.dispatch(e.payload.event);
    });
    try {
      await invoke('online_connect', {
        serverUrl, token, pairingCode: spec.keyMaterial, keyContext: spec.keyContext,
        clientSessionId: this.sessionId, nativeTask: spec.nativeTask ?? null,
      });
    } catch (error) {
      this.unlisten?.(); this.unlisten = null; this.sessionId = null;
      throw error;
    }
  }

  private dispatch(ev: OnlineUiEvent['event']): void {
    const events = this.events;
    // Native tasks never let legacy TS protocol handlers decide completion,
    // cancellation or byte accounting. Render Rust snapshots only.
    if (this.nativeTask && !['native-task', 'p2p', 'p2p-log', 'path-diagnostic', 'connection-diagnostic', 'peer', 'disconnected'].includes(ev.kind)) return;
    try {
      switch (ev.kind) {
        case 'native-task':
          events.onNativeTask?.(ev.status as NativeTaskStatus);
          break;
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
        case 'peer-progress':
          events.onPeerProgress?.(
            ev.transfer_id as string,
            ev.received_bytes as number,
            ev.total_bytes as number,
            ev.complete === true,
          );
          break;
        case 'file-send-started':
          events.onFileSendStarted?.(ev.transfer_id as string, ev.total_bytes as number);
          break;
        case 'file-manifest':
          // Receiver-side: Rust got a manifest, frontend needs to create
          // the transferId -> item mapping for progress display.
          events.onFileManifest?.({
            id: ev.transfer_id as string,
            name: ev.name as string,
            size: ev.size as number,
            kind: ev.file_kind as string,
          } as never, String(ev.from ?? ''));
          break;
        case 'p2p':
          // Native signals candidate/fallback intent separately from confirmed route.
          // Only relay_established is evidence that selected TURN's DataChannel opened.
          if (ev.state === 'relay_established') {
            this.directActive = true;
            events.onNativeRoute?.('relay');
            events.onFileRoute?.('relay', '');
          } else if (ev.state === 'direct_established' || ev.state === 'open' || ev.state === 'unknown_established') {
            this.directActive = true;
            if (ev.state === 'direct_established') events.onNativeRoute?.('direct');
            else events.onNativeRoute?.('unknown');
            events.onFileRoute?.('p2p', '');
          } else if (ev.state === 'relay_candidate' || ev.state === 'fallback_requested') {
            // ICE/TURN candidate selection and fallback request are not an open DataChannel.
          } else if (ev.state === 'failed' || ev.state === 'closed') {
            // Direct failure is not proof that an alternate DataChannel opened.
            this.directActive = false;
            events.onNativeRoute?.(undefined);
          }
          break;
        case 'connection-diagnostic': {
          const summary = safeConnectionDiagnostic(ev.summary);
          if (summary) log('p2p', 'connection diagnostic', summary);
          break;
        }
        case 'path-diagnostic': {
          const summary = safeRouteDiagnostic(ev.summary);
          if (summary) log('p2p', 'path diagnostic', summary);
          break;
        }
        case 'p2p-log':
          // Old arbitrary native strings are not a safe diagnostic contract.
          break;
        case 'transfer-done':
          if (ev.ok) {
            // The Rust receiver already persisted the file (SHA verified,
            // renamed). Notify the task layer via onFileProgress with full
            // bytes, then let it mark done. We don't have total_bytes in
            // TransferDone, so use a dedicated handler if available.
            const tid = ev.transfer_id as string;
            // Try the file-complete path first (receiver side), passing
            // the actual saved path so the UI shows the real filename
            // (which may carry a (n) suffix after a name conflict).
            if ((events as any).onTransferDone) {
              (events as any).onTransferDone(tid, ev.saved_path as string | undefined);
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
          log('net', 'room disconnected', { reason: ev.reason });
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
      log('net', 'event dispatch failed', { error: String(e) });
    }
  }

  private requireSession(): string {
    if (!this.sessionId) throw new Error('online session not connected');
    return this.sessionId;
  }

  async sendNativeSource(sourceId: string): Promise<NativeTaskStatus> {
    return invoke('online_send_source', { sessionId: this.requireSession(), sourceId });
  }

  /** Batch ordinary-file source: only capability + session IDs cross IPC. */
  async sendRegisteredSource(
    source: { sourceId?: string; name: string; size: number; relPath?: string; kind?: FileKind },
    onProgress?: (sent: number, total: number, receiverSaved?: boolean) => void,
  ): Promise<FileManifest> {
    if (!source.sourceId) throw new Error('Missing sourceId');
    this.activeSendProgress = onProgress ? (sent, total) => onProgress(sent, total) : null;
    let id: string;
    try {
      id = await invoke<string>('online_send_registered_source', { sessionId: this.requireSession(), sourceId: source.sourceId });
    } finally { this.activeSendProgress = null; }
    return { id, name: source.name, size: source.size, sha256: '', chunkSize: 32 * 1024,
      totalChunks: Math.ceil(source.size / (32 * 1024)), kind: source.kind ?? 'file', path: source.relPath };
  }
  async cancelNative(): Promise<void> {
    await invoke('online_cancel_native', { sessionId: this.requireSession() });
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
    // WebView-owned Blob only: stage it in Rust memory (no disk file),
    // then let the Rust sender stream it from RAM.
    const memoryId = await ensureMemorySource(file, kind);
    // Progress arrives via fling://online/event DURING the transfer, but
    // `online_send_memory` only returns after the receiver's complete ACK —
    // so the callback MUST be installed before the invoke starts. (The old
    // code bridged events.onFileProgress after the invoke resolved, which
    // meant the sender UI showed 0 bytes for the whole transfer.)
    this.activeSendProgress = onProgress ? (sent, total) => onProgress(sent, total) : null;
    let transferId: string;
    try {
      transferId = (await invoke('online_send_memory', {
        sessionId: this.requireSession(),
        memoryId,
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

  async sendTaskCancel(taskId: string): Promise<void> {
    await invoke('online_send_message', {
      sessionId: this.requireSession(),
      message: { kind: 'task-cancel', taskId },
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
  events: NativeRoomEvents,
  serverUrl: string,
): Promise<RustFlingRoom> {
  const room = new RustFlingRoom(events);
  await room.connect(spec, serverUrl);
  return room;
}

// Re-export key contexts for callers.
export { KEY_CTX };
