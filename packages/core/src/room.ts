// FlingRoom: transport-agnostic room logic.
// Joins a LiveKit room derived from the pairing code / room number and
// exchanges end-to-end encrypted messages + files over the data channel.

import { deriveRoomKey, openText, sealText } from './crypto.js';
import { FileReceiver, type FileLike, buildManifest } from './files.js';
import {
  P2P_DRAIN_THRESHOLD,
  P2P_MANIFEST_ACK_TIMEOUT_MS,
  type DirectChannel,
} from './p2p.js';
import {
  FLING_TOPIC,
  isFlingMessage,
  randomId,
  type FileKind,
  type FileManifest,
  type FlingMessage,
  type OfferMessage,
} from './protocol.js';
import type { FlingTransport } from './transport.js';

export interface FlingRoomEvents {
  onText?(text: string, from: string): void;
  onPeerJoined?(identity: string): void;
  onPeerLeft?(identity: string): void;
  onFileManifest?(manifest: FileManifest, from: string): void;
  onFileProgress?(id: string, receivedBytes: number, totalBytes: number): void;
  onFileComplete?(id: string, manifest: FileManifest, bytes: Uint8Array): void;
  onFileError?(id: string, error: string): void;
  // v2 transfer-tool signaling. Optional so existing v1 callers (CLI)
  // keep working without changes.
  onOffer?(offer: OfferMessage, from: string): void;
  onOfferWithdraw?(taskId: string, from: string): void;
  onOfferRejected?(taskId: string, from: string): void;
  onTaskDone?(taskId: string, from: string): void;
  /** P2P handshake signaling (SDP / ICE), carried over the encrypted relay. */
  onP2PSignal?(sig: Extract<FlingMessage, { kind: `p2p-${string}` }>, from: string): void;
  /** Receiver confirms all items received AND saved (see task-ack). */
  onTaskAck?(taskId: string, from: string): void;
  /** Receiver cancelled mid-transfer (see task-cancel). */
  onTaskCancel?(taskId: string, from: string): void;
  /** Receiver's periodic real progress report (see task-progress). */
  onTaskProgress?(taskId: string, doneBytes: number, totalBytes: number, from: string): void;
  /** A direct path was found unhealthy; callers should close their P2P link. */
  onDirectFallback?(reason: string): void;
  /** Optional diagnostics sink (the app wires this to its log file). */
  onLog?(tag: string, msg: string): void;
}

const enc = new TextEncoder();
const dec = new TextDecoder();

export class FlingRoom {
  private roomKey!: CryptoKey;
  private events!: FlingRoomEvents;
  private helloTimer?: ReturnType<typeof setInterval>;
  private incoming = new Map<string, { receiver: FileReceiver; from: string }>();
  /**
   * Chunks that arrived before their manifest. Manifests and chunks can
   * travel on different channels (relay vs direct), so a chunk may win the
   * race; buffer it and replay when the manifest arrives instead of dropping.
   */
  private earlyChunks = new Map<string, Extract<FlingMessage, { kind: 'file-chunk' }>[]>();
  // Data channels are ordered, but decrypt+parse is async — serialize
  // handling so a chunk can never overtake its manifest.
  private handleQueue: Promise<void> = Promise.resolve();
  /**
   * Direct P2P channel to the peer (set by the app after a successful
   * WebRTC handshake). While open, file-chunk messages take this path
   * instead of the LiveKit relay. Null/absent = relay everything.
   */
  private direct: DirectChannel | null = null;
  /** Resolve functions for manifests sent over a direct channel. */
  private manifestAckWaiters = new Map<string, (ok: boolean) => void>();
  /** Prevent a manifest retry from creating a second receiver. */
  private completedFiles = new Set<string>();
  /** Changes whenever an established direct path is abandoned. */
  private directGeneration = 0;

  constructor(
    private transport: FlingTransport,
    private deviceName: string,
  ) {}

  get isConnected(): boolean {
    return this.transport.isConnected;
  }

  /** True while a direct P2P channel is open (file bytes bypass the relay). */
  get directActive(): boolean {
    return !!this.direct && this.direct.readyState === 'open';
  }

  /**
   * Attach/detach the direct channel. The room routes file-chunk messages
   * through it while open and feeds inbound direct messages into the same
   * receive pipeline. Safe to call at any time (per-chunk routing).
   */
  setDirectChannel(ch: DirectChannel | null): void {
    const old = this.direct;
    if (old) {
      old.onmessage = null;
      // NOTE: do NOT touch onclose — it belongs to the link that owns the
      // channel lifecycle (overwriting it broke the link's failure path).
    }
    this.direct = ch;
    if (ch) {
      this.events.onLog?.('p2p', `direct channel attached (${ch.label})`);
      ch.onmessage = (data) => {
        // Synchronous: see handleDirectMessage. Never chain onto handleQueue.
        try {
          this.handleDirectMessage(data, ch.label);
        } catch (e) {
          console.error('fling: direct payload handling failed:', e);
        }
      };
    } else {
      this.events.onLog?.('p2p', 'direct channel detached (relay mode)');
    }
  }

  /**
   * Stop using the direct channel immediately. The app-level P2P owner is
   * notified so it can close the RTCPeerConnection too; all subsequent file
   * messages then use the LiveKit relay.
   */
  fallbackToRelay(reason: string): void {
    const hadDirect = !!this.direct;
    this.setDirectChannel(null);
    if (hadDirect) {
      this.directGeneration++;
      this.events.onLog?.('p2p', `direct fallback to relay: ${reason}`);
      this.events.onDirectFallback?.(reason);
    }
  }

  /** Publish a signaling message over the encrypted relay (e.g. P2P SDP/ICE). */
  async sendSignal(msg: FlingMessage): Promise<void> {
    await this.send(msg);
  }

  async connect(
    serverUrl: string,
    token: string,
    pairingCode: string,
    events: FlingRoomEvents,
    /** PBKDF2 context for the room key; defaults to the v1 salt for CLI compat. */
    keyContext: string = 'fling-v1-key',
  ): Promise<void> {
    this.events = events;
    this.roomKey = await deriveRoomKey(pairingCode, keyContext);

    this.transport.onData((payload, from, topic) => {
      if (topic !== FLING_TOPIC) return;
      this.handleQueue = this.handleQueue
        .then(() => this.handlePayload(payload, from))
        .catch((e) => console.error('fling: payload handling failed:', e));
    });
    this.transport.onPeerJoined((id) => {
      events.onPeerJoined?.(id);
      // Re-announce so a late joiner learns we are here.
      void this.send({ kind: 'hello', device: this.deviceName }).catch(() => {});
    });
    this.transport.onPeerLeft((id) => events.onPeerLeft?.(id));

    await this.transport.connect(serverUrl, token);
    await this.send({ kind: 'hello', device: this.deviceName });

    this.helloTimer = setInterval(() => {
      void this.send({ kind: 'hello', device: this.deviceName }).catch(() => {});
    }, 15_000);
  }

  async sendText(text: string): Promise<void> {
    await this.send({ kind: 'text', id: randomId(), text });
  }

  /** Send a file with progress callback (sentBytes, totalBytes). */
  async sendFile(
    file: FileLike,
    onProgress?: (sentBytes: number, totalBytes: number) => void,
    kind: FileKind = 'file',
    /** Reuse the same id when a partially delivered file must be replayed. */
    existingManifest?: FileManifest,
  ): Promise<FileManifest> {
    const manifest = existingManifest ?? await buildManifest(file, kind);
    await this.sendManifestWithAck(manifest);
    const generationAtChunkStart = this.directGeneration;
    let sent = 0;
    let reported = 0;
    let directChunksSent = false;

    // Keep this loop reusable: if the direct path dies after accepting some
    // chunks, replay the same manifest's chunks through the relay. The
    // receiver de-duplicates already present chunks and fills the holes.
    const sendChunks = async (): Promise<void> => {
      for (let seq = 0; seq < manifest.totalChunks; seq++) {
        const start = seq * manifest.chunkSize;
        const bytes = await file.slice(start, Math.min(start + manifest.chunkSize, file.size));
        const direct = this.direct?.readyState === 'open';
        await this.send({
          kind: 'file-chunk',
          id: manifest.id,
          seq,
          data: FileReceiver.encodeChunk(bytes),
        });
        directChunksSent ||= direct;
        sent += bytes.length;
        // A replay must not make the sender UI go backwards. The task-level
        // progress is still the receiver's authoritative value.
        const nextReported = Math.min(file.size, Math.max(reported, sent));
        if (nextReported > reported) {
          reported = nextReported;
          onProgress?.(reported, file.size);
        }
      }
    };

    await sendChunks();
    if (directChunksSent && this.directGeneration !== generationAtChunkStart) {
      this.events.onLog?.('p2p', `direct path changed during ${manifest.name}; replaying chunks via relay`);
      await sendChunks();
    }
    return manifest;
  }

  /**
   * A direct send only means that bytes entered the local SCTP buffer. Wait
   * for the receiver's relay-side confirmation before putting chunks behind
   * it. If the confirmation does not arrive, disable direct and send one
   * relay copy; the task-level ack is the final end-to-end confirmation.
   */
  private async sendManifestWithAck(manifest: FileManifest): Promise<void> {
    // Relay-only transfers already have the task-level ack and should not pay
    // an extra round trip. This handshake is specifically for direct P2P.
    const wasDirect = this.direct?.readyState === 'open';
    if (!wasDirect) {
      await this.send({ kind: 'file-manifest', manifest });
      return;
    }

    const ack = this.waitForManifestAck(manifest.id, P2P_MANIFEST_ACK_TIMEOUT_MS);
    let route: 'direct' | 'relay';
    try {
      route = await this.send({ kind: 'file-manifest', manifest });
    } catch (e) {
      this.resolveManifestAck(manifest.id, false);
      throw e;
    }
    if (await ack) return;

    // A direct manifest that was accepted locally but never acknowledged is
    // exactly the black-hole case. Abandon it before sending any chunks.
    if (route === 'direct' && this.direct?.readyState === 'open') {
      this.fallbackToRelay(`manifest ack timeout (${manifest.id.slice(0, 8)})`);
    }

    // Send at most one relay copy after the direct attempt. Repeating a
    // manifest is unsafe with pre-0.5.38 receivers that recreate their
    // FileReceiver on every duplicate; the task-level ack remains the final
    // end-to-end confirmation if this small control ack is lost.
    if (route === 'direct') {
      const relayAck = this.waitForManifestAck(manifest.id, P2P_MANIFEST_ACK_TIMEOUT_MS);
      await this.send({ kind: 'file-manifest', manifest });
      if (!(await relayAck)) {
        this.events.onLog?.('p2p', `relay manifest ack missing for ${manifest.name}; continuing with task ack`);
      }
    } else {
      this.events.onLog?.('p2p', `manifest ack missing for ${manifest.name}; continuing with task ack`);
    }
  }

  private waitForManifestAck(id: string, timeoutMs: number): Promise<boolean> {
    return new Promise((resolve) => {
      let timer: ReturnType<typeof setTimeout>;
      const finish = (ok: boolean) => {
        if (this.manifestAckWaiters.get(id) !== finish) return;
        this.manifestAckWaiters.delete(id);
        clearTimeout(timer);
        resolve(ok);
      };
      timer = setTimeout(() => finish(false), timeoutMs);
      const previous = this.manifestAckWaiters.get(id);
      previous?.(false);
      this.manifestAckWaiters.set(id, finish);
    });
  }

  private resolveManifestAck(id: string, ok: boolean): void {
    this.manifestAckWaiters.get(id)?.(ok);
  }

  /** v2: announce a new transfer task in a lobby / inbox room. */
  async sendOffer(offer: OfferMessage): Promise<void> {
    await this.send(offer);
  }

  /** v2: retract a previously announced offer. */
  async sendOfferRejected(taskId: string): Promise<void> {
    await this.send({ kind: 'offer-rejected', taskId });
  }

  async sendOfferWithdraw(taskId: string): Promise<void> {
    await this.send({ kind: 'offer-withdraw', taskId });
  }

  /** v2: signal that a task's transfer is fully complete. */
  async sendTaskDone(taskId: string): Promise<void> {
    await this.send({ kind: 'task-done', taskId });
  }

  /** v2: receiver confirms every item is received and saved to disk. */
  async sendTaskAck(taskId: string): Promise<void> {
    await this.send({ kind: 'task-ack', taskId });
  }

  /** v2: receiver cancelled the transfer midway; sender must not report success. */
  async sendTaskCancel(taskId: string): Promise<void> {
    await this.send({ kind: 'task-cancel', taskId });
  }

  /** v2: receiver reports real receive progress (throttled by caller). */
  async sendTaskProgress(taskId: string, doneBytes: number, totalBytes: number): Promise<void> {
    await this.send({ kind: 'task-progress', taskId, doneBytes, totalBytes });
  }

  private async send(msg: FlingMessage): Promise<'direct' | 'relay'> {
    // File bytes prefer the direct P2P channel when it's open: the DTLS-SCTP
    // data channel is already encrypted, so no AES-GCM envelope needed here.
    // The manifest goes through the same channel so it can never arrive
    // AFTER its chunks (previously: manifest via relay, chunks via direct
    // → chunks arrived first and were dropped as "unknown manifest").
    // Everything else (signaling, small messages) uses the relay.
    // A failing direct channel falls back to the relay for this chunk.
    const dc = this.direct;
    // Only file payload/control travels over direct SCTP. Keep task-progress
    // on the encrypted relay: it is the sender's independent health signal,
    // and putting it behind a congested ordered data channel makes the
    // watchdog blind precisely when the direct path becomes a black hole.
    const directKinds = msg.kind === 'file-chunk' || msg.kind === 'file-manifest';
    if (directKinds && dc && dc.readyState === 'open') {
      try {
        if (dc.bufferedAmount > P2P_DRAIN_THRESHOLD) {
          await dc.waitForDrain(P2P_DRAIN_THRESHOLD);
          if (dc.readyState !== 'open') throw new Error('direct channel closed while draining');
        }
        dc.send(enc.encode(JSON.stringify(msg)));
        return 'direct';
      } catch {
        this.events.onLog?.('p2p', `direct send failed for ${msg.kind} ${msg.kind === 'file-chunk' ? msg.seq : ''}, falling back to relay`);
        this.fallbackToRelay(`direct send failed for ${msg.kind}`);
      }
    }
    const sealed = await sealText(this.roomKey, JSON.stringify(msg));
    await this.transport.publish(enc.encode(sealed), FLING_TOPIC);
    return 'relay';
  }

  private async handlePayload(payload: Uint8Array, from: string): Promise<void> {
    const t0 = performance.now();
    let plain: string;
    try {
      plain = await openText(this.roomKey, dec.decode(payload));
    } catch {
      return; // not ours (wrong key / garbage) — ignore
    }
    const decryptMs = performance.now() - t0;
    if (decryptMs > 200) {
      this.events.onLog?.('xfer', `slow relay decrypt: ${Math.round(decryptMs)}ms`);
    }
    let msg: unknown;
    try {
      msg = JSON.parse(plain);
    } catch {
      return;
    }
    if (!isFlingMessage(msg)) return;
    this.dispatchMessage(msg, from);
  }

  /**
   * Inbound message from the direct P2P channel (already DTLS-encrypted).
   * Synchronous fast path: no AES-GCM, just JSON parse. Handled immediately
   * in onmessage instead of chaining onto the shared handleQueue, so a slow
   * relay trickle (decrypt-bound) can never head-of-line-block direct chunks.
   * Manifest/chunk races are covered by early-chunk buffering.
   */
  private handleDirectMessage(data: Uint8Array, from: string): void {
    let msg: unknown;
    try {
      msg = JSON.parse(dec.decode(data));
    } catch {
      this.events.onLog?.('xfer', 'dropping non-JSON direct payload');
      return;
    }
    if (!isFlingMessage(msg)) {
      this.events.onLog?.('xfer', 'dropping unknown direct message');
      return;
    }
    if (msg.kind === 'file-chunk') {
      const n = (this.directChunkCounts.get(msg.id) ?? 0) + 1;
      this.directChunkCounts.set(msg.id, n);
      if (n % 20 === 0) {
        this.events.onLog?.('xfer', `direct chunks received: ${n} (${msg.id.slice(0, 8)})`);
      }
    }
    this.dispatchMessage(msg, from);
  }

  /** Per-file direct-chunk counters (diagnostics). Cleared in finishFile. */
  private directChunkCounts = new Map<string, number>();

  private dispatchMessage(msg: FlingMessage, from: string): void {
    switch (msg.kind) {
      case 'text':
        this.events.onText?.(msg.text, from);
        break;
      case 'hello':
        this.events.onPeerJoined?.(from);
        break;
      case 'bye':
        this.events.onPeerLeft?.(from);
        break;
      case 'file-manifest': {
        const m = msg.manifest;
        if (!m || typeof m.id !== 'string') return; // malformed — ignore
        const manifest: FileManifest = {
          id: m.id,
          name: m.name,
          size: m.size,
          sha256: m.sha256,
          chunkSize: m.chunkSize,
          totalChunks: m.totalChunks,
          // tolerate missing/unknown kind from older senders
          kind: m.kind === 'clipboard-image' ? 'clipboard-image' : 'file',
          // 文件夹发送的相对路径（如 "docs/report.pdf"），老版本没有则为 undefined
          path: typeof m.path === 'string' ? m.path : undefined,
        };
        if (this.incoming.has(manifest.id) || this.completedFiles.has(manifest.id)) {
          this.events.onLog?.('xfer', `duplicate manifest ${manifest.id.slice(0, 8)}, acknowledging without reset`);
          void this.send({ kind: 'file-manifest-ack', id: manifest.id }).catch(() => {});
          break;
        }
        this.incoming.set(manifest.id, { receiver: new FileReceiver(manifest), from });
        // The acknowledgement is deliberately not a directKinds message, so
        // it always travels over LiveKit even while the data channel is sick.
        void this.send({ kind: 'file-manifest-ack', id: manifest.id }).catch(() => {});
        this.events.onFileManifest?.(manifest, from);
        // Replay any chunks that beat the manifest here.
        const buffered = this.earlyChunks.get(manifest.id);
        if (buffered) {
          this.earlyChunks.delete(manifest.id);
          this.events.onLog?.('xfer', `replaying ${buffered.length} early chunks for ${manifest.name}`);
          for (const c of buffered) this.handleChunkMessage(c);
        }
        // Empty file: complete immediately.
        if (manifest.totalChunks === 1 && manifest.size === 0) {
          this.finishFile(manifest.id);
        }
        break;
      }
      case 'file-chunk': {
        this.handleChunkMessage(msg);
        break;
      }
      case 'file-manifest-ack':
        this.resolveManifestAck(msg.id, true);
        break;
      case 'offer':
        this.events.onOffer?.(msg, from);
        break;
      case 'offer-withdraw':
        this.events.onOfferWithdraw?.(msg.taskId, from);
        break;
      case 'offer-rejected':
        this.events.onOfferRejected?.(msg.taskId, from);
        break;
      case 'task-done':
        this.events.onTaskDone?.(msg.taskId, from);
        break;
      case 'task-ack':
        this.events.onTaskAck?.(msg.taskId, from);
        break;
      case 'task-cancel':
        this.events.onTaskCancel?.(msg.taskId, from);
        break;
      case 'task-progress':
        this.events.onTaskProgress?.(msg.taskId, msg.doneBytes, msg.totalBytes, from);
        break;
      case 'p2p-offer':
      case 'p2p-answer':
      case 'p2p-ice':
        this.events.onP2PSignal?.(msg, from);
        break;
    }
  }

  private finishFile(id: string): void {
    const entry = this.incoming.get(id);
    if (!entry) return;
    this.incoming.delete(id);
    this.earlyChunks.delete(id);
    this.directChunkCounts.delete(id);
    try {
      const bytes = entry.receiver.assemble();
      this.completedFiles.add(id);
      this.events.onFileComplete?.(id, entry.receiver.manifest, bytes);
    } catch (e) {
      this.events.onFileError?.(id, (e as Error).message);
    }
  }

  /** Single place that applies a file-chunk to its receiver. */
  private handleChunkMessage(msg: Extract<FlingMessage, { kind: 'file-chunk' }>): void {
    if (this.completedFiles.has(msg.id)) return;
    const entry = this.incoming.get(msg.id);
    if (!entry) {
      // Manifest not seen yet (chunk beat it on a faster channel): buffer
      // instead of dropping. Cap the buffer so a garbage id can't grow it.
      const buf = this.earlyChunks.get(msg.id);
      if (buf) {
        if (buf.length < 2000) buf.push(msg);
      } else {
        this.earlyChunks.set(msg.id, [msg]);
      }
      return;
    }
    const { receiver } = entry;
    let complete = false;
    try {
      complete = receiver.addChunk(msg.seq, msg.data);
    } catch (e) {
      this.events.onFileError?.(msg.id, (e as Error).message);
      this.incoming.delete(msg.id);
      return;
    }
    this.events.onFileProgress?.(msg.id, receiver.progress * receiver.totalBytes, receiver.totalBytes);
    if (complete) this.finishFile(msg.id);
  }

  /** Wait until all received payloads have been processed. */
  flush(): Promise<void> {
    return this.handleQueue;
  }

  async close(): Promise<void> {
    if (this.helloTimer) clearInterval(this.helloTimer);
    this.earlyChunks.clear();
    this.completedFiles.clear();
    for (const resolve of this.manifestAckWaiters.values()) resolve(false);
    this.manifestAckWaiters.clear();
    if (this.direct) {
      try {
        this.direct.close();
      } catch {
        /* best effort */
      }
      this.direct = null;
    }
    try {
      if (this.transport.isConnected) {
        await this.send({ kind: 'bye', device: this.deviceName });
      }
    } catch {
      /* best effort */
    }
    await this.transport.disconnect();
  }
}
