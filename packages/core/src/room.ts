// FlingRoom: transport-agnostic room logic.
// Joins a LiveKit room derived from the pairing code / room number and
// exchanges end-to-end encrypted messages + files over the data channel.

import { deriveRoomKey, openText, sealText } from './crypto.js';
import { FileReceiver, type FileLike, buildManifest } from './files.js';
import { P2P_DRAIN_THRESHOLD, type DirectChannel } from './p2p.js';
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
  // Data channels are ordered, but decrypt+parse is async — serialize
  // handling so a chunk can never overtake its manifest.
  private handleQueue: Promise<void> = Promise.resolve();
  /**
   * Direct P2P channel to the peer (set by the app after a successful
   * WebRTC handshake). While open, file-chunk messages take this path
   * instead of the LiveKit relay. Null/absent = relay everything.
   */
  private direct: DirectChannel | null = null;

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
        this.handleQueue = this.handleQueue
          .then(() => this.handleDirectData(data, ch.label))
          .catch((e) => console.error('fling: direct payload handling failed:', e));
      };
    } else {
      this.events.onLog?.('p2p', 'direct channel detached (relay mode)');
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
  ): Promise<FileManifest> {
    const manifest = await buildManifest(file, kind);
    await this.send({ kind: 'file-manifest', manifest });
    let sent = 0;
    for (let seq = 0; seq < manifest.totalChunks; seq++) {
      const start = seq * manifest.chunkSize;
      const bytes = await file.slice(start, Math.min(start + manifest.chunkSize, file.size));
      await this.send({
        kind: 'file-chunk',
        id: manifest.id,
        seq,
        data: FileReceiver.encodeChunk(bytes),
      });
      sent += bytes.length;
      onProgress?.(sent, file.size);
    }
    return manifest;
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

  private async send(msg: FlingMessage): Promise<void> {
    // File bytes prefer the direct P2P channel when it's open: the DTLS-SCTP
    // data channel is already encrypted, so no AES-GCM envelope needed here.
    // Everything else (signaling, manifests, small messages) uses the relay.
    // A failing direct channel falls back to the relay for this chunk.
    const dc = this.direct;
    if (msg.kind === 'file-chunk' && dc && dc.readyState === 'open') {
      try {
        if (dc.bufferedAmount > P2P_DRAIN_THRESHOLD) {
          await dc.waitForDrain(P2P_DRAIN_THRESHOLD);
        }
        dc.send(enc.encode(JSON.stringify(msg)));
        return;
      } catch {
        this.events.onLog?.('p2p', `direct send failed for chunk ${msg.seq}, falling back to relay`);
        this.setDirectChannel(null);
      }
    }
    const sealed = await sealText(this.roomKey, JSON.stringify(msg));
    await this.transport.publish(enc.encode(sealed), FLING_TOPIC);
  }

  private async handlePayload(payload: Uint8Array, from: string): Promise<void> {
    let plain: string;
    try {
      plain = await openText(this.roomKey, dec.decode(payload));
    } catch {
      return; // not ours (wrong key / garbage) — ignore
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

  /** Inbound message from the direct P2P channel (already DTLS-encrypted). */
  private async handleDirectData(data: Uint8Array, from: string): Promise<void> {
    let msg: unknown;
    try {
      msg = JSON.parse(dec.decode(data));
    } catch {
      return;
    }
    if (!isFlingMessage(msg)) return;
    this.dispatchMessage(msg, from);
  }

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
        };
        this.incoming.set(manifest.id, { receiver: new FileReceiver(manifest), from });
        this.events.onFileManifest?.(manifest, from);
        // Empty file: complete immediately.
        if (manifest.totalChunks === 1 && manifest.size === 0) {
          this.finishFile(manifest.id);
        }
        break;
      }
      case 'file-chunk': {
        const entry = this.incoming.get(msg.id);
        if (!entry) return; // unknown / already finished
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
        break;
      }
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
    try {
      const bytes = entry.receiver.assemble();
      this.events.onFileComplete?.(id, entry.receiver.manifest, bytes);
    } catch (e) {
      this.events.onFileError?.(id, (e as Error).message);
    }
  }

  /** Wait until all received payloads have been processed. */
  flush(): Promise<void> {
    return this.handleQueue;
  }

  async close(): Promise<void> {
    if (this.helloTimer) clearInterval(this.helloTimer);
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
