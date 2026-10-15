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
  decodeDirectFileChunkFrame,
  encodeDirectFileChunkFrame,
  isFlingMessage,
  randomId,
  type FileKind,
  type FileManifest,
  type DirectFileChunkFrame,
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
  onFileRoute?(route: 'p2p' | 'relay', id: string): void;
  onFileComplete?(id: string, manifest: FileManifest, bytes: Uint8Array): void | Promise<void>;
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
  onTaskFailed?(taskId: string, failures: { name: string; size: number; error: string }[], from: string): void;
  /** Receiver's periodic real progress report (see task-progress). */
  onTaskProgress?(taskId: string, doneBytes: number, totalBytes: number, from: string): void;
  /** A direct path was found unhealthy; callers should close their P2P link. */
  onDirectFallback?(reason: string): void;
  /** Optional diagnostics sink (the app wires this to its log file). */
  onLog?(tag: string, msg: string): void;
}

const enc = new TextEncoder();
const dec = new TextDecoder();

/**
 * Keep a direct SCTP window small enough that one bad UDP burst cannot turn
 * the browser's send buffer into an unbounded queue.  The receiver confirms
 * each window over the independent encrypted relay before we continue.
 */
const DIRECT_CHUNK_WINDOW = 32;
const RELAY_CHUNK_WINDOW = 8;
// The receive ACK travels over LiveKit, not over the RTCDataChannel.  A
// healthy direct transfer can therefore finish a window before the ACK gets
// through the relay.  Three seconds turned a delayed final ACK into a false
// P2P failure (the receiver had already got the complete file).
const DIRECT_CHUNK_ACK_TIMEOUT_MS = 8_000;
const RELAY_CHUNK_ACK_TIMEOUT_MS = 5_000;
const DIRECT_CHUNK_ACK_EVERY = DIRECT_CHUNK_WINDOW;
// Report real receive progress often enough to keep a slow relay alive, but
// do not mirror every duplicate chunk.  The old 250ms cadence produced a
// large number of duplicate ACKs while the relay was already congested.
const CHUNK_ACK_PROGRESS_INTERVAL_MS = 500;
const CHUNK_ACK_RESEND_INTERVAL_MS = 1_000;
// nextSeq is the first contiguous chunk that the receiver is still missing.
// Both direct repair and relay retry can therefore resume at that exact
// position; replaying an arbitrary overlap only adds duplicate payloads.
const RELAY_REPLAY_OVERLAP_CHUNKS = 0;
const MAX_RELAY_WINDOW_RETRIES = 2;

interface ManifestSendResult {
  chunkAckCapable: boolean;
  binaryChunkCapable: boolean;
  relaySent: boolean;
}

interface ReceivedFileChunk {
  id: string;
  seq: number;
  source: 'relay' | 'direct';
  dataB64?: string;
  bytes?: Uint8Array<ArrayBuffer>;
}

export class FlingRoom {
  private roomKey!: CryptoKey;
  private events!: FlingRoomEvents;
  private helloTimer?: ReturnType<typeof setInterval>;
  private incoming = new Map<string, {
    receiver: FileReceiver;
    from: string;
    lastAckSeq: number;
    lastAckAt: number;
    ackChain: Promise<void>;
    ackPending: boolean;
    finalizing: boolean;
    pendingAck?: { nextSeq: number; receivedBytes: number; complete: boolean };
  }>();
  /**
   * Chunks that arrived before their manifest. Manifests and chunks can
   * travel on different channels (relay vs direct), so a chunk may win the
   * race; buffer it and replay when the manifest arrives instead of dropping.
   */
  private earlyChunks = new Map<string, ReceivedFileChunk[]>();
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
  /** Highest contiguous receive position reported for each direct file. */
  private chunkAckProgress = new Map<string, { nextSeq: number; receivedBytes: number; complete: boolean }>();
  /** Peer capability advertised by the file-manifest acknowledgement. */
  private chunkAckCapabilities = new Set<string>();
  /** Peers advertising support for raw binary direct-channel chunks. */
  private binaryChunkCapabilities = new Set<string>();
  /** Prevent a manifest retry from creating a second receiver. */
  private completedFiles = new Set<string>();

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
    /** Start a relay replay near the last confirmed receive position. */
    fromSeq = 0,
  ): Promise<FileManifest> {
    const manifest = existingManifest ?? await buildManifest(file, kind);

    // A partial replay is a repair operation. Do not send its manifest and
    // then continue on a still-open direct path while the prefix may contain
    // holes; force the repair copy through the relay.
    const firstSeq = Math.max(0, Math.min(manifest.totalChunks, Math.floor(fromSeq)));
    if (firstSeq > 0 && this.direct?.readyState === 'open') {
      this.fallbackToRelay(`partial replay requires relay (${manifest.id.slice(0, 8)})`);
    }
    const manifestResult = await this.sendManifestWithAck(manifest);
    let directMode = firstSeq === 0 && this.direct?.readyState === 'open' && manifestResult.chunkAckCapable;
    let binaryChunkMode = directMode && manifestResult.binaryChunkCapable;
    let relayManifestSent = manifestResult.relaySent;
    let relayChunkAckCapable = manifestResult.chunkAckCapable;
    this.events.onLog?.('xfer', `file route selected for ${manifest.name} (${manifest.id.slice(0, 8)}): ${directMode ? 'direct' : 'relay'}, relayManifest=${relayManifestSent}, chunkAck=${relayChunkAckCapable}`);
    let sent = Math.min(file.size, firstSeq * manifest.chunkSize);
    let reported = sent;

    // Empty files are completed by the receiver as soon as the manifest is
    // accepted; there is no data window to acknowledge.
    if (manifest.size === 0) {
      onProgress?.(0, 0);
    this.chunkAckProgress.delete(manifest.id);
    this.chunkAckCapabilities.delete(manifest.id);
    this.binaryChunkCapabilities.delete(manifest.id);
    return manifest;
    }

    const sendChunk = async (seq: number): Promise<'direct' | 'relay'> => {
      const start = seq * manifest.chunkSize;
      const bytes = await file.slice(start, Math.min(start + manifest.chunkSize, file.size));
      let route: 'direct' | 'relay';
      const dc = this.direct;
      if (binaryChunkMode && dc?.readyState === 'open') {
        try {
          const frame = encodeDirectFileChunkFrame(manifest.id, seq, bytes);
          if (Number.isFinite(dc.maxMessageSize) && frame.byteLength > dc.maxMessageSize) {
            throw new Error(`binary frame exceeds SCTP maxMessageSize (${frame.byteLength} > ${dc.maxMessageSize})`);
          }
          const drainLimit = Math.max(0, P2P_DRAIN_THRESHOLD - frame.byteLength);
          if (dc.bufferedAmount > drainLimit) {
            await dc.waitForDrain(drainLimit);
          }
          if (dc.readyState !== 'open') throw new Error('direct channel closed while draining');
          dc.send(frame);
          this.events.onFileRoute?.('p2p', manifest.id);
          route = 'direct';
        } catch (e) {
          this.events.onLog?.('p2p', `raw direct chunk failed at ${seq}, falling back to relay: ${(e as Error).message}`);
          this.fallbackToRelay(`raw direct chunk failed (${manifest.id.slice(0, 8)})`);
          binaryChunkMode = false;
          route = await this.send({
            kind: 'file-chunk',
            id: manifest.id,
            seq,
            data: FileReceiver.encodeChunk(bytes),
          });
        }
      } else {
        route = await this.send({
          kind: 'file-chunk',
          id: manifest.id,
          seq,
          data: FileReceiver.encodeChunk(bytes),
        });
      }
      sent += bytes.length;
      // A replay must not make the sender UI go backwards. The task-level
      // progress is still the receiver's authoritative value.
      const nextReported = Math.min(file.size, Math.max(reported, sent));
      if (nextReported > reported) {
        reported = nextReported;
        onProgress?.(reported, file.size);
      }
      return route;
    };

    const sendRelayManifest = async (): Promise<void> => {
      if (relayManifestSent) return;
      if (this.direct?.readyState === 'open') {
        this.fallbackToRelay(`relay window requires a relay manifest (${manifest.id.slice(0, 8)})`);
      }
      const result = await this.sendManifestWithAck(manifest);
      relayManifestSent = result.relaySent;
      relayChunkAckCapable = result.chunkAckCapable;
    };

    if (!directMode) await sendRelayManifest();

    let nextSeq = firstSeq;
    let relayWindowRetries = 0;
    let relayWindowNumber = 0;
    while (nextSeq < manifest.totalChunks) {
      // A late completion ACK can arrive after a direct timeout/replay has
      // already started. Do not publish the rest of an already completed file
      // into the relay queue.
      if (this.chunkAckProgress.get(manifest.id)?.complete) {
        onProgress?.(file.size, file.size);
        nextSeq = manifest.totalChunks;
        break;
      }
      if (directMode && this.direct?.readyState === 'open') {
        const windowStart = nextSeq;
        const windowEnd = Math.min(manifest.totalChunks, windowStart + DIRECT_CHUNK_WINDOW);
        let route: 'direct' | 'relay' = 'direct';
        for (let seq = windowStart; seq < windowEnd; seq++) {
          route = await sendChunk(seq);
          if (route !== 'direct') break;
        }

        if (route === 'direct') {
          const confirmed = await this.waitForChunkAck(
            manifest.id,
            windowEnd,
            DIRECT_CHUNK_ACK_TIMEOUT_MS,
          );
          if (confirmed) {
            nextSeq = windowEnd;
            continue;
          }

          const lastAck = this.chunkAckProgress.get(manifest.id)?.nextSeq ?? windowStart;
          const replayFrom = Math.max(firstSeq, lastAck);
          this.events.onLog?.('p2p', `direct chunk ACK timeout for ${manifest.name}; relay repair from ${replayFrom} (ack ${lastAck}, target ${windowEnd})`);
          this.fallbackToRelay(`direct chunk ack timeout (${manifest.id.slice(0, 8)})`);
          directMode = false;
          await sendRelayManifest();
          nextSeq = replayFrom;
          continue;
        }

        // send() detected a failed/closed direct channel while writing this
        // window. Repair from the last contiguous receive position, not from
        // the next local sequence, because the failed send may have left a
        // hole in the receiver.
        const lastAck = this.chunkAckProgress.get(manifest.id)?.nextSeq ?? windowStart;
        const replayFrom = Math.max(firstSeq, lastAck);
        directMode = false;
        await sendRelayManifest();
        nextSeq = replayFrom;
        continue;
      }

      await sendRelayManifest();
      // A late ACK can arrive while a retry is being prepared.  Never send a
      // window that is already covered by the receiver's contiguous prefix.
      const acknowledged = this.chunkAckProgress.get(manifest.id)?.nextSeq ?? firstSeq;
      if (acknowledged > nextSeq) {
        this.events.onLog?.('xfer', `relay window skipped ${manifest.name} (${manifest.id.slice(0, 8)}): next=${nextSeq}, ack=${acknowledged}`);
        nextSeq = Math.min(manifest.totalChunks, acknowledged);
        relayWindowRetries = 0;
        continue;
      }
      const windowStart = nextSeq;
      const windowEnd = Math.min(manifest.totalChunks, windowStart + RELAY_CHUNK_WINDOW);
      const windowNumber = ++relayWindowNumber;
      const windowStartedAt = Date.now();
      this.events.onLog?.('xfer', `relay window start ${manifest.name} (${manifest.id.slice(0, 8)}) #${windowNumber} [${windowStart},${windowEnd})`);
      for (let seq = windowStart; seq < windowEnd; seq++) await sendChunk(seq);
      this.events.onLog?.('xfer', `relay window published ${manifest.name} (${manifest.id.slice(0, 8)}) #${windowNumber} [${windowStart},${windowEnd}) in ${Date.now() - windowStartedAt}ms, chunkAck=${relayChunkAckCapable}`);

      // Peers before the receive-ACK capability continue on the old relay
      // path. New peers use the same bounded window on LiveKit as on P2P;
      // publish() alone only proves local enqueue, not remote processing.
      if (!relayChunkAckCapable) {
        this.events.onLog?.('xfer', `relay window unconfirmed ${manifest.name} (${manifest.id.slice(0, 8)}) #${windowNumber}; peer has no chunk-ACK capability`);
        nextSeq = windowEnd;
        continue;
      }
      // Start the timeout after all chunks have been accepted by the local
      // LiveKit client.  More importantly, waitForChunkAck treats any ACK
      // progress as activity and only times out after a period of silence.
      // A slow but healthy relay must not be mistaken for a dead one.
      const confirmed = await this.waitForChunkAck(
        manifest.id,
        windowEnd,
        RELAY_CHUNK_ACK_TIMEOUT_MS,
      );
      if (confirmed) {
        const ack = this.chunkAckProgress.get(manifest.id);
        this.events.onLog?.('xfer', `relay window ACK confirmed ${manifest.name} (${manifest.id.slice(0, 8)}) #${windowNumber}: next=${ack?.nextSeq ?? 0}, bytes=${ack?.receivedBytes ?? 0}, complete=${Boolean(ack?.complete)}, wait=${Date.now() - windowStartedAt}ms`);
        relayWindowRetries = 0;
        nextSeq = Math.max(windowEnd, ack?.nextSeq ?? 0);
        continue;
      }
      const lastAck = this.chunkAckProgress.get(manifest.id)?.nextSeq ?? windowStart;
      const replayFrom = Math.max(firstSeq, lastAck - RELAY_REPLAY_OVERLAP_CHUNKS);
      relayWindowRetries++;
      this.events.onLog?.('xfer', `relay chunk ACK timeout for ${manifest.name} (${manifest.id.slice(0, 8)}) #${windowNumber} after ${Date.now() - windowStartedAt}ms; retry ${relayWindowRetries}/${MAX_RELAY_WINDOW_RETRIES} from ${replayFrom} (ack ${lastAck}, target ${windowEnd})`);
      if (relayWindowRetries > MAX_RELAY_WINDOW_RETRIES) {
        this.chunkAckProgress.delete(manifest.id);
        this.chunkAckCapabilities.delete(manifest.id);
        throw new Error(`relay chunk ack timeout (${manifest.id.slice(0, 8)})`);
      }
      nextSeq = replayFrom;
    }
    this.chunkAckProgress.delete(manifest.id);
    this.chunkAckCapabilities.delete(manifest.id);
    this.binaryChunkCapabilities.delete(manifest.id);
    return manifest;
  }

  /**
   * A manifest ACK confirms that the peer has created its receiver. New peers
   * also advertise chunk ACK support, which lets relay sends use bounded
   * windows instead of filling LiveKit's local reliable-data queue.
   */
  private async sendManifestWithAck(manifest: FileManifest): Promise<ManifestSendResult> {
    const wasDirect = this.direct?.readyState === 'open';
    if (!wasDirect) {
      this.events.onLog?.('xfer', `manifest send route=relay ${manifest.name} (${manifest.id.slice(0, 8)})`);
      const ack = this.waitForManifestAck(manifest.id, P2P_MANIFEST_ACK_TIMEOUT_MS);
      await this.send({ kind: 'file-manifest', manifest });
      const accepted = await ack;
      const chunkAckCapable = accepted && this.chunkAckCapabilities.has(manifest.id);
      const binaryChunkCapable = accepted && this.binaryChunkCapabilities.has(manifest.id);
      this.events.onLog?.('xfer', `manifest ACK route=relay ${manifest.name} (${manifest.id.slice(0, 8)}): accepted=${accepted}, chunkAck=${chunkAckCapable}`);
      if (!accepted) {
        this.events.onLog?.('xfer', `relay manifest ack missing for ${manifest.name}; using legacy relay pacing`);
      }
      return {
        chunkAckCapable,
        binaryChunkCapable,
        relaySent: true,
      };
    }

    this.events.onLog?.('xfer', `manifest send route=direct ${manifest.name} (${manifest.id.slice(0, 8)})`);
    const ack = this.waitForManifestAck(manifest.id, P2P_MANIFEST_ACK_TIMEOUT_MS);
    let route: 'direct' | 'relay';
    try {
      route = await this.send({ kind: 'file-manifest', manifest });
    } catch (e) {
      this.resolveManifestAck(manifest.id, false);
      throw e;
    }
    const accepted = await ack;
    if (accepted) {
      const chunkAckCapable = this.chunkAckCapabilities.has(manifest.id);
      const binaryChunkCapable = this.binaryChunkCapabilities.has(manifest.id);
      this.events.onLog?.('xfer', `manifest ACK route=${route} ${manifest.name} (${manifest.id.slice(0, 8)}): accepted=true, chunkAck=${chunkAckCapable}`);
      return {
        chunkAckCapable,
        binaryChunkCapable,
        relaySent: route === 'relay',
      };
    }
    this.events.onLog?.('xfer', `manifest ACK timeout route=${route} ${manifest.name} (${manifest.id.slice(0, 8)})`);

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
      const relayAccepted = await relayAck;
      if (!relayAccepted) {
        this.events.onLog?.('p2p', `relay manifest ack missing for ${manifest.name}; continuing with task ack`);
      }
      const chunkAckCapable = relayAccepted && this.chunkAckCapabilities.has(manifest.id);
      const binaryChunkCapable = relayAccepted && this.binaryChunkCapabilities.has(manifest.id);
      this.events.onLog?.('xfer', `manifest ACK route=relay-after-direct ${manifest.name} (${manifest.id.slice(0, 8)}): accepted=${relayAccepted}, chunkAck=${chunkAckCapable}`);
      return {
        chunkAckCapable,
        binaryChunkCapable,
        relaySent: true,
      };
    } else {
      this.events.onLog?.('p2p', `manifest ack missing for ${manifest.name}; continuing with task ack`);
      return {
        chunkAckCapable: this.chunkAckCapabilities.has(manifest.id),
        binaryChunkCapable: this.binaryChunkCapabilities.has(manifest.id),
        relaySent: true,
      };
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

  private resolveManifestAck(id: string, ok: boolean, source: 'relay' | 'direct' = 'relay'): void {
    if (ok && this.manifestAckWaiters.has(id)) {
      this.events.onLog?.('xfer', `manifest ACK received (${id.slice(0, 8)}) source=${source}`);
    }
    this.manifestAckWaiters.get(id)?.(ok);
  }

  /**
   * Wait for the receiver to confirm a contiguous data window.
   *
   * The timeout is a silence timeout, not a total-window deadline.  LiveKit
   * can deliver a relay ACK several seconds after the first chunk, especially
   * when the reliable data queue is draining.  As long as the receiver keeps
   * advancing its contiguous prefix or byte count, retrying would only add
   * duplicate payloads to that same queue.
   */
  private async waitForChunkAck(id: string, targetSeq: number, timeoutMs: number): Promise<boolean> {
    let lastSeq = this.chunkAckProgress.get(id)?.nextSeq ?? 0;
    let lastBytes = this.chunkAckProgress.get(id)?.receivedBytes ?? 0;
    let lastProgressAt = Date.now();
    while (true) {
      const progress = this.chunkAckProgress.get(id);
      if ((progress?.nextSeq ?? 0) >= targetSeq || progress?.complete) return true;
      if (
        (progress?.nextSeq ?? 0) > lastSeq ||
        (progress?.receivedBytes ?? 0) > lastBytes
      ) {
        lastSeq = progress?.nextSeq ?? lastSeq;
        lastBytes = progress?.receivedBytes ?? lastBytes;
        lastProgressAt = Date.now();
      }
      if (Date.now() - lastProgressAt >= timeoutMs) return false;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  private resolveChunkAck(
    msg: Extract<FlingMessage, { kind: 'file-chunk-ack' }>,
    source: 'relay' | 'direct' = 'relay',
  ): void {
    const nextSeq = Math.max(0, Math.floor(Number(msg.nextSeq) || 0));
    const receivedBytes = Math.max(0, Math.floor(Number(msg.receivedBytes) || 0));
    const previous = this.chunkAckProgress.get(msg.id);
    if (
      !previous ||
      nextSeq > previous.nextSeq ||
      receivedBytes > previous.receivedBytes ||
      (msg.complete && !previous.complete)
    ) {
      this.chunkAckProgress.set(msg.id, {
        nextSeq: Math.max(previous?.nextSeq ?? 0, nextSeq),
        receivedBytes: Math.max(previous?.receivedBytes ?? 0, receivedBytes),
        complete: Boolean(msg.complete) || Boolean(previous?.complete),
      });
      this.events.onLog?.('xfer', `chunk ACK received (${msg.id.slice(0, 8)}): source=${source}, next=${nextSeq}, bytes=${receivedBytes}, complete=${Boolean(msg.complete)}`);
    }
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

  async sendTaskFailed(taskId: string, failures: { name: string; size: number; error: string }[]): Promise<void> {
    const bounded = failures.slice(0, 64).map((f) => ({
      name: f.name.slice(0, 180),
      size: Math.max(0, Math.floor(Number(f.size) || 0)),
      error: f.error.slice(0, 240),
    }));
    await this.send({ kind: 'task-failed', taskId, failures: bounded });
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
        if (msg.kind === 'file-chunk') this.events.onFileRoute?.('p2p', msg.id);
        return 'direct';
      } catch {
        this.events.onLog?.('p2p', `direct send failed for ${msg.kind} ${msg.kind === 'file-chunk' ? msg.seq : ''}, falling back to relay`);
        this.fallbackToRelay(`direct send failed for ${msg.kind}`);
      }
    } else if (directKinds && dc) {
      // A browser can expose a closed RTCDataChannel for a short period
      // before its owner receives the close event. Detach it here so the
      // next file cannot keep believing that a direct path exists.
      this.events.onLog?.('p2p', `direct channel not open for ${msg.kind}, falling back to relay`);
      this.fallbackToRelay(`direct channel state is ${dc.readyState}`);
    }
    const sealed = await sealText(this.roomKey, JSON.stringify(msg));
    await this.transport.publish(enc.encode(sealed), FLING_TOPIC);
    if (msg.kind === 'file-chunk') this.events.onFileRoute?.('relay', msg.id);
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
    if (msg.kind === 'file-chunk') this.events.onFileRoute?.('relay', msg.id);
    this.dispatchMessage(msg, from, 'relay');
  }

  /**
   * Inbound message from the direct P2P channel (already DTLS-encrypted).
   * Synchronous fast path: no AES-GCM, just JSON parse. Handled immediately
   * in onmessage instead of chaining onto the shared handleQueue, so a slow
   * relay trickle (decrypt-bound) can never head-of-line-block direct chunks.
   * Manifest/chunk races are covered by early-chunk buffering.
   */
  private handleDirectMessage(data: Uint8Array, from: string): void {
    let frame: DirectFileChunkFrame | undefined;
    try {
      frame = decodeDirectFileChunkFrame(data);
    } catch (e) {
      this.events.onLog?.('xfer', `dropping invalid direct file frame: ${(e as Error).message}`);
      return;
    }
    if (frame) {
      this.events.onFileRoute?.('p2p', frame.id);
      const n = (this.directChunkCounts.get(frame.id) ?? 0) + 1;
      this.directChunkCounts.set(frame.id, n);
      if (n % 20 === 0) this.events.onLog?.('xfer', `direct binary chunks received: ${n} (${frame.id.slice(0, 8)})`);
      this.handleChunkMessage({ id: frame.id, seq: frame.seq, bytes: frame.payload, source: 'direct' });
      return;
    }
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
      this.events.onFileRoute?.('p2p', msg.id);
      const n = (this.directChunkCounts.get(msg.id) ?? 0) + 1;
      this.directChunkCounts.set(msg.id, n);
      if (n % 20 === 0) {
        this.events.onLog?.('xfer', `direct chunks received: ${n} (${msg.id.slice(0, 8)})`);
      }
    }
    this.dispatchMessage(msg, from, 'direct');
  }

  /** Per-file direct-chunk counters (diagnostics). Cleared in finishFile. */
  private directChunkCounts = new Map<string, number>();

  private dispatchMessage(msg: FlingMessage, from: string, source: 'relay' | 'direct' = 'relay'): void {
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
          mimeType: typeof m.mimeType === 'string' ? m.mimeType.slice(0, 256) : undefined,
          // 文件夹发送的相对路径（如 "docs/report.pdf"），老版本没有则为 undefined
          path: typeof m.path === 'string' ? m.path : undefined,
        };
        if (this.incoming.has(manifest.id) || this.completedFiles.has(manifest.id)) {
          const completed = this.completedFiles.has(manifest.id);
          this.events.onLog?.('xfer', `duplicate manifest received (${manifest.id.slice(0, 8)}) source=${source}, completed=${completed}, acknowledging without reset`);
          void this.send({ kind: 'file-manifest-ack', id: manifest.id, chunkAck: true, binaryChunks: true }).catch(() => {});
          // If the sender lost the final chunk ACK and is repairing through
          // relay, there is no incoming receiver entry left to emit another
          // ACK. Re-advertise completion on the duplicate manifest so the
          // sender can stop the replay instead of resending the whole file.
          if (completed) {
            void this.send({
              kind: 'file-chunk-ack',
              id: manifest.id,
              nextSeq: manifest.totalChunks,
              receivedBytes: manifest.size,
              complete: true,
            }).catch(() => {});
          }
          break;
        }
        this.incoming.set(manifest.id, {
          receiver: new FileReceiver(manifest),
          from,
          lastAckSeq: 0,
          lastAckAt: 0,
          ackChain: Promise.resolve(),
          ackPending: false,
          finalizing: false,
        });
        this.events.onLog?.('xfer', `manifest received ${manifest.name} (${manifest.id.slice(0, 8)}) source=${source}, chunks=${manifest.totalChunks}, size=${manifest.size}`);
        // The acknowledgement is deliberately not a directKinds message, so
        // it always travels over LiveKit even while the data channel is sick.
        void this.send({ kind: 'file-manifest-ack', id: manifest.id, chunkAck: true, binaryChunks: true }).catch(() => {});
        this.events.onFileManifest?.(manifest, from);
        // Replay any chunks that beat the manifest here.
        const buffered = this.earlyChunks.get(manifest.id);
        if (buffered) {
          this.earlyChunks.delete(manifest.id);
          this.events.onLog?.('xfer', `replaying ${buffered.length} early chunks for ${manifest.name}`);
          for (const c of buffered) {
            this.events.onFileRoute?.(c.source === 'direct' ? 'p2p' : 'relay', c.id);
            this.handleChunkMessage(c);
          }
        }
        // Empty file: complete immediately.
        if (manifest.totalChunks === 1 && manifest.size === 0) {
          void this.finishFile(manifest.id);
        }
        break;
      }
      case 'file-chunk': {
        this.handleChunkMessage({ id: msg.id, seq: msg.seq, dataB64: msg.data, source });
        break;
      }
      case 'file-manifest-ack':
        if (msg.chunkAck === true) this.chunkAckCapabilities.add(msg.id);
        if (msg.binaryChunks === true) this.binaryChunkCapabilities.add(msg.id);
        this.resolveManifestAck(msg.id, true, source);
        break;
      case 'file-chunk-ack':
        this.resolveChunkAck(msg, source);
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
      case 'task-failed':
        if (Array.isArray(msg.failures)) {
          const failures = msg.failures.filter(
            (f) => f && typeof f.name === 'string' && Number.isFinite(f.size) && typeof f.error === 'string',
          );
          this.events.onTaskFailed?.(msg.taskId, failures, from);
        }
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

  private async finishFile(id: string): Promise<void> {
    const entry = this.incoming.get(id);
    if (!entry || entry.finalizing) return;
    entry.finalizing = true;
    let bytes: Uint8Array;
    try {
      bytes = await entry.receiver.assembleAsync();
      await this.events.onFileComplete?.(id, entry.receiver.manifest, bytes);
    } catch (e) {
      this.events.onFileError?.(id, (e as Error).message);
      this.incoming.delete(id);
      this.earlyChunks.delete(id);
      return;
    }

    this.completedFiles.add(id);
    this.incoming.delete(id);
    this.earlyChunks.delete(id);
    this.directChunkCounts.delete(id);
    // Do not wait for a queued progress publish: LiveKit can stall that
    // promise. The final ACK carries the monotonic total and resolveChunkAck
    // ignores any late/stale progress ACK, so it can safely be sent now.
    try {
      await this.send({
        kind: 'file-chunk-ack',
        id,
        nextSeq: entry.receiver.nextSeq,
        receivedBytes: entry.receiver.receivedByteCount,
        complete: true,
      });
      this.events.onLog?.('xfer', `final chunk ACK sent (${id.slice(0, 8)}): bytes=${entry.receiver.receivedByteCount}`);
    } catch (e) {
      this.events.onLog?.('xfer', `final chunk ACK failed after save (${id.slice(0, 8)}): ${(e as Error).message}`);
    }
  }

  /** Single place that applies a file-chunk to its receiver. */
  private handleChunkMessage(
    msg: ReceivedFileChunk,
  ): void {
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
        this.events.onLog?.('xfer', `chunk before manifest (${msg.id.slice(0, 8)}): source=${msg.source}, seq=${msg.seq}`);
      }
      return;
    }
    if (entry.finalizing) return;
    const { receiver } = entry;
    let complete = false;
    try {
      if (msg.bytes) complete = receiver.addChunkBytes(msg.seq, msg.bytes);
      else if (typeof msg.dataB64 === 'string') complete = receiver.addChunk(msg.seq, msg.dataB64);
      else throw new Error('missing chunk payload');
    } catch (e) {
      this.events.onFileError?.(msg.id, (e as Error).message);
      this.incoming.delete(msg.id);
      return;
    }
    // Use the integer accumulator as the source of truth. Computing
    // `progress * totalBytes` introduced values such as 3932159.9999999995
    // into task-progress, which made sender/receiver progress comparisons
    // look inconsistent and could leave a target one fraction short.
    this.events.onFileProgress?.(msg.id, receiver.receivedByteCount, receiver.totalBytes);
    // Both transports use the same application-level receive ACK. Direct
    // ACKs detect a UDP black hole; relay ACKs keep LiveKit's reliable queue
    // from being flooded by publish() calls that only completed locally.
    this.maybeSendChunkAck(msg.id, entry, complete, msg.source);
    if (complete) void this.finishFile(msg.id);
  }

  /**
   * ACK data windows through the encrypted LiveKit control path. A small
   * cadence keeps control traffic bounded while still giving the sender a
   * hard end-to-end signal for every direct or relay window.
   */
  private maybeSendChunkAck(
    id: string,
    entry: {
      receiver: FileReceiver;
      lastAckSeq: number;
      lastAckAt: number;
      ackChain: Promise<void>;
      ackPending: boolean;
      pendingAck?: { nextSeq: number; receivedBytes: number; complete: boolean };
    },
    complete: boolean,
    source: 'relay' | 'direct',
  ): void {
    const now = Date.now();
    const nextSeq = entry.receiver.nextSeq;
    const progressAck = complete || (
      nextSeq > entry.lastAckSeq &&
      nextSeq > 0 &&
      (
        nextSeq % DIRECT_CHUNK_ACK_EVERY === 0 ||
        now - entry.lastAckAt >= CHUNK_ACK_PROGRESS_INTERVAL_MS
      )
    );
    const resendAck = !complete &&
      entry.lastAckSeq > 0 &&
      now - entry.lastAckAt >= CHUNK_ACK_RESEND_INTERVAL_MS;
    if (!progressAck && !resendAck) return;

    // If the receiver has advanced by a non-window amount, resend the last
    // known contiguous position as a heartbeat.  A sender that did not see
    // the original ACK can then replay only from that exact position.
    const ack = {
      nextSeq: progressAck ? nextSeq : entry.lastAckSeq,
      receivedBytes: entry.receiver.receivedByteCount,
      // This ACK is strictly byte-received progress. A separate complete=true
      // ACK is sent only after async SHA-256 validation and the save callback.
      complete: false,
    };
    entry.lastAckSeq = Math.max(entry.lastAckSeq, ack.nextSeq);
    entry.lastAckAt = now;
    if (entry.ackPending) {
      const pending = entry.pendingAck;
      if (!pending || ack.nextSeq > pending.nextSeq || ack.complete) {
        entry.pendingAck = ack;
      }
    } else {
      this.enqueueChunkAck(id, entry, ack);
    }
    this.events.onLog?.('xfer', `chunk ACK queued (${id.slice(0, 8)}): source=${source}, next=${ack.nextSeq}, bytes=${ack.receivedBytes}, complete=${complete}`);
  }

  /** Serialize receive ACKs so a slow relay cannot accumulate promises. */
  private enqueueChunkAck(
    id: string,
    entry: {
      ackChain: Promise<void>;
      ackPending: boolean;
      pendingAck?: { nextSeq: number; receivedBytes: number; complete: boolean };
    },
    ack: { nextSeq: number; receivedBytes: number; complete: boolean },
  ): void {
    entry.ackPending = true;
    entry.ackChain = entry.ackChain
      .catch(() => {})
      .then(async () => {
        await this.send({
          kind: 'file-chunk-ack',
          id,
          nextSeq: ack.nextSeq,
          receivedBytes: ack.receivedBytes,
          complete: ack.complete,
        });
        this.events.onLog?.('xfer', `chunk ACK sent (${id.slice(0, 8)}): next=${ack.nextSeq}, bytes=${ack.receivedBytes}, complete=${ack.complete}`);
      })
      .catch(() => {})
      .finally(() => {
        entry.ackPending = false;
        const pending = entry.pendingAck;
        entry.pendingAck = undefined;
        if (pending) this.enqueueChunkAck(id, entry, pending);
      });
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
    this.chunkAckProgress.clear();
    this.chunkAckCapabilities.clear();
    this.binaryChunkCapabilities.clear();
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
