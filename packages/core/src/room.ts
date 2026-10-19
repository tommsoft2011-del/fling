// FlingRoom: transport-agnostic room logic.
// Joins a LiveKit room derived from the pairing code / room number and
// exchanges end-to-end encrypted messages + files over the data channel.

import { deriveRoomKey, openText, sealText } from './crypto.js';
import { FileReceiver, type FileLike, buildManifest } from './files.js';
import { AdaptiveDirectWindow, DIRECT_WINDOW_DEFAULT_MAX_BYTES, DIRECT_WINDOW_STEPS_BYTES } from './direct-window.js';
import {
  P2P_DRAIN_THRESHOLD,
  P2P_DRAIN_TIMEOUT_MS,
  P2P_MANIFEST_ACK_TIMEOUT_MS,
  type DirectChannel,
} from './p2p.js';
import {
  DIRECT_FIRST_FRAME_STARTUP_TIMEOUT_MS,
  FILE_CHUNK_PREPARATION_TIMEOUT_MS,
  FILE_STARTUP_PREPARATION_TIMEOUT_MS,
  MANIFEST_PREPARATION_TIMEOUT_MS,
  MANIFEST_PUBLISH_TIMEOUT_MS,
  RELAY_PUBLISH_TIMEOUT_MS,
  runWithStageTimeout,
  StageTimeoutError,
} from './stage-timeout.js';
import {
  FLING_TOPIC,
  DIRECT_FILE_BATCH_FRAME_HEADER_SIZE,
  DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES,
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
  /** Desktop sinks consume bounded batches and verify before finalizing the save. */
  onFileCompleteStream?(id: string, manifest: FileManifest, receiver: FileReceiver): void | Promise<void>;
  /** All bytes arrived; receiver is assembling, hashing, or persisting the file. */
  onFileFinalizing?(id: string, size: number): void;
  onFileError?(id: string, error: string): void;
  /** Rust transfer completed (file persisted by Rust). Transfer ID is the manifest ID. */
  onTransferDone?(transferId: string): void;
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
  /** Receiver-confirmed arrival and Rust append progress (see task-progress). */
  onTaskProgress?(taskId: string, receivedBytes: number, totalBytes: number, from: string, savedBytes?: number): void;
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
const RELAY_CHUNK_WINDOW = 8;
// The receive ACK travels over LiveKit, not over the RTCDataChannel.  A
// healthy direct transfer can therefore finish a window before the ACK gets
// through the relay.  Three seconds turned a delayed final ACK into a false
// P2P failure (the receiver had already got the complete file).
const DIRECT_CHUNK_ACK_TIMEOUT_MS = 8_000;
const RELAY_CHUNK_ACK_TIMEOUT_MS = 5_000;
/** Bound the receiver's assemble/hash/save phase without treating it as a stalled transfer. */
const FILE_FINALIZE_ACK_TIMEOUT_MS = 120_000;
// Keep receiver feedback cadence at 1 MiB even when the sender's bounded
// direct window grows to 4/8 MiB, so progress and black-hole evidence remain
// available while a larger window is in flight.
const DIRECT_CHUNK_ACK_EVERY = 32;
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
  directFramePayloadBytes: number;
  relaySent: boolean;
}

export interface FlingRoomTimeouts {
  manifestPreparationMs: number;
  fileChunkPreparationMs: number;
  manifestPublishMs: number;
  firstDirectFrameStartupMs: number;
  relayPublishMs: number;
}

interface PendingChunkAck {
  nextSeq: number;
  receivedBytes: number;
  complete: boolean;
  ackId: string;
  queuedAtMonoMs: number;
  source: 'relay' | 'direct';
}

interface ChunkAckProgress {
  nextSeq: number;
  receivedBytes: number;
  complete: boolean;
  failed?: boolean;
  updatedMonoAt: number;
  ackId: string;
  callbackToHandlerMs: number;
  handlerMs: number;
}

interface ChunkAckWaitResult {
  ackId: string;
  nextSeq: number;
  receivedAtMonoMs: number;
  callbackToHandlerMs: number;
  handlerMs: number;
  waiterUnlockMs: number;
}

interface ReceivedFileChunk {
  id: string;
  seq: number;
  source: 'relay' | 'direct';
  dataB64?: string;
  bytes?: Uint8Array<ArrayBuffer>;
}

export class FlingRoom {
  private readonly directDrainThresholdBytes: number;
  private readonly directWindowMaxBytes: number;
  private readonly timeouts: FlingRoomTimeouts;
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
    pendingAck?: PendingChunkAck;
    progressAckTimer?: ReturnType<typeof setTimeout>;
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
  private chunkAckProgress = new Map<string, ChunkAckProgress>();
  /** Event-driven ACK waiters; notifications are independent of the polling interval. */
  private chunkAckWaiters = new Map<string, Set<(cancelled: boolean) => void>>();
  /** Sizes let the sender distinguish a full-byte ACK from a saved-file ACK. */
  private outgoingFileSizes = new Map<string, number>();
  private finalizingFiles = new Set<string>();
  /** Peer capability advertised by the file-manifest acknowledgement. */
  private chunkAckCapabilities = new Set<string>();
  /** Peers advertising support for raw binary direct-channel chunks. */
  private binaryChunkCapabilities = new Set<string>();
  /** Per-file remote payload cap for coalesced direct frames, negotiated by manifest ACK. */
  private directFramePayloadLimits = new Map<string, number>();
  /** Prevent a manifest retry from creating a second receiver. */
  private completedFiles = new Set<string>();
  private firstDirectJsonFrameIds = new Set<string>();
  private ackDiagnosticState = new Map<string, {
    counts: Record<string, number>;
    lastAt: Record<string, number>;
    boundaryLogged: {
      receiver_queue: { direct: boolean; relay: boolean };
      receiver_publish: { direct: boolean; relay: boolean };
    };
    legacyAckLogged: boolean;
  }>();

  private shouldLogAckDiagnostic(
    id: string,
    stage: 'receiver_queue' | 'receiver_publish' | 'sender_received' | 'sender_ignored',
    terminal = false,
    boundary?: 'direct' | 'relay',
    legacyAck = false,
  ): boolean {
    let state = this.ackDiagnosticState.get(id);
    if (!state) {
      state = {
        counts: {},
        lastAt: {},
        boundaryLogged: {
          receiver_queue: { direct: false, relay: false },
          receiver_publish: { direct: false, relay: false },
        },
        legacyAckLogged: false,
      };
      this.ackDiagnosticState.set(id, state);
    }
    const count = (state.counts[stage] ?? 0) + 1;
    state.counts[stage] = count;
    const now = Date.now();
    const boundaryState = stage === 'receiver_queue'
      ? state.boundaryLogged.receiver_queue
      : stage === 'receiver_publish'
        ? state.boundaryLogged.receiver_publish
        : undefined;
    const boundaryDue = boundary !== undefined && boundaryState !== undefined && !boundaryState[boundary];
    if (boundaryDue && boundary !== undefined && boundaryState) boundaryState[boundary] = true;
    const legacyAckDue = legacyAck && !state.legacyAckLogged;
    if (legacyAckDue) state.legacyAckLogged = true;
    const shouldLog = boundaryDue || legacyAckDue || count === 1 || terminal || count % 16 === 0 || now - (state.lastAt[stage] ?? 0) >= 5_000;
    if (shouldLog) state.lastAt[stage] = now;
    return shouldLog;
  }

  constructor(
    private transport: FlingTransport,
    private deviceName: string,
    /** @internal Isolated backpressure-test seam; production callers use the 256 KiB default. */
    directDrainThresholdBytes = P2P_DRAIN_THRESHOLD,
    /** @internal Isolated experiment seam; production callers remain capped at 8 MiB. */
    directWindowMaxBytes = DIRECT_WINDOW_DEFAULT_MAX_BYTES,
    timeouts: Partial<FlingRoomTimeouts> = {},
  ) {
    if (!Number.isSafeInteger(directDrainThresholdBytes) || directDrainThresholdBytes < 64 * 1024 || directDrainThresholdBytes > 2 * 1024 * 1024) {
      throw new Error(`direct drain threshold out of test bounds (${directDrainThresholdBytes})`);
    }
    if (!DIRECT_WINDOW_STEPS_BYTES.includes(directWindowMaxBytes as (typeof DIRECT_WINDOW_STEPS_BYTES)[number])) {
      throw new Error(`direct window maximum out of test bounds (${directWindowMaxBytes})`);
    }
    this.directDrainThresholdBytes = directDrainThresholdBytes;
    this.directWindowMaxBytes = directWindowMaxBytes;
    this.timeouts = {
      manifestPreparationMs: timeouts.manifestPreparationMs ?? MANIFEST_PREPARATION_TIMEOUT_MS,
      fileChunkPreparationMs: timeouts.fileChunkPreparationMs ?? FILE_CHUNK_PREPARATION_TIMEOUT_MS,
      manifestPublishMs: timeouts.manifestPublishMs ?? MANIFEST_PUBLISH_TIMEOUT_MS,
      firstDirectFrameStartupMs: timeouts.firstDirectFrameStartupMs ?? DIRECT_FIRST_FRAME_STARTUP_TIMEOUT_MS,
      relayPublishMs: timeouts.relayPublishMs ?? RELAY_PUBLISH_TIMEOUT_MS,
    };
  }

  private async runStage<T>(stage: string, timeoutMs: number, operation: () => T | PromiseLike<T>): Promise<T> {
    try {
      return await runWithStageTimeout(stage, timeoutMs, operation);
    } catch (error) {
      if (error instanceof StageTimeoutError) this.logTimerStage(error);
      throw error;
    }
  }

  private logTimerStage(error: StageTimeoutError): void {
    this.events?.onLog?.('p2p', `transfer timer stage fired: stage=${error.stage}, timeoutMs=${error.timeoutMs}`);
  }

  private startupStageTimeoutMs(requestedMs: number, deadlineAt?: number): number {
    if (!deadlineAt) return requestedMs;
    const remainingMs = deadlineAt - Date.now();
    if (remainingMs <= 0) {
      const error = new StageTimeoutError('source_manifest_startup', FILE_STARTUP_PREPARATION_TIMEOUT_MS);
      this.logTimerStage(error);
      throw error;
    }
    return Math.min(requestedMs, remainingMs);
  }

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
      this.events.onLog?.('p2p', 'direct channel attached');
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
      const receivedAtMonoMs = performance.now();
      this.handleQueue = this.handleQueue
        .then(() => this.handlePayload(payload, from, receivedAtMonoMs))
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

  /** Send a file; receiverSaved is true only after complete=true confirms persistence. */
  async sendFile(
    file: FileLike,
    onProgress?: (sentBytes: number, totalBytes: number, receiverSaved?: boolean) => void,
    kind: FileKind = 'file',
    /** Reuse the same id when a partially delivered file must be replayed. */
    existingManifest?: FileManifest,
    /** Start a relay replay near the last confirmed receive position. */
    fromSeq = 0,
    /** Absolute source/manifest startup deadline shared with the desktop task. */
    startupDeadlineAt?: number,
  ): Promise<FileManifest> {
    const manifestPreparationStartedAt = performance.now();
    const manifest = existingManifest ?? await this.runStage(
      'manifest_preparation',
      this.startupStageTimeoutMs(this.timeouts.manifestPreparationMs, startupDeadlineAt),
      () => buildManifest(file, kind),
    );
    const manifestPreparedAt = performance.now();
    this.events.onLog?.('xfer', `manifest preparation complete (${manifest.id.slice(0, 8)}): source=${existingManifest ? 'prehashed' : 'hash_in_sendFile'}, size=${manifest.size}, chunks=${manifest.totalChunks}, elapsedMs=${Math.round(manifestPreparedAt - manifestPreparationStartedAt)}`);
    this.outgoingFileSizes.set(manifest.id, manifest.size);
    this.finalizingFiles.delete(manifest.id);

    // A partial replay is a repair operation. Do not send its manifest and
    // then continue on a still-open direct path while the prefix may contain
    // holes; force the repair copy through the relay.
    const firstSeq = Math.max(0, Math.min(manifest.totalChunks, Math.floor(fromSeq)));
    if (firstSeq > 0 && this.direct?.readyState === 'open') {
      this.fallbackToRelay(`partial replay requires relay (${manifest.id.slice(0, 8)})`);
    }
    const manifestSendStartedAt = performance.now();
    const manifestResult = await this.sendManifestWithAck(manifest, startupDeadlineAt);
    const manifestAckAt = performance.now();
    const directChannelStateAtDecision = this.direct?.readyState ?? 'absent';
    const routeReason = firstSeq > 0
      ? 'partial_replay_requires_relay'
      : directChannelStateAtDecision !== 'open'
        ? 'data_channel_not_open'
        : !manifestResult.chunkAckCapable
          ? 'manifest_ack_missing_chunk_ack_capability'
          : 'direct_eligible';
    this.events.onLog?.('xfer', `manifest send/ack phase complete (${manifest.id.slice(0, 8)}): elapsedMs=${Math.round(manifestAckAt - manifestSendStartedAt)}, relayManifest=${manifestResult.relaySent}, chunkAck=${manifestResult.chunkAckCapable}, binaryChunk=${manifestResult.binaryChunkCapable}, directPayloadLimit=${manifestResult.directFramePayloadBytes}, dcState=${directChannelStateAtDecision}`);
    let directMode = firstSeq === 0 && directChannelStateAtDecision === 'open' && manifestResult.chunkAckCapable;
    let binaryChunkMode = directMode && manifestResult.binaryChunkCapable;
    let directFramePayloadBytes = manifestResult.directFramePayloadBytes;
    let relayManifestSent = manifestResult.relaySent;
    let relayChunkAckCapable = manifestResult.chunkAckCapable;
    this.events.onLog?.('xfer', `file route selected (${manifest.id.slice(0, 8)}): route=${directMode ? 'direct' : 'relay'}, reason=${routeReason}, dcState=${directChannelStateAtDecision}, relayManifest=${relayManifestSent}, chunkAck=${relayChunkAckCapable}, directFramePayloadBytes=${directFramePayloadBytes}`);
    let sent = Math.min(file.size, firstSeq * manifest.chunkSize);
    let reported = sent;
    const directWindow = new AdaptiveDirectWindow(this.directWindowMaxBytes);
    let directWindowCount = 0;
    let directAckedWindowCount = 0;
    let directAckElapsedTotalMs = 0;
    let directAckElapsedMaxMs = 0;
    let directDrainWaitCount = 0;
    let directDrainWaitTotalMs = 0;
    let directBufferedHighWater = 0;
    let directReplayBytes = 0;
    let directCoalescedFrames = 0;
    let directAckSummaryAt = performance.now();
    let firstDirectFrameState: 'none' | 'attempted' | 'accepted' | 'failed' | 'not_submitted' = 'none';
    let firstDirectPayloadPrepared = false;
    let firstDirectFrameStartupExpired = false;
    const logFirstDirectFrame = (
      state: 'payload_prepared' | 'send_attempt' | 'send_accepted' | 'send_rejected' | 'not_submitted',
      seq: number,
      frameBytes: number,
      bufferedAmount: number,
      detail = '',
    ) => {
      this.events.onLog?.('p2p', `first direct frame ${state} (${manifest.id.slice(0, 8)}): seq=${seq}, frameBytes=${frameBytes}, bufferedAmount=${bufferedAmount}, sinceManifestAckMs=${Math.max(0, Math.round(performance.now() - manifestAckAt))}${detail ? `, ${detail}` : ''}`);
    };
    const assertFirstDirectFrameStartupOpen = (startupAttempt: boolean): void => {
      if (startupAttempt && firstDirectFrameStartupExpired) {
        throw new StageTimeoutError('first_direct_frame_startup', this.timeouts.firstDirectFrameStartupMs);
      }
    };

    // Empty files are completed by the receiver as soon as the manifest is
    // accepted; there is no data window to acknowledge.
    if (manifest.size === 0) {
      onProgress?.(0, 0);
      if (manifestResult.chunkAckCapable) {
        this.finalizingFiles.add(manifest.id);
        this.events.onFileFinalizing?.(manifest.id, 0);
        const finalized = await this.waitForFileComplete(manifest, FILE_FINALIZE_ACK_TIMEOUT_MS);
        if (!finalized) {
          const failed = this.chunkAckProgress.get(manifest.id)?.failed;
          this.events.onLog?.('p2p', `transfer timer stage fired: stage=durable_completion_ack, timeoutMs=${FILE_FINALIZE_ACK_TIMEOUT_MS}`);
          this.outgoingFileSizes.delete(manifest.id);
          this.finalizingFiles.delete(manifest.id);
          this.chunkAckProgress.delete(manifest.id);
          this.chunkAckCapabilities.delete(manifest.id);
          this.binaryChunkCapabilities.delete(manifest.id);
          this.directFramePayloadLimits.delete(manifest.id);
          throw new Error(failed
            ? `receiver failed to finalize file (${manifest.id.slice(0, 8)})`
            : `file finalize ack timeout (${manifest.id.slice(0, 8)})`);
        }
        onProgress?.(0, 0, true);
      }
      this.outgoingFileSizes.delete(manifest.id);
      this.finalizingFiles.delete(manifest.id);
      this.chunkAckProgress.delete(manifest.id);
      this.chunkAckCapabilities.delete(manifest.id);
      this.binaryChunkCapabilities.delete(manifest.id);
      this.directFramePayloadLimits.delete(manifest.id);
      return manifest;
    }

    const sendChunk = async (seq: number, startupAttempt = false): Promise<'direct' | 'relay'> => {
      const start = seq * manifest.chunkSize;
      const sliceStartedAt = performance.now();
      const bytes = await this.runStage(
        'file_chunk_preparation',
        this.timeouts.fileChunkPreparationMs,
        () => file.slice(start, Math.min(start + manifest.chunkSize, file.size)),
      );
      assertFirstDirectFrameStartupOpen(startupAttempt);
      if (
        !firstDirectPayloadPrepared && binaryChunkMode && seq === firstSeq &&
        this.direct?.readyState === 'open'
      ) {
        firstDirectPayloadPrepared = true;
        logFirstDirectFrame(
          'payload_prepared', seq, bytes.byteLength, this.direct.bufferedAmount,
          `sliceMs=${Math.round(performance.now() - sliceStartedAt)}`,
        );
      }
      let route: 'direct' | 'relay';
      const dc = this.direct;
      if (binaryChunkMode && dc?.readyState === 'open') {
        try {
          const frame = encodeDirectFileChunkFrame(manifest.id, seq, bytes);
          if (Number.isFinite(dc.maxMessageSize) && frame.byteLength > dc.maxMessageSize) {
            throw new Error(`binary frame exceeds SCTP maxMessageSize (${frame.byteLength} > ${dc.maxMessageSize})`);
          }
          // Keep the wait target equal to bufferedAmountLowThreshold. Waiting
          // for a smaller per-frame target can miss the already-fired low event.
          if (dc.bufferedAmount + frame.byteLength > this.directDrainThresholdBytes) {
            directDrainWaitCount++;
            directBufferedHighWater = Math.max(directBufferedHighWater, dc.bufferedAmount + frame.byteLength);
            const drainStartedAt = Date.now();
            try {
              await this.runStage('direct_channel_drain', P2P_DRAIN_TIMEOUT_MS,
                () => dc.waitForDrain(this.directDrainThresholdBytes));
            } finally {
              directDrainWaitTotalMs += Date.now() - drainStartedAt;
            }
          }
          if (dc.readyState !== 'open') throw new Error('direct channel closed while draining');
          directBufferedHighWater = Math.max(directBufferedHighWater, dc.bufferedAmount + frame.byteLength);
          const isFirstFrame = firstDirectFrameState === 'none';
          if (isFirstFrame) {
            firstDirectFrameState = 'attempted';
            logFirstDirectFrame('send_attempt', seq, frame.byteLength, dc.bufferedAmount, 'nativeSendAttempted=true');
          }
          dc.send(frame);
          if (isFirstFrame) {
            firstDirectFrameState = 'accepted';
            logFirstDirectFrame('send_accepted', seq, frame.byteLength, dc.bufferedAmount, 'sendReturned=true; remoteDeliveryNotImplied=true');
          }
          directBufferedHighWater = Math.max(directBufferedHighWater, dc.bufferedAmount);
          this.events.onFileRoute?.('p2p', manifest.id);
          route = 'direct';
        } catch (e) {
          if (firstDirectFrameState === 'none') {
            firstDirectFrameState = 'not_submitted';
            logFirstDirectFrame('not_submitted', seq, bytes.byteLength, dc?.bufferedAmount ?? 0,
              `nativeSendAttempted=false,errorType=${e instanceof Error ? e.name : typeof e}`);
          } else if (firstDirectFrameState === 'attempted') {
            firstDirectFrameState = 'failed';
            logFirstDirectFrame('send_rejected', seq, bytes.byteLength, dc?.bufferedAmount ?? 0,
              `nativeSendAttempted=true,errorType=${e instanceof Error ? e.name : typeof e}`);
          }
          this.events.onLog?.('p2p', `raw direct chunk send failed at ${seq}, falling back to relay: ${e instanceof Error ? e.name : typeof e}`);
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
        if (directMode && dc?.readyState !== 'open' && firstDirectFrameState === 'none') {
          firstDirectFrameState = 'not_submitted';
          logFirstDirectFrame('not_submitted', seq, bytes.byteLength, dc?.bufferedAmount ?? 0,
            `nativeSendAttempted=false,reason=direct_channel_not_open`);
        }
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

    const sendDirectChunkBatch = async (firstSeq: number, chunkCount: number, startupAttempt = false): Promise<'direct' | 'relay'> => {
      if (!binaryChunkMode || chunkCount < 2) return sendChunk(firstSeq);
      const batchPrepareStartedAt = performance.now();
      const start = firstSeq * manifest.chunkSize;
      const end = Math.min(file.size, (firstSeq + chunkCount) * manifest.chunkSize);
      const payload = new Uint8Array(end - start);
      let offset = 0;
      for (let seq = firstSeq; seq < firstSeq + chunkCount; seq++) {
        const chunkStart = seq * manifest.chunkSize;
        const bytes = await this.runStage(
          'file_chunk_preparation',
          this.timeouts.fileChunkPreparationMs,
          () => file.slice(chunkStart, Math.min(chunkStart + manifest.chunkSize, file.size)),
        );
        assertFirstDirectFrameStartupOpen(startupAttempt);
        payload.set(bytes, offset);
        offset += bytes.byteLength;
      }
      if (!firstDirectPayloadPrepared && firstDirectFrameState === 'none') {
        firstDirectPayloadPrepared = true;
        logFirstDirectFrame('payload_prepared', firstSeq, payload.byteLength, this.direct?.bufferedAmount ?? 0,
          `logicalChunks=${chunkCount},assembleMs=${Math.round(performance.now() - batchPrepareStartedAt)}`);
      }

      const dc = this.direct;
      let isFirstFrame = false;
      try {
        if (!dc || dc.readyState !== 'open') throw new Error('direct channel is not open');
        const frame = encodeDirectFileChunkFrame(manifest.id, firstSeq, payload, manifest.chunkSize, chunkCount);
        if (Number.isFinite(dc.maxMessageSize) && frame.byteLength > dc.maxMessageSize) {
          throw new Error(`coalesced frame exceeds SCTP maxMessageSize (${frame.byteLength} > ${dc.maxMessageSize})`);
        }
        if (dc.bufferedAmount + frame.byteLength > this.directDrainThresholdBytes) {
          directDrainWaitCount++;
          directBufferedHighWater = Math.max(directBufferedHighWater, dc.bufferedAmount + frame.byteLength);
          const drainStartedAt = Date.now();
          try {
            await this.runStage('direct_channel_drain', P2P_DRAIN_TIMEOUT_MS,
              () => dc.waitForDrain(this.directDrainThresholdBytes));
          } finally {
            directDrainWaitTotalMs += Date.now() - drainStartedAt;
          }
        }
        if (dc.readyState !== 'open') throw new Error('direct channel closed while draining');
        directBufferedHighWater = Math.max(directBufferedHighWater, dc.bufferedAmount + frame.byteLength);
        isFirstFrame = firstDirectFrameState === 'none';
        if (isFirstFrame) {
          firstDirectFrameState = 'attempted';
          logFirstDirectFrame('send_attempt', firstSeq, frame.byteLength, dc.bufferedAmount,
            `logicalChunks=${chunkCount},nativeSendCallFollows=true`);
        }
        dc.send(frame);
        if (isFirstFrame) {
          firstDirectFrameState = 'accepted';
          logFirstDirectFrame('send_accepted', firstSeq, frame.byteLength, dc.bufferedAmount,
            `logicalChunks=${chunkCount},sendReturned=true; remoteDeliveryNotImplied=true`);
        }
        directBufferedHighWater = Math.max(directBufferedHighWater, dc.bufferedAmount);
        directCoalescedFrames++;
        this.events.onFileRoute?.('p2p', manifest.id);
      } catch (e) {
        if (firstDirectFrameState === 'none') {
          firstDirectFrameState = 'not_submitted';
          logFirstDirectFrame('not_submitted', firstSeq, payload.byteLength, dc?.bufferedAmount ?? 0,
            `logicalChunks=${chunkCount},nativeSendCallStarted=false,errorType=${e instanceof Error ? e.name : typeof e}`);
        } else if (firstDirectFrameState === 'attempted') {
          firstDirectFrameState = 'failed';
          logFirstDirectFrame('send_rejected', firstSeq, payload.byteLength, dc?.bufferedAmount ?? 0,
            `logicalChunks=${chunkCount},nativeSendCallStarted=true,errorType=${e instanceof Error ? e.name : typeof e}`);
        }
        this.events.onLog?.('p2p', `coalesced direct frame failed at ${firstSeq} (${chunkCount} logical chunks), falling back to relay: ${e instanceof Error ? e.name : typeof e}`);
        this.fallbackToRelay(`coalesced direct frame failed (${manifest.id.slice(0, 8)})`);
        binaryChunkMode = false;
        return 'relay';
      }

      sent += payload.byteLength;
      const nextReported = Math.min(file.size, Math.max(reported, sent));
      if (nextReported > reported) {
        reported = nextReported;
        onProgress?.(reported, file.size);
      }
      return 'direct';
    };

    const sendRelayManifest = async (): Promise<void> => {
      if (relayManifestSent) return;
      if (this.direct?.readyState === 'open') {
        this.fallbackToRelay(`relay window requires a relay manifest (${manifest.id.slice(0, 8)})`);
      }
      const result = await this.sendManifestWithAck(manifest);
      relayManifestSent = result.relaySent;
      relayChunkAckCapable = result.chunkAckCapable;
      directFramePayloadBytes = result.directFramePayloadBytes;
    };

    if (!directMode) await sendRelayManifest();

    let nextSeq = firstSeq;
    let lastLoggedDirectWindowCap = 0;
    let lastDataAckId: string | undefined;
    let lastDataAckRoute: 'direct' | 'relay' | 'none' = 'none';
    let lastDataAckNextSeq = firstSeq;
    let relayWindowRetries = 0;
    let relayWindowNumber = 0;
    let relayDiagWindowCount = 0;
    let relayDiagAckedCount = 0;
    let relayDiagUnconfirmedCount = 0;
    let relayDiagRetryCount = 0;
    let relayDiagBytes = 0;
    let relayDiagPublishMs = 0;
    let relayDiagAckMsTotal = 0;
    let relayDiagAckMsMax = 0;
    let relayDiagLastNextSeq = firstSeq;
    let relayDiagLastAckSeq = firstSeq;
    let relayDiagLastAt = performance.now();
    let relayDiagSummaryWritten = false;
    const logRelayWindowAggregate = (reason: string, force = false): void => {
      const now = performance.now();
      if (!force && (relayDiagWindowCount === 0 || (relayDiagSummaryWritten && relayDiagWindowCount < 16 && now - relayDiagLastAt < 5_000))) return;
      this.events.onLog?.('xfer', `relay window aggregate (${manifest.id.slice(0, 8)}): reason=${reason}, windows=${relayDiagWindowCount}, acked=${relayDiagAckedCount}, unconfirmed=${relayDiagUnconfirmedCount}, retries=${relayDiagRetryCount}, bytes=${relayDiagBytes}, publishMsTotal=${Math.round(relayDiagPublishMs)}, ackMsAvg=${relayDiagAckedCount ? Math.round(relayDiagAckMsTotal / relayDiagAckedCount) : 0}, ackMsMax=${Math.round(relayDiagAckMsMax)}, lastNextSeq=${relayDiagLastNextSeq}, lastAckSeq=${relayDiagLastAckSeq}`);
      relayDiagWindowCount = 0;
      relayDiagAckedCount = 0;
      relayDiagUnconfirmedCount = 0;
      relayDiagRetryCount = 0;
      relayDiagBytes = 0;
      relayDiagPublishMs = 0;
      relayDiagAckMsTotal = 0;
      relayDiagAckMsMax = 0;
      relayDiagLastAt = now;
      relayDiagSummaryWritten = true;
    };
    while (nextSeq < manifest.totalChunks) {
      // A late completion ACK can arrive after a direct timeout/replay has
      // already started. Do not publish the rest of an already completed file
      // into the relay queue.
      if (this.chunkAckProgress.get(manifest.id)?.complete) {
        onProgress?.(file.size, file.size);
        this.firstDirectJsonFrameIds.delete(manifest.id);
        nextSeq = manifest.totalChunks;
        break;
      }
      if (directMode && this.direct?.readyState === 'open') {
        const windowStart = nextSeq;
        const windowCapBytes = directWindow.byteLimit;
        const windowChunkLimit = directWindow.chunksFor(manifest.chunkSize);
        const dc = this.direct;
        const localPayloadLimit = dc && Number.isFinite(dc.maxMessageSize)
          ? Math.max(0, dc.maxMessageSize - DIRECT_FILE_BATCH_FRAME_HEADER_SIZE)
          : DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES;
        const negotiatedPayloadLimit = Math.min(
          directFramePayloadBytes,
          localPayloadLimit,
          DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES,
        );
        const directFrameChunkLimit = binaryChunkMode
          ? Math.max(1, Math.floor(negotiatedPayloadLimit / manifest.chunkSize))
          : 1;
        const windowEnd = Math.min(manifest.totalChunks, windowStart + windowChunkLimit);
        const windowBytes = Math.min(file.size, windowEnd * manifest.chunkSize) - windowStart * manifest.chunkSize;
        const windowStartedAt = performance.now();
        const windowNumber = ++directWindowCount;
        let lastDirectChunkSentAt = windowStartedAt;
        if (windowCapBytes !== lastLoggedDirectWindowCap) {
          this.events.onLog?.('xfer', `direct window start (${manifest.id.slice(0, 8)}): window=${windowNumber}, seq=[${windowStart},${windowEnd}), bytes=${windowBytes}, cap=${windowCapBytes}, framePayloadCap=${negotiatedPayloadLimit}, frameChunks=${directFrameChunkLimit}, ackTimeoutMs=${DIRECT_CHUNK_ACK_TIMEOUT_MS}`);
          lastLoggedDirectWindowCap = windowCapBytes;
        }
        let route: 'direct' | 'relay' = 'direct';
        for (let seq = windowStart; seq < windowEnd;) {
          const frameChunks = Math.min(directFrameChunkLimit, windowEnd - seq);
          const submitFrame = (startupAttempt = false) => frameChunks > 1
            ? sendDirectChunkBatch(seq, frameChunks, startupAttempt)
            : sendChunk(seq, startupAttempt);
          if (firstDirectFrameState === 'none') {
            try {
              route = await this.runStage(
                'first_direct_frame_startup',
                this.timeouts.firstDirectFrameStartupMs,
                () => submitFrame(true),
              );
            } catch (error) {
              if (!(error instanceof StageTimeoutError) || error.stage !== 'first_direct_frame_startup') throw error;
              firstDirectFrameStartupExpired = true;
              firstDirectFrameState = 'not_submitted';
              logFirstDirectFrame('not_submitted', seq, 0, this.direct?.bufferedAmount ?? 0,
                `nativeSendAttempted=false,timerStage=${error.stage},timeoutMs=${error.timeoutMs}`);
              this.fallbackToRelay(`first direct frame startup timeout (${manifest.id.slice(0, 8)})`);
              binaryChunkMode = false;
              route = 'relay';
            }
          } else {
            route = await submitFrame();
          }
          if (route === 'direct') lastDirectChunkSentAt = performance.now();
          if (route !== 'direct') break;
          seq += frameChunks;
        }

        if (route === 'direct') {
          const publishedAt = performance.now();
          if (windowNumber === 1) {
            this.events.onLog?.('xfer', `first direct window send returned (${manifest.id.slice(0, 8)}): windowBytes=${windowBytes}, sendMs=${Math.round(publishedAt - windowStartedAt)}, bufferedHighWater=${directBufferedHighWater}`);
          }
          const ackResult = await this.waitForChunkAck(
            manifest.id,
            windowEnd,
            DIRECT_CHUNK_ACK_TIMEOUT_MS,
          );
          if (ackResult !== null) {
            lastDataAckId = ackResult.ackId;
            lastDataAckRoute = 'direct';
            lastDataAckNextSeq = ackResult.nextSeq;
            const elapsedMs = Math.max(0, ackResult.receivedAtMonoMs - windowStartedAt);
            const sendToAckMs = Math.max(0, ackResult.receivedAtMonoMs - lastDirectChunkSentAt);
            const ackWaitMs = Math.max(0, ackResult.receivedAtMonoMs - publishedAt);
            const previousCapBytes = directWindow.byteLimit;
            directAckedWindowCount++;
            directAckElapsedTotalMs += elapsedMs;
            directAckElapsedMaxMs = Math.max(directAckElapsedMaxMs, elapsedMs);
            directWindow.onAcknowledged(elapsedMs);
            const ackSummaryDue = windowNumber === 1 || windowNumber % 16 === 0 || performance.now() - directAckSummaryAt >= 5_000;
            if (ackSummaryDue) {
              this.events.onLog?.('xfer', `direct window ACK summary (${manifest.id.slice(0, 8)}): window=${windowNumber}, ackedWindows=${directAckedWindowCount}/${directWindowCount}, ackId=${ackResult.ackId}, next=${ackResult.nextSeq}, bytes=${windowBytes}, sendToAckMs=${Math.round(sendToAckMs)}, elapsedMs=${Math.round(elapsedMs)}, ackWaitMs=${Math.round(ackWaitMs)}, callbackToHandlerMs=${Math.round(ackResult.callbackToHandlerMs)}, handlerMs=${Math.round(ackResult.handlerMs)}, waiterUnlockMs=${Math.round(ackResult.waiterUnlockMs)}, cap=${previousCapBytes}->${directWindow.byteLimit}`);
              directAckSummaryAt = performance.now();
            }
            nextSeq = windowEnd;
            continue;
          }

          const lastAck = this.chunkAckProgress.get(manifest.id)?.nextSeq ?? windowStart;
          const replayFrom = Math.max(firstSeq, lastAck);
          const repairBytes = Math.max(0, file.size - Math.min(file.size, replayFrom * manifest.chunkSize));
          directReplayBytes += repairBytes;
          this.events.onLog?.('p2p', `transfer timer stage fired: stage=direct_window_ack, timeoutMs=${DIRECT_CHUNK_ACK_TIMEOUT_MS}`);
          this.events.onLog?.('p2p', `direct chunk ACK timeout (${manifest.id.slice(0, 8)}): timeoutMs=${DIRECT_CHUNK_ACK_TIMEOUT_MS}, elapsedMs=${Math.round(Math.max(0, performance.now() - windowStartedAt))}, windowBytes=${windowBytes}, ack=${lastAck}, target=${windowEnd}, replayFrom=${replayFrom}, repairBytes=${repairBytes}, directReplayBytes=${directReplayBytes}, firstFrameState=${firstDirectFrameState}, dcState=${this.direct?.readyState ?? 'absent'}`);
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
        const repairBytes = Math.max(0, file.size - Math.min(file.size, replayFrom * manifest.chunkSize));
        directReplayBytes += repairBytes;
        this.events.onLog?.('p2p', `direct send failure repair file (${manifest.id.slice(0, 8)}): ack=${lastAck}, replayFrom=${replayFrom}, repairBytes=${repairBytes}, directReplayBytes=${directReplayBytes}, bufferedHighWater=${directBufferedHighWater}, drainWaits=${directDrainWaitCount}, drainWaitMs=${directDrainWaitTotalMs}`);
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
        this.events.onLog?.('xfer', `relay window skipped file (${manifest.id.slice(0, 8)}): next=${nextSeq}, ack=${acknowledged}`);
        nextSeq = Math.min(manifest.totalChunks, acknowledged);
        relayWindowRetries = 0;
        continue;
      }
      const windowStart = nextSeq;
      const windowEnd = Math.min(manifest.totalChunks, windowStart + RELAY_CHUNK_WINDOW);
      const windowNumber = ++relayWindowNumber;
      const windowStartedAt = performance.now();
      for (let seq = windowStart; seq < windowEnd; seq++) await sendChunk(seq);
      const windowBytes = Math.min(file.size, windowEnd * manifest.chunkSize) - windowStart * manifest.chunkSize;
      const relayPublishMs = Math.max(0, performance.now() - windowStartedAt);
      relayDiagWindowCount++;
      relayDiagBytes += windowBytes;
      relayDiagPublishMs += relayPublishMs;
      relayDiagLastNextSeq = windowEnd;

      // Peers before the receive-ACK capability continue on the old relay
      // path. New peers use the same bounded window on LiveKit as on P2P;
      // publish() alone only proves local enqueue, not remote processing.
      if (!relayChunkAckCapable) {
        relayDiagUnconfirmedCount++;
        logRelayWindowAggregate('peer_has_no_chunk_ack_capability');
        nextSeq = windowEnd;
        continue;
      }
      // Start the timeout after all chunks have been accepted by the local
      // LiveKit client.  More importantly, waitForChunkAck treats any ACK
      // progress as activity and only times out after a period of silence.
      // A slow but healthy relay must not be mistaken for a dead one.
      const ackResult = await this.waitForChunkAck(
        manifest.id,
        windowEnd,
        RELAY_CHUNK_ACK_TIMEOUT_MS,
      );
      if (ackResult !== null) {
        lastDataAckId = ackResult.ackId;
        lastDataAckRoute = 'relay';
        lastDataAckNextSeq = ackResult.nextSeq;
        const ack = this.chunkAckProgress.get(manifest.id);
        const relayAckMs = Math.max(0, ackResult.receivedAtMonoMs - windowStartedAt);
        relayDiagAckedCount++;
        relayDiagAckMsTotal += relayAckMs;
        relayDiagAckMsMax = Math.max(relayDiagAckMsMax, relayAckMs);
        relayDiagLastAckSeq = ackResult.nextSeq;
        logRelayWindowAggregate('ack_received');
        relayWindowRetries = 0;
        nextSeq = Math.max(windowEnd, ack?.nextSeq ?? 0);
        continue;
      }
      const lastAck = this.chunkAckProgress.get(manifest.id)?.nextSeq ?? windowStart;
      const replayFrom = Math.max(firstSeq, lastAck - RELAY_REPLAY_OVERLAP_CHUNKS);
      relayWindowRetries++;
      relayDiagRetryCount++;
      relayDiagLastAckSeq = lastAck;
      this.events.onLog?.('p2p', `transfer timer stage fired: stage=relay_window_ack, timeoutMs=${RELAY_CHUNK_ACK_TIMEOUT_MS}`);
      this.events.onLog?.('xfer', `relay window ACK timeout (${manifest.id.slice(0, 8)}): window=${windowNumber}, timeoutMs=${RELAY_CHUNK_ACK_TIMEOUT_MS}, elapsedMs=${Math.round(Math.max(0, performance.now() - windowStartedAt))}, retry=${relayWindowRetries}/${MAX_RELAY_WINDOW_RETRIES}, replayFrom=${replayFrom}, ack=${lastAck}, target=${windowEnd}, bytes=${windowBytes}`);
      logRelayWindowAggregate('ack_timeout', true);
      if (relayWindowRetries > MAX_RELAY_WINDOW_RETRIES) {
        this.chunkAckProgress.delete(manifest.id);
        this.chunkAckCapabilities.delete(manifest.id);
        throw new Error(`relay chunk ack timeout (${manifest.id.slice(0, 8)})`);
      }
      nextSeq = replayFrom;
    }
    if (relayDiagWindowCount > 0 || relayDiagRetryCount > 0 || relayDiagUnconfirmedCount > 0) {
      logRelayWindowAggregate('data_phase_complete', true);
    }
    if (directWindowCount > 0) {
      const ackAverageMs = directAckedWindowCount > 0
        ? Math.round(directAckElapsedTotalMs / directAckedWindowCount)
        : 0;
      this.events.onLog?.('xfer', `direct metrics file (${manifest.id.slice(0, 8)}): finalRoute=${directMode ? 'direct' : 'relay'}, windows=${directWindowCount}, ackedWindows=${directAckedWindowCount}, ackAvgMs=${ackAverageMs}, ackMaxMs=${directAckElapsedMaxMs}, coalescedFrames=${directCoalescedFrames}, drainWaits=${directDrainWaitCount}, drainWaitMs=${directDrainWaitTotalMs}, bufferedHighWater=${directBufferedHighWater}, relayRepairBytes=${directReplayBytes}, finalWindowCap=${directWindow.byteLimit}`);
    }
    this.events.onLog?.('xfer', `file data ACK phase complete (${manifest.id.slice(0, 8)}): ackCapable=${manifestResult.chunkAckCapable}, route=${lastDataAckRoute}, lastAckId=${lastDataAckId ?? 'none'}, lastAckSeq=${lastDataAckNextSeq}, totalChunks=${manifest.totalChunks}, directWindows=${directWindowCount}, relayWindows=${relayWindowNumber}`);
    // A progress ACK (complete=false) only confirms that bytes reached the
    // receiver's buffer. Do not return / permit the next manifest until the
    // complete=true ACK after assembly, hash verification, and durable save.
    if (manifestResult.chunkAckCapable) {
      const finalized = await this.waitForFileComplete(manifest, FILE_FINALIZE_ACK_TIMEOUT_MS);
      if (!finalized) {
        const failed = this.chunkAckProgress.get(manifest.id)?.failed;
        this.events.onLog?.('p2p', `transfer timer stage fired: stage=durable_completion_ack, timeoutMs=${FILE_FINALIZE_ACK_TIMEOUT_MS}`);
        this.outgoingFileSizes.delete(manifest.id);
        this.finalizingFiles.delete(manifest.id);
        this.chunkAckProgress.delete(manifest.id);
        this.chunkAckCapabilities.delete(manifest.id);
        this.binaryChunkCapabilities.delete(manifest.id);
        this.directFramePayloadLimits.delete(manifest.id);
        throw new Error(failed
          ? `receiver failed to finalize file (${manifest.id.slice(0, 8)})`
          : `file finalize ack timeout (${manifest.id.slice(0, 8)})`);
      }
      onProgress?.(file.size, file.size, true);
    }
    this.outgoingFileSizes.delete(manifest.id);
    this.finalizingFiles.delete(manifest.id);
    this.chunkAckProgress.delete(manifest.id);
    this.chunkAckCapabilities.delete(manifest.id);
      this.binaryChunkCapabilities.delete(manifest.id);
      this.directFramePayloadLimits.delete(manifest.id);
      this.firstDirectJsonFrameIds.delete(manifest.id);
      this.ackDiagnosticState.delete(manifest.id);
      return manifest;
  }

  /**
   * A manifest ACK confirms that the peer has created its receiver. New peers
   * also advertise chunk ACK support, which lets relay sends use bounded
   * windows instead of filling LiveKit's local reliable-data queue.
   */
  private async sendManifestWithAck(manifest: FileManifest, startupDeadlineAt?: number): Promise<ManifestSendResult> {
    const wasDirect = this.direct?.readyState === 'open';
    if (!wasDirect) {
      const ackTimeoutMs = this.startupStageTimeoutMs(P2P_MANIFEST_ACK_TIMEOUT_MS, startupDeadlineAt);
      this.events.onLog?.('xfer', `manifest send attempt (${manifest.id.slice(0, 8)}): route=relay, ackTimeoutMs=${ackTimeoutMs}`);
      const publishStartedAt = performance.now();
      const ack = this.waitForManifestAck(manifest.id, ackTimeoutMs);
      try {
        await this.runStage('manifest_publish', this.startupStageTimeoutMs(this.timeouts.manifestPublishMs, startupDeadlineAt),
          () => this.send({ kind: 'file-manifest', manifest }));
      } catch (error) {
        this.resolveManifestAck(manifest.id, false);
        throw error;
      }
      const publishReturnedAt = performance.now();
      this.events.onLog?.('xfer', `manifest publish returned (${manifest.id.slice(0, 8)}): route=relay, elapsedMs=${Math.round(publishReturnedAt - publishStartedAt)}`);
      const accepted = await ack;
      const ackResolvedAt = performance.now();
      if (!accepted) {
        this.events.onLog?.('p2p', `transfer timer stage fired: stage=manifest_ack, timeoutMs=${ackTimeoutMs}`);
        this.startupStageTimeoutMs(1, startupDeadlineAt);
      }
      const chunkAckCapable = accepted && this.chunkAckCapabilities.has(manifest.id);
      const binaryChunkCapable = accepted && this.binaryChunkCapabilities.has(manifest.id);
      const directFramePayloadBytes = accepted ? this.directFramePayloadLimits.get(manifest.id) ?? 0 : 0;
      this.events.onLog?.('xfer', `manifest ACK resolved (${manifest.id.slice(0, 8)}): route=relay, accepted=${accepted}, ackWaitMs=${Math.round(ackResolvedAt - publishReturnedAt)}, chunkAck=${chunkAckCapable}, binaryChunk=${binaryChunkCapable}, directPayloadLimit=${directFramePayloadBytes}`);
      if (!accepted) {
        this.events.onLog?.('xfer', `relay manifest ack missing for file; using legacy relay pacing`);
      }
      return { chunkAckCapable, binaryChunkCapable, directFramePayloadBytes, relaySent: true };
    }

    const directAckTimeoutMs = this.startupStageTimeoutMs(P2P_MANIFEST_ACK_TIMEOUT_MS, startupDeadlineAt);
    this.events.onLog?.('xfer', `manifest send attempt (${manifest.id.slice(0, 8)}): route=direct, ackTimeoutMs=${directAckTimeoutMs}`);
    const directPublishStartedAt = performance.now();
    const ack = this.waitForManifestAck(manifest.id, directAckTimeoutMs);
    let route: 'direct' | 'relay';
    let manifestPublishTimedOut = false;
    try {
      route = await this.runStage('manifest_publish', this.startupStageTimeoutMs(this.timeouts.manifestPublishMs, startupDeadlineAt),
        () => this.send({ kind: 'file-manifest', manifest }));
    } catch (e) {
      this.resolveManifestAck(manifest.id, false);
      if (!(e instanceof StageTimeoutError) || e.stage !== 'manifest_publish') throw e;
      manifestPublishTimedOut = true;
      this.events.onLog?.('p2p', `manifest publish deadline reached; starting relay recovery (${manifest.id.slice(0, 8)}): timerStage=${e.stage}, timeoutMs=${e.timeoutMs}`);
      this.fallbackToRelay(`manifest publish timeout (${manifest.id.slice(0, 8)})`);
      route = 'direct';
    }
    const directPublishReturnedAt = performance.now();
    this.events.onLog?.('xfer', `manifest publish returned (${manifest.id.slice(0, 8)}): route=${route}, elapsedMs=${Math.round(directPublishReturnedAt - directPublishStartedAt)}`);
    const accepted = await ack;
    const directAckResolvedAt = performance.now();
    if (!accepted && !manifestPublishTimedOut) {
      this.events.onLog?.('p2p', `transfer timer stage fired: stage=manifest_ack, timeoutMs=${directAckTimeoutMs}`);
      this.startupStageTimeoutMs(1, startupDeadlineAt);
    }
    if (accepted) {
      const chunkAckCapable = this.chunkAckCapabilities.has(manifest.id);
      const binaryChunkCapable = this.binaryChunkCapabilities.has(manifest.id);
      const directFramePayloadBytes = this.directFramePayloadLimits.get(manifest.id) ?? 0;
      this.events.onLog?.('xfer', `manifest ACK resolved (${manifest.id.slice(0, 8)}): route=${route}, accepted=true, ackWaitMs=${Math.round(directAckResolvedAt - directPublishReturnedAt)}, chunkAck=${chunkAckCapable}, binaryChunk=${binaryChunkCapable}, directPayloadLimit=${directFramePayloadBytes}`);
      return { chunkAckCapable, binaryChunkCapable, directFramePayloadBytes, relaySent: route === 'relay' };
    }
    if (!manifestPublishTimedOut) {
      this.events.onLog?.('xfer', `manifest ACK timeout (${manifest.id.slice(0, 8)}): route=${route}, timeoutMs=${directAckTimeoutMs}, elapsedMs=${Math.round(directAckResolvedAt - directPublishReturnedAt)}`);
    }

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
      const relayAckTimeoutMs = this.startupStageTimeoutMs(P2P_MANIFEST_ACK_TIMEOUT_MS, startupDeadlineAt);
      this.events.onLog?.('xfer', `manifest retry attempt (${manifest.id.slice(0, 8)}): route=relay-after-direct, ackTimeoutMs=${relayAckTimeoutMs}`);
      const relayRetryStartedAt = performance.now();
      const relayAck = this.waitForManifestAck(manifest.id, relayAckTimeoutMs);
      try {
        await this.runStage('manifest_publish', this.startupStageTimeoutMs(this.timeouts.manifestPublishMs, startupDeadlineAt),
          () => this.send({ kind: 'file-manifest', manifest }));
      } catch (error) {
        this.resolveManifestAck(manifest.id, false);
        throw error;
      }
      const relayRetryPublishedAt = performance.now();
      const relayAccepted = await relayAck;
      const relayRetryAckResolvedAt = performance.now();
      if (!relayAccepted) {
        this.events.onLog?.('p2p', `transfer timer stage fired: stage=manifest_ack, timeoutMs=${relayAckTimeoutMs}`);
        this.startupStageTimeoutMs(1, startupDeadlineAt);
        this.events.onLog?.('p2p', `relay manifest ack missing for file; continuing with task ack`);
      }
      const chunkAckCapable = relayAccepted && this.chunkAckCapabilities.has(manifest.id);
      const binaryChunkCapable = relayAccepted && this.binaryChunkCapabilities.has(manifest.id);
      const directFramePayloadBytes = relayAccepted ? this.directFramePayloadLimits.get(manifest.id) ?? 0 : 0;
      this.events.onLog?.('xfer', `manifest retry ACK resolved (${manifest.id.slice(0, 8)}): route=relay-after-direct, accepted=${relayAccepted}, publishMs=${Math.round(relayRetryPublishedAt - relayRetryStartedAt)}, ackWaitMs=${Math.round(relayRetryAckResolvedAt - relayRetryPublishedAt)}, chunkAck=${chunkAckCapable}, binaryChunk=${binaryChunkCapable}, directPayloadLimit=${directFramePayloadBytes}`);
      return { chunkAckCapable, binaryChunkCapable, directFramePayloadBytes, relaySent: true };
    }
    this.events.onLog?.('p2p', `manifest ack missing for file; continuing with task ack`);
    return {
      chunkAckCapable: this.chunkAckCapabilities.has(manifest.id),
      binaryChunkCapable: this.binaryChunkCapabilities.has(manifest.id),
      directFramePayloadBytes: this.directFramePayloadLimits.get(manifest.id) ?? 0,
      relaySent: true,
    };
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

  private directFramePayloadLimit(chunkSize: number): number {
    const dc = this.direct;
    if (!dc || dc.readyState !== 'open' || !Number.isSafeInteger(chunkSize) || chunkSize <= 0) return 0;
    const channelPayloadLimit = Number.isFinite(dc.maxMessageSize)
      ? dc.maxMessageSize - DIRECT_FILE_BATCH_FRAME_HEADER_SIZE
      : DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES;
    return Math.max(0, Math.min(DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES, channelPayloadLimit));
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
  private waitForChunkAck(id: string, targetSeq: number, timeoutMs: number): Promise<ChunkAckWaitResult | null> {
    return new Promise((resolve) => {
      let lastSeq = this.chunkAckProgress.get(id)?.nextSeq ?? 0;
      let lastBytes = this.chunkAckProgress.get(id)?.receivedBytes ?? 0;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;
      let unsubscribe = () => {};
      const finish = (result: ChunkAckWaitResult | null) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        unsubscribe();
        resolve(result);
      };
      const armSilenceTimeout = () => {
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => finish(null), timeoutMs);
      };
      const onChange = (cancelled: boolean) => {
        if (cancelled || !this.transport.isConnected) {
          finish(null);
          return;
        }
        const progress = this.chunkAckProgress.get(id);
        if ((progress?.nextSeq ?? 0) >= targetSeq || progress?.complete) {
          const waiterResumedAtMonoMs = performance.now();
          const receivedAtMonoMs = progress?.updatedMonoAt ?? waiterResumedAtMonoMs;
          finish({
            ackId: progress?.ackId ?? `legacy-${id}-${progress?.nextSeq ?? 0}`,
            nextSeq: progress?.nextSeq ?? 0,
            receivedAtMonoMs,
            callbackToHandlerMs: progress?.callbackToHandlerMs ?? 0,
            handlerMs: progress?.handlerMs ?? 0,
            waiterUnlockMs: Math.max(0, waiterResumedAtMonoMs - receivedAtMonoMs),
          });
          return;
        }
        if ((progress?.nextSeq ?? 0) > lastSeq || (progress?.receivedBytes ?? 0) > lastBytes) {
          lastSeq = progress?.nextSeq ?? lastSeq;
          lastBytes = progress?.receivedBytes ?? lastBytes;
          armSilenceTimeout();
        }
      };
      unsubscribe = this.subscribeChunkAckWaiter(id, onChange);
      armSilenceTimeout();
      // Register before checking the current state so an ACK can neither race
      // between the initial read and subscription nor be lost after a window.
      onChange(false);
    });
  }

  /** Wait for complete=true, which is sent only after receiver-side save succeeds. */
  private waitForFileComplete(manifest: FileManifest, timeoutMs: number): Promise<boolean> {
    const startedAt = Date.now();
    let finalizingLogged = false;
    return new Promise((resolve) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;
      let unsubscribe = () => {};
      const finish = (result: boolean) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        unsubscribe();
        resolve(result);
      };
      const onChange = (cancelled: boolean) => {
        const waitedMs = Date.now() - startedAt;
        if (cancelled || !this.transport.isConnected) {
          this.events.onLog?.('xfer', `file finalize wait aborted (${manifest.id.slice(0, 8)}): size=${manifest.size}, wait=${waitedMs}ms`);
          finish(false);
          return;
        }
        const progress = this.chunkAckProgress.get(manifest.id);
        if (progress?.complete) {
          this.events.onLog?.('xfer', `file finalize ACK confirmed (${manifest.id.slice(0, 8)}): size=${manifest.size}, wait=${waitedMs}ms`);
          finish(true);
          return;
        }
        if (progress?.failed) {
          this.events.onLog?.('xfer', `file finalize ACK failed (${manifest.id.slice(0, 8)}): size=${manifest.size}, wait=${waitedMs}ms`);
          finish(false);
          return;
        }
        if (!finalizingLogged && (manifest.size === 0 || (
          (progress?.receivedBytes ?? 0) >= manifest.size &&
          (progress?.nextSeq ?? 0) >= manifest.totalChunks
        ))) {
          finalizingLogged = true;
          const newlyFinalizing = !this.finalizingFiles.has(manifest.id);
          this.finalizingFiles.add(manifest.id);
          if (newlyFinalizing) this.events.onFileFinalizing?.(manifest.id, manifest.size);
          this.events.onLog?.('xfer', `file finalize wait start (${manifest.id.slice(0, 8)}): size=${manifest.size}, timeout=${timeoutMs}ms`);
        }
      };
      unsubscribe = this.subscribeChunkAckWaiter(manifest.id, onChange);
      timer = setTimeout(() => {
        this.events.onLog?.('xfer', `file finalize ACK timeout (${manifest.id.slice(0, 8)}): size=${manifest.size}, wait=${Date.now() - startedAt}ms, finalizing=${finalizingLogged}`);
        finish(false);
      }, timeoutMs);
      onChange(false);
    });
  }

  private subscribeChunkAckWaiter(id: string, waiter: (cancelled: boolean) => void): () => void {
    let waiters = this.chunkAckWaiters.get(id);
    if (!waiters) {
      waiters = new Set();
      this.chunkAckWaiters.set(id, waiters);
    }
    waiters.add(waiter);
    return () => {
      const current = this.chunkAckWaiters.get(id);
      current?.delete(waiter);
      if (current?.size === 0) this.chunkAckWaiters.delete(id);
    };
  }

  private notifyChunkAckWaiters(id: string, cancelled = false): void {
    const waiters = this.chunkAckWaiters.get(id);
    if (!waiters) return;
    if (cancelled) this.chunkAckWaiters.delete(id);
    for (const waiter of [...waiters]) waiter(cancelled);
  }

  private cancelAllChunkAckWaiters(): void {
    for (const id of [...this.chunkAckWaiters.keys()]) this.notifyChunkAckWaiters(id, true);
  }

  private resolveChunkAck(
    msg: Extract<FlingMessage, { kind: 'file-chunk-ack' }>,
    source: 'relay' | 'direct' = 'relay',
    receivedAtMonoMs = performance.now(),
  ): void {
    const handlerStartedAtMonoMs = performance.now();
    const nextSeq = Math.max(0, Math.floor(Number(msg.nextSeq) || 0));
    const receivedBytes = Math.max(0, Math.floor(Number(msg.receivedBytes) || 0));
    const previous = this.chunkAckProgress.get(msg.id);
    const hasTraceId = typeof msg.ackId === 'string' && /^[a-zA-Z0-9_-]{1,64}$/.test(msg.ackId);
    const ackId = hasTraceId
      ? msg.ackId!
      : `legacy-${msg.id.slice(0, 8)}-${nextSeq}-${Boolean(msg.complete) ? 'final' : 'progress'}`;
    const advanced = (
      !previous ||
      nextSeq > previous.nextSeq ||
      receivedBytes > previous.receivedBytes ||
      (msg.complete && !previous.complete) ||
      (msg.failed && !previous.failed)
    );
    const advancesConfirmedPosition = !previous || nextSeq > previous.nextSeq ||
      (msg.complete && !previous.complete) || (msg.failed && !previous.failed);
    const callbackToHandlerMs = Math.max(0, handlerStartedAtMonoMs - receivedAtMonoMs);
    const handlerMs = Math.max(0, performance.now() - handlerStartedAtMonoMs);
    if (advanced) {
      this.chunkAckProgress.set(msg.id, {
        nextSeq: Math.max(previous?.nextSeq ?? 0, nextSeq),
        receivedBytes: Math.max(previous?.receivedBytes ?? 0, receivedBytes),
        complete: Boolean(msg.complete) || Boolean(previous?.complete),
        failed: Boolean(msg.failed) || Boolean(previous?.failed),
        updatedMonoAt: advancesConfirmedPosition ? handlerStartedAtMonoMs : previous?.updatedMonoAt ?? handlerStartedAtMonoMs,
        ackId: advancesConfirmedPosition ? ackId : previous?.ackId ?? ackId,
        callbackToHandlerMs: advancesConfirmedPosition ? callbackToHandlerMs : previous?.callbackToHandlerMs ?? callbackToHandlerMs,
        handlerMs: advancesConfirmedPosition ? handlerMs : previous?.handlerMs ?? handlerMs,
      });
      this.notifyChunkAckWaiters(msg.id);
      if (this.shouldLogAckDiagnostic(msg.id, 'sender_received', Boolean(msg.complete || msg.failed), undefined, !hasTraceId)) {
        this.events.onLog?.('xfer', `chunk ACK received (${msg.id.slice(0, 8)}): ackId=${ackId}, source=${source}, next=${nextSeq}, bytes=${receivedBytes}, complete=${Boolean(msg.complete)}, failed=${Boolean(msg.failed)}, callbackToHandlerMs=${callbackToHandlerMs}, handlerMs=${handlerMs}`);
      }
    } else {
      if (this.shouldLogAckDiagnostic(msg.id, 'sender_ignored', Boolean(msg.complete || msg.failed))) {
        this.events.onLog?.('xfer', `chunk ACK ignored (${msg.id.slice(0, 8)}): ackId=${ackId}, source=${source}, reason=duplicate-or-regressing, next=${nextSeq}, bytes=${receivedBytes}, currentNext=${previous?.nextSeq ?? 0}, currentBytes=${previous?.receivedBytes ?? 0}`);
      }
    }
    const size = this.outgoingFileSizes.get(msg.id);
    if (msg.complete || msg.failed) {
      this.finalizingFiles.delete(msg.id);
    } else if (size !== undefined && receivedBytes >= size && !this.finalizingFiles.has(msg.id)) {
      this.finalizingFiles.add(msg.id);
      this.events.onFileFinalizing?.(msg.id, size);
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

  /** v2: receiver reports receive and Rust append progress (throttled by caller). */
  async sendTaskProgress(taskId: string, receivedBytes: number, totalBytes: number, savedBytes = 0): Promise<void> {
    // Keep doneBytes for older peers; new peers consume the explicit fields.
    await this.send({ kind: 'task-progress', taskId, doneBytes: receivedBytes, receivedBytes, savedBytes, totalBytes });
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
      const isFirstFileFrame = msg.kind === 'file-chunk' && !this.firstDirectJsonFrameIds.has(msg.id);
      if (isFirstFileFrame) this.firstDirectJsonFrameIds.add(msg.id);
      const xfer = msg.kind === 'file-chunk' ? msg.id.slice(0, 8) : undefined;
      let nativeSendCallStarted = false;
      let nativeSendCallReturned = false;
      try {
        if (dc.bufferedAmount > this.directDrainThresholdBytes) {
          await this.runStage('direct_channel_drain', P2P_DRAIN_TIMEOUT_MS,
            () => dc.waitForDrain(this.directDrainThresholdBytes));
          if (dc.readyState !== 'open') throw new Error('direct channel closed while draining');
        }
        const encoded = enc.encode(JSON.stringify(msg));
        const bufferedBefore = dc.bufferedAmount;
        if (isFirstFileFrame) {
          this.events.onLog?.('p2p', `first direct JSON frame send_attempt (${xfer}): seq=${msg.kind === 'file-chunk' ? msg.seq : -1}, frameBytes=${encoded.byteLength}, bufferedAmount=${bufferedBefore}`);
        }
        nativeSendCallStarted = true;
        dc.send(encoded);
        nativeSendCallReturned = true;
        if (isFirstFileFrame) {
          this.events.onLog?.('p2p', `first direct JSON frame send_accepted (${xfer}): seq=${msg.kind === 'file-chunk' ? msg.seq : -1}, frameBytes=${encoded.byteLength}, bufferedBefore=${bufferedBefore}, bufferedAfter=${dc.bufferedAmount}, sendReturned=true; remoteDeliveryNotImplied=true`);
        }
        if (msg.kind === 'file-chunk') this.events.onFileRoute?.('p2p', msg.id);
        return 'direct';
      } catch (error) {
        if (isFirstFileFrame && !nativeSendCallReturned) {
          this.events.onLog?.('p2p', `first direct JSON frame ${nativeSendCallStarted ? 'send_rejected' : 'not_submitted'} (${xfer}): seq=${msg.kind === 'file-chunk' ? msg.seq : -1}, nativeSendCallStarted=${nativeSendCallStarted}, errorType=${error instanceof Error ? error.name : typeof error}`);
        }
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
    const payload = enc.encode(sealed);
    if (directKinds) {
      await this.runStage('relay_publish', this.timeouts.relayPublishMs,
        () => this.transport.publish(payload, FLING_TOPIC));
    } else {
      await this.transport.publish(payload, FLING_TOPIC);
    }
    if (msg.kind === 'file-chunk') this.events.onFileRoute?.('relay', msg.id);
    return 'relay';
  }

  private async handlePayload(payload: Uint8Array, from: string, receivedAtMonoMs = performance.now()): Promise<void> {
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
    if (msg.kind === 'file-chunk') {
      this.events.onFileRoute?.('relay', msg.id);
      this.logFirstReceiverFrame(msg.id, 'relay', msg.seq, msg.data.length);
    }
    this.dispatchMessage(msg, from, 'relay', receivedAtMonoMs);
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
      this.events.onLog?.('xfer', `dropping invalid direct file frame: ${e instanceof Error ? e.name : typeof e}`);
      return;
    }
    if (frame) {
      this.events.onFileRoute?.('p2p', frame.id);
      const chunkCount = frame.chunkCount ?? 1;
      this.logFirstReceiverFrame(frame.id, 'direct-binary', frame.seq, frame.payload.byteLength, chunkCount);
      const n = (this.directChunkCounts.get(frame.id) ?? 0) + chunkCount;
      this.directChunkCounts.set(frame.id, n);
      if (chunkCount === 1) {
        this.handleChunkMessage({ id: frame.id, seq: frame.seq, bytes: frame.payload, source: 'direct' });
      } else {
        const chunkSize = frame.chunkSize!;
        for (let index = 0; index < chunkCount; index++) {
          const start = index * chunkSize;
          const end = Math.min(start + chunkSize, frame.payload.byteLength);
          this.handleChunkMessage({
            id: frame.id,
            seq: frame.seq + index,
            bytes: frame.payload.subarray(start, end) as Uint8Array<ArrayBuffer>,
            source: 'direct',
          });
        }
      }
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
      this.logFirstReceiverFrame(msg.id, 'direct-json', msg.seq, msg.data.length);
      const n = (this.directChunkCounts.get(msg.id) ?? 0) + 1;
      this.directChunkCounts.set(msg.id, n);
    }
    this.dispatchMessage(msg, from, 'direct');
  }

  /** Per-file direct-chunk counters (diagnostics). Cleared in finishFile. */
  private directChunkCounts = new Map<string, number>();
  private firstReceiveFrameIds = new Set<string>();
  private receiveProgressLogState = new Map<string, { lastBytes: number; lastAt: number }>();
  private firstReceiveChunkIds = new Set<string>();

  private logFirstReceiverFrame(
    id: string,
    source: 'direct-binary' | 'direct-json' | 'relay',
    seq: number,
    payloadBytes: number,
    chunkCount = 1,
  ): void {
    if (this.firstReceiveFrameIds.has(id)) return;
    this.firstReceiveFrameIds.add(id);
    this.events.onLog?.('xfer', `first receiver frame arrived (${id.slice(0, 8)}): source=${source}, seq=${seq}, payloadBytes=${payloadBytes}, chunkCount=${chunkCount}`);
  }

  private logReceiverChunkProgress(id: string, receiver: FileReceiver, source: 'relay' | 'direct', seq: number): void {
    const receivedBytes = receiver.receivedByteCount;
    const now = Date.now();
    const previous = this.receiveProgressLogState.get(id) ?? { lastBytes: 0, lastAt: 0 };
    if (receivedBytes > previous.lastBytes) {
      if (!this.firstReceiveChunkIds.has(id)) {
        this.firstReceiveChunkIds.add(id);
        this.events.onLog?.('xfer', `first receiver chunk accepted (${id.slice(0, 8)}): source=${source}, seq=${seq}, receivedBytes=${receivedBytes}, nextSeq=${receiver.nextSeq}`);
      }
      const report = previous.lastAt === 0 || receivedBytes - previous.lastBytes >= 32 * 1024 * 1024 || now - previous.lastAt >= 10_000;
      if (report) {
        if (previous.lastAt !== 0) {
          this.events.onLog?.('xfer', `receiver chunk progress aggregate (${id.slice(0, 8)}): source=${source}, receivedBytes=${receivedBytes}, deltaBytes=${receivedBytes - previous.lastBytes}, nextSeq=${receiver.nextSeq}, directFrames=${this.directChunkCounts.get(id) ?? 0}`);
        }
        previous.lastAt = now;
        previous.lastBytes = receivedBytes;
      }
      this.receiveProgressLogState.set(id, previous);
    }
  }

  private clearPerFileDiagnosticState(id: string): void {
    this.directChunkCounts.delete(id);
    this.firstReceiveFrameIds.delete(id);
    this.receiveProgressLogState.delete(id);
    this.firstReceiveChunkIds.delete(id);
    this.firstDirectJsonFrameIds.delete(id);
    this.ackDiagnosticState.delete(id);
  }

  private dispatchMessage(
    msg: FlingMessage,
    from: string,
    source: 'relay' | 'direct' = 'relay',
    receivedAtMonoMs = performance.now(),
  ): void {
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
        const directFramePayloadBytes = this.directFramePayloadLimit(manifest.chunkSize);
        if (this.incoming.has(manifest.id) || this.completedFiles.has(manifest.id)) {
          const completed = this.completedFiles.has(manifest.id);
          this.events.onLog?.('xfer', `duplicate manifest received (${manifest.id.slice(0, 8)}) source=${source}, completed=${completed}, acknowledging without reset`);
          void this.send({ kind: 'file-manifest-ack', id: manifest.id, chunkAck: true, binaryChunks: true, directFramePayloadBytes }).catch(() => {});
          // If the sender lost the final chunk ACK and is repairing through
          // relay, there is no incoming receiver entry left to emit another
          // ACK. Re-advertise completion on the duplicate manifest so the
          // sender can stop the replay instead of resending the whole file.
          if (completed) {
            const ackId = randomId(8);
            this.events.onLog?.('xfer', `final ACK repair publish queued (${manifest.id.slice(0, 8)}): ackId=${ackId}, next=${manifest.totalChunks}, bytes=${manifest.size}`);
            void this.send({
              kind: 'file-chunk-ack',
              id: manifest.id,
              nextSeq: manifest.totalChunks,
              receivedBytes: manifest.size,
              ackId,
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
        this.events.onLog?.('xfer', `manifest received file (${manifest.id.slice(0, 8)}) source=${source}, chunks=${manifest.totalChunks}, size=${manifest.size}`);
        // The acknowledgement is deliberately not a directKinds message, so
        // it always travels over LiveKit even while the data channel is sick.
        void this.send({ kind: 'file-manifest-ack', id: manifest.id, chunkAck: true, binaryChunks: true, directFramePayloadBytes }).catch(() => {});
        this.events.onFileManifest?.(manifest, from);
        // Replay any chunks that beat the manifest here.
        const buffered = this.earlyChunks.get(manifest.id);
        if (buffered) {
          this.earlyChunks.delete(manifest.id);
          this.events.onLog?.('xfer', `replaying ${buffered.length} early chunks for file`);
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
        if (Number.isSafeInteger(msg.directFramePayloadBytes) && (msg.directFramePayloadBytes ?? 0) > 0) {
          this.directFramePayloadLimits.set(
            msg.id,
            Math.min(DIRECT_FILE_BATCH_PAYLOAD_LIMIT_BYTES, Math.floor(msg.directFramePayloadBytes!)),
          );
        } else {
          this.directFramePayloadLimits.delete(msg.id);
        }
        this.resolveManifestAck(msg.id, true, source);
        break;
      case 'file-chunk-ack':
        this.resolveChunkAck(msg, source, receivedAtMonoMs);
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
        this.events.onTaskProgress?.(msg.taskId, msg.receivedBytes ?? msg.doneBytes, msg.totalBytes, from, msg.savedBytes);
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
    this.clearProgressAckHeartbeat(entry);
    const manifest = entry.receiver.manifest;
    const xfer = id.slice(0, 8);
    let stage = 'assemble/hash';
    let stageStartedAt = Date.now();
    try {
      if (this.events.onFileCompleteStream) {
        stage = 'stream save callback';
        this.events.onLog?.('xfer', `stream save callback start (${xfer}): size=${manifest.size}`);
        await this.events.onFileCompleteStream(id, manifest, entry.receiver);
        this.events.onLog?.('xfer', `stream save callback end (${xfer}): size=${manifest.size}, status=ok, elapsedMs=${Date.now() - stageStartedAt}`);
      } else {
        this.events.onLog?.('xfer', `assemble/hash start (${xfer}): size=${manifest.size}`);
        const bytes = await entry.receiver.assembleAsync();
        this.events.onLog?.('xfer', `assemble/hash end (${xfer}): size=${manifest.size}, status=ok, elapsedMs=${Date.now() - stageStartedAt}`);
        stage = 'save callback';
        stageStartedAt = Date.now();
        this.events.onLog?.('xfer', `save callback start (${xfer}): size=${manifest.size}`);
        await this.events.onFileComplete?.(id, manifest, bytes);
        this.events.onLog?.('xfer', `save callback end (${xfer}): size=${manifest.size}, status=ok, elapsedMs=${Date.now() - stageStartedAt}`);
      }
    } catch (e) {
      this.events.onLog?.('xfer', `${stage} end (${xfer}): size=${manifest.size}, status=error, elapsedMs=${Date.now() - stageStartedAt}`);
      this.events.onFileError?.(id, e instanceof Error ? e.message : String(e));
      await this.publishFinalizeFailureAck(id, entry.receiver);
      this.incoming.delete(id);
      this.earlyChunks.delete(id);
      this.clearPerFileDiagnosticState(id);
      return;
    }

    this.completedFiles.add(id);
    this.incoming.delete(id);
    this.earlyChunks.delete(id);
    this.directChunkCounts.delete(id);
    // Do not wait for a queued progress publish: LiveKit can stall that
    // promise. The final ACK carries the monotonic total and resolveChunkAck
    // ignores any late/stale progress ACK, so it can safely be sent now.
    const ackId = randomId(8);
    const finalAckStartedAtMonoMs = performance.now();
    this.events.onLog?.('xfer', `final ACK publish start (${xfer}): ackId=${ackId}, size=${manifest.size}`);
    try {
      await this.send({
        kind: 'file-chunk-ack',
        id,
        nextSeq: entry.receiver.nextSeq,
        receivedBytes: entry.receiver.receivedByteCount,
        ackId,
        complete: true,
      });
      const elapsedMs = Math.max(0, performance.now() - finalAckStartedAtMonoMs);
      this.events.onLog?.('xfer', `final ACK publish end (${xfer}): ackId=${ackId}, size=${manifest.size}, status=ok, elapsedMs=${elapsedMs}`);
      this.events.onLog?.('xfer', `final chunk ACK sent (${id.slice(0, 8)}): ackId=${ackId}, bytes=${entry.receiver.receivedByteCount}`);
    } catch (e) {
      const elapsedMs = Math.max(0, performance.now() - finalAckStartedAtMonoMs);
      this.events.onLog?.('xfer', `final ACK publish end (${xfer}): ackId=${ackId}, size=${manifest.size}, status=error, elapsedMs=${elapsedMs}`);
      this.events.onLog?.('xfer', `final chunk ACK failed after save (${id.slice(0, 8)}): ${e instanceof Error ? e.name : typeof e}`);
    }
    this.clearPerFileDiagnosticState(id);
  }

  private async publishFinalizeFailureAck(id: string, receiver: FileReceiver): Promise<void> {
    const manifest = receiver.manifest;
    const ackId = randomId(8);
    const startedAtMonoMs = performance.now();
    try {
      await this.send({
        kind: 'file-chunk-ack',
        id,
        nextSeq: receiver.nextSeq,
        receivedBytes: receiver.receivedByteCount,
        ackId,
        complete: false,
        failed: true,
      });
      const elapsedMs = Math.max(0, performance.now() - startedAtMonoMs);
      this.events.onLog?.('xfer', `finalize failure ACK publish (${id.slice(0, 8)}): ackId=${ackId}, size=${manifest.size}, status=ok, elapsedMs=${elapsedMs}`);
    } catch {
      const elapsedMs = Math.max(0, performance.now() - startedAtMonoMs);
      this.events.onLog?.('xfer', `finalize failure ACK publish (${id.slice(0, 8)}): ackId=${ackId}, size=${manifest.size}, status=error, elapsedMs=${elapsedMs}`);
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
      this.clearProgressAckHeartbeat(entry);
      this.incoming.delete(msg.id);
      this.clearPerFileDiagnosticState(msg.id);
      return;
    }
    // Use the integer accumulator as the source of truth. Computing
    // `progress * totalBytes` introduced values such as 3932159.9999999995
    // into task-progress, which made sender/receiver progress comparisons
    // look inconsistent and could leave a target one fraction short.
    this.events.onFileProgress?.(msg.id, receiver.receivedByteCount, receiver.totalBytes);
    this.logReceiverChunkProgress(msg.id, receiver, msg.source, msg.seq);
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
      finalizing: boolean;
      pendingAck?: PendingChunkAck;
      progressAckTimer?: ReturnType<typeof setTimeout>;
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
    if (!progressAck && !resendAck) {
      if (!complete && entry.lastAckSeq > 0 && nextSeq > entry.lastAckSeq) {
        this.scheduleProgressAckHeartbeat(id, entry, source);
      }
      return;
    }
    this.clearProgressAckHeartbeat(entry);

    // If the receiver has advanced by a non-window amount, resend the last
    // known contiguous position as a heartbeat.  A sender that did not see
    // the original ACK can then replay only from that exact position.
    const ack: PendingChunkAck = {
      nextSeq: progressAck ? nextSeq : entry.lastAckSeq,
      receivedBytes: entry.receiver.receivedByteCount,
      // This ACK is strictly byte-received progress. A separate complete=true
      // ACK is sent only after async SHA-256 validation and the save callback.
      complete: false,
      ackId: randomId(8),
      queuedAtMonoMs: performance.now(),
      source,
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
    const boundarySize = source === 'direct' ? DIRECT_CHUNK_ACK_EVERY : RELAY_CHUNK_WINDOW;
    const boundary = ack.nextSeq >= boundarySize && ack.nextSeq % boundarySize === 0 ? source : undefined;
    if (this.shouldLogAckDiagnostic(id, 'receiver_queue', complete, boundary)) {
      this.events.onLog?.('xfer', `chunk ACK queued (${id.slice(0, 8)}): ackId=${ack.ackId}, source=${source}, next=${ack.nextSeq}, bytes=${ack.receivedBytes}, complete=${complete}`);
    }
  }

  private scheduleProgressAckHeartbeat(
    id: string,
    entry: {
      receiver: FileReceiver;
      lastAckSeq: number;
      lastAckAt: number;
      ackChain: Promise<void>;
      ackPending: boolean;
      finalizing: boolean;
      pendingAck?: PendingChunkAck;
      progressAckTimer?: ReturnType<typeof setTimeout>;
    },
    source: 'relay' | 'direct',
  ): void {
    if (entry.progressAckTimer || entry.finalizing) return;
    const delayMs = Math.max(0, CHUNK_ACK_PROGRESS_INTERVAL_MS - (Date.now() - entry.lastAckAt));
    entry.progressAckTimer = setTimeout(() => {
      entry.progressAckTimer = undefined;
      if (this.incoming.get(id) !== entry || entry.finalizing) return;
      this.maybeSendChunkAck(id, entry, false, source);
    }, delayMs);
  }

  private clearProgressAckHeartbeat(entry: { progressAckTimer?: ReturnType<typeof setTimeout> }): void {
    if (entry.progressAckTimer) clearTimeout(entry.progressAckTimer);
    entry.progressAckTimer = undefined;
  }

  /** Serialize receive ACKs so a slow relay cannot accumulate promises. */
  private enqueueChunkAck(
    id: string,
    entry: {
      ackChain: Promise<void>;
      ackPending: boolean;
      pendingAck?: PendingChunkAck;
    },
    ack: PendingChunkAck,
  ): void {
    entry.ackPending = true;
    const boundarySize = ack.source === 'direct' ? DIRECT_CHUNK_ACK_EVERY : RELAY_CHUNK_WINDOW;
    const boundary = ack.nextSeq >= boundarySize && ack.nextSeq % boundarySize === 0 ? ack.source : undefined;
    const logAckLifecycle = this.shouldLogAckDiagnostic(id, 'receiver_publish', ack.complete, boundary);
    entry.ackChain = entry.ackChain
      .catch(() => {})
      .then(async () => {
        const publishStartedAtMonoMs = performance.now();
        const queueWaitMs = Math.max(0, publishStartedAtMonoMs - ack.queuedAtMonoMs);
        if (logAckLifecycle) {
          this.events.onLog?.('xfer', `chunk ACK publish start (${id.slice(0, 8)}): ackId=${ack.ackId}, next=${ack.nextSeq}, bytes=${ack.receivedBytes}, queueWaitMs=${queueWaitMs}`);
        }
        try {
          const route = await this.send({
            kind: 'file-chunk-ack',
            id,
            nextSeq: ack.nextSeq,
            receivedBytes: ack.receivedBytes,
            ackId: ack.ackId,
            complete: ack.complete,
          });
          const publishMs = Math.max(0, performance.now() - publishStartedAtMonoMs);
          if (logAckLifecycle) {
            this.events.onLog?.('xfer', `chunk ACK publish end (${id.slice(0, 8)}): ackId=${ack.ackId}, route=${route}, status=ok, next=${ack.nextSeq}, bytes=${ack.receivedBytes}, publishMs=${publishMs}, queueToPublishMs=${queueWaitMs + publishMs}`);
          }
        } catch (e) {
          const publishMs = Math.max(0, performance.now() - publishStartedAtMonoMs);
          this.events.onLog?.('xfer', `chunk ACK publish end (${id.slice(0, 8)}): ackId=${ack.ackId}, status=error, publishMs=${publishMs}, error=${e instanceof Error ? e.name : typeof e}`);
          throw e;
        }
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
    for (const entry of this.incoming.values()) this.clearProgressAckHeartbeat(entry);
    this.completedFiles.clear();
    this.cancelAllChunkAckWaiters();
    for (const resolve of this.manifestAckWaiters.values()) resolve(false);
    this.manifestAckWaiters.clear();
    this.chunkAckProgress.clear();
    this.outgoingFileSizes.clear();
    this.finalizingFiles.clear();
    this.chunkAckCapabilities.clear();
    this.binaryChunkCapabilities.clear();
    this.directFramePayloadLimits.clear();
    this.firstDirectJsonFrameIds.clear();
    this.ackDiagnosticState.clear();
    this.directChunkCounts.clear();
    this.firstReceiveFrameIds.clear();
    this.receiveProgressLogState.clear();
    this.firstReceiveChunkIds.clear();
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
