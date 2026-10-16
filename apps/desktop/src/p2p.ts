// Experimental Rust-owned WebRTC data plane. The LiveKit room remains the
// existing end-to-end encrypted signaling/control path; this bridge forwards
// the unchanged p2p-* signals and FLNG frames to the native Rust peer.

import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import {
  P2P_CONNECT_TIMEOUT_MS,
  P2P_DRAIN_THRESHOLD,
  P2P_DRAIN_TIMEOUT_MS,
  randomId,
  type DirectChannel,
  type FlingMessage,
  type FlingRoom,
} from '@fling/core';
import { log } from './log.js';

export type P2PState = 'idle' | 'connecting' | 'open' | 'failed';
type P2PSignal = Extract<FlingMessage, { kind: `p2p-${string}` }>;

/** Default STUN servers for ICE host/srflx discovery. */
export const DEFAULT_ICE_SERVERS: RTCIceServer[] = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];

/** Runtime TURN override. */
let customIce: RTCIceServer[] | null = null;
export function setCustomIceServers(servers: RTCIceServer[]): void {
  customIce = servers;
}
export function getIceServers(): RTCIceServer[] {
  return customIce ?? DEFAULT_ICE_SERVERS;
}

interface NativeP2PEvent {
  sessionId: string;
  event: 'signal' | 'open' | 'message' | 'closed' | 'failed';
  signal?: P2PSignal;
  data?: number[];
  maxMessageSize?: number;
  error?: string;
}

interface DrainWaiter {
  threshold: number;
  resolve: () => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * Implements the existing Fling DirectChannel contract over native Rust IPC.
 * Rust and this bridge keep at most 1 MiB of unsent frames; FlingRoom's
 * existing adaptive windows and application ACKs remain the authoritative
 * transfer/backpressure/recovery mechanism.
 */
class RustDirectChannel implements DirectChannel {
  readonly label: string;
  readonly maxMessageSize = 64 * 1024;
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null = null;
  onclose: (() => void) | null = null;

  private state: 'connecting' | 'open' | 'closed' = 'connecting';
  private queuedBytes = 0;
  private queue: Promise<void> = Promise.resolve();
  private waiters = new Set<DrainWaiter>();
  private readonly maxQueuedBytes = 1024 * 1024;

  constructor(private readonly sessionId: string, label: string) {
    this.label = label;
  }

  get readyState(): string {
    return this.state;
  }
  get bufferedAmount(): number {
    return this.queuedBytes;
  }

  markOpen(): void {
    if (this.state !== 'closed') this.state = 'open';
  }

  deliver(data: Uint8Array): void {
    if (this.state !== 'open') return;
    if (data.byteLength > 128 * 1024 + 30) {
      this.markClosed(new Error('native frame exceeds the POC limit'));
      return;
    }
    try {
      this.onmessage?.(new Uint8Array(data) as Uint8Array<ArrayBuffer>);
    } finally {
      // Rust waits for this handoff before reading the next DataChannel message.
      void invoke('native_p2p_ack_receive', { sessionId: this.sessionId }).catch(() => {});
    }
  }

  send(data: Uint8Array<ArrayBuffer>): void {
    if (this.state !== 'open') throw new Error('native direct channel is not open');
    const length = data.byteLength;
    if (!length || length > 128 * 1024 + 30) throw new Error('invalid native direct frame size');
    if (this.queuedBytes + length > this.maxQueuedBytes) {
      throw new Error('native direct send queue reached its 1 MiB bound');
    }
    const payload = Array.from(data);
    this.queuedBytes += length;
    const operation = this.queue.then(async () => {
      await invoke<number>('native_p2p_send', { sessionId: this.sessionId, data: payload });
    });
    this.queue = operation.catch((error: unknown) => {
      this.markClosed(error instanceof Error ? error : new Error(String(error)));
    });
    void operation.then(
      () => this.completeSend(length),
      () => this.completeSend(length),
    );
  }

  private completeSend(length: number): void {
    this.queuedBytes = Math.max(0, this.queuedBytes - length);
    this.resolveDrainWaiters();
  }

  private resolveDrainWaiters(): void {
    for (const waiter of [...this.waiters]) {
      if (this.state === 'closed') {
        clearTimeout(waiter.timer);
        this.waiters.delete(waiter);
        waiter.reject(new Error('native direct channel closed while draining'));
      } else if (this.queuedBytes <= waiter.threshold) {
        clearTimeout(waiter.timer);
        this.waiters.delete(waiter);
        waiter.resolve();
      }
    }
  }

  waitForDrain(threshold: number): Promise<void> {
    if (this.queuedBytes <= threshold) return Promise.resolve();
    if (this.state !== 'open') return Promise.reject(new Error('native direct channel is not open'));
    return new Promise((resolve, reject) => {
      const waiter: DrainWaiter = {
        threshold,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.waiters.delete(waiter);
          reject(new Error('native direct channel drain timeout'));
        }, P2P_DRAIN_TIMEOUT_MS),
      };
      this.waiters.add(waiter);
      this.resolveDrainWaiters();
    });
  }

  markClosed(error?: Error): void {
    if (this.state === 'closed') return;
    this.state = 'closed';
    this.resolveDrainWaiters();
    if (error) log('p2p', 'native direct channel closed', { error: error.message });
    this.onclose?.();
  }

  close(): void {
    if (this.state === 'closed') return;
    this.markClosed();
  }
}

export class P2PLink {
  private readonly sessionId = randomId(16);
  private readonly channel: RustDirectChannel;
  private listeners: UnlistenFn[] = [];
  private listenerPromise: Promise<void> | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private _state: P2PState = 'idle';
  private closed = false;
  private nativeStarted = false;
  private remoteSet = false;
  private pendingIce: string[] = [];
  private pendingAnswer: P2PSignal | null = null;
  onstate: ((s: P2PState) => void) | null = null;

  constructor(
    private room: FlingRoom,
    private role: 'initiator' | 'responder',
    private peerLabel: string,
    private iceServers: RTCIceServer[] = getIceServers(),
  ) {
    this.channel = new RustDirectChannel(this.sessionId, this.peerLabel);
    this.channel.onclose = () => {
      if (!this.closed && this._state === 'open') this.fail('native data channel closed');
    };
  }

  get state(): P2PState {
    return this._state;
  }

  private setState(state: P2PState): void {
    if (this._state === state) return;
    this._state = state;
    try {
      this.onstate?.(state);
    } catch {
      /* ignore observer errors */
    }
  }

  private async ensureListeners(): Promise<void> {
    if (this.listenerPromise) return this.listenerPromise;
    this.listenerPromise = (async () => {
      this.listeners.push(await listen<NativeP2PEvent>('fling-native-p2p', (event) => {
        const payload = event.payload;
        if (payload.sessionId !== this.sessionId || this.closed) return;
        switch (payload.event) {
          case 'signal':
            if (payload.signal) void this.room.sendSignal(payload.signal).catch(() => {});
            break;
          case 'open':
            if (this.timer) clearTimeout(this.timer);
            this.timer = null;
            this.channel.markOpen();
            this.room.setDirectChannel(this.channel);
            log('p2p', 'Rust-owned direct channel open', {
              peer: this.peerLabel,
              maxMessageSize: payload.maxMessageSize,
              signaling: 'existing encrypted LiveKit p2p-*',
            });
            this.setState('open');
            break;
          case 'message':
            if (Array.isArray(payload.data)) this.channel.deliver(new Uint8Array(payload.data));
            break;
          case 'closed':
            this.channel.markClosed();
            break;
          case 'failed':
            this.fail(payload.error ?? 'native WebRTC failed');
            break;
        }
      }));
    })();
    return this.listenerPromise;
  }

  private armTimeout(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = null;
      if (this._state !== 'open') this.fail('connect timeout');
    }, P2P_CONNECT_TIMEOUT_MS);
  }

  private async startNativePeer(): Promise<void> {
    if (this.nativeStarted) return;
    await this.ensureListeners();
    await invoke('native_p2p_start', {
      sessionId: this.sessionId,
      role: this.role,
      iceServers: this.iceServers,
    });
    this.nativeStarted = true;
  }

  /** Initiator: Rust creates the DataChannel and emits the existing p2p-offer. */
  async start(): Promise<void> {
    if (this.role !== 'initiator' || this.nativeStarted || this.closed) return;
    this.setState('connecting');
    this.armTimeout();
    log('p2p', 'starting Rust WebRTC initiator', { peer: this.peerLabel });
    try {
      await this.startNativePeer();
      if (this.pendingAnswer) {
        const answer = this.pendingAnswer;
        this.pendingAnswer = null;
        await this.applyAnswer(answer);
      }
    } catch (error) {
      this.fail(`native initiator start failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /** Responder: accept the unchanged offer and answer over LiveKit signaling. */
  async acceptOffer(sdp: string): Promise<void> {
    if (this.role !== 'responder' || this.nativeStarted || this.closed) return;
    this.setState('connecting');
    this.armTimeout();
    log('p2p', 'starting Rust WebRTC responder', { peer: this.peerLabel, sdpBytes: sdp.length });
    try {
      await this.startNativePeer();
      await invoke('native_p2p_accept_offer', { sessionId: this.sessionId, sdp });
      this.remoteSet = true;
      await this.flushIce();
    } catch (error) {
      this.fail(`native responder offer failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /** Route inbound p2p-answer / p2p-ice messages from the existing FlingRoom. */
  async handleSignal(signal: P2PSignal): Promise<void> {
    if (this.closed) return;
    try {
      if (signal.kind === 'p2p-answer' && this.role === 'initiator') {
        if (!this.nativeStarted) {
          this.pendingAnswer = signal;
          return;
        }
        await this.applyAnswer(signal);
      } else if (signal.kind === 'p2p-ice' && signal.candidate) {
        if (!this.nativeStarted || !this.remoteSet) {
          this.pendingIce.push(signal.candidate);
          return;
        }
        await invoke('native_p2p_handle_signal', { sessionId: this.sessionId, signal });
      }
    } catch {
      // Preserve the production relay path if an experimental signal is rejected.
    }
  }

  private async applyAnswer(signal: P2PSignal): Promise<void> {
    await invoke('native_p2p_handle_signal', { sessionId: this.sessionId, signal });
    this.remoteSet = true;
    await this.flushIce();
  }

  /** ICE can arrive before its offer/answer; keep it until the remote SDP is set. */
  addRemoteIce(candidate: RTCIceCandidateInit): void {
    this.pendingIce.push(JSON.stringify(candidate));
  }

  private async flushIce(): Promise<void> {
    if (!this.nativeStarted || !this.remoteSet) return;
    const pending = this.pendingIce;
    this.pendingIce = [];
    for (const candidate of pending) {
      await invoke('native_p2p_handle_signal', {
        sessionId: this.sessionId,
        signal: { kind: 'p2p-ice', candidate },
      });
    }
  }

  private fail(reason: string): void {
    if (this._state === 'failed' || this.closed) return;
    log('p2p', `Rust WebRTC link failed: ${reason}`, { role: this.role, peer: this.peerLabel });
    this.setState('failed');
    try {
      this.room.fallbackToRelay(reason);
    } catch {
      /* keep existing transfer fallback available */
    }
    this.disposeNative();
  }

  fallbackToRelay(reason: string): void {
    this.fail(reason);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.room.setDirectChannel(null);
    this.channel.markClosed();
    this.disposeNative();
    if (this._state !== 'idle') this.setState('idle');
  }

  private disposeNative(): void {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.room.setDirectChannel(null);
    this.channel.markClosed();
    void invoke('native_p2p_close', { sessionId: this.sessionId }).catch(() => {});
    for (const unlisten of this.listeners) unlisten();
    this.listeners = [];
  }
}
