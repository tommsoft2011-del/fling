// P2P direct file channel over WebRTC.
//
// LiveKit stays the signaling pipe (SDP / ICE go through the end-to-end
// encrypted relay) and provides the ICE/STUN hole-punching rendezvous.
// File bytes then flow over a direct RTCPeerConnection data channel between
// the two machines — LAN or WAN, wherever ICE can establish a path.
// If the handshake fails or the channel drops, FlingRoom silently falls
// back to relaying file chunks over LiveKit (per-chunk routing).

import {
  P2P_CONNECT_TIMEOUT_MS,
  P2P_DRAIN_THRESHOLD,
  P2P_DRAIN_TIMEOUT_MS,
  type DirectChannel,
  type FlingMessage,
  type FlingRoom,
} from '@fling/core';
import { log } from './log.js';

export type P2PState = 'idle' | 'connecting' | 'open' | 'failed';

/** Default STUN servers for ICE host/srflx discovery. No TURN here: when a
 *  direct path can't be built (e.g. symmetric NAT on both ends) we fall back
 *  to the LiveKit relay instead of paying for a TURN server. */
export const DEFAULT_ICE_SERVERS: RTCIceServer[] = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];

const DC_LABEL = 'fling-file';

/** 运行时覆盖 ICE（配置文件里配了 TURN 就追加到默认 STUN 后面） */
let customIce: RTCIceServer[] | null = null;
export function setCustomIceServers(servers: RTCIceServer[]): void {
  customIce = servers;
}
export function getIceServers(): RTCIceServer[] {
  return customIce ?? DEFAULT_ICE_SERVERS;
}

class RTCDirectChannel implements DirectChannel {
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null = null;
  onclose: (() => void) | null = null;
  private drainSummaryAt = 0;
  private drainSummaryCount = 0;
  private drainSummaryMs = 0;
  private drainSummaryMaxMs = 0;

  constructor(
    private dc: RTCDataChannel,
    readonly label: string,
    readonly maxMessageSize: number,
  ) {
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = P2P_DRAIN_THRESHOLD;
    dc.onmessage = (e) => {
      const data = e.data;
      if (data instanceof ArrayBuffer) this.onmessage?.(new Uint8Array(data) as Uint8Array<ArrayBuffer>);
      else if (data instanceof Blob) {
        void data.arrayBuffer().then((buf) => this.onmessage?.(new Uint8Array(buf)));
      }
    };
    dc.onclose = () => this.onclose?.();
    dc.addEventListener('bufferedamountlow', () => {
      const now = Date.now();
      if (now - this.drainSummaryAt >= 10_000) {
        this.drainSummaryAt = now;
        log('p2p', 'data channel buffered amount low', {
          state: dc.readyState,
          bufferedAmount: dc.bufferedAmount,
          lowThreshold: dc.bufferedAmountLowThreshold,
        });
      }
    });
    dc.addEventListener('close', () => {
      log('p2p', 'data channel close event', { state: dc.readyState, bufferedAmount: dc.bufferedAmount });
    });
    dc.addEventListener('error', () => {
      log('p2p', 'data channel error event', { state: dc.readyState, bufferedAmount: dc.bufferedAmount });
    });
  }

  get readyState(): string {
    return this.dc.readyState;
  }
  get bufferedAmount(): number {
    return this.dc.bufferedAmount;
  }
  send(data: Uint8Array<ArrayBuffer>): void {
    this.dc.send(data);
  }
  close(): void {
    try {
      this.dc.close();
    } catch {
      /* ignore */
    }
  }
  waitForDrain(threshold: number): Promise<void> {
    if (this.dc.bufferedAmount <= threshold) return Promise.resolve();
    if (this.dc.readyState !== 'open') return Promise.reject(new Error('direct channel is not open'));
    const startedAt = performance.now();
    const initialBufferedAmount = this.dc.bufferedAmount;
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout>;
      let onLow: () => void;
      let onClose: () => void;
      const finish = (error?: Error) => {
        clearTimeout(timer);
        this.dc.removeEventListener('bufferedamountlow', onLow);
        this.dc.removeEventListener('close', onClose);
        const elapsedMs = performance.now() - startedAt;
        this.drainSummaryCount++;
        this.drainSummaryMs += elapsedMs;
        this.drainSummaryMaxMs = Math.max(this.drainSummaryMaxMs, elapsedMs);
        const now = Date.now();
        if (error || this.drainSummaryAt === 0 || now - this.drainSummaryAt >= 10_000) {
          log('p2p', 'data channel drain summary', {
            status: error ? error.name : 'drained',
            waitCount: this.drainSummaryCount,
            waitTotalMs: Math.round(this.drainSummaryMs),
            waitMaxMs: Math.round(this.drainSummaryMaxMs),
            initialBufferedAmount,
            finalBufferedAmount: this.dc.bufferedAmount,
            lowThreshold: threshold,
            channelState: this.dc.readyState,
          });
          this.drainSummaryAt = now;
          this.drainSummaryCount = 0;
          this.drainSummaryMs = 0;
          this.drainSummaryMaxMs = 0;
        }
        if (error) reject(error);
        else resolve();
      };
      onLow = () => {
        if (this.dc.readyState === 'open' && this.dc.bufferedAmount <= threshold) finish();
        else finish(new Error('direct channel did not drain'));
      };
      onClose = () => finish(new Error('direct channel closed while draining'));
      timer = setTimeout(() => finish(new Error('direct channel drain timeout')), P2P_DRAIN_TIMEOUT_MS);
      this.dc.addEventListener('bufferedamountlow', onLow);
      this.dc.addEventListener('close', onClose);
      if (this.dc.bufferedAmount <= threshold) onLow();
      else if (this.dc.readyState !== 'open') onClose();
    });
  }
}

export class P2PLink {
  private pc: RTCPeerConnection | null = null;
  private dataChannel: RTCDataChannel | null = null;
  private pendingIce: RTCIceCandidateInit[] = [];
  private remoteSet = false;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private _state: P2PState = 'idle';
  private iceLocal = 0;
  private iceRemote = 0;
  /** 诊断用：收集到的本地/远端 candidate 类型（host/srflx/relay + 协议） */
  private iceLocalTypes = new Set<string>();
  private iceRemoteTypes = new Set<string>();
  private readonly startedAt = performance.now();
  private lastCandidateSummaryAt = 0;
  private localIceAddFailures = 0;
  private remoteIceAddFailures = 0;
  /** 正常关闭（任务结束）时不报 failed，避免日志误导 */
  private closed = false;
  onstate: ((s: P2PState) => void) | null = null;

  constructor(
    private room: FlingRoom,
    private role: 'initiator' | 'responder',
    private peerLabel: string,
    private iceServers: RTCIceServer[] = getIceServers(),
  ) {}

  get state(): P2PState {
    return this._state;
  }

  /** 从 candidate SDP 行提取 "类型/协议"，如 "host/udp"、"relay/tcp"。失败不抛错。 */
  private static candKind(cand: string): string {
    try {
      const t = /typ (host|srflx|prflx|relay)/.exec(cand)?.[1] ?? '?';
      const p = /\s(udp|tcp)\s/i.exec(cand)?.[1]?.toLowerCase() ?? '?';
      return `${t}/${p}`;
    } catch {
      return '?/?';
    }
  }

  private snapshot(pc = this.pc): Record<string, unknown> {
    return {
      role: this.role,
      linkState: this._state,
      pcConnectionState: pc?.connectionState ?? 'unavailable',
      iceConnectionState: pc?.iceConnectionState ?? 'unavailable',
      iceGatheringState: pc?.iceGatheringState ?? 'unavailable',
      signalingState: pc?.signalingState ?? 'unavailable',
      dtlsState: pc?.sctp?.transport.state ?? 'unavailable',
      sctpState: pc?.sctp?.state ?? 'unavailable',
      dataChannelState: this.dataChannel?.readyState ?? 'unavailable',
      bufferedAmount: this.dataChannel?.bufferedAmount ?? 0,
      maxMessageSize: pc?.sctp?.maxMessageSize ?? 0,
      pendingRemoteCandidates: this.pendingIce.length,
      localCandidateCount: this.iceLocal,
      remoteCandidateCount: this.iceRemote,
      localCandidateTypes: [...this.iceLocalTypes],
      remoteCandidateTypes: [...this.iceRemoteTypes],
    };
  }

  private iceServerProfile(): { stunCount: number; turnCount: number; otherCount: number } {
    let stunCount = 0;
    let turnCount = 0;
    let otherCount = 0;
    for (const server of this.iceServers) {
      const urls = Array.isArray(server.urls) ? server.urls : [server.urls];
      for (const url of urls) {
        if (/^stuns?:/i.test(url)) stunCount++;
        else if (/^turns?:/i.test(url)) turnCount++;
        else otherCount++;
      }
    }
    return { stunCount, turnCount, otherCount };
  }

  private logCandidateSummary(side: 'local' | 'remote', force = false): void {
    const count = side === 'local' ? this.iceLocal : this.iceRemote;
    const now = Date.now();
    if (!force && count !== 1 && count % 8 !== 0 && now - this.lastCandidateSummaryAt < 10_000) return;
    this.lastCandidateSummaryAt = now;
    log('p2p', `${side} ICE candidate summary`, {
      role: this.role,
      candidateCount: count,
      candidateTypes: [...(side === 'local' ? this.iceLocalTypes : this.iceRemoteTypes)],
      pendingRemoteCandidates: this.pendingIce.length,
    });
  }

  /**
   * 诊断：直连建立后，从 getStats 里找出选中的 candidate pair 并记日志。
   * 这是判断"到底走了直连/host、srflx 打洞还是 TURN 中转"的唯一可靠证据。
   */
  private logSelectedPair(pc: RTCPeerConnection): void {
    void (async () => {
      try {
        const stats = await pc.getStats();
        const locals = new Map<string, any>();
        const remotes = new Map<string, any>();
        let pair: any = null;
        stats.forEach((s: any) => {
          if (s.type === 'local-candidate') locals.set(s.id, s);
          else if (s.type === 'remote-candidate') remotes.set(s.id, s);
          else if (s.type === 'candidate-pair' && s.nominated && s.state === 'succeeded') pair = s;
        });
        if (!pair) {
          stats.forEach((s: any) => {
            if (s.type === 'candidate-pair' && s.state === 'succeeded' && (!pair || s.priority > pair.priority))
              pair = s;
          });
        }
        if (pair) {
          const l = locals.get(pair.localCandidateId);
          const r = remotes.get(pair.remoteCandidateId);
          const relayed = l?.candidateType === 'relay' || r?.candidateType === 'relay';
          log('p2p', 'selected pair', {
            role: this.role,
            local: `${l?.candidateType ?? '?'}/${l?.protocol ?? '?'}`,
            remote: `${r?.candidateType ?? '?'}/${r?.protocol ?? '?'}`,
            relayed,
            connection: this.snapshot(pc),
            currentRoundTripTimeMs: Number.isFinite(pair.currentRoundTripTime)
              ? Math.round(pair.currentRoundTripTime * 1_000)
              : undefined,
            availableOutgoingBitrateMbps: Number.isFinite(pair.availableOutgoingBitrate)
              ? Math.round(pair.availableOutgoingBitrate / 1_000_000 * 100) / 100
              : undefined,
            pairBytesSent: Number.isFinite(pair.bytesSent) ? pair.bytesSent : undefined,
            pairBytesReceived: Number.isFinite(pair.bytesReceived) ? pair.bytesReceived : undefined,
          });
        } else {
          log('p2p', 'no selected pair in stats', this.snapshot(pc));
        }
      } catch {
        /* 诊断失败不影响传输 */
      }
    })();
  }

  private setState(s: P2PState): void {
    if (this._state === s) return;
    const previous = this._state;
    this._state = s;
    log('p2p', 'link state changed', {
      role: this.role,
      from: previous,
      to: s,
      elapsedMs: Math.round(performance.now() - this.startedAt),
      ...this.snapshot(),
    });
    try {
      this.onstate?.(s);
    } catch {
      /* ignore */
    }
  }

  private fail(reason: string, error?: unknown): void {
    if (this._state === 'failed' || this.closed) return;
    // Whether the handshake never completed or an established channel just
    // dropped: detach the direct path so traffic falls back to the relay.
    log('p2p', 'link failed', {
      role: this.role,
      reason,
      errorType: error instanceof Error ? error.name : error === undefined ? undefined : typeof error,
      was: this._state,
      elapsedMs: Math.round(performance.now() - this.startedAt),
      iceLocal: this.iceLocal,
      iceRemote: this.iceRemote,
      // 诊断关键：两端各收集到了什么类型的 candidate。
      // 若本地没有 relay/udp，说明 TURN 没生效（配置未加载/凭证错/端口不通）；
      // 若远端类型为空，说明对端 candidate 没送达（信令问题）。
      iceLocalTypes: [...this.iceLocalTypes],
      iceRemoteTypes: [...this.iceRemoteTypes],
      localIceAddFailures: this.localIceAddFailures,
      remoteIceAddFailures: this.remoteIceAddFailures,
      ...this.snapshot(),
    });
    this.setState('failed');
    try {
      // Route all established-path failures through the room's fallback
      // transition so file replay, progress health, and the app-level link
      // owner observe the same generation change.
      this.room.fallbackToRelay(reason);
    } catch {
      /* ignore */
    }
    this.closePc();
  }

  /** Force an established but unresponsive path over to the relay. */
  fallbackToRelay(reason: string): void {
    this.fail(reason);
  }

  private newPc(): RTCPeerConnection | null {
    if (typeof RTCPeerConnection === 'undefined') return null;
    const pc = new RTCPeerConnection({ iceServers: this.iceServers });
    pc.onicecandidate = (e) => {
      if (e.candidate) {
        this.iceLocal++;
        this.iceLocalTypes.add(P2PLink.candKind(e.candidate.candidate));
        this.logCandidateSummary('local');
      }
      const candidate = e.candidate ? JSON.stringify(e.candidate.toJSON()) : null;
      void this.room
        .sendSignal({ kind: 'p2p-ice', candidate } as FlingMessage)
        .catch((error: unknown) => {
          log('p2p', 'local ICE signal publish failed', {
            errorType: error instanceof Error ? error.name : typeof error,
            ...this.snapshot(pc),
          });
        });
    };
    pc.onconnectionstatechange = () => {
      log('p2p', 'PeerConnection state changed', this.snapshot(pc));
      if (pc.connectionState === 'failed') this.fail('pc connection failed');
    };
    pc.oniceconnectionstatechange = () => log('p2p', 'ICE connection state changed', this.snapshot(pc));
    pc.onicegatheringstatechange = () => log('p2p', 'ICE gathering state changed', this.snapshot(pc));
    pc.onsignalingstatechange = () => log('p2p', 'signaling state changed', this.snapshot(pc));
    return pc;
  }

  private wireDataChannel(dc: RTCDataChannel): void {
    this.dataChannel = dc;
    dc.onopen = () => {
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      log('p2p', 'direct channel open', {
        role: this.role,
        bufferedAmount: dc.bufferedAmount,
        bufferedAmountLowThreshold: dc.bufferedAmountLowThreshold,
        maxMessageSize: this.pc?.sctp?.maxMessageSize ?? 0,
        elapsedMs: Math.round(performance.now() - this.startedAt),
        iceLocal: this.iceLocal,
        iceRemote: this.iceRemote,
        iceLocalTypes: [...this.iceLocalTypes],
        iceRemoteTypes: [...this.iceRemoteTypes],
      });
      // 诊断关键：记录最终选中的链路（host 直连 / srflx 打洞 / relay 中转）
      if (this.pc) this.logSelectedPair(this.pc);
      this.setState('open');
      try {
        const sctp = this.pc?.sctp;
        sctp?.addEventListener('statechange', () => log('p2p', 'SCTP state changed', this.snapshot(this.pc ?? undefined)));
        sctp?.transport.addEventListener('statechange', () => log('p2p', 'DTLS state changed', this.snapshot(this.pc ?? undefined)));
        const direct = new RTCDirectChannel(
          dc,
          this.peerLabel,
          this.pc?.sctp?.maxMessageSize ?? 64 * 1024,
        );
        // RTCDirectChannel owns the native onclose slot after wrapping the
        // browser channel, so wire the P2P link's failure path through the
        // wrapper as well. Without this, a closed channel could remain
        // attached to FlingRoom until the next send happened to notice it.
        direct.onclose = () => {
          if (!this.closed && this._state === 'open') this.fail('data channel closed');
        };
        this.room.setDirectChannel(direct);
      } catch (e) {
        this.fail('set_direct_channel_exception', e);
      }
    };
    dc.onclose = () => {
      if (this.closed) return;
      if (this._state === 'open') this.fail('data channel closed');
    };
    dc.onerror = () => {
      if (this._state !== 'open') this.fail('data channel error');
    };
  }

  private armTimeout(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = null;
      if (this._state !== 'open') this.fail('connect timeout');
    }, P2P_CONNECT_TIMEOUT_MS);
  }

  /** Initiator: create offer and start ICE. */
  async start(): Promise<void> {
    if (this.role !== 'initiator' || this.pc) return;
    const pc = this.newPc();
    if (!pc) {
      this.fail('RTCPeerConnection unavailable');
      return;
    }
    this.pc = pc;
    this.setState('connecting');
    this.armTimeout();
    log('p2p', 'initiator start', {
      role: this.role,
      timeoutMs: P2P_CONNECT_TIMEOUT_MS,
      iceServers: this.iceServerProfile(),
      startedAt: Date.now(),
    });
    try {
      const dc = pc.createDataChannel(DC_LABEL, { ordered: true });
      this.wireDataChannel(dc);
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      log('p2p', 'offer sent', { sdpBytes: (offer.sdp ?? '').length });
      await this.room.sendSignal({ kind: 'p2p-offer', sdp: offer.sdp ?? '' });
    } catch (e) {
      this.fail('initiator_start_exception', e);
    }
  }

  /** Responder: accept the initiator's offer. */
  async acceptOffer(sdp: string): Promise<void> {
    if (this.role !== 'responder' || this.pc) return;
    const pc = this.newPc();
    if (!pc) {
      this.fail('RTCPeerConnection unavailable');
      return;
    }
    this.pc = pc;
    this.setState('connecting');
    this.armTimeout();
    pc.ondatachannel = (e) => this.wireDataChannel(e.channel);
    log('p2p', 'responder accept', {
      role: this.role,
      sdpBytes: sdp.length,
      timeoutMs: P2P_CONNECT_TIMEOUT_MS,
      iceServers: this.iceServerProfile(),
      startedAt: Date.now(),
    });
    try {
      await pc.setRemoteDescription({ type: 'offer', sdp });
      this.remoteSet = true;
      await this.flushIce();
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      log('p2p', 'answer sent', { sdpBytes: (answer.sdp ?? '').length });
      await this.room.sendSignal({ kind: 'p2p-answer', sdp: answer.sdp ?? '' });
    } catch (e) {
      this.fail('responder_accept_exception', e);
    }
  }

  /** Route an inbound p2p-* signal to this link. */
  async handleSignal(sig: Extract<FlingMessage, { kind: `p2p-${string}` }>): Promise<void> {
    const pc = this.pc;
    if (!pc) return;
    try {
      if (sig.kind === 'p2p-answer' && this.role === 'initiator') {
        log('p2p', 'answer received');
        await pc.setRemoteDescription({ type: 'answer', sdp: sig.sdp });
        this.remoteSet = true;
        await this.flushIce();
      } else if (sig.kind === 'p2p-ice') {
        if (!sig.candidate) return; // end-of-candidates
        this.iceRemote++;
        const init = JSON.parse(sig.candidate) as RTCIceCandidateInit;
        if (init.candidate) this.iceRemoteTypes.add(P2PLink.candKind(init.candidate));
        this.logCandidateSummary('remote');
        if (this.remoteSet) {
          await pc.addIceCandidate(new RTCIceCandidate(init));
        } else {
          this.pendingIce.push(init);
        }
      }
    } catch (e) {
      this.remoteIceAddFailures++;
      if (this.remoteIceAddFailures === 1 || this.remoteIceAddFailures % 8 === 0) {
        log('p2p', 'remote signaling operation failed', {
          errorType: e instanceof Error ? e.name : typeof e,
          failures: this.remoteIceAddFailures,
          ...this.snapshot(pc),
        });
      }
    }
  }

  /** offer 到达前就收到的远端 ICE candidate（task 层缓存转交），进 pending 队列等 setRemoteDescription 后 flush */
  addRemoteIce(init: RTCIceCandidateInit): void {
    this.pendingIce.push(init);
    this.iceRemote++;
    if (init.candidate) this.iceRemoteTypes.add(P2PLink.candKind(init.candidate));
    this.logCandidateSummary('remote');
  }

  private async flushIce(): Promise<void> {
    const pc = this.pc;
    if (!pc) return;
    const pending = this.pendingIce;
    this.pendingIce = [];
    for (const init of pending) {
      try {
        await pc.addIceCandidate(new RTCIceCandidate(init));
      } catch (e) {
        this.localIceAddFailures++;
        if (this.localIceAddFailures === 1 || this.localIceAddFailures % 8 === 0) {
          log('p2p', 'queued ICE candidate rejected', {
            errorType: e instanceof Error ? e.name : typeof e,
            failures: this.localIceAddFailures,
            ...this.snapshot(pc),
          });
        }
      }
    }
  }

  close(): void {
    log('p2p', 'link close requested', {
      was: this._state,
      elapsedMs: Math.round(performance.now() - this.startedAt),
      ...this.snapshot(),
    });
    this.closed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.closePc();
    this.dataChannel = null;
    if (this._state !== 'idle') this.setState('idle');
  }

  private closePc(): void {
    const pc = this.pc;
    this.pc = null;
    if (pc) {
      try {
        pc.onicecandidate = null;
        pc.ondatachannel = null;
        pc.onconnectionstatechange = null;
        pc.close();
      } catch {
        /* ignore */
      }
    }
  }
}
