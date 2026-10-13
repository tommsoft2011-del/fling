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

class RTCDirectChannel implements DirectChannel {
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null = null;
  onclose: (() => void) | null = null;

  constructor(
    private dc: RTCDataChannel,
    readonly label: string,
  ) {
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = P2P_DRAIN_THRESHOLD;
    dc.onmessage = (e) => {
      const data = e.data;
      if (data instanceof ArrayBuffer) this.onmessage?.(new Uint8Array(data));
      else if (data instanceof Blob) {
        void data.arrayBuffer().then((buf) => this.onmessage?.(new Uint8Array(buf)));
      }
    };
    dc.onclose = () => this.onclose?.();
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
    if (this.dc.bufferedAmount <= threshold || this.dc.readyState !== 'open') {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const done = () => {
        this.dc.removeEventListener('bufferedamountlow', done);
        this.dc.removeEventListener('close', done);
        resolve();
      };
      this.dc.addEventListener('bufferedamountlow', done);
      // Never hang if the channel dies while we wait: the send() wrapper
      // treats a closed channel as "fall back to relay".
      this.dc.addEventListener('close', done);
      if (this.dc.bufferedAmount <= threshold || this.dc.readyState !== 'open') done();
    });
  }
}

export class P2PLink {
  private pc: RTCPeerConnection | null = null;
  private pendingIce: RTCIceCandidateInit[] = [];
  private remoteSet = false;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private _state: P2PState = 'idle';
  private iceLocal = 0;
  private iceRemote = 0;
  /** 正常关闭（任务结束）时不报 failed，避免日志误导 */
  private closed = false;
  onstate: ((s: P2PState) => void) | null = null;

  constructor(
    private room: FlingRoom,
    private role: 'initiator' | 'responder',
    private peerLabel: string,
    private iceServers: RTCIceServer[] = DEFAULT_ICE_SERVERS,
  ) {}

  get state(): P2PState {
    return this._state;
  }

  private setState(s: P2PState): void {
    if (this._state === s) return;
    this._state = s;
    try {
      this.onstate?.(s);
    } catch {
      /* ignore */
    }
  }

  private fail(reason: string): void {
    if (this._state === 'failed') return;
    // Whether the handshake never completed or an established channel just
    // dropped: detach the direct path so traffic falls back to the relay.
    log('p2p', `link failed: ${reason}`, {
      role: this.role,
      peer: this.peerLabel,
      was: this._state,
      iceLocal: this.iceLocal,
      iceRemote: this.iceRemote,
    });
    this.setState('failed');
    try {
      this.room.setDirectChannel(null);
    } catch {
      /* ignore */
    }
    this.closePc();
  }

  private newPc(): RTCPeerConnection | null {
    if (typeof RTCPeerConnection === 'undefined') return null;
    const pc = new RTCPeerConnection({ iceServers: this.iceServers });
    pc.onicecandidate = (e) => {
      if (e.candidate) this.iceLocal++;
      const candidate = e.candidate ? JSON.stringify(e.candidate.toJSON()) : null;
      void this.room
        .sendSignal({ kind: 'p2p-ice', candidate } as FlingMessage)
        .catch(() => {});
    };
    pc.onconnectionstatechange = () => {
      log('p2p', `pc connectionState -> ${pc.connectionState}`, {
        role: this.role,
        peer: this.peerLabel,
      });
      if (pc.connectionState === 'failed') this.fail('pc connection failed');
    };
    return pc;
  }

  private wireDataChannel(dc: RTCDataChannel): void {
    dc.onopen = () => {
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      log('p2p', 'direct channel open', {
        role: this.role,
        peer: this.peerLabel,
        iceLocal: this.iceLocal,
        iceRemote: this.iceRemote,
      });
      this.setState('open');
      try {
        this.room.setDirectChannel(new RTCDirectChannel(dc, this.peerLabel));
      } catch (e) {
        this.fail(`setDirectChannel threw: ${(e as Error).message}`);
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
    log('p2p', 'initiator start', { peer: this.peerLabel });
    try {
      const dc = pc.createDataChannel(DC_LABEL, { ordered: true });
      this.wireDataChannel(dc);
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      log('p2p', 'offer sent', { sdpBytes: (offer.sdp ?? '').length });
      await this.room.sendSignal({ kind: 'p2p-offer', sdp: offer.sdp ?? '' });
    } catch (e) {
      this.fail(`initiator start threw: ${(e as Error).message}`);
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
    log('p2p', 'responder accept', { peer: this.peerLabel, sdpBytes: sdp.length });
    try {
      await pc.setRemoteDescription({ type: 'offer', sdp });
      this.remoteSet = true;
      await this.flushIce();
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      log('p2p', 'answer sent', { sdpBytes: (answer.sdp ?? '').length });
      await this.room.sendSignal({ kind: 'p2p-answer', sdp: answer.sdp ?? '' });
    } catch (e) {
      this.fail(`responder accept threw: ${(e as Error).message}`);
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
        if (this.remoteSet) {
          await pc.addIceCandidate(new RTCIceCandidate(init));
        } else {
          this.pendingIce.push(init);
        }
      }
    } catch {
      /* a bad signal must not kill the relay fallback */
    }
  }

  private async flushIce(): Promise<void> {
    const pc = this.pc;
    if (!pc) return;
    const pending = this.pendingIce;
    this.pendingIce = [];
    for (const init of pending) {
      try {
        await pc.addIceCandidate(new RTCIceCandidate(init));
      } catch {
        /* ignore */
      }
    }
  }

  close(): void {
    this.closed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.closePc();
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
