// @livekit/rtc-node transport for Node/Bun (CLI).
// Import from '@fling/core/node' — never from '@fling/core' (native binding).

import { Room, RoomEvent } from '@livekit/rtc-node';
import type { DataHandler, FlingTransport } from '../transport.js';

export class NodeTransport implements FlingTransport {
  private room = new Room();
  private dataHandler: DataHandler | null = null;
  private peerJoined: ((id: string) => void) | null = null;
  private peerLeft: ((id: string) => void) | null = null;

  get isConnected(): boolean {
    return this.room.isConnected;
  }

  get localIdentity(): string {
    return this.room.localParticipant?.identity ?? '';
  }

  async connect(url: string, token: string): Promise<void> {
    this.room.on(RoomEvent.DataReceived, (payload, participant, _kind, topic) => {
      this.dataHandler?.(payload, participant?.identity ?? 'unknown', topic ?? '');
    });
    this.room.on(RoomEvent.ParticipantConnected, (p) => this.peerJoined?.(p.identity));
    this.room.on(RoomEvent.ParticipantDisconnected, (p) => this.peerLeft?.(p.identity));
    await this.room.connect(url, token);
  }

  async disconnect(): Promise<void> {
    await this.room.disconnect();
  }

  async publish(payload: Uint8Array, topic: string): Promise<void> {
    await this.room.localParticipant.publishData(payload, { reliable: true, topic });
  }

  onData(handler: DataHandler): void {
    this.dataHandler = handler;
  }
  onPeerJoined(handler: (identity: string) => void): void {
    this.peerJoined = handler;
  }
  onPeerLeft(handler: (identity: string) => void): void {
    this.peerLeft = handler;
  }
}
