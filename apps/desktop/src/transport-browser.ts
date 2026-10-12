// livekit-client transport for browsers / Tauri webview.
import { Room, RoomEvent, type RemoteParticipant } from 'livekit-client';
import type { DataHandler, FlingTransport } from '@fling/core';

export class BrowserTransport implements FlingTransport {
  private room = new Room();
  private dataHandler: DataHandler | null = null;
  private peerJoined: ((id: string) => void) | null = null;
  private peerLeft: ((id: string) => void) | null = null;

  get isConnected(): boolean {
    return this.room.state === 'connected';
  }

  get localIdentity(): string {
    return this.room.localParticipant?.identity ?? '';
  }

  /** Identities of currently connected remote participants. */
  get remoteIdentities(): string[] {
    return [...this.room.remoteParticipants.values()].map((p: RemoteParticipant) => p.identity);
  }

  async connect(url: string, token: string): Promise<void> {
    this.room.on(RoomEvent.DataReceived, (payload, participant, _kind, topic) => {
      this.dataHandler?.(payload, participant?.identity ?? 'unknown', topic ?? '');
    });
    this.room.on(RoomEvent.ParticipantConnected, (p: RemoteParticipant) =>
      this.peerJoined?.(p.identity),
    );
    this.room.on(RoomEvent.ParticipantDisconnected, (p: RemoteParticipant) =>
      this.peerLeft?.(p.identity),
    );
    await this.room.connect(url, token);
  }

  async disconnect(): Promise<void> {
    await this.room.disconnect();
  }

  async publish(payload: Uint8Array, topic: string): Promise<void> {
    // livekit-client wants Uint8Array<ArrayBuffer>; copy to satisfy the type
    const buf = new Uint8Array(payload.byteLength);
    buf.set(payload);
    await this.room.localParticipant.publishData(buf, { reliable: true, topic });
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
