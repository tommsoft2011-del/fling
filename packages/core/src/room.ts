// FlingRoom: joins a LiveKit room derived from the pairing code,
// exchanges end-to-end encrypted messages over the data channel.
//
// Transport note: built on @livekit/rtc-node (Node/Bun). The web app (v3)
// will provide the same shape via livekit-client; the message/crypto/
// pairing logic above stays identical.

import { Room, RoomEvent } from '@livekit/rtc-node';
import { deriveRoomKey, openText, sealText } from './crypto.js';
import { FLING_TOPIC, isFlingMessage, randomId, type FlingMessage } from './protocol.js';

export interface FlingRoomEvents {
  onText(text: string, from: string): void;
  onPeerJoined(identity: string): void;
  onPeerLeft(identity: string): void;
}

const enc = new TextEncoder();

export class FlingRoom {
  private room = new Room();
  private roomKey!: CryptoKey;
  private deviceName: string;
  private events!: FlingRoomEvents;
  private helloTimer?: ReturnType<typeof setInterval>;

  constructor(deviceName: string) {
    this.deviceName = deviceName;
  }

  get isConnected(): boolean {
    return this.room.isConnected;
  }

  async connect(
    serverUrl: string,
    token: string,
    pairingCode: string,
    events: FlingRoomEvents,
  ): Promise<void> {
    this.events = events;
    this.roomKey = await deriveRoomKey(pairingCode);

    this.room.on(RoomEvent.DataReceived, (payload, participant, _kind, topic) => {
      if (topic !== FLING_TOPIC) return;
      void this.handlePayload(payload, participant?.identity ?? 'unknown');
    });
    this.room.on(RoomEvent.ParticipantConnected, (p) => {
      events.onPeerJoined(p.identity);
      // Re-announce so a late joiner learns we are here.
      void this.send({ kind: 'hello', device: this.deviceName }).catch(() => {});
    });
    this.room.on(RoomEvent.ParticipantDisconnected, (p) => {
      events.onPeerLeft(p.identity);
    });

    await this.room.connect(serverUrl, token);
    await this.send({ kind: 'hello', device: this.deviceName });

    // Periodic re-announce; cheap and heals missed hellos.
    this.helloTimer = setInterval(() => {
      void this.send({ kind: 'hello', device: this.deviceName }).catch(() => {});
    }, 15_000);
  }

  async sendText(text: string): Promise<void> {
    await this.send({ kind: 'text', id: randomId(), text });
  }

  private async send(msg: FlingMessage): Promise<void> {
    const sealed = await sealText(this.roomKey, JSON.stringify(msg));
    await this.room.localParticipant.publishData(enc.encode(sealed), {
      reliable: true,
      topic: FLING_TOPIC,
    });
  }

  private async handlePayload(payload: Uint8Array, from: string): Promise<void> {
    let plain: string;
    try {
      plain = await openText(this.roomKey, new TextDecoder().decode(payload));
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
    if (msg.kind === 'text') this.events.onText(msg.text, from);
    else if (msg.kind === 'hello') this.events.onPeerJoined(from);
    else if (msg.kind === 'bye') this.events.onPeerLeft(from);
  }

  async close(): Promise<void> {
    if (this.helloTimer) clearInterval(this.helloTimer);
    try {
      if (this.room.isConnected) {
        await this.send({ kind: 'bye', device: this.deviceName });
      }
    } catch {
      /* best effort */
    }
    await this.room.disconnect();
  }
}
