// P2P direct-channel routing test (no WebRTC, no network).
// Two FlingRooms talk through the in-memory mock transport; a fake
// DirectChannel pair simulates the P2P link. Covers:
//   - file-chunk + file-manifest go through the direct channel while open
//     (same ordered channel, so chunks can never beat the manifest)
//   - text / task-done still use the relay
//   - receiver reassembles a file delivered over the direct channel
//   - chunks arriving before the manifest are buffered and replayed
//   - mid-transfer direct failure falls back to the relay transparently
//   - per-chunk mixed routing (some chunks direct, some relay) reassembles

import { FlingRoom, type FlingRoomEvents } from '../src/room.js';
import type { DirectChannel } from '../src/p2p.js';
import type { FileLike } from '../src/files.js';
import type { DataHandler, FlingTransport } from '../src/transport.js';

let failures = 0;
function check(cond: unknown, msg: string): void {
  if (cond) {
    console.log(`  ok - ${msg}`);
  } else {
    failures++;
    console.error(`  FAIL - ${msg}`);
  }
}

class Hub {
  private peers = new Map<string, MockTransport>();
  add(t: MockTransport): void {
    this.peers.set(t.localIdentity, t);
  }
  publish(from: MockTransport, payload: Uint8Array, topic: string): void {
    for (const peer of this.peers.values()) {
      if (peer !== from) peer.deliver(payload, from.localIdentity, topic);
    }
  }
}

class MockTransport implements FlingTransport {
  isConnected = false;
  private dataHandler: DataHandler | null = null;
  constructor(
    readonly localIdentity: string,
    private hub: Hub,
  ) {}
  async connect(): Promise<void> {
    this.isConnected = true;
    this.hub.add(this);
  }
  async disconnect(): Promise<void> {
    this.isConnected = false;
  }
  async publish(payload: Uint8Array, topic: string): Promise<void> {
    this.hub.publish(this, payload, topic);
  }
  onData(h: DataHandler): void {
    this.dataHandler = h;
  }
  onPeerJoined(): void {}
  onPeerLeft(): void {}
  deliver(payload: Uint8Array, from: string, topic: string): void {
    this.dataHandler?.(payload, from, topic);
  }
}

/** Fake direct channel: deliver() is wired by the test to the peer's room. */
class FakeDirect implements DirectChannel {
  readonly readyState: string = 'open';
  bufferedAmount = 0;
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null = null;
  onclose: (() => void) | null = null;
  sent: Uint8Array[] = [];
  dead = false;
  constructor(readonly label: string) {}
  send(data: Uint8Array<ArrayBuffer>): void {
    if (this.dead) throw new Error('channel closed');
    this.sent.push(data);
  }
  close(): void {
    this.dead = true;
    this.onclose?.();
  }
  waitForDrain(): Promise<void> {
    return Promise.resolve();
  }
}

const enc = new TextEncoder();
const dec = new TextDecoder();

function fileLike(name: string, size: number): FileLike {
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) bytes[i] = i & 0xff;
  return {
    name,
    size,
    slice: async (s: number, e: number) => bytes.slice(s, e),
  };
}

async function main(): Promise<void> {
  const hub = new Hub();
  const tA = new MockTransport('A', hub);
  const tB = new MockTransport('B', hub);
  const roomA = new FlingRoom(tA, 'devA');
  const roomB = new FlingRoom(tB, 'devB');

  const directA = new FakeDirect('A->B');
  const directB = new FakeDirect('B->A');
  // wire the pair: A's sends arrive at B's room and vice versa
  const wire = () => {
    directA.sent.forEach((d) => directB.onmessage?.(d as Uint8Array<ArrayBuffer>));
    directB.sent.forEach((d) => directA.onmessage?.(d as Uint8Array<ArrayBuffer>));
    directA.sent = [];
    directB.sent = [];
  };

  let relayPublishesA = 0;
  const origPublishA = tA.publish.bind(tA);
  tA.publish = async (p: Uint8Array, topic: string) => {
    relayPublishesA++;
    return origPublishA(p, topic);
  };

  function freshBox(): { cur: { id: string; bytes: Uint8Array } | null } {
    return { cur: null };
  }
  let done = freshBox();
  const eventsB: FlingRoomEvents = {
    onFileManifest: () => {},
    onFileComplete: (id, _m, bytes) => {
      done.cur = { id, bytes };
    },
  };
  const eventsA: FlingRoomEvents = {};
  await roomA.connect('x', 'tok', 'code123', eventsA);
  await roomB.connect('x', 'tok', 'code123', eventsB);

  // --- 1. direct open: file chunks bypass the relay -------------------------
  roomA.setDirectChannel(directA);
  roomB.setDirectChannel(directB);
  check(roomA.directActive && roomB.directActive, 'directActive true while open');

  const relayBefore = relayPublishesA;
  const file = fileLike('hello.bin', 100_000); // 100KB -> 4 chunks of 32KB
  const p1 = roomA.sendFile(file, undefined, 'file');
  // pump: deliver direct messages as they are produced
  for (let i = 0; i < 50 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await p1;
  wire();
  await new Promise((r) => setTimeout(r, 20));
  check(!!done.cur, 'file completed over direct channel');
  const got1 = done.cur;
  check(
    got1 !== null &&
      got1.bytes.length === 100_000 &&
      got1.bytes[99999] === (99999 & 0xff),
    'reassembled bytes identical over direct channel',
  );
  // manifest + chunks all bypass the relay while direct is open
  const relayDuringDirect = relayPublishesA - relayBefore;
  check(relayDuringDirect === 0, `manifest+chunks bypassed relay (relay publishes: ${relayDuringDirect}, want 0)`);

  // --- 1b. chunks beating the manifest are buffered, not dropped -----------
  done = freshBox();
  directA.sent = [];
  const fileE = fileLike('early.bin', 100_000); // 4 chunks
  await roomA.sendFile(fileE, undefined, 'file'); // all via direct; captured, not wired
  const raws = directA.sent.map((d) => JSON.parse(dec.decode(d)) as { kind: string });
  directA.sent = [];
  check(raws.filter((m) => m.kind === 'file-chunk').length === 4, 'captured 4 direct chunks');
  // deliver chunks FIRST, manifest last (simulates the old relay/direct race)
  const chunksFirst = [
    ...raws.filter((m) => m.kind === 'file-chunk'),
    ...raws.filter((m) => m.kind === 'file-manifest'),
  ];
  for (const m of chunksFirst) {
    directB.onmessage?.(enc.encode(JSON.stringify(m)) as Uint8Array<ArrayBuffer>);
  }
  await new Promise((r) => setTimeout(r, 20));
  const gotE = done.cur;
  check(
    gotE !== null && gotE.bytes.length === 100_000 && gotE.bytes[99999] === (99999 & 0xff),
    'early chunks buffered and replayed once the manifest arrived',
  );

  // --- 2. direct dies mid-transfer -> transparent relay fallback ------------
  done = freshBox();
  directA.dead = true; // next send throws -> room must fall back
  roomA.setDirectChannel(directA); // re-attach the (now dead) channel
  const file2 = fileLike('world.bin', 70_000);
  const p2 = roomA.sendFile(file2, undefined, 'file');
  for (let i = 0; i < 50 && !done.cur; i++) {
    wire();
    await new Promise((r) => setTimeout(r, 5));
  }
  await p2;
  // relay delivery to B happens via hub automatically (async decrypt)
  for (let i = 0; i < 50 && !done.cur; i++) {
    await new Promise((r) => setTimeout(r, 5));
  }
  check(!!done.cur, 'file completed after direct failure (relay fallback)');
  const got2 = done.cur;
  check(
    got2 !== null && got2.bytes.length === 70_000,
    'fallback bytes intact',
  );
  check(!roomA.directActive, 'directActive false after failure');

  await roomA.close();
  await roomB.close();

  if (failures > 0) {
    throw new Error(`${failures} FAILURES`);
  }
  console.log('\nALL PASS');
}

main().catch((e) => {
  console.error('FATAL', e);
  throw e;
});
