// Fling protocol v2 loopback test.
// Two FlingRooms talk through an in-memory mock transport — no LiveKit,
// no network (the VM sandbox blocks UDP).
//
// Covers:
//   - offer -> received with all fields intact
//   - clipboard-image manifest kind consistent on both ends
//   - file-manifest + chunks reassemble with sha256 verification (kind=clipboard-image)
//   - offer-withdraw and task-done delivery
//   - pairing helpers: lobbyNameForCode / inboxRoomName / newTaskRoomName / newTaskKey

import { FlingRoom, type FlingRoomEvents } from '../src/room.js';
import {
  isFlingMessage,
  newTaskKey,
  randomId,
  type FileManifest,
  type OfferMessage,
} from '../src/protocol.js';
import { type FileLike } from '../src/files.js';
import { inboxRoomName, lobbyNameForCode, newTaskRoomName } from '../src/pairing.js';
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

// ---------------------------------------------------------------------------
// In-memory transport: a Hub routes published payloads to every peer except
// the publisher (LiveKit data channels never echo back to the sender).
// ---------------------------------------------------------------------------

class Hub {
  private peers = new Map<string, MockTransport>();

  add(t: MockTransport): void {
    for (const other of this.peers.values()) {
      other.peerJoined(t.localIdentity);
      t.peerJoined(other.localIdentity);
    }
    this.peers.set(t.localIdentity, t);
  }

  remove(t: MockTransport): void {
    this.peers.delete(t.localIdentity);
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
  private joinedHandlers: Array<(id: string) => void> = [];
  private leftHandlers: Array<(id: string) => void> = [];

  constructor(
    private hub: Hub,
    readonly localIdentity: string,
  ) {}

  async connect(_url: string, _token: string): Promise<void> {
    this.isConnected = true;
    this.hub.add(this);
  }

  async disconnect(): Promise<void> {
    this.isConnected = false;
    this.hub.remove(this);
  }

  async publish(payload: Uint8Array, topic: string): Promise<void> {
    const bytes = payload.slice(); // copy, like a real channel would
    await Promise.resolve();
    this.hub.publish(this, bytes, topic);
  }

  onData(handler: DataHandler): void {
    this.dataHandler = handler;
  }

  onPeerJoined(handler: (identity: string) => void): void {
    this.joinedHandlers.push(handler);
  }

  onPeerLeft(handler: (identity: string) => void): void {
    this.leftHandlers.push(handler);
  }

  deliver(payload: Uint8Array, from: string, topic: string): void {
    this.dataHandler?.(payload, from, topic);
  }

  peerJoined(id: string): void {
    for (const h of this.joinedHandlers) h(id);
  }
}

class MockDirectChannel implements DirectChannel {
  readonly maxMessageSize = 1024 * 1024;
  bufferedAmount = 0;
  readyState = 'open';
  onmessage: ((data: Uint8Array<ArrayBuffer>) => void) | null = null;
  onclose: (() => void) | null = null;
  peer: MockDirectChannel | null = null;

  constructor(readonly label: string) {}

  send(data: Uint8Array<ArrayBuffer>): void {
    if (this.readyState !== 'open') throw new Error('mock direct channel is closed');
    this.peer?.onmessage?.(new Uint8Array(data) as Uint8Array<ArrayBuffer>);
  }

  close(): void {
    this.readyState = 'closed';
    this.onclose?.();
  }

  async waitForDrain(_threshold: number): Promise<void> {}
}

// ---------------------------------------------------------------------------
// Event collector.
// ---------------------------------------------------------------------------

interface Collected {
  offers: Array<{ offer: OfferMessage; from: string }>;
  withdraws: Array<{ taskId: string; from: string }>;
  dones: Array<{ taskId: string; from: string }>;
  failures: Array<{ taskId: string; failures: { name: string; size: number; error: string }[]; from: string }>;
  manifests: Array<{ manifest: FileManifest; from: string }>;
  completed: Array<{ id: string; manifest: FileManifest; bytes: Uint8Array }>;
}

function makeEvents(c: Collected): FlingRoomEvents {
  return {
    onText: () => {},
    onPeerJoined: () => {},
    onPeerLeft: () => {},
    onFileManifest: (manifest, from) => c.manifests.push({ manifest, from }),
    onFileProgress: () => {},
    onFileComplete: (id, manifest, bytes) => { c.completed.push({ id, manifest, bytes }); },
    onFileError: (id, err) => {
      failures++;
      console.error(`  FAIL - file error ${id}: ${err}`);
    },
    onOffer: (offer, from) => c.offers.push({ offer, from }),
    onOfferWithdraw: (taskId, from) => c.withdraws.push({ taskId, from }),
    onTaskDone: (taskId, from) => c.dones.push({ taskId, from }),
    onTaskFailed: (taskId, failures, from) => c.failures.push({ taskId, failures, from }),
  };
}

function newCollected(): Collected {
  return { offers: [], withdraws: [], dones: [], failures: [], manifests: [], completed: [] };
}

function memFile(name: string, bytes: Uint8Array): FileLike {
  return {
    name,
    size: bytes.length,
    slice: async (s, e) => bytes.slice(s, e),
  };
}

// ---------------------------------------------------------------------------
// Main.
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log('== task-progress compatibility ==');
  check(isFlingMessage({ kind: 'task-progress', taskId: 'old', doneBytes: 10, totalBytes: 20 }), 'legacy doneBytes task-progress remains valid');
  check(isFlingMessage({ kind: 'task-progress', taskId: 'new', doneBytes: 15, receivedBytes: 15, savedBytes: 8, totalBytes: 20 }), 'new receivedBytes/savedBytes progress is valid');
  check(!isFlingMessage({ kind: 'task-progress', taskId: 'bad', doneBytes: 5, receivedBytes: 5, savedBytes: 6, totalBytes: 20 }), 'rejects saved progress beyond received progress');

  console.log('== pairing helpers ==');
  const lobby = await lobbyNameForCode('K7Q2M9');
  check(lobby.startsWith('fling-lobby-') && lobby.length === 'fling-lobby-'.length + 16, `lobbyNameForCode format (${lobby})`);
  check((await lobbyNameForCode('k7q2m9')) === lobby, 'lobbyNameForCode is case/space-insensitive');
  check(inboxRoomName('Alice-PC_01') === 'fling-inbox-alicepc01', 'inboxRoomName sanitizes to [a-z0-9]');
  let threw = false;
  try {
    inboxRoomName('---');
  } catch {
    threw = true;
  }
  check(threw, 'inboxRoomName rejects empty-after-sanitize deviceId');
  const taskRoom = newTaskRoomName();
  check(taskRoom.startsWith('fling-task-') && taskRoom.length === 'fling-task-'.length + 24, `newTaskRoomName format (${taskRoom})`);
  check(newTaskRoomName() !== newTaskRoomName(), 'newTaskRoomName is random');
  const taskKey = newTaskKey();
  check(taskKey.length === 44 && taskKey.endsWith('='), 'newTaskKey is base64 of 32 bytes');

  console.log('== loopback: offer / withdraw / task-done ==');
  const hub = new Hub();
  const CODE = 'K7Q2M9';
  const roomA = new FlingRoom(new MockTransport(hub, 'alice'), 'AlicePC');
  const roomB = new FlingRoom(new MockTransport(hub, 'bob'), 'BobPhone');
  const colA = newCollected();
  const colB = newCollected();
  let mappedProgress: { receivedBytes: number; savedBytes?: number } | undefined;
  const eventsA = makeEvents(colA);
  eventsA.onTaskProgress = (_taskId, receivedBytes, _totalBytes, _from, savedBytes) => {
    mappedProgress = { receivedBytes, savedBytes };
  };
  await roomA.connect('mock://local', 'token-a', CODE, eventsA);
  await roomB.connect('mock://local', 'token-b', CODE, makeEvents(colB));
  await roomA.flush();
  await roomB.flush();

  await roomB.sendTaskProgress('progress-mapping', 15, 20, 8);
  await roomA.flush();
  check(mappedProgress?.receivedBytes === 15 && mappedProgress.savedBytes === 8, 'task-progress dispatch keeps receive and saved counters distinct');

  const offer: OfferMessage = {
    kind: 'offer',
    taskId: randomId(12),
    fromDevice: 'AlicePC',
    fromDeviceId: 'alicepc01',
    taskRoom: newTaskRoomName(),
    taskKey: newTaskKey(),
    items: [
      { name: 'note.txt', size: 11, kind: 'text' },
      { name: 'paste.png', size: 1024, kind: 'clipboard-image' },
      { name: 'doc.pdf', size: 2048, kind: 'file' },
    ],
  };
  await roomA.sendOffer(offer);
  await roomB.flush();

  check(colB.offers.length === 1, 'offer received exactly once');
  const got = colB.offers[0]?.offer;
  check(got?.taskId === offer.taskId, 'offer.taskId intact');
  check(got?.fromDevice === 'AlicePC' && got?.fromDeviceId === 'alicepc01', 'offer.fromDevice/fromDeviceId intact');
  check(got?.taskRoom === offer.taskRoom, 'offer.taskRoom intact');
  check(got?.taskKey === offer.taskKey && got.taskKey.length === 44, 'offer.taskKey intact');
  check(
    got?.items.length === 3 &&
      got.items[0].kind === 'text' &&
      got.items[1].kind === 'clipboard-image' &&
      got.items[2].kind === 'file',
    'offer.items kinds preserved (text/clipboard-image/file)',
  );
  check(colB.offers[0]?.from === 'alice', 'offer from identity');
  check(colA.offers.length === 0, 'sender does not receive its own offer');

  await roomA.sendOfferWithdraw(offer.taskId);
  await roomB.flush();
  check(colB.withdraws.length === 1 && colB.withdraws[0]?.taskId === offer.taskId, 'offer-withdraw received with taskId');

  await roomA.sendTaskDone(offer.taskId);
  await roomB.flush();
  check(colB.dones.length === 1 && colB.dones[0]?.taskId === offer.taskId, 'task-done received with taskId');

  await roomB.sendTaskFailed(offer.taskId, [{ name: 'disk-full.bin', size: 99, error: '磁盘空间不足' }]);
  await roomA.flush();
  check(
    colA.failures.length === 1 && colA.failures[0]?.failures[0]?.error === '磁盘空间不足',
    'task-failed preserves receiver save error for sender',
  );

  console.log('== loopback: clipboard-image file transfer ==');
  const bytes = new Uint8Array(100_000);
  crypto.getRandomValues(bytes);
  const file = memFile('paste.png', bytes);
  const manifest = await roomA.sendFile(file, undefined, 'clipboard-image');
  await roomB.flush();

  check(manifest.kind === 'clipboard-image', 'sender manifest kind is clipboard-image');
  check(colB.manifests.length === 1, 'manifest received');
  const rm = colB.manifests[0]?.manifest;
  check(rm?.kind === 'clipboard-image', 'receiver manifest kind is clipboard-image');
  check(rm?.id === manifest.id, 'manifest id matches');
  check(rm?.sha256 === manifest.sha256, 'manifest sha256 matches');
  check(rm?.totalChunks === manifest.totalChunks && manifest.totalChunks > 1, `multi-chunk transfer (${manifest.totalChunks} chunks)`);
  check(colB.completed.length === 1, 'file completed');
  const done = colB.completed[0];
  check(done?.manifest.kind === 'clipboard-image', 'completed manifest kind is clipboard-image');
  let equal = done?.bytes.length === bytes.length;
  if (done) {
    for (let i = 0; i < bytes.length && equal; i++) {
      if (done.bytes[i] !== bytes[i]) equal = false;
    }
  }
  check(equal, 'reassembled bytes identical (100KB random, sha256 verified by assemble)');

  console.log('== loopback: plain file still kind=file ==');
  const bytes2 = new Uint8Array(1000);
  crypto.getRandomValues(bytes2);
  const m2 = await roomA.sendFile(memFile('doc.pdf', bytes2));
  await roomB.flush();
  check(m2.kind === 'file', 'default kind is file');
  check(colB.manifests[colB.manifests.length - 1]?.manifest.kind === 'file', 'receiver sees kind=file');

  console.log('== loopback: 122,891,016-byte multi-file finalize ACK regression ==');
  const delayedHub = new Hub();
  const senderRoom = new FlingRoom(new MockTransport(delayedHub, 'sender-large'), 'Sender');
  const receiverRoom = new FlingRoom(new MockTransport(delayedHub, 'receiver-large'), 'Receiver');
  const senderCollected = newCollected();
  const receiverCollected = newCollected();
  let finalizingNotices = 0;
  const senderEvents = makeEvents(senderCollected);
  senderEvents.onFileFinalizing = () => { finalizingNotices++; };
  const receiverEvents = makeEvents(receiverCollected);
  receiverEvents.onFileError = () => {};
  let signalSaveStarted!: () => void;
  const saveStarted = new Promise<void>((resolve) => { signalSaveStarted = resolve; });
  let releaseSave!: () => void;
  const saveGate = new Promise<void>((resolve) => { releaseSave = resolve; });
  let firstFilePersisted = false;
  const completionOrder: string[] = [];
  receiverEvents.onFileCompleteStream = async (id, manifest, receiver) => {
    if (manifest.name === 'disk-full.bin') throw new Error('simulated disk failure');
    let gateUsed = false;
    await receiver.streamTo(async () => {
      if (manifest.name === 'large.bin' && !gateUsed) {
        gateUsed = true;
        signalSaveStarted();
        await saveGate;
      }
    });
    if (manifest.name === 'large.bin') {
      firstFilePersisted = true;
      completionOrder.push('persisted');
    }
    receiverCollected.completed.push({ id, manifest, bytes: new Uint8Array(0) });
  };
  await senderRoom.connect('mock://local', 'sender-token', 'FINALIZE1', senderEvents);
  await receiverRoom.connect('mock://local', 'receiver-token', 'FINALIZE1', receiverEvents);
  await senderRoom.flush();
  await receiverRoom.flush();
  const directSender = new MockDirectChannel('mock-sender');
  const directReceiver = new MockDirectChannel('mock-receiver');
  directSender.peer = directReceiver;
  directReceiver.peer = directSender;
  senderRoom.setDirectChannel(directSender);
  receiverRoom.setDirectChannel(directReceiver);
  const largeBytes = new Uint8Array(122_891_016);
  largeBytes.fill(0xa5);
  let firstSendReturned = false;
  let senderObservedSaved = false;
  const sequence = (async () => {
    await senderRoom.sendFile(memFile('large.bin', largeBytes), (_sent, _total, saved) => {
      if (saved) {
        senderObservedSaved = true;
        completionOrder.push('sender-final-ack');
      }
    });
    firstSendReturned = true;
    await senderRoom.sendFile(memFile('next.bin', new Uint8Array(8192)));
  })();
  await saveStarted;
  await new Promise((resolve) => setTimeout(resolve, 8_500));
  check(!firstSendReturned, 'sendFile remains pending during >8s receiver finalization');
  check(receiverCollected.manifests.length === 1, 'next manifest is not processed before final save ACK');
  check(finalizingNotices > 0, 'sender is explicitly notified that the receiver is finalizing');
  releaseSave();
  let sequenceError: unknown;
  await sequence.catch((e) => { sequenceError = e; });
  await receiverRoom.flush();
  check(!sequenceError, 'multi-file sequence completes after delayed persistence');
  check(firstFilePersisted && senderObservedSaved, 'final ACK follows persistence and reaches sender callback');
  check(completionOrder.indexOf('persisted') < completionOrder.indexOf('sender-final-ack'), 'sender final ACK is observed after persistence');
  check(receiverCollected.manifests.length === 2, 'second manifest arrives only after first final ACK');
  const failureStartedAt = Date.now();
  let saveFailureReported = false;
  try {
    await senderRoom.sendFile(memFile('disk-full.bin', new Uint8Array(4096)));
  } catch (e) {
    saveFailureReported = String(e).includes('receiver failed to finalize file');
  }
  check(saveFailureReported && Date.now() - failureStartedAt < 5_000, 'receiver save failure is ACKed promptly without full finalize timeout');
  await senderRoom.close();
  await receiverRoom.close();

  await roomA.close();
  await roomB.close();

  console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILURE(S)`);
  if (failures > 0) throw new Error(`${failures} check(s) failed`);
}

main().catch((e) => {
  console.error('FATAL', e);
  throw e;
});
import type { DirectChannel } from '../src/p2p.js';
