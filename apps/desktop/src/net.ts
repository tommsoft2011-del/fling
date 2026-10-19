// LiveKit 连接 helpers：token 签发走 Rust（secret 不进 webview）
import { invoke } from '@tauri-apps/api/core';
import { log } from './log.js';
import {
  deriveRoomKey,
  inboxRoomName,
  lobbyNameForCode,
  newTaskRoomName,
  newTaskKey,
  normalizeCode,
  type FlingRoomEvents,
} from '@fling/core';
import type { RustFlingRoom } from './online-rust.js';

export { inboxRoomName, lobbyNameForCode, newTaskRoomName, newTaskKey, normalizeCode };

let serverUrl = '';
let deviceId = '';
let deviceName = '';

export async function initNet(): Promise<void> {
  serverUrl = (await invoke('server_url')) as string;
  const info = (await invoke('get_device_info')) as { device_id: string; device_name: string };
  deviceId = info.device_id;
  deviceName = info.device_name;
  // 配置文件里有 TURN 就追加到 ICE 列表（STUN 之后）
  try {
    const cfg = (await invoke('get_config')) as {
      turn_url?: string; turn_username?: string; turn_password?: string;
    } | null;
    if (cfg?.turn_url) {
      const { setCustomIceServers, DEFAULT_ICE_SERVERS } = await import('./p2p.js');
      setCustomIceServers([
        ...DEFAULT_ICE_SERVERS,
        {
          urls: cfg.turn_url,
          username: cfg.turn_username ?? '',
          credential: cfg.turn_password ?? '',
        },
      ]);
      // 诊断：只记 TURN 主机（不记用户名/密码），确认配置真的进了 ICE 列表
      const host = /^(?:turns?:)?([^/:;]+)/.exec(cfg.turn_url)?.[1] ?? '?';
      log('p2p', 'ice servers: STUN x2 + TURN', { turnHost: host });
    } else {
      log('p2p', 'ice servers: STUN x2 only (no TURN configured)', {});
    }
  } catch {
    /* 无 TURN 配置就只用 STUN */
    log('p2p', 'ice servers: STUN x2 only (get_config failed)', {});
  }
}

export function getDeviceId(): string {
  return deviceId;
}
export function getDeviceName(): string {
  return deviceName;
}
export async function renameDevice(name: string): Promise<void> {
  await invoke('set_device_name', { name });
  deviceName = name;
}

/** 每个连接用独立 identity，避免自己挤掉自己 */
function freshIdentity(): string {
  return 'fling-' + deviceId.slice(0, 8) + '-' + Math.random().toString(16).slice(2, 8);
}

export async function mintToken(room: string): Promise<string> {
  return (await invoke('mint_token', { room, identity: freshIdentity() })) as string;
}

/** 密钥上下文：lobby / task / inbox 各用各的密钥 */
export const KEY_CTX = {
  lobby: 'fling-v2-lobby',
  room: 'fling-v2-room', // 远程任务（配对码派生）
  task: 'fling-v2-task', // 近场任务（offer 里带的随机密钥派生）
  inbox: 'fling-v2-inbox',
} as const;

export interface RoomSpec {
  roomName: string;
  keyMaterial: string;
  keyContext: string;
}

export function inboxSpec(): RoomSpec {
  return {
    roomName: inboxRoomName(deviceId),
    keyMaterial: 'inbox:' + deviceId,
    keyContext: KEY_CTX.inbox,
  };
}

export async function lobbySpec(code: string): Promise<RoomSpec> {
  const c = normalizeCode(code);
  return { roomName: await lobbyNameForCode(c), keyMaterial: c, keyContext: KEY_CTX.lobby };
}

export function remoteTaskSpec(code: string): RoomSpec {
  return { roomName: newTaskRoomName(), keyMaterial: normalizeCode(code), keyContext: KEY_CTX.room };
}

/** 接收方：加入发送方已建好的任务房间 */
export function remoteTaskSpecWithRoom(taskRoom: string, code: string): RoomSpec {
  return { roomName: taskRoom, keyMaterial: normalizeCode(code), keyContext: KEY_CTX.room };
}

export function nearbyTaskSpec(taskKeyB64: string): RoomSpec {
  return { roomName: newTaskRoomName(), keyMaterial: taskKeyB64, keyContext: KEY_CTX.task };
}

/** 注意：近场任务房间名由发送方生成后经 offer 告知接收方 */
export function nearbyTaskSpecWithRoom(taskRoom: string, taskKeyB64: string): RoomSpec {
  return { roomName: taskRoom, keyMaterial: taskKeyB64, keyContext: KEY_CTX.task };
}

export async function connectFlingRoom(spec: RoomSpec, events: FlingRoomEvents): Promise<RustFlingRoom> {
  // WebRTC is deprecated in the WebView. Signaling and the data plane both
  // live in Rust (online_session.rs); this returns a Rust-backed room.
  const { connectRustRoom } = await import('./online-rust.js');
  return connectRustRoom(spec, events, serverUrl);
}

/** 供调试/测试：密钥派生是否一致 */
export async function deriveKeyFor(spec: RoomSpec): Promise<CryptoKey> {
  return deriveRoomKey(spec.keyMaterial, spec.keyContext);
}
