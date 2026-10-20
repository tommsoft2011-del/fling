import { Channel, invoke } from '@tauri-apps/api/core';
export type UnlistenFn = () => void;
export interface Event<T> { event: string; id: number; payload: T }
/** Rust allowlists names; the default event listener IPC is explicitly denied. */
export async function listen<T>(event: string, handler: (event: Event<T>) => void): Promise<UnlistenFn> {
  const channel = new Channel<Event<T>>();
  let active = true;
  channel.onmessage = (message) => { if (active) handler(message); };
  const id = await invoke<number>('ui_listen', { event, channel });
  return () => { active = false; void invoke('ui_unlisten', { id }).catch(() => {}); };
}
