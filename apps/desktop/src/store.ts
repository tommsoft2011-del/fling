// 历史记录 + 本地设置持久化（localStorage）
import { invoke } from '@tauri-apps/api/core';
import type { ItemKind } from './tasks.js';

export interface HistoryItem {
  name: string;
  size: number;
  kind: ItemKind;
  status: 'ok' | 'fail';
  error?: string;
  /** 文本内容（文本条目常驻，用于"复制/重发"） */
  text?: string;
  /** 剪贴板图片 base64（<1MB 才存，用于"复制到剪贴板"） */
  imageB64?: string;
  /** 接收端落盘后的完整路径（硬盘历史记录用，分辨每次的文件名） */
  savedPath?: string;
  /** 文件 sha256（发送端计算、接收端校验一致，用于完整性核对） */
  sha256?: string;
}

export interface HistoryEntry {
  taskId: string;
  dir: 'send' | 'recv';
  peer: string;
  time: number;
  items: HistoryItem[];
  totalBytes: number;
}

const HIST_KEY = 'fling-history-v3';
const MAX_ENTRIES = 300;
/** 剪贴板图片超过此大小就不进历史（localStorage 配额） */
export const HIST_IMAGE_LIMIT = 1024 * 1024;

export function loadHistory(): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(HIST_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw) as HistoryEntry[];
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export function saveHistoryEntry(entry: HistoryEntry): void {
  try {
    const list = loadHistory();
    list.unshift(entry);
    while (list.length > MAX_ENTRIES) list.pop();
    localStorage.setItem(HIST_KEY, JSON.stringify(list));
  } catch {
    /* 配额满了就丢弃本次写入，不阻塞主流程 */
  }
  // 硬盘完整存档：JSONL，每行一条，含保存路径等 localStorage 放不下的信息
  void invoke('append_history', { line: JSON.stringify(entry) }).catch(() => {});
}

// ---------- 简单键值设置 ----------
function getStr(key: string, def: string): string {
  return localStorage.getItem(key) ?? def;
}
function setStr(key: string, val: string): void {
  localStorage.setItem(key, val);
}

export type ThemeMode = 'light' | 'dark' | 'system';

export const settings = {
  get theme(): ThemeMode {
    const v = getStr('fling-theme', 'system');
    return v === 'light' || v === 'dark' || v === 'system' ? v : 'system';
  },
  set theme(v: ThemeMode) {
    setStr('fling-theme', v);
    applyTheme();
  },
  /** 允许附近设备发现我（mDNS 广播开关） */
  get discoverable(): boolean {
    return getStr('fling-discoverable', '1') === '1';
  },
  set discoverable(v: boolean) {
    setStr('fling-discoverable', v ? '1' : '0');
  },
};

export function applyTheme(): void {
  const mode = settings.theme;
  const dark =
    mode === 'dark' ||
    (mode === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches);
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
}

if (window.matchMedia) {
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    if (settings.theme === 'system') applyTheme();
  });
}
