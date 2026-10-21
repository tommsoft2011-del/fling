// Transfer history is backed by the Tauri JSONL archive; localStorage is only a small fast cache.
import { invoke } from '@tauri-apps/api/core';
import type { ItemKind } from './tasks.js';

export interface HistoryItem {
  name: string;
  path?: string;
  size: number;
  kind: ItemKind;
  status: 'ok' | 'fail';
  error?: string;
  /** Text content (available for copy/re-send). */
  text?: string;
  /** Small clipboard image preview (base64). */
  imageB64?: string;
  /** Actual receive destination, preserved in the on-disk archive. */
  savedPath?: string;
  /** Whole-file SHA-256. */
  sha256?: string;
  /** Browser-provided MIME type when available. */
  mimeType?: string;
  /** Observed payload route for this item. */
  transport?: 'p2p' | 'relay' | 'mixed' | 'unknown';
}

export interface HistoryEntry {
  taskId: string;
  dir: 'send' | 'recv';
  peer: string;
  time: number;
  endedAt?: number;
  items: HistoryItem[];
  totalBytes: number;
  transport?: 'p2p' | 'relay' | 'mixed' | 'unknown';
}

const HIST_KEY = 'fling-history-v4';
const LEGACY_HIST_KEYS = ['fling-history-v3'];
const LOCAL_CACHE_LIMIT = 300;
/** Clipboard images above this size are not stored in the history/cache. */
export const HIST_IMAGE_LIMIT = 1024 * 1024;

let diskHistory: HistoryEntry[] | null = null;
let writeChain: Promise<void> = Promise.resolve();

function readLocalHistory(): HistoryEntry[] {
  const all: HistoryEntry[] = [];
  for (const key of [HIST_KEY, ...LEGACY_HIST_KEYS]) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      const arr = JSON.parse(raw) as unknown;
      if (Array.isArray(arr)) all.push(...arr.filter(isHistoryEntry));
    } catch {
      // Ignore invalid/corrupt browser cache; the JSONL archive is authoritative.
    }
  }
  return mergeHistory(all);
}

function isHistoryEntry(value: unknown): value is HistoryEntry {
  if (!value || typeof value !== 'object') return false;
  const e = value as Partial<HistoryEntry>;
  return typeof e.taskId === 'string' && (e.dir === 'send' || e.dir === 'recv') &&
    typeof e.peer === 'string' && Number.isFinite(e.time) && Array.isArray(e.items) &&
    Number.isFinite(e.totalBytes);
}

function keyOf(entry: HistoryEntry): string {
  return `${entry.dir}\u0000${entry.taskId}`;
}

function mergeHistory(...groups: HistoryEntry[][]): HistoryEntry[] {
  const merged = new Map<string, HistoryEntry>();
  for (const group of groups) {
    for (const entry of group) {
      const key = keyOf(entry);
      const previous = merged.get(key);
      if (!previous) {
        merged.set(key, entry);
        continue;
      }
      // Cache and JSONL copies retain the same item order. Merge by position
      // rather than by name/size so duplicate same-name files remain distinct.
      const items = entry.items.map((item, index) => {
        const old = previous.items[index];
        const same = old && old.kind === item.kind && old.name === item.name &&
          old.size === item.size && old.path === item.path;
        return same ? { ...old, ...item } : item;
      });
      if (previous.items.length > items.length) items.push(...previous.items.slice(items.length));
      merged.set(key, {
        ...previous,
        ...entry,
        items,
      });
    }
  }
  return [...merged.values()].sort((a, b) => (b.endedAt ?? b.time) - (a.endedAt ?? a.time));
}

function cacheHistory(entries: HistoryEntry[]): void {
  try {
    localStorage.setItem(HIST_KEY, JSON.stringify(entries.slice(0, LOCAL_CACHE_LIMIT)));
  } catch (e) {
    console.error('fling: history cache write failed:', e);
  }
}

function persistLine(line: string): Promise<void> {
  const next = writeChain.then(async () => {
    await invoke('append_history', { line });
  });
  writeChain = next.then(() => {}, (e) => {
    console.error('fling: disk history write failed:', e);
  });
  return next;
}

/** Load the complete JSONL archive and migrate cache-only records on first run. */
export async function hydrateHistory(): Promise<void> {
  const local = readLocalHistory();
  const rawLines = await invoke<string[]>('read_history');
  const disk: HistoryEntry[] = [];
  for (const line of rawLines) {
    try {
      const parsed: unknown = JSON.parse(line);
      if (isHistoryEntry(parsed)) disk.push(parsed);
    } catch {
      console.error('fling: skipping malformed history line');
    }
  }
  const diskKeys = new Set(disk.map(keyOf));
  for (const entry of local) {
    if (!diskKeys.has(keyOf(entry))) {
      await persistLine(JSON.stringify(entry));
      disk.push(entry);
      diskKeys.add(keyOf(entry));
    }
  }
  diskHistory = mergeHistory(local, disk);
  cacheHistory(diskHistory);
}

export function loadHistory(): HistoryEntry[] {
  const local = readLocalHistory();
  return diskHistory ? mergeHistory(local, diskHistory) : local;
}

export async function saveHistoryEntry(entry: HistoryEntry): Promise<void> {
  const all = mergeHistory([entry], loadHistory());
  if (diskHistory) diskHistory = mergeHistory([entry], diskHistory);
  cacheHistory(all);
  try {
    await persistLine(JSON.stringify(entry));
  } catch (e) {
    console.error('fling: durable history append failed:', e);
  }
}

// ---------- Simple key/value settings ----------
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
  /** Nearby discovery / mDNS broadcast switch. */
  get discoverable(): boolean {
    return getStr('fling-discoverable', '1') === '1';
  },
  set discoverable(v: boolean) {
    setStr('fling-discoverable', v ? '1' : '0');
  },
  /** Show system notifications when transfers finish or fail. */
  get notifyOnDone(): boolean {
    return getStr('fling-notify', '1') === '1';
  },
  set notifyOnDone(v: boolean) {
    setStr('fling-notify', v ? '1' : '0');
  },
};

export function applyTheme(): void {
  const mode = settings.theme;
  const dark = mode === 'dark' ||
    (mode === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches);
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
}

if (window.matchMedia) {
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
    if (settings.theme === 'system') applyTheme();
  });
}
