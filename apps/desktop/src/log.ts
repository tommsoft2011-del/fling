// 结构化诊断日志：批量写入 Rust 日志文件（按天分文件，保留 7 天）。
// 不写入文件名/路径、内容、设备名、地址、端口、SDP、凭据或原始异常文本。
import { invoke } from '@tauri-apps/api/core';

export type LogTag = 'app' | 'task' | 'p2p' | 'xfer' | 'net' | 'ui' | 'err';

const queue: string[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;
const SENSITIVE_FIELDS = /^(?:name|filename|fileName|path|filePath|relPath|url|host|turnHost|peer|device|deviceName|sdp|candidate|token|secret|credential|password|apiKey|apiSecret|text|content|error|msg|message)$/i;

function sanitizeText(value: string): string {
  return value
    .replace(/\b(?:https?|wss?|turns?|file):\/\/[^\s"'<>]+/gi, '[url-redacted]')
    .replace(/\b(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?\b/g, '[address-redacted]')
    .replace(/\b(?:[a-f0-9]{1,4}:){2,}[a-f0-9:]{1,}\b/gi, '[address-redacted]')
    .replace(/\b(?:[a-z0-9-]+\.)+[a-z]{2,}:\d{1,5}\b/gi, '[address-redacted]')
    .replace(/(?:[A-Za-z]:[\\/]|\\\\)[^\s"'<>]+/g, '[path-redacted]')
    .replace(/(^|[\s=])\/(?:[^/\s"'<>]+\/)+[^/\s"'<>]*/g, '$1[path-redacted]')
    .replace(/\b[^\s"'<>/\\]+\.(?:pdf|docx?|xlsx?|pptx?|rtf|txt|md|csv|json|png|jpe?g|gif|webp|bmp|svg|mp4|mkv|avi|mov|wmv|mp3|wav|flac|ogg|m4a|exe|msi|dll|so|dylib|zip|7z|tar|gz|bin|log)\b/gi, '[filename-redacted]');
}

function sanitizeValue(value: unknown, key = ''): unknown {
  if (SENSITIVE_FIELDS.test(key)) return '[redacted]';
  if (typeof value === 'string') return sanitizeText(value);
  if (Array.isArray(value)) return value.map((item) => sanitizeValue(item));
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([childKey, child]) => [childKey, sanitizeValue(child, childKey)]));
  }
  return value;
}

function dayStamp(d = new Date()): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

function timeStamp(d = new Date()): string {
  const p = (n: number, w = 2) => String(n).padStart(w, '0');
  const tz = -d.getTimezoneOffset();
  const sign = tz >= 0 ? '+' : '-';
  const abs = Math.abs(tz);
  return (
    `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}` +
    `T${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}` +
    `${sign}${p(Math.floor(abs / 60))}${p(abs % 60)}`
  );
}

export function log(tag: LogTag, msg: string, data?: Record<string, unknown>): void {
  try {
    const safeMsg = sanitizeText(msg);
    const safeData = data ? sanitizeValue(data) as Record<string, unknown> : undefined;
    const line = `${timeStamp()} [${tag}] ${safeMsg}${safeData ? ' ' + JSON.stringify(safeData) : ''}`;
    queue.push(line);
    // 顺手也打到控制台，方便 devtools 联调
    if (tag === 'err') console.error(line);
    if (queue.length >= 50) void flushLogs();
    else if (!flushTimer) flushTimer = setTimeout(() => void flushLogs(), 2000);
  } catch {
    /* 日志永不干扰主流程 */
  }
}

/** 把缓冲的行刷进日志文件。 */
export async function flushLogs(): Promise<void> {
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  if (queue.length === 0) return;
  const lines = queue.splice(0, queue.length);
  try {
    await invoke('append_log', { file: `fling-${dayStamp()}.log`, lines });
  } catch {
    /* 忽略，后续批次会重试（行已丢弃，避免无限堆积） */
  }
}

if (typeof window !== 'undefined') {
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') void flushLogs();
  });
  window.addEventListener('beforeunload', () => {
    void flushLogs();
  });
}
