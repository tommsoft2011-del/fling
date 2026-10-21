// Structured diagnostics are buffered into the daily Rust log file (7-day retention).
// Diagnostic payloads are metadata-only; sanitize again at the single log boundary.
import { invoke } from '@tauri-apps/api/core';

export type LogTag = 'app' | 'task' | 'p2p' | 'xfer' | 'net' | 'ui' | 'err';

const queue: string[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;

// Never serialize identifying, path-like, address, credential, content, or hash fields.
const PRIVATE_FIELD = /^(?:id|taskid|sessionid|session|transferid|sourceid|roomid|roomname|identity|username|credential|credentials|errMsg|reason|name|filename|file_name|displayname|peer|peerlabel|peername|device|devicename|deviceid|path|filepath|relpath|savedpath|directory|dir|folder|url|urls|host|hostname|address|ip|port|candidate|sdp|token|secret|password|apikey|apisecret|key|taskkey|content|text|payload|data|datab64|sha256|hash|checksum|error|msg|message)$/i;

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

function sanitizeText(text: string): string {
  return text
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/[^\s"'<>)]*/gi, '[redacted-endpoint]')
    .replace(/\b(?:stun|stuns|turn|turns):[^\s"'<>)]*/gi, '[redacted-endpoint]')
    .replace(/\b(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?\b/g, '[redacted-address]')
    .replace(/\[[\da-f:]+\](?::\d{1,5})?/gi, '[redacted-address]')
    .replace(/\bport\s*[=:]\s*\d{1,5}\b/gi, 'port=[redacted]')
    .replace(/(^|[\s"'(=])(?:[A-Za-z]:\\|\\\\)[^\s"'<>)]*/g, '$1[redacted-path]')
    .replace(/(^|[\s"'(=])\/(?:[^\s\t\r\n"'<>)]*)/g, '$1[redacted-path]')
    .slice(0, 500);
}

function sanitizeValue(value: unknown, depth = 0): unknown {
  if (depth > 6) return '[truncated]';
  if (typeof value === 'string') return sanitizeText(value);
  if (typeof value === 'number' || typeof value === 'boolean' || value === null) return value;
  if (Array.isArray(value)) return value.slice(0, 40).map((item) => sanitizeValue(item, depth + 1));
  if (typeof value === 'object') {
    const safe: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      if (PRIVATE_FIELD.test(key.replace(/[-_]/g, ''))) continue;
      safe[key] = sanitizeValue(item, depth + 1);
    }
    return safe;
  }
  return String(value);
}

export function log(tag: LogTag, msg: string, data?: Record<string, unknown>): void {
  try {
    const safeData = data ? sanitizeValue(data) : undefined;
    const line = `${timeStamp()} [${tag}] ${sanitizeText(msg)}${safeData ? ' ' + JSON.stringify(safeData) : ''}`;
    queue.push(line);
    // Also write to the console for devtools, using exactly the same sanitized line.
    if (tag === 'err') console.error(line);
    if (queue.length >= 50) void flushLogs();
    else if (!flushTimer) flushTimer = setTimeout(() => void flushLogs(), 2000);
  } catch {
    // Logging must never interfere with transfer behavior.
  }
}

/** Flush buffered lines to the daily log file. */
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
    // Ignore; later batches retry (this batch is dropped to cap memory use).
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
