// 结构化诊断日志：批量写入 Rust 日志文件（按天分文件，保留 7 天）。
// 用户复现问题后，从设置页打开日志目录，把当天文件发回来分析。
// 注意：只记元数据（文件名/大小/状态/错误信息），不记文本内容与剪贴板内容。
import { invoke } from '@tauri-apps/api/core';

export type LogTag = 'app' | 'task' | 'p2p' | 'xfer' | 'err';

const queue: string[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;

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
    const line = `${timeStamp()} [${tag}] ${msg}${data ? ' ' + JSON.stringify(data) : ''}`;
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
