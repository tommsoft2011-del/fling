// 通用 UI helpers
export const $ = <T extends HTMLElement = HTMLElement>(sel: string, root: ParentNode = document) =>
  root.querySelector(sel) as T;
export const $$ = <T extends HTMLElement = HTMLElement>(sel: string, root: ParentNode = document) =>
  [...root.querySelectorAll(sel)] as T[];

export function el(tag: string, cls = '', text = ''): HTMLElement {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text) e.textContent = text;
  return e;
}

export const esc = (s: string) =>
  s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));

export function fmtSize(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1048576) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1073741824) return `${(n / 1048576).toFixed(1)} MB`;
  return `${(n / 1073741824).toFixed(2)} GB`;
}

export function fmtSpeed(bps: number): string {
  return fmtSize(bps) + '/s';
}

export function fmtClock(ts: number): string {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

export function fmtDur(sec: number): string {
  if (sec < 60) return `${Math.round(sec)}s`;
  const m = Math.floor(sec / 60);
  return `${m}分${Math.round(sec % 60)}秒`;
}

export function toast(msg: string, isErr = false, ms = 3500): void {
  const host = $('#toasts');
  const t = el('div', 'toast' + (isErr ? ' err' : ''), msg);
  host.appendChild(t);
  while (host.children.length > 4) host.firstChild?.remove();
  setTimeout(() => {
    t.style.transition = 'opacity .3s';
    t.style.opacity = '0';
    setTimeout(() => t.remove(), 320);
  }, ms);
}

/** base64 编码 Uint8Array（分块，避免大数组爆栈） */
export function b64encode(bytes: Uint8Array): string {
  let s = '';
  const CH = 0x8000;
  for (let i = 0; i < bytes.length; i += CH) {
    s += String.fromCharCode(...bytes.subarray(i, i + CH));
  }
  return btoa(s);
}

export function b64decode(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export function utf8bytes(s: string): number {
  return new TextEncoder().encode(s).length;
}

/** 文件名清理：去路径分隔符与非法字符 */
export function sanitizeName(name: string): string {
  const base = name.split(/[\\/]/).pop() ?? name;
  return base.replace(/[<>:"|?*\x00-\x1f]/g, '_').trim() || 'file';
}

let uidCounter = 0;
export function uid(prefix = 'id'): string {
  return `${prefix}-${Date.now().toString(36)}-${(uidCounter++).toString(36)}`;
}
