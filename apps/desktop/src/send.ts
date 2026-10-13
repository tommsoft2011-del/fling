// 发送页：统一发送框 + 目标选择 + 进行中的发送
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { $, el, fmtSize, toast, uid, utf8bytes } from './ui.js';
import { manager, type ComposeItem, type SendTarget } from './tasks.js';
import { createTaskCard, type CardHandle } from './taskcard.js';
import { openPairDialog } from './pairdialog.js';

let clipItems: ComposeItem[] = []; // 输入框里的剪贴板图片（chip）
let fileItems: ComposeItem[] = []; // 下方文件列表里的文件
let selectedTarget: SendTarget | null = null;
const cards = new Map<string, CardHandle>();
let inited = false;

export function initSendPage(): void {
  if (inited) return;
  inited = true;

  const composeBox = $('#composeBox');
  const textarea = $('#composeText') as HTMLTextAreaElement;

  // --- Ctrl+V 粘贴：文本走 textarea 默认行为；剪贴板图片进输入框 chip；文件进下方文件列表 ---
  composeBox.addEventListener('paste', (e: ClipboardEvent) => {
    const cd = e.clipboardData;
    if (!cd) return;
    let handled = false;
    for (const item of Array.from(cd.items)) {
      if (item.kind !== 'file') continue;
      const f = item.getAsFile();
      if (!f) continue;
      if (item.type.startsWith('image/') && f.name === 'image.png') {
        // Chrome 给剪贴板位图默认起名 image.png；资源管理器复制的图片文件带真实文件名，走文件列表
        addClipItem(f);
      } else {
        addFileItem(f);
      }
      handled = true;
    }
    if (handled) e.preventDefault();
    refreshSendBtn();
  });

  // --- 拖拽文件到发送页任意位置 → 文件列表 ---
  const pageSend = $('#page-send');
  const fileCard = $('#fileListWrap');
  const dropOverlay = $('#dropOverlay');
  let dragDepth = 0;
  const hasFiles = (e: DragEvent) =>
    !!e.dataTransfer && Array.from(e.dataTransfer.types).includes('Files');
  const setDropHint = (on: boolean) => {
    fileCard.classList.toggle('dragover', on);
    dropOverlay.classList.toggle('show', on);
  };
  pageSend.addEventListener('dragenter', (e) => {
    if (!hasFiles(e as DragEvent)) return;
    dragDepth++;
    setDropHint(true);
  });
  pageSend.addEventListener('dragleave', () => {
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) setDropHint(false);
  });
  pageSend.addEventListener('dragover', (e) => e.preventDefault());
  pageSend.addEventListener('drop', (e) => {
    e.preventDefault();
    dragDepth = 0;
    setDropHint(false);
    const files = (e as DragEvent).dataTransfer?.files;
    if (files) {
      for (const f of Array.from(files)) addFileItem(f);
      refreshSendBtn();
    }
  });

  // --- Tauri 原生拖放事件：wry 会在 OS 层拦截文件拖放（此时 DOM drop 收不到文件），
  // 用 tauri://drag-drop 兜底，经 Rust 命令读文件内容（不受 fs scope 限制）---
  listen<unknown>('tauri://drag-drop', async (e) => {
    if (!$('#page-send').classList.contains('active')) return;
    // 兼容不同版本 payload：{ paths: string[] } 或直接 string[]
    const pl = e.payload as { paths?: string[] } | string[];
    const paths = Array.isArray(pl) ? pl : (pl.paths ?? []);
    if (paths.length === 0) return;
    let added = 0;
    for (const p of paths) {
      try {
        const b64 = (await invoke('read_file_bytes', { path: p })) as string;
        const bin = b64ToBytes(b64);
        const name = p.split(/[/\\]/).pop() || '文件';
        addFileItem(new File([bin as unknown as BlobPart], name));
        added++;
      } catch (err) {
        toast(`读取拖放文件失败：${err}`, true);
      }
    }
    if (added > 0) toast(`已添加 ${added} 个文件`);
    refreshSendBtn();
  }).catch(() => { /* 非 Tauri 环境忽略 */ });

  ($('#fileInput') as HTMLInputElement).addEventListener('change', (e) => {
    const files = (e.target as HTMLInputElement).files;
    if (files) {
      for (const f of Array.from(files)) addFileItem(f);
      (e.target as HTMLInputElement).value = '';
      refreshSendBtn();
    }
  });

  textarea.addEventListener('input', refreshSendBtn);

  // --- 远程配对发送 ---
  $('#remotePairBtn').onclick = () => {
    openPairDialog('send', (code) => {
      const items = snapshotItems();
      if (items.length === 0) {
        toast('发送内容是空的', true);
        return;
      }
      doSend(items, { type: 'remote', code });
      toast(`等待对方输入配对码 ${code} 加入…`);
    });
  };

  // --- 发送按钮 ---
  $('#sendBtn').onclick = () => {
    const target = selectedTarget;
    const items = snapshotItems();
    if (items.length === 0 || !target) return;
    doSend(items, target);
  };

  manager.onChange(() => {
    renderNearby();
    syncTaskCards();
    refreshSendBtn();
  });
  renderNearby();
  renderChips();
  renderFileList();
  refreshSendBtn();
}

/** 历史页"重发"文本时调用：把文本填回发送框 */
export function setComposeText(text: string): void {
  ($('#composeText') as HTMLTextAreaElement).value = text;
  refreshSendBtn();
  $('#composeText').focus();
}

/** base64 → Uint8Array（分段解码，避免大文件栈溢出） */
function b64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/** 防重复：DOM drop 和 tauri://drag-drop 可能对同一次拖放各触发一次 */
let lastAddKey = '';
let lastAddAt = 0;

function addFileItem(file: File): void {
  const key = `${file.name}\n${file.size}`;
  const now = Date.now();
  if (key === lastAddKey && now - lastAddAt < 3000) return;
  lastAddKey = key;
  lastAddAt = now;
  fileItems.push({
    uid: uid('c'),
    kind: 'file',
    name: file.name,
    size: file.size,
    blob: file,
  });
  renderFileList();
}

function addClipItem(blob: Blob): void {
  clipItems.push({
    uid: uid('c'),
    kind: 'clipboard-image',
    name: '剪贴板图片',
    size: blob.size,
    blob,
    previewUrl: URL.createObjectURL(blob),
  });
  renderChips();
}

function removeFileItem(uid_: string): void {
  const i = fileItems.findIndex((c) => c.uid === uid_);
  if (i >= 0) {
    fileItems.splice(i, 1);
    renderFileList();
    refreshSendBtn();
  }
}

function removeComposeItem(uid_: string): void {
  const i = clipItems.findIndex((c) => c.uid === uid_);
  if (i >= 0) {
    const [c] = clipItems.splice(i, 1);
    if (c.previewUrl) URL.revokeObjectURL(c.previewUrl);
    renderChips();
    refreshSendBtn();
  }
}

function renderChips(): void {
  const row = $('#chipRow');
  row.innerHTML = '';
  for (const c of clipItems) {
    const chip = el('div', 'chip clip');
    if (c.previewUrl) {
      const img = document.createElement('img');
      img.src = c.previewUrl;
      chip.appendChild(img);
      chip.appendChild(el('span', 'ctag', '剪贴板'));
    }
    const nm = el('span', 'cname', c.name);
    nm.title = c.name;
    chip.append(nm, el('span', 'csize', fmtSize(c.size)));
    const x = el('button', 'cx', '✕') as HTMLButtonElement;
    x.title = '移除';
    x.onclick = () => removeComposeItem(c.uid);
    chip.appendChild(x);
    row.appendChild(chip);
  }
  const ch = $('#composeHint');
  if (ch) ch.textContent =
    clipItems.length > 0 ? `${clipItems.length} 张剪贴板图片` : '';
}

function renderFileList(): void {
  const host = $('#fileList');
  host.innerHTML = '';
  if (fileItems.length === 0) {
    host.appendChild(el('div', 'empty', '还没有文件 — 点「添加文件」，或直接把文件拖进来'));
  } else {
    for (const c of fileItems) {
      const row = el('div', 'frow');
      const nm = el('span', 'fname', c.name);
      nm.title = c.name;
      row.append(nm, el('span', 'fsize', fmtSize(c.size)));
      const x = el('button', 'fx', '✕') as HTMLButtonElement;
      x.title = '移除';
      x.onclick = () => removeFileItem(c.uid);
      row.appendChild(x);
      host.appendChild(row);
    }
  }
  const total = fileItems.reduce((a, c) => a + c.size, 0);
  const fh = $('#fileCountHint');
  if (fh) fh.textContent =
    fileItems.length > 0 ? `${fileItems.length} 个文件 · 共 ${fmtSize(total)}` : '';
}

function snapshotItems(): ComposeItem[] {
  const text = ($('#composeText') as HTMLTextAreaElement).value.trim();
  const items = [...clipItems, ...fileItems];
  if (text) {
    items.unshift({ uid: uid('t'), kind: 'text', name: '文本', size: utf8bytes(text), text });
  }
  return items;
}

/** 点击发送：快照 → 建任务 → 立即清空发送框 */
function doSend(items: ComposeItem[], target: SendTarget): void {
  const btn = $('#sendBtn') as HTMLButtonElement;
  btn.disabled = true;
  btn.textContent = '准备中…';
  try {
    manager.startSend(items, target);
  } catch (e) {
    toast('创建任务失败：' + (e as Error).message, true);
  } finally {
    ($('#composeText') as HTMLTextAreaElement).value = '';
    for (const c of clipItems) if (c.previewUrl) URL.revokeObjectURL(c.previewUrl);
    clipItems = [];
    fileItems = [];
    if (target.type === 'remote') selectedTarget = null;
    renderChips();
    renderFileList();
    renderNearby();
    btn.textContent = '发送';
    refreshSendBtn();
  }
}

function refreshSendBtn(): void {
  const btn = $('#sendBtn') as HTMLButtonElement;
  if (btn.textContent === '准备中…') return;
  const text = ($('#composeText') as HTMLTextAreaElement).value.trim();
  const hasContent = text !== '' || clipItems.length > 0 || fileItems.length > 0;
  btn.disabled = !hasContent || !selectedTarget;
}

function renderNearby(): void {
  const host = $('#nearbyList');
  // 选中的设备若已下线，清除选择
  if (
    selectedTarget?.type === 'nearby' &&
    !manager.peers.has(selectedTarget.deviceId)
  ) {
    selectedTarget = null;
  }
  host.innerHTML = '';
  const peers = [...manager.peers.entries()];
  if (peers.length === 0) {
    host.appendChild(el('div', 'empty', '正在搜索附近的设备…（对方需开启 Fling 并允许被发现）'));
    return;
  }
  for (const [id, name] of peers) {
    const b = el('button', 'dev') as HTMLButtonElement;
    if (selectedTarget?.type === 'nearby' && selectedTarget.deviceId === id) {
      b.classList.add('selected');
    }
    const dot = el('span', 'dot');
    const nm = el('span', 'dname', name);
    const sub = el('span', 'dsub', '点击选择');
    b.append(dot, nm, sub);
    b.onclick = () => {
      if (selectedTarget?.type === 'nearby' && selectedTarget.deviceId === id) {
        selectedTarget = null;
      } else {
        selectedTarget = { type: 'nearby', deviceId: id, deviceName: name };
      }
      renderNearby();
      refreshSendBtn();
    };
    host.appendChild(b);
  }
}

function syncTaskCards(): void {
  const host = $('#sendTasks');
  const tasks = [...manager.sendTasks.values()].reverse(); // 最新在上
  for (const t of tasks) {
    let h = cards.get(t.taskId);
    if (!h) {
      h = createTaskCard(t, 'send');
      cards.set(t.taskId, h);
    }
    host.appendChild(h.root);
    h.update();
  }
  for (const [id, h] of [...cards]) {
    if (!manager.sendTasks.has(id)) {
      h.destroy();
      cards.delete(id);
    }
  }
}
