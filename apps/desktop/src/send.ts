// 发送页：原生工作台 — 内容区 + 设备选择 + 底部发送栏 + 任务抽屉
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import { $, el, fmtSize, toast, uid, utf8bytes } from './ui.js';
import { manager, type ComposeItem, type SendTarget } from './tasks.js';
import { createTaskRow, type CardHandle } from './taskcard.js';
import { normalizeCode } from '@fling/core';

let clipItems: ComposeItem[] = []; // 输入框里的剪贴板图片（chip）
let fileItems: ComposeItem[] = []; // 下方文件列表里的文件
let selectedTarget: SendTarget | null = null;
let remoteCode: string | null = null; // 已配对的远程设备（一次一个，新的替换旧的）
const rows = new Map<string, CardHandle>();
let inited = false;
let drawerOpen = false;

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

  // --- 远程配对磁贴 → 弹窗输配对码 → 加入设备列表并自动选中 ---
  $('#pairTile').onclick = () => openPairModal();
  $('#pairCancel').onclick = () => closePairModal();
  $('#pairOk').onclick = () => confirmPairModal();
  ($('#pairCodeInput') as HTMLInputElement).addEventListener('keydown', (e) => {
    if (e.key === 'Enter') confirmPairModal();
    if (e.key === 'Escape') closePairModal();
  });
  $('#pairModal').addEventListener('click', (e) => {
    if ((e.target as HTMLElement).id === 'pairModal') closePairModal();
  });

  // --- 发送按钮：有内容 + 选中目标才可用 ---
  $('#sendBtn').onclick = () => {
    const target = selectedTarget;
    const items = snapshotItems();
    if (items.length === 0 || !target) return;
    doSend(items, target);
  };

  // --- 任务抽屉开关 ---
  $('#taskStrip').onclick = () => {
    drawerOpen = !drawerOpen;
    $('#taskRows').hidden = !drawerOpen;
    $('#taskCar').textContent = drawerOpen ? '▾' : '▴';
  };

  manager.onChange(() => {
    renderTiles();
    syncTaskRows();
    refreshSendBtn();
  });
  renderTiles();
  renderChips();
  renderFileList();
  syncTaskRows();
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
  refreshSendBtn();
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
  refreshSendBtn();
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

/** 点击发送：快照 → 建任务 → 立即清空发送框（目标保留，方便连发） */
function doSend(items: ComposeItem[], target: SendTarget): void {
  try {
    manager.startSend(items, target);
  } catch (e) {
    toast('创建任务失败：' + (e as Error).message, true);
    return;
  }
  ($('#composeText') as HTMLTextAreaElement).value = '';
  for (const c of clipItems) if (c.previewUrl) URL.revokeObjectURL(c.previewUrl);
  clipItems = [];
  fileItems = [];
  renderChips();
  renderFileList();
  refreshSendBtn();
  toast(`正在发送给 ${targetLabel(target)}…`);
}

function targetLabel(t: SendTarget): string {
  return t.type === 'remote' ? `远程设备（${t.code}）` : t.deviceName;
}

function hasContent(): boolean {
  const text = ($('#composeText') as HTMLTextAreaElement).value.trim();
  return text !== '' || clipItems.length > 0 || fileItems.length > 0;
}

function refreshSendBtn(): void {
  const btn = $('#sendBtn') as HTMLButtonElement;
  const ok = hasContent() && !!selectedTarget;
  btn.disabled = !ok;
  if (!selectedTarget) {
    btn.textContent = hasContent() ? '发送（先选择设备）' : '发送';
  } else {
    const items = snapshotItems();
    const total = items.reduce((a, c) => a + c.size, 0);
    btn.textContent = `发送给 ${targetLabel(selectedTarget)} · ${fmtSize(total)}`;
  }
}

// ---------- 设备磁贴 ----------

function isSelected(t: SendTarget): boolean {
  if (!selectedTarget) return false;
  if (t.type !== selectedTarget.type) return false;
  return t.type === 'remote'
    ? (selectedTarget as { code: string }).code === (t as { code: string }).code
    : (selectedTarget as { deviceId: string }).deviceId === (t as { deviceId: string }).deviceId;
}

function renderTiles(): void {
  const host = $('#nearbyTiles');
  host.innerHTML = '';
  // 选中的附近设备若已下线，清除选择
  if (
    selectedTarget?.type === 'nearby' &&
    !manager.peers.has(selectedTarget.deviceId)
  ) {
    selectedTarget = null;
  }
  const peers = [...manager.peers.entries()];
  if (peers.length === 0 && !remoteCode) {
    host.appendChild(el('div', 'devtile ghost', '正在搜索…'));
  }
  for (const [id, name] of peers) {
    const target: SendTarget = { type: 'nearby', deviceId: id, deviceName: name };
    host.appendChild(makeTile('💻', name, '在线', isSelected(target), () =>
      toggleSelect(target),
    ));
  }
  // 已配对的远程设备（一次一个）
  if (remoteCode) {
    const target: SendTarget = { type: 'remote', code: remoteCode };
    const tile = makeTile('🔗', '远程设备', `配对码 ${remoteCode}`, isSelected(target), () =>
      toggleSelect(target),
    );
    tile.classList.add('remote');
    const rx = el('span', 'rx', '✕');
    rx.title = '移除该远程设备';
    (rx as HTMLElement).setAttribute('role', 'button');
    rx.onclick = (e) => {
      e.stopPropagation();
      if (selectedTarget?.type === 'remote') selectedTarget = null;
      remoteCode = null;
      renderTiles();
      refreshSendBtn();
    };
    tile.appendChild(rx);
    host.appendChild(tile);
  }
  refreshSendBtn();
}

function makeTile(pic: string, name: string, sub: string, selected: boolean, onClick: () => void): HTMLElement {
  const t = el('button', 'devtile' + (selected ? ' selected' : '')) as HTMLButtonElement;
  t.append(el('div', 'pic', pic), el('div', 'dname', name), el('div', 'st', sub));
  t.title = selected ? `已选择 ${name}（再点取消）` : `选择 ${name}`;
  t.onclick = onClick;
  return t;
}

function toggleSelect(t: SendTarget): void {
  selectedTarget = isSelected(t) ? null : t;
  renderTiles();
  refreshSendBtn();
}

// ---------- 远程配对弹窗 ----------

function openPairModal(): void {
  ($('#pairModal') as HTMLElement).hidden = false;
  const input = $('#pairCodeInput') as HTMLInputElement;
  input.value = '';
  setTimeout(() => input.focus(), 50);
}

function closePairModal(): void {
  ($('#pairModal') as HTMLElement).hidden = true;
}

function confirmPairModal(): void {
  const code = normalizeCode(($('#pairCodeInput') as HTMLInputElement).value);
  if (!code) {
    toast('请输入配对码', true);
    return;
  }
  remoteCode = code; // 新的替换旧的
  selectedTarget = { type: 'remote', code };
  closePairModal();
  renderTiles();
  refreshSendBtn();
  toast(`已添加远程设备（配对码 ${code}），对方输入相同配对码即可接收`);
}

// ---------- 任务抽屉 ----------

function syncTaskRows(): void {
  const drawer = $('#taskDrawer');
  const list = $('#taskRows');
  const tasks = [...manager.sendTasks.values()].reverse(); // 最新在上
  for (const t of tasks) {
    let h = rows.get(t.taskId);
    if (!h) {
      h = createTaskRow(t, 'send');
      rows.set(t.taskId, h);
    }
    list.appendChild(h.root);
    h.update();
  }
  for (const [id, h] of [...rows]) {
    if (!manager.sendTasks.has(id)) {
      h.destroy();
      rows.delete(id);
    }
  }
  // 状态条：有任务才显示
  const n = tasks.length;
  drawer.hidden = n === 0;
  if (n > 0) {
    const cnt = $('#taskCount');
    cnt.hidden = false;
    cnt.textContent = String(n);
    const first = tasks.find((t) => t.active) ?? tasks[0];
    $('#taskMini').textContent = first.active ? `→ ${first.peerLabel}` : `${first.peerLabel} · 完成`;
  }
  if (!drawerOpen) list.hidden = true;
}
