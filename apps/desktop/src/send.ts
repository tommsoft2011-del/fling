// 发送页：原生工作台 — 内容区 + 设备选择 + 底部发送栏 + 任务抽屉
import { invoke } from '@tauri-apps/api/core';
import { listen } from './ui-events.js';
import { $, el, fmtSize, toast, uid, utf8bytes } from './ui.js';
import { manager, type ComposeItem, type SendTarget } from './tasks.js';
import { createTaskRow, type CardHandle } from './taskcard.js';
import { normalizeCode } from '@fling/core';
import { NativeSourcesPicker, type NativeSourceMetadata } from './native-source-picker.js';

let clipItems: ComposeItem[] = []; // 输入框里的剪贴板图片（chip）
let fileItems: ComposeItem[] = []; // 下方文件列表里的文件
let selectedTarget: SendTarget | null = null;
let remoteCode: string | null = null; // 已配对的远程设备（一次一个，新的替换旧的）
const rows = new Map<string, CardHandle>();
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

  // --- 拖拽文件到文件区 → 加入文件列表 ---
  // 提示只出现在文件区内部（虚线框高亮 + "松开"文字），不铺满全屏
  const fileCard = $('#fileListWrap');
  const fileDropHint = $('#fileDropHint') as HTMLElement;
  let dragDepth = 0;
  const hasFiles = (e: DragEvent) =>
    !!e.dataTransfer && Array.from(e.dataTransfer.types).includes('Files');
  const setDropHint = (on: boolean, count?: number) => {
    fileCard.classList.toggle('dragover', on);
    fileDropHint.hidden = !on;
    if (on) {
      fileDropHint.querySelector('span')!.textContent =
        count ? `松开鼠标添加 ${count} 个文件` : '松开鼠标添加文件';
    }
  };
  const clearDropHint = () => {
    dragDepth = 0;
    setDropHint(false);
  };
  fileCard.addEventListener('dragenter', (e) => {
    if (!hasFiles(e as DragEvent)) return;
    e.preventDefault();
    dragDepth++;
    setDropHint(true);
  });
  fileCard.addEventListener('dragleave', () => {
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) setDropHint(false);
  });
  fileCard.addEventListener('dragover', (e) => e.preventDefault());
  fileCard.addEventListener('drop', (e) => {
    e.preventDefault();
    clearDropHint();
    if (isTauri) return; // Native OS drop is registered by Rust; never read DOM file bytes.
    const files = (e as DragEvent).dataTransfer?.files;
    if (files) {
      addFileItems(Array.from(files));
      refreshSendBtn();
    }
  });

  // Rust alone consumes OS paths. Tauri's default path-bearing JS events are denied.
  listen<number>('fling://sources/enter', (e) => {
    if ($('#page-send').classList.contains('active')) setDropHint(true, e.payload);
  }).catch(() => {});
  listen('fling://sources/leave', () => clearDropHint()).catch(() => {});
  const dropInFileCard = (pos: { x: number; y: number }): boolean => {
    const r = fileCard.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    const x = pos.x / dpr; const y = pos.y / dpr;
    return x >= r.left && x <= r.right && y >= r.top && y <= r.bottom;
  };
  listen<{ sources: NativeSourceMetadata[]; x: number; y: number; error?: string }>('fling://sources/drop', (e) => {
    clearDropHint();
    if (!$('#page-send').classList.contains('active') || !dropInFileCard(e.payload)) {
      for (const source of e.payload.sources) releaseSource(source.sourceId);
      return;
    }
    if (e.payload.error) { toast(e.payload.error, true); return; }
    addNativeSources(e.payload.sources);
  }).catch(() => {});
  const nativePicker = new NativeSourcesPicker((folders) => invoke<NativeSourceMetadata[]>('online_pick_sources', { folders }));
  $('#fileInput').addEventListener('click', (event) => {
    if (!isTauri) return;
    event.preventDefault();
    void nativePicker.pick().then(addNativeSources)
      .catch((error) => toast(`选择文件失败，可重新选择：${error}`, true));
  });
  ($('#fileInput') as HTMLInputElement).addEventListener('change', (e) => {
    if (isTauri) return; // Ordinary disk inputs must use the native picker.
    const files = (e.target as HTMLInputElement).files;
    if (files) {
      addFileItems(Array.from(files));
      (e.target as HTMLInputElement).value = '';
      refreshSendBtn();
    }
  });

  // --- 添加文件夹：Tauri 用系统对话框多选；纯浏览器用 webkitdirectory 兜底 ---
  ($('#clearFilesBtn') as HTMLButtonElement).onclick = () => {
    for (const item of fileItems) if (item.sourceId) releaseSource(item.sourceId);
    fileItems.length = 0;
    renderFileList();
    refreshSendBtn();
  };
  $('#addFolderBtn').onclick = () => {
    if (isTauri) {
      void nativePicker.pick(true).then(addNativeSources).catch((err) => toast(`选择文件夹失败：${err}`, true));
    } else {
      ($('#folderInput') as HTMLInputElement).click();
    }
  };
  ($('#folderInput') as HTMLInputElement).addEventListener('change', (e) => {
    if (isTauri) return; // Ordinary disk inputs must use the native picker.
    const files = (e.target as HTMLInputElement).files;
    if (files) {
      let added = 0;
      let skipped = 0;
      for (const f of Array.from(files)) {
        const rel = (f as File & { webkitRelativePath?: string }).webkitRelativePath || f.name;
        const folder = rel.split('/')[0] || undefined;
        if (addFolderFileItem(f.name, rel, f.size, f, folder)) added++;
        else skipped++;
      }
      if (added > 0) toast(`已添加 ${added} 个文件`);
      if (skipped > 0) toast(`已跳过 ${skipped} 个重复文件`);
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

/** 防重复：同名同大小的文件已在列表中则跳过（浏览器 drop 与原生 drop 使用独立入口；用户也可能重复选择同一文件） */
function addFileItem(file: File): boolean {
  const dup = fileItems.some((c) => (c.relPath ?? c.name) === file.name && c.size === file.size);
  if (dup) return false;
  fileItems.push({
    uid: uid('c'),
    kind: 'file',
    name: file.name,
    size: file.size,
    blob: file,
  });
  renderFileList();
  refreshSendBtn();
  return true;
}

/** 批量添加，跳过重复并提示 */
function addFileItems(files: File[]): void {
  let skipped = 0;
  for (const f of files) {
    if (!addFileItem(f)) skipped++;
  }
  if (skipped > 0) toast(`已跳过 ${skipped} 个重复文件`);
}

function releaseSource(sourceId: string): void {
  void invoke('online_release_source', { sourceId }).catch(() => {});
}
function addNativeSources(sources: NativeSourceMetadata[]): void {
  let added = 0;
  for (const source of sources) {
    if (fileItems.some((item) => (item.relPath ?? item.name) === (source.relPath ?? source.name) && item.size === source.size)) {
      releaseSource(source.sourceId); continue;
    }
    fileItems.push({ uid: uid('c'), kind: 'file', ...source }); added++;
  }
  renderFileList(); refreshSendBtn();
  if (added) toast(`已添加 ${added} 个 Rust 原生文件源`);
}

/** 添加文件夹中的单个文件（relPath 如 "docs/report.pdf"） */
function addFolderFileItem(
  name: string,
  relPath: string,
  size: number,
  blob?: Blob,
  folder?: string,
): boolean {
  const dup = fileItems.some((c) => (c.relPath ?? c.name) === relPath && c.size === size);
  if (dup) return false;
  fileItems.push({ uid: uid('c'), kind: 'file', name, size, blob, relPath, folder });
  renderFileList();
  refreshSendBtn();
  return true;
}

const isTauri = typeof window !== 'undefined' && !!(window as unknown as { __TAURI_INTERNALS__?: unknown }).__TAURI_INTERNALS__;

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
    const [item] = fileItems.splice(i, 1);
    if (item?.sourceId) void invoke('online_release_source', { sourceId: item.sourceId }).catch(() => {});
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

// 按扩展名给文件配图标（emoji 近似）。真系统图标需走 Shell API
// （Rust 调 SHGetFileInfo 取图标再转码），成本高，先用扩展名映射。
function fileIcon(name: string): string {
  const ext = name.split('.').pop()?.toLowerCase() ?? '';
  const map: Record<string, string> = {
    pdf: '\u{1F4D5}',
    doc: '\u{1F4D8}', docx: '\u{1F4D8}', rtf: '\u{1F4D8}',
    xls: '\u{1F4CA}', xlsx: '\u{1F4CA}', csv: '\u{1F4CA}',
    ppt: '\u{1F4CA}', pptx: '\u{1F4CA}',
    txt: '\u{1F4DD}', md: '\u{1F4DD}', log: '\u{1F4DD}',
    png: '\u{1F5BC}\uFE0F', jpg: '\u{1F5BC}\uFE0F', jpeg: '\u{1F5BC}\uFE0F',
    gif: '\u{1F5BC}\uFE0F', bmp: '\u{1F5BC}\uFE0F', webp: '\u{1F5BC}\uFE0F', svg: '\u{1F5BC}\uFE0F',
    mp4: '\u{1F3AC}', mkv: '\u{1F3AC}', avi: '\u{1F3AC}', mov: '\u{1F3AC}', wmv: '\u{1F3AC}',
    mp3: '\u{1F3B5}', wav: '\u{1F3B5}', flac: '\u{1F3B5}', ogg: '\u{1F3B5}', m4a: '\u{1F3B5}',
    zip: '\u{1F4E6}', rar: '\u{1F4E6}', '7z': '\u{1F4E6}', tar: '\u{1F4E6}', gz: '\u{1F4E6}',
    ino: '\u2699\uFE0F', c: '\u2699\uFE0F', h: '\u2699\uFE0F', cpp: '\u2699\uFE0F',
    cs: '\u2699\uFE0F', go: '\u2699\uFE0F', rs: '\u2699\uFE0F', java: '\u2615',
    js: '\u{1F4DC}', ts: '\u{1F4DC}', py: '\u{1F40D}', html: '\u{1F310}', css: '\u{1F3A8}',
    json: '\u{1F4CB}', xml: '\u{1F4CB}', filters: '\u{1F4CB}', vcxproj: '\u{1F4CB}',
    exe: '\u2699\uFE0F', msi: '\u2699\uFE0F', apk: '\u{1F4F1}', dmg: '\u{1F4BF}',
  };
  return map[ext] ?? '\u{1F4C4}';
}

function renderFileList(): void {
  const host = $('#fileList');
  host.innerHTML = '';
  if (fileItems.length === 0) {
    host.appendChild(el('div', 'empty', '还没有文件 — 点「添加文件」，或把文件拖到这里'));
  } else {
    // 按文件夹折叠：同文件夹的显示为一行「📁 文件夹名（共 N 个文件）」，不展开
    const singles: typeof fileItems = [];
    const folders = new Map<string, typeof fileItems>();
    for (const c of fileItems) {
      if (c.folder) {
        const g = folders.get(c.folder) ?? [];
        g.push(c);
        folders.set(c.folder, g);
      } else {
        singles.push(c);
      }
    }
    for (const c of singles) host.appendChild(fileRow(c));
    for (const [fname, items] of folders) {
      const row = el('div', 'frow ffolder');
      const icon = el('span', 'fic', '📁');
      const total = items.reduce((a, i) => a + i.size, 0);
      const nm = el('span', 'fname', `${fname}（共 ${items.length} 个文件）`);
      nm.title = items.map((i) => i.relPath ?? i.name).join('\n');
      row.append(icon, nm, el('span', 'fsize', fmtSize(total)));
      const x = el('button', 'fx', '✕') as HTMLButtonElement;
      x.title = '移除整个文件夹';
      x.onclick = () => {
        const uids = new Set(items.map((i) => i.uid));
        for (const item of items) if (item.sourceId) releaseSource(item.sourceId);
        fileItems = fileItems.filter((c) => !uids.has(c.uid));
        renderFileList();
        refreshSendBtn();
      };
      row.appendChild(x);
      host.appendChild(row);
    }
  }
  const total = fileItems.reduce((a, c) => a + c.size, 0);
  const fh = $('#fileCountHint');
  if (fh) fh.textContent =
    fileItems.length > 0 ? `${fileItems.length} 个文件 · 共 ${fmtSize(total)} · ${fileItems.every((item) => item.sourceId) ? 'Rust 原生文件源' : '含内存 Blob 兼容源（非 Rust-owned）'}` : '';
  ($('#clearFilesBtn') as HTMLButtonElement).hidden = fileItems.length === 0;
}

/** 单文件行 */
function fileRow(c: (typeof fileItems)[number]): HTMLElement {
  const row = el('div', 'frow');
  const label = c.relPath ?? c.name;
  const icon = el('span', 'fic', c.relPath ? '📄' : fileIcon(c.name));
  const nm = el('span', 'fname', label);
  nm.title = label;
  row.append(icon, nm, el('span', 'fsize', fmtSize(c.size)));
  const x = el('button', 'fx', '✕') as HTMLButtonElement;
  x.title = '移除';
  x.onclick = () => removeFileItem(c.uid);
  row.appendChild(x);
  return row;
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
  const orderedRows: HTMLElement[] = [];
  for (const t of tasks) {
    let h = rows.get(t.taskId);
    if (!h) {
      h = createTaskRow(t, 'send');
      rows.set(t.taskId, h);
    }
    if (!h.isGone()) orderedRows.push(h.root);
    h.update();
  }
  // Do not re-append every row on each progress tick; move nodes only when
  // insertion or task-order changes actually require it.
  for (let index = 0; index < orderedRows.length; index++) {
    const row = orderedRows[index];
    if (list.children[index] !== row) list.insertBefore(row, list.children[index] ?? null);
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
    // 角标只显示进行中的任务数，全部完成后隐藏
    const activeCount = tasks.filter((t) => t.active).length;
    const cnt = $('#taskCount');
    cnt.hidden = activeCount === 0;
    cnt.textContent = String(activeCount);
    // 右侧文案只在有进行中任务时显示；全部完成后不保留"xx·完成"
    const firstActive = tasks.find((t) => t.active);
    $('#taskMini').textContent = firstActive ? `→ ${firstActive.peerLabel}` : '';
  }
  // 常驻面板：始终显示，无任务时显示"暂无任务"
  list.hidden = false;
  const hasVisible = [...rows.values()].some((h) => !h.isGone());
  let emptyEl = list.querySelector('.tempty');
  if (!hasVisible) {
    if (!emptyEl) {
      emptyEl = el('div', 'tempty', '暂无任务');
      list.appendChild(emptyEl);
    }
  } else if (emptyEl) {
    emptyEl.remove();
  }
}
