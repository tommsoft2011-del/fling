// 发送页：统一发送框 + 目标选择 + 进行中的发送
import { $, el, fmtSize, toast, uid, utf8bytes } from './ui.js';
import { manager, type ComposeItem, type SendTarget } from './tasks.js';
import { createTaskCard, type CardHandle } from './taskcard.js';
import { openPairDialog } from './pairdialog.js';

let composeItems: ComposeItem[] = [];
let selectedTarget: SendTarget | null = null;
const cards = new Map<string, CardHandle>();
let inited = false;

export function initSendPage(): void {
  if (inited) return;
  inited = true;

  const composeBox = $('#composeBox');
  const textarea = $('#composeText') as HTMLTextAreaElement;

  // --- Ctrl+V 粘贴：文本走 textarea 默认行为；图片/文件拦截成 chip ---
  composeBox.addEventListener('paste', (e: ClipboardEvent) => {
    const cd = e.clipboardData;
    if (!cd) return;
    let handled = false;
    for (const item of Array.from(cd.items)) {
      if (item.kind !== 'file') continue;
      const f = item.getAsFile();
      if (!f) continue;
      if (item.type.startsWith('image/')) {
        // Chrome 把截图/网页复制的位图命名为 image.png；资源管理器复制的图片文件带真实文件名
        if (f.name === 'image.png') addClipItem(f);
        else addFileItem(f);
      } else {
        addFileItem(f);
      }
      handled = true;
    }
    if (handled) e.preventDefault();
    refreshSendBtn();
  });

  // --- 拖拽文件 ---
  composeBox.addEventListener('dragover', (e) => e.preventDefault());
  composeBox.addEventListener('drop', (e) => {
    e.preventDefault();
    const files = e.dataTransfer?.files;
    if (files) {
      for (const f of Array.from(files)) addFileItem(f);
      refreshSendBtn();
    }
  });

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
  refreshSendBtn();
}

/** 历史页"重发"文本时调用：把文本填回发送框 */
export function setComposeText(text: string): void {
  ($('#composeText') as HTMLTextAreaElement).value = text;
  refreshSendBtn();
  $('#composeText').focus();
}

function addFileItem(file: File): void {
  composeItems.push({
    uid: uid('c'),
    kind: 'file',
    name: file.name,
    size: file.size,
    blob: file,
  });
  renderChips();
}

function addClipItem(blob: Blob): void {
  composeItems.push({
    uid: uid('c'),
    kind: 'clipboard-image',
    name: '剪贴板图片',
    size: blob.size,
    blob,
    previewUrl: URL.createObjectURL(blob),
  });
  renderChips();
}

function removeComposeItem(uid_: string): void {
  const i = composeItems.findIndex((c) => c.uid === uid_);
  if (i >= 0) {
    const [c] = composeItems.splice(i, 1);
    if (c.previewUrl) URL.revokeObjectURL(c.previewUrl);
    renderChips();
    refreshSendBtn();
  }
}

function renderChips(): void {
  const row = $('#chipRow');
  row.innerHTML = '';
  for (const c of composeItems) {
    const chip = el('div', 'chip' + (c.kind === 'clipboard-image' ? ' clip' : ''));
    if (c.kind === 'clipboard-image' && c.previewUrl) {
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
  $('#composeHint').textContent =
    composeItems.length > 0 ? `${composeItems.length} 个附件` : '';
}

function snapshotItems(): ComposeItem[] {
  const text = ($('#composeText') as HTMLTextAreaElement).value.trim();
  const items = [...composeItems];
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
    for (const c of composeItems) if (c.previewUrl) URL.revokeObjectURL(c.previewUrl);
    composeItems = [];
    if (target.type === 'remote') selectedTarget = null;
    renderChips();
    renderNearby();
    btn.textContent = '发送';
    refreshSendBtn();
  }
}

function refreshSendBtn(): void {
  const btn = $('#sendBtn') as HTMLButtonElement;
  if (btn.textContent === '准备中…') return;
  const text = ($('#composeText') as HTMLTextAreaElement).value.trim();
  const hasContent = text !== '' || composeItems.length > 0;
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
