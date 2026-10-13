// 接收页：原生分组列表 — 等待动画 / 配对码行 / 待确认 / 接收中
// （"允许被发现"开关已移到设置页）
import { $, el, fmtSize, toast } from './ui.js';
import { manager, type PendingOffer } from './tasks.js';
import { createTaskRow, type CardHandle } from './taskcard.js';
import { normalizeCode } from '@fling/core';

let inited = false;
const rows = new Map<string, CardHandle>();
let lobbyLeave: (() => void) | null = null;
let lobbyCode: string | null = null;

export function initRecvPage(): void {
  if (inited) return;
  inited = true;

  // 配对码接收：行内输入 + 开始监听
  const codeInput = $('#pairRecvInput') as HTMLInputElement;
  const listenBtn = $('#pairRecvBtn') as HTMLButtonElement;
  const startListen = async () => {
    const code = normalizeCode(codeInput.value);
    if (!code) {
      toast('请输入 6 位配对码', true);
      codeInput.focus();
      return;
    }
    try {
      if (lobbyLeave) lobbyLeave();
      lobbyLeave = await manager.joinLobby(code);
      lobbyCode = code;
      renderLobbyStatus();
      codeInput.value = '';
      toast(`正在通过配对码 ${code} 等待对方的传输…`);
    } catch (e) {
      toast('加入配对房间失败：' + (e as Error).message, true);
    }
  };
  listenBtn.onclick = () => void startListen();
  codeInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') void startListen();
  });
  $('#lobbyStopBtn').onclick = () => stopListen();

  // 待确认超过 15 分钟自动清理
  setInterval(() => {
    const now = Date.now();
    let changed = false;
    for (const [id, p] of manager.pending) {
      if (now - p.at > 15 * 60 * 1000) {
        manager.pending.delete(id);
        changed = true;
      }
    }
    if (changed) manager.notify();
  }, 60_000);

  manager.onChange(() => {
    renderPending();
    syncRecvRows();
  });
  renderLobbyStatus();
  renderPending();
  syncRecvRows();
}

/** 离开接收页：断开配对码监听（附近 inbox 常驻不受影响） */
export function blurRecvPage(): void {
  stopListen();
}

function stopListen(): void {
  if (lobbyLeave) {
    lobbyLeave();
    lobbyLeave = null;
    lobbyCode = null;
    renderLobbyStatus();
  }
}

function renderLobbyStatus(): void {
  const row = $('#lobbyStatusRow');
  if (lobbyCode) {
    row.hidden = false;
    $('#lobbyCodeLabel').textContent = lobbyCode;
  } else {
    row.hidden = true;
  }
}

function renderPending(): void {
  const host = $('#pendingList');
  host.innerHTML = '';
  const offers = [...manager.pending.values()].sort((a, b) => b.at - a.at);
  const cnt = $('#pendingCount');
  if (offers.length === 0) {
    cnt.hidden = true;
    host.appendChild(el('div', 'empty', '暂无待确认的传输'));
    return;
  }
  cnt.hidden = false;
  cnt.textContent = String(offers.length);
  for (const p of offers) {
    const row = el('div', 'pend');
    const info = el('div', 'pinfo');
    const from = el('div', 'pfrom', p.offer.fromDevice);
    const items = el('div', 'pitems', summarizeOffer(p));
    items.title = items.textContent ?? '';
    info.append(from, items);
    const act = el('div', 'pact');
    const ok = el('button', 'btn-ok', '接受') as HTMLButtonElement;
    ok.onclick = () => manager.acceptOffer(p.offer.taskId);
    const no = el('button', 'btn-no', '拒绝') as HTMLButtonElement;
    no.onclick = () => manager.rejectOffer(p.offer.taskId);
    act.append(ok, no);
    row.append(info, act);
    host.appendChild(row);
  }
}

function summarizeOffer(p: PendingOffer): string {
  const items = p.offer.items;
  const parts: string[] = [];
  const nText = items.filter((i) => i.kind === 'text').length;
  const nClip = items.filter((i) => i.kind === 'clipboard-image').length;
  const nFile = items.filter((i) => i.kind === 'file').length;
  if (nText) parts.push(`${nText} 段文本`);
  if (nClip) parts.push(`${nClip} 张剪贴板图片`);
  if (nFile) parts.push(`${nFile} 个文件`);
  const total = items.reduce((a, i) => a + i.size, 0);
  return `${parts.join('、')} · 共 ${fmtSize(total)}${p.via === 'remote' ? ` · 配对码 ${p.code}` : ''}`;
}

function syncRecvRows(): void {
  const host = $('#recvRows');
  const tasks = [...manager.recvTasks.values()].reverse();
  for (const t of tasks) {
    let h = rows.get(t.taskId);
    if (!h) {
      h = createTaskRow(t, 'recv');
      rows.set(t.taskId, h);
    }
    host.appendChild(h.root);
    h.update();
  }
  for (const [id, h] of [...rows]) {
    if (!manager.recvTasks.has(id)) {
      h.destroy();
      rows.delete(id);
    }
  }
  const emptyEl = host.querySelector('.empty');
  if (tasks.length === 0) {
    if (!emptyEl) host.appendChild(el('div', 'empty', '暂无正在接收的任务'));
  } else {
    if (emptyEl) emptyEl.remove();
  }
}
