// 接收页：等待动画 / 配对码按钮 / 等待确认（页面内分组）/ 接收任务（底部抽屉）
// 配对监听作为"等待确认"的一行：配对码大字 + 倒计时 + 取消
import { $, el, fmtSize, toast } from './ui.js';
import { manager, type PendingOffer } from './tasks.js';
import { createTaskRow, type CardHandle } from './taskcard.js';
import { normalizeCode } from '@fling/core';

let inited = false;
const rows = new Map<string, CardHandle>();

// 配对码监听状态（同时只保留一个）
let lobbyLeave: (() => void) | null = null;
let lobbyCode: string | null = null;
let lobbyTimer: ReturnType<typeof setTimeout> | null = null;
let lobbyDeadline = 0;
let countdownTimer: ReturnType<typeof setInterval> | null = null;
const LOBBY_TIMEOUT_MS = 5 * 60 * 1000; // 5 分钟无请求自动停止

export function initRecvPage(): void {
  if (inited) return;
  inited = true;

  // 配对码接收：按钮 → 弹窗输入 → 进入等待确认列表
  const modal = $('#recvPairModal');
  const codeInput = $('#recvPairCodeInput') as HTMLInputElement;
  const openModal = () => {
    modal.hidden = false;
    codeInput.value = '';
    setTimeout(() => codeInput.focus(), 50);
  };
  const closeModal = () => {
    modal.hidden = true;
  };
  $('#pairRecvBtn').onclick = openModal;
  $('#recvPairCancel').onclick = closeModal;
  modal.addEventListener('click', (e) => {
    if (e.target === modal) closeModal();
  });
  const confirmListen = async () => {
    const code = normalizeCode(codeInput.value);
    if (!code) {
      toast('请输入 8 位配对码', true);
      codeInput.focus();
      return;
    }
    closeModal();
    await startListen(code);
  };
  $('#recvPairOk').onclick = () => void confirmListen();
  codeInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') void confirmListen();
  });


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
    // 监听中收到 offer → 自动停止监听（行会变成接收/拒绝）
    if (lobbyCode && manager.pending.size > 0) {
      const viaThisCode = [...manager.pending.values()].some(
        (p) => p.via === 'remote' && p.code === lobbyCode,
      );
      if (viaThisCode) stopListen(true);
    }
    renderPending();
    syncRecvDrawer();
  });
  renderPending();
  syncRecvDrawer();
}

/** 开始配对码监听：在等待确认列表加一行（配对码大字 + 倒计时） */
async function startListen(code: string): Promise<void> {
  try {
    if (lobbyLeave) stopListen();
    lobbyLeave = await manager.joinLobby(code);
    lobbyCode = code;
    lobbyDeadline = Date.now() + LOBBY_TIMEOUT_MS;
    if (lobbyTimer) clearTimeout(lobbyTimer);
    lobbyTimer = setTimeout(() => {
      stopListen();
      toast(`配对码 ${code} 等待超时，已停止监听`);
    }, LOBBY_TIMEOUT_MS);
    // 倒计时每秒刷新
    if (countdownTimer) clearInterval(countdownTimer);
    countdownTimer = setInterval(() => {
      const cd = document.querySelector('.pend.pairing .countdown');
      if (cd && lobbyCode) {
        cd.textContent = fmtCountdown(lobbyDeadline - Date.now()) + ' 后自动停止';
      } else if (countdownTimer) {
        clearInterval(countdownTimer);
        countdownTimer = null;
      }
    }, 1000);
    renderPending();
    toast(`正在等待配对码 ${code} 的传输请求…`);
  } catch (e) {
    toast('加入配对房间失败：' + (e as Error).message, true);
  }
}

function fmtCountdown(ms: number): string {
  const s = Math.max(0, Math.ceil(ms / 1000));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

/** 停止监听（silent=true 时不重复 toast，由调用方提示） */
function stopListen(silent = false): void {
  if (lobbyTimer) {
    clearTimeout(lobbyTimer);
    lobbyTimer = null;
  }
  if (countdownTimer) {
    clearInterval(countdownTimer);
    countdownTimer = null;
  }
  if (lobbyLeave) {
    lobbyLeave();
    lobbyLeave = null;
  }
  lobbyCode = null;
  if (!silent) {
    renderPending();
    syncRecvDrawer();
  }
}

/** 离开接收页：断开配对码监听（附近 inbox 常驻不受影响） */
export function blurRecvPage(): void {
  stopListen();
}

function renderPending(): void {
  const host = $('#pendingList');
  host.innerHTML = '';
  const offers = [...manager.pending.values()].sort((a, b) => b.at - a.at);
  const cnt = $('#pendingCount');
  const total = offers.length + (lobbyCode ? 1 : 0);
  if (total === 0) {
    cnt.hidden = true;
    host.appendChild(el('div', 'empty', '暂无待确认的传输'));
    return;
  }
  cnt.hidden = false;
  cnt.textContent = String(total);
  // 配对监听行置顶：配对码大字 + 倒计时 + 取消
  if (lobbyCode) {
    const row = el('div', 'pend pairing');
    const info = el('div', 'pinfo');
    const codeLine = el('div', 'pcode', lobbyCode);
    const cd = el('span', 'countdown', fmtCountdown(lobbyDeadline - Date.now()) + ' 后自动停止');
    codeLine.appendChild(cd);
    info.append(codeLine, el('div', 'pstatus', '正在进行配对'));
    const cancel = el('button', 'btn-ok', '取消') as HTMLButtonElement;
    cancel.onclick = () => stopListen();
    row.append(info, cancel);
    host.appendChild(row);
  }
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
  const totalN = p.offer.totalItems ?? items.length;
  const totalB = p.offer.totalBytes ?? items.reduce((a, i) => a + i.size, 0);
  const truncated = items.length < totalN;
  const parts: string[] = [];
  if (truncated) {
    // items 被截断只作预览：直接显示总数，不按 kind 细分（细分数字会对不上）
    parts.push(`${totalN} 项`);
  } else {
    const nText = items.filter((i) => i.kind === 'text').length;
    const nClip = items.filter((i) => i.kind === 'clipboard-image').length;
    const nFile = items.filter((i) => i.kind === 'file').length;
    if (nText) parts.push(`${nText} 段文本`);
    if (nClip) parts.push(`${nClip} 张剪贴板图片`);
    if (nFile) parts.push(`${nFile} 个文件`);
  }
  return `${parts.join('、')} · 共 ${fmtSize(totalB)}${p.via === 'remote' ? ` · 配对码 ${p.code}` : ''}`;
}

function syncRecvDrawer(): void {
  const list = $('#recvTaskList');
  const tasks = [...manager.recvTasks.values()].reverse();
  // 接收中行
  const host = $('#recvRows');
  const orderedRows: HTMLElement[] = [];
  for (const t of tasks) {
    let h = rows.get(t.taskId);
    if (!h) {
      h = createTaskRow(t, 'recv');
      rows.set(t.taskId, h);
    }
    if (!h.isGone()) orderedRows.push(h.root);
    h.update();
  }
  // Progress notifications should update rows in place, not move every row.
  for (let index = 0; index < orderedRows.length; index++) {
    const row = orderedRows[index];
    if (host.children[index] !== row) host.insertBefore(row, host.children[index] ?? null);
  }
  for (const [id, h] of [...rows]) {
    if (!manager.recvTasks.has(id)) {
      h.destroy();
      rows.delete(id);
    }
  }
  const emptyEl = host.querySelector('.empty');
  const hasVisibleRecv = [...rows.values()].some((h) => !h.isGone());
  if (!hasVisibleRecv) {
    if (!emptyEl) host.appendChild(el('div', 'empty', '暂无正在接收的任务'));
  } else if (emptyEl) {
    emptyEl.remove();
  }

  // 抽屉状态条：只统计真正的接收任务（监听不进抽屉）
  const activeN = tasks.filter((t) => t.active).length;
  const cnt = $('#recvCount');
  cnt.hidden = activeN === 0;
  cnt.textContent = String(activeN);
  const firstActive = tasks.find((t) => t.active);
  $('#recvMini').textContent = firstActive ? `← ${firstActive.peerLabel}` : '';
  // 常驻面板：始终显示
  list.hidden = false;
}
