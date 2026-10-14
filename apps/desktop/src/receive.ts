// 接收页：等待动画 / 配对码按钮 / 底部任务抽屉（等待确认 + 接收中，可折叠）
// （"允许被发现"开关已移到设置页）
import { $, el, fmtSize, toast } from './ui.js';
import { manager, type PendingOffer } from './tasks.js';
import { createTaskRow, type CardHandle } from './taskcard.js';
import { normalizeCode } from '@fling/core';

let inited = false;
const rows = new Map<string, CardHandle>();
let drawerOpen = false;

// 配对码监听状态
let lobbyLeave: (() => void) | null = null;
let lobbyCode: string | null = null;
let lobbyTimer: ReturnType<typeof setTimeout> | null = null;
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
      toast('请输入 6 位配对码', true);
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

  // 抽屉折叠
  $('#recvStrip').onclick = () => {
    drawerOpen = !drawerOpen;
    $('#recvCar').textContent = drawerOpen ? '▾' : '▴';
    syncRecvDrawer();
  };

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
    syncRecvDrawer();
  });
  renderPending();
  syncRecvDrawer();
}

/** 开始配对码监听：在等待确认列表里加一个"监听中"伪行，超时自动删除 */
async function startListen(code: string): Promise<void> {
  try {
    if (lobbyLeave) stopListen();
    lobbyLeave = await manager.joinLobby(code);
    lobbyCode = code;
    // 5 分钟超时：无请求则自动停止并删除
    if (lobbyTimer) clearTimeout(lobbyTimer);
    lobbyTimer = setTimeout(() => {
      stopListen();
      toast(`配对码 ${code} 等待超时，已停止监听`);
    }, LOBBY_TIMEOUT_MS);
    // 开始监听时自动展开抽屉，方便看到等待行
    drawerOpen = true;
    $('#recvCar').textContent = '▾';
    renderPending();
    syncRecvDrawer();
    toast(`正在等待配对码 ${code} 的传输请求…`);
  } catch (e) {
    toast('加入配对房间失败：' + (e as Error).message, true);
  }
}

function stopListen(): void {
  if (lobbyTimer) {
    clearTimeout(lobbyTimer);
    lobbyTimer = null;
  }
  if (lobbyLeave) {
    lobbyLeave();
    lobbyLeave = null;
  }
  lobbyCode = null;
  renderPending();
  syncRecvDrawer();
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
  // 监听中伪行（虚线）：可手动取消
  if (lobbyCode) {
    const row = el('div', 'pend listening');
    const info = el('div', 'pinfo');
    info.append(
      el('div', 'pfrom', `🔗 正在等待配对码 ${lobbyCode} 的传输请求…`),
      el('div', 'pitems', '对方发送后会出现在这里确认，5 分钟无请求自动停止'),
    );
    const cancel = el('button', 'btn-no pcancel', '取消') as HTMLButtonElement;
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

function syncRecvDrawer(): void {
  const list = $('#recvTaskList');
  const tasks = [...manager.recvTasks.values()].reverse();
  // 接收中行
  const host = $('#recvRows');
  for (const t of tasks) {
    let h = rows.get(t.taskId);
    if (!h) {
      h = createTaskRow(t, 'recv');
      rows.set(t.taskId, h);
    }
    if (!h.isGone()) host.appendChild(h.root);
    h.update();
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

  // 抽屉状态条：待确认 + 进行中的总数
  const pendingN = manager.pending.size + (lobbyCode ? 1 : 0);
  const activeN = tasks.filter((t) => t.active).length;
  const n = pendingN + activeN;
  const cnt = $('#recvCount');
  cnt.hidden = n === 0;
  cnt.textContent = String(n);
  const firstActive = tasks.find((t) => t.active);
  const firstPending = [...manager.pending.values()].sort((a, b) => b.at - a.at)[0];
  $('#recvMini').textContent = firstActive
    ? `← ${firstActive.peerLabel}`
    : firstPending
      ? `待确认：${firstPending.offer.fromDevice}`
      : lobbyCode
        ? `监听中：${lobbyCode}`
        : '';
  list.hidden = !drawerOpen;
}
