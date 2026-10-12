// 接收页：附近发现开关 + 待确认 + 接收中
import { $, el, fmtSize, toast } from './ui.js';
import { manager, type PendingOffer } from './tasks.js';
import { createTaskCard, type CardHandle } from './taskcard.js';
import { openPairDialog } from './pairdialog.js';
import { settings } from './store.js';

let inited = false;
const cards = new Map<string, CardHandle>();
let lobbyLeave: (() => void) | null = null;
let lobbyCode: string | null = null;
let lobbyBar: HTMLElement | null = null;

export function initRecvPage(): void {
  if (inited) return;
  inited = true;

  const tgl = $('#discoverToggle') as HTMLInputElement;
  tgl.checked = settings.discoverable;
  tgl.onchange = async () => {
    settings.discoverable = tgl.checked;
    try {
      await manager.setDiscoverable(tgl.checked);
    } catch (e) {
      toast('切换失败：' + (e as Error).message, true);
      tgl.checked = !tgl.checked;
      settings.discoverable = tgl.checked;
    }
  };

  $('#remoteRecvBtn').onclick = () => {
    openPairDialog('recv', async (code) => {
      try {
        if (lobbyLeave) lobbyLeave();
        lobbyLeave = await manager.joinLobby(code);
        lobbyCode = code;
        renderLobbyStatus();
        toast(`正在通过配对码 ${code} 等待对方的传输…`);
      } catch (e) {
        toast('加入配对房间失败：' + (e as Error).message, true);
      }
    });
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
    syncRecvCards();
  });
  renderPending();
  syncRecvCards();
}

/** 离开接收页：断开配对码监听（附近 inbox 常驻不受影响） */
export function blurRecvPage(): void {
  if (lobbyLeave) {
    lobbyLeave();
    lobbyLeave = null;
    lobbyCode = null;
    renderLobbyStatus();
  }
}

function renderLobbyStatus(): void {
  const host = $('#pendingList').parentElement;
  if (!host) return;
  if (lobbyCode && !lobbyBar) {
    lobbyBar = el('div', 'hint');
    lobbyBar.style.margin = '0 0 8px';
    host.insertBefore(lobbyBar, $('#pendingList'));
  }
  if (!lobbyCode && lobbyBar) {
    lobbyBar.remove();
    lobbyBar = null;
    return;
  }
  if (lobbyBar && lobbyCode) {
    lobbyBar.innerHTML = '';
    lobbyBar.append(`配对码 ${lobbyCode} 监听中… `);
    const off = el('button', 'linkbtn', '断开') as HTMLButtonElement;
    off.onclick = () => {
      if (lobbyLeave) lobbyLeave();
      lobbyLeave = null;
      lobbyCode = null;
      renderLobbyStatus();
    };
    lobbyBar.appendChild(off);
  }
}

function renderPending(): void {
  renderLobbyStatus();
  const host = $('#pendingList');
  host.innerHTML = '';
  const offers = [...manager.pending.values()].sort((a, b) => b.at - a.at);
  if (offers.length === 0) {
    host.appendChild(el('div', 'empty', '暂无待确认的传输'));
    return;
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

function syncRecvCards(): void {
  const host = $('#recvTasks');
  const tasks = [...manager.recvTasks.values()].reverse();
  if (tasks.length === 0 && cards.size === 0) {
    host.innerHTML = '';
    host.appendChild(el('div', 'empty', '暂无正在接收的任务'));
    return;
  }
  // 清掉 empty 占位
  const emptyEl = host.querySelector('.empty');
  if (emptyEl && tasks.length > 0) emptyEl.remove();
  for (const t of tasks) {
    let h = cards.get(t.taskId);
    if (!h) {
      h = createTaskCard(t, 'recv');
      cards.set(t.taskId, h);
    }
    host.appendChild(h.root);
    h.update();
  }
  for (const [id, h] of [...cards]) {
    if (!manager.recvTasks.has(id)) {
      h.destroy();
      cards.delete(id);
    }
  }
  if (host.children.length === 0) {
    host.appendChild(el('div', 'empty', '暂无正在接收的任务'));
  }
}
