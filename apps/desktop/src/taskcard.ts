// 任务行：紧凑单行渲染（底部抽屉 / 接收中分组用）
// DOM 只建一次，之后原地更新
import type { SendTask, RecvTask } from './tasks.js';
import { fmtSize, fmtSpeed, el } from './ui.js';

type AnyTask = SendTask | RecvTask;

export interface CardHandle {
  root: HTMLElement;
  update: () => void;
  destroy: () => void;
  /** 行是否已被自动消除（5s 淡出后）；sync 循环不应再把它加回 DOM */
  isGone: () => boolean;
}

function summarizeItems(task: AnyTask): string {
  const items = task.items;
  const nText = items.filter((i) => i.kind === 'text').length;
  const nClip = items.filter((i) => i.kind === 'clipboard-image').length;
  const nFile = items.filter((i) => i.kind === 'file').length;
  const parts: string[] = [];
  if (nText) parts.push(nText > 1 ? `${nText} 段文本` : '文本');
  if (nClip) parts.push(nClip > 1 ? `${nClip} 张图片` : '图片');
  if (nFile) {
    const nm = items.find((i) => i.kind === 'file')?.name ?? '';
    parts.push(nFile > 1 ? `${nFile} 个文件` : nm || '文件');
  }
  return parts.join(' + ') || '传输';
}

export function createTaskRow(task: AnyTask, dir: 'send' | 'recv'): CardHandle {
  const root = el('div', 'trow');
  const arrow = el('span', 'tdir', dir === 'send' ? '→' : '←');
  const peer = el('span', 'tpeer', task.peerLabel);
  const sum = el('span', 'tsum', summarizeItems(task));
  const bar = el('div', 'pbar');
  const fill = el('div', 'pfill');
  bar.appendChild(fill);
  const pct = el('span', 'tpct', '');
  const closeBtn = el('button', 'x', '✕') as HTMLButtonElement;
  closeBtn.title = '取消';
  closeBtn.onclick = () => task.cancel();
  root.append(arrow, peer, sum, bar, pct, closeBtn);

  let dismissTimer: ReturnType<typeof setTimeout> | undefined;
  let gone = false;
  // 停滞检测：记录字节数最后一次推进的时间
  let lastDoneB = -1;
  let lastAdvanceT = Date.now();

  const update = () => {
    if (gone) return;
    const phase = task.phase;
    const active = task.active;

    // 进度：发送端优先用对方真实接收进度
    let doneB = task.doneBytes;
    let totalB = task.totalBytes;
    if (dir === 'send') {
      const pd = (task as SendTask).peerDoneBytes ?? 0;
      const pt = (task as SendTask).peerTotalBytes ?? 0;
      if (pt > 0) {
        doneB = pd;
        totalB = pt;
      }
    }
    const pctNum = totalB > 0 ? Math.min(100, Math.round((doneB / totalB) * 100)) : 0;
    fill.style.width = pctNum + '%';
    bar.classList.toggle('live', active && (phase === 'sending' || (phase as string) === 'receiving'));

    let status: string;
    let statusCls = '';
    if (phase === 'preparing' || phase === 'waiting') {
      status = dir === 'send' ? (task as SendTask).waitLabel + '…' : '连接中…';
    } else if (phase === 'sending' || (phase as string) === 'receiving') {
      // 字节推进跟踪：3s 无推进速度显示为 0；15s 无推进显示"停滞"
      if (doneB !== lastDoneB) {
        lastDoneB = doneB;
        lastAdvanceT = Date.now();
      }
      const idleMs = Date.now() - lastAdvanceT;
      const effSpeed = idleMs > 3000 ? 0 : task.speedBps;
      if (idleMs > 15000 && pctNum < 100) {
        status = `⚠ 停滞 ${pctNum}%`;
        statusCls = 'bad';
      } else {
        status = `${pctNum}%` + (effSpeed > 0 ? ` · ${fmtSpeed(effSpeed)}` : '');
      }
    } else if (phase === 'done') {
      status = '✓ 完成';
      statusCls = 'ok';
    } else if (phase === 'error') {
      status = '✗ ' + (task.error ?? '失败');
      statusCls = 'bad';
    } else if (phase === 'cancelled') {
      status = '已取消';
    } else if ((phase as string) === 'rejected') {
      status = '对方拒绝了传输';
      statusCls = 'bad';
    } else {
      status = String(phase);
    }
    pct.textContent = status;
    pct.className = 'tpct ' + statusCls;
    sum.textContent = summarizeItems(task) + ` · ${fmtSize(totalB)}`;
    closeBtn.style.visibility = active ? 'visible' : 'hidden';

    // P2P 直连标记（紧凑：加到 peer 后）
    const p2p = task.p2p;
    peer.textContent = task.peerLabel + (p2p === 'open' ? ' ⚡' : p2p === 'failed' ? ' (中转)' : '');
    peer.title = p2p === 'open' ? 'P2P 直连' : p2p === 'failed' ? '经服务器中转' : p2p === 'connecting' ? '正在建立直连…' : '';

    // 完成/取消/拒绝后 5s 自动淡出
    if ((phase === 'done' || phase === 'cancelled' || (phase as string) === 'rejected') && !dismissTimer) {
      dismissTimer = setTimeout(() => {
        root.classList.add('fading');
        setTimeout(() => {
          gone = true;
          clearInterval(tick);
          root.remove();
          root.dispatchEvent(new CustomEvent('cardgone', { bubbles: true }));
        }, 450);
      }, 5000);
    }
  };

  const tick = setInterval(() => {
    if (task.active) update();
  }, 1000);

  const destroy = () => {
    gone = true;
    clearInterval(tick);
    if (dismissTimer) clearTimeout(dismissTimer);
    root.remove();
  };

  update();
  return { root, update, destroy, isGone: () => gone };
}
