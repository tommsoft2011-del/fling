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
  // 上行：基本信息
  const top = el('div', 'trow-top');
  const arrow = el('span', 'tdir', dir === 'send' ? '→' : '←');
  const peer = el('span', 'tpeer', task.peerLabel);
  const sum = el('span', 'tsum', summarizeItems(task));
  const pct = el('span', 'tpct', '');
  const closeBtn = el('button', 'x', '✕') as HTMLButtonElement;
  closeBtn.title = '取消';
  top.append(arrow, peer, sum, pct, closeBtn);
  // 下方：双进度条纵向排列（当前文件在上，总进度在下）与预计剩余时间
  const bars = el('div', 'trow-bars');
  const fileRow = el('div', 'tbar-row');
  const fileLabel = el('span', 'tfile', '');
  const fileBar = el('div', 'pbar thin');
  const fileFill = el('div', 'pfill');
  fileBar.appendChild(fileFill);
  const filePct = el('span', 'tfpct', '');
  fileRow.append(fileLabel, fileBar, filePct);
  const totalRow = el('div', 'tbar-row');
  const totalLabel = el('span', 'tfile', '总计');
  const totalBar = el('div', 'pbar thin');
  const totalFill = el('div', 'pfill');
  totalBar.appendChild(totalFill);
  const eta = el('span', 'teta', '');
  totalRow.append(totalLabel, totalBar, eta);
  bars.append(fileRow, totalRow);
  // 详情：点击上行展开，显示每个文件的状态/进度
  const detail = el('div', 'trow-detail');
  detail.hidden = true;
  top.style.cursor = 'pointer';
  top.title = '点击展开/收起文件详情';
  top.onclick = (e) => {
    // 点 ✕ 按钮不触发展开
    if ((e.target as HTMLElement).closest('.x')) return;
    detail.hidden = !detail.hidden;
    if (!detail.hidden) renderDetail();
  };
  const renderDetail = () => {
    detail.innerHTML = '';
    for (const it of task.items) {
      const r = el('div', 'tdetail-row');
      const st = it.status === 'done' ? '✓' : it.status === 'error' ? '✗' : it.status === 'active' ? '…' : '○';
      const p = it.totalBytes > 0 ? Math.min(100, Math.round((it.doneBytes / it.totalBytes) * 100)) : 0;
      r.append(
        el('span', 'tdot ' + it.status, st),
        el('span', 'tname', it.name),
        el('span', 'tsize', fmtSize(it.size)),
        el('span', 'tp', it.status === 'done' ? '完成' : it.status === 'error' ? (it.error ?? '失败') : p + '%'),
      );
      (r.querySelector('.tname') as HTMLElement).title = it.name;
      detail.appendChild(r);
    }
  };
  root.append(top, bars, detail);

  let dismissTimer: ReturnType<typeof setTimeout> | undefined;
  let gone = false;
  // 停滞检测：记录字节数最后一次推进的时间
  let lastDoneB = -1;
  let lastAdvanceT = Date.now();

  const update = () => {
    if (gone) return;
    const phase = task.phase;
    const active = task.active;
    // 详情展开时跟随刷新
    if (!detail.hidden) renderDetail();

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
    const pctNum = phase === 'done' ? 100 : totalB > 0 ? Math.min(100, Math.round((doneB / totalB) * 100)) : 0;
    totalFill.style.width = pctNum + '%';
    totalBar.classList.toggle('live', active && (phase === 'sending' || (phase as string) === 'receiving'));

    // 当前文件：找正在传输的条目（active），没有则找第一个未完成的；
    // 若全部发完在等 ack（phase 仍为 sending），用最后一个条目占位，避免进度条消失
    const cur = task.items.find((i) => i.status === 'active')
      ?? task.items.find((i) => i.status === 'waiting')
      ?? (active ? task.items[task.items.length - 1] : undefined);
    const waitingAck = active && task.items.length > 0 && task.items.every((i) => i.status === 'done' || i.status === 'error');
    if (cur) {
      const fp = cur.totalBytes > 0 ? Math.min(100, Math.round((cur.doneBytes / cur.totalBytes) * 100)) : 0;
      fileLabel.textContent = (waitingAck ? '等待对方确认 · ' : '') + (cur.name.length > 28 ? '…' + cur.name.slice(-27) : cur.name);
      fileLabel.title = cur.name;
      fileFill.style.width = fp + '%';
      filePct.textContent = fp + '%';
      bars.style.display = '';
    } else {
      bars.style.display = 'none';
    }

    // 预计剩余时间：按当前速度估算
    const effSpeedForEta = task.speedBps > 0 ? task.speedBps : 0;
    if (active && effSpeedForEta > 0 && totalB > doneB) {
      const sec = Math.ceil((totalB - doneB) / effSpeedForEta);
      eta.textContent = '剩余 ' + (sec >= 3600 ? `${Math.floor(sec / 3600)}:${String(Math.floor(sec / 60) % 60).padStart(2, '0')}:${String(sec % 60).padStart(2, '0')}`
        : sec >= 60 ? `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, '0')}` : `${sec}秒`);
    } else {
      eta.textContent = '';
    }

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
    // 进行中显示取消按钮；失败/完成/取消后显示为消除按钮（之前 error 状态按钮隐藏，失败任务卡死消不掉）
    closeBtn.style.visibility = 'visible';
    closeBtn.title = active ? '取消' : '消除';
    closeBtn.onclick = () => {
      if (active) task.cancel();
      else destroy();
    };

    // P2P 直连标记（紧凑：加到 peer 后）
    const p2p = task.p2p;
    peer.textContent = task.peerLabel + (p2p === 'open' ? ' ⚡' : p2p === 'failed' ? ' (中转)' : '');
    peer.title = p2p === 'open' ? 'P2P 直连' : p2p === 'failed' ? '经服务器中转' : p2p === 'connecting' ? '正在建立直连…' : '';

    // 完成/失败/取消/拒绝后 5s 自动淡出
    if ((phase === 'done' || phase === 'error' || phase === 'cancelled' || (phase as string) === 'rejected') && !dismissTimer) {
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
