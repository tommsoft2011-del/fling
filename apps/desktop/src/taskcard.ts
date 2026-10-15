// 任务行：紧凑单行渲染（底部抽屉 / 接收中分组用）
// DOM 只建一次，之后原地更新
import type { SendTask, RecvTask } from './tasks.js';
import { fmtSize, fmtSpeed, el } from './ui.js';
import { replaceChildrenPreservingScroll } from './dom-utils.js';

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
  // 发送端主进度采用接收端 append 确认值，另列真实网络接收进度。
  const bars = el('div', 'trow-bars');
  const totalRow = el('div', 'tbar-row');
  const totalLabel = el('span', 'tfile', dir === 'send' ? '对方已写入' : '总计');
  const totalBar = el('div', 'pbar thin');
  const totalFill = el('div', 'pfill');
  totalBar.appendChild(totalFill);
  const eta = el('span', 'teta', '');
  totalRow.append(totalLabel, totalBar, eta);
  bars.append(totalRow);
  const receivedRow = dir === 'send' ? el('div', 'tbar-row') : undefined;
  const receivedFill = dir === 'send' ? el('div', 'pfill') : undefined;
  if (receivedRow && receivedFill) {
    const receivedBar = el('div', 'pbar thin');
    receivedBar.appendChild(receivedFill);
    receivedRow.append(el('span', 'tfile', '对方已接收'), receivedBar, el('span', 'tfpct', '0%'));
    bars.append(receivedRow);
  }
  // 文件列表：点击上行展开/收起，每个文件自带进度条
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
    const content: Node[] = [];
    const savingProgress = dir === 'recv' ? (task as RecvTask).savingProgress : undefined;
    // "正在传送 x/y" 提示
    const activeIdx = task.items.findIndex((i) => i.status === 'active');
    const doneCount = task.items.filter((i) => i.status === 'done').length;
    if (task.active) {
      const hint = el('div', 'tdetail-hint',
        dir === 'send' && (task as SendTask).finalizing ? '对方已接收，正在校验并最终保存…'
          : savingProgress && activeIdx >= 0 ? `正在保存 ${activeIdx + 1}/${task.items.length}`
          : activeIdx >= 0 ? `正在传送 ${activeIdx + 1}/${task.items.length}`
            : `已完成 ${doneCount}/${task.items.length}`);
      content.push(hint);
    }
    // 文件按顺序发送：分别把接收确认和 Rust append 确认分配到文件行。
    let remainingReceived = dir === 'send' ? (task as SendTask).peerDoneBytes : -1;
    let remainingSaved = dir === 'send' ? (task as SendTask).peerSavedBytes : -1;
    for (const it of task.items) {
      const r = el('div', 'tdetail-row');
      let fileDoneB = it.doneBytes;
      let fileReceivedB = it.doneBytes;
      let confirmedDone = it.status === 'done';
      const saving = dir === 'recv' && it.saving === true;
      if (saving) fileDoneB = it.saveBytes ?? 0;
      if (remainingReceived >= 0) {
        fileReceivedB = Math.max(0, Math.min(it.totalBytes, remainingReceived));
        remainingReceived -= fileReceivedB;
        const itSaved = Math.max(0, Math.min(it.totalBytes, remainingSaved));
        remainingSaved -= itSaved;
        fileDoneB = itSaved;
        // 只有 Rust append 确认、且文件最终 ACK 完成后才显示完成。
        confirmedDone = itSaved >= it.totalBytes && it.status === 'done';
      } else if (dir !== 'send') {
        confirmedDone = it.status === 'done';
      }
      const st = confirmedDone ? '✓' : it.status === 'error' ? '✗' : it.status === 'active' ? '…' : '○';
      const p = it.totalBytes > 0 ? Math.min(100, Math.round((fileDoneB / it.totalBytes) * 100)) : 0;
      const ptxt = dir === 'send' ? `写入 ${fmtSize(fileDoneB)} · 收到 ${fmtSize(fileReceivedB)}`
        : saving ? `${fmtSize(fileDoneB)} / ${fmtSize(it.totalBytes)}`
        : confirmedDone ? '100%' : it.status === 'error' ? (it.error ?? '失败') : p + '%';
      const bar = el('div', 'pbar thin');
      const fill = el('div', 'pfill');
      fill.style.width = (confirmedDone ? 100 : p) + '%';
      bar.appendChild(fill);
      r.append(
        el('span', 'tdot ' + (confirmedDone ? 'done' : it.status), st),
        el('span', 'tname', it.name),
        el('span', 'tprog', ptxt),
        bar,
        el('span', 'tsize', fmtSize(it.size)),
      );
      (r.querySelector('.tname') as HTMLElement).title = it.name;
      content.push(r);
    }
    replaceChildrenPreservingScroll(detail, content);
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

    // 主进度：发送端以接收端的 Rust append 确认作为已写入，不采纳本机 send() 数字。
    let doneB = task.doneBytes;
    let totalB = task.totalBytes;
    let receivedB = doneB;
    if (dir === 'send') {
      const sendTask = task as SendTask;
      const pd = sendTask.peerSavedBytes;
      const pt = sendTask.peerTotalBytes;
      receivedB = sendTask.peerDoneBytes;
      doneB = pd;
      if (pt > 0) {
        totalB = pt;
      }
    }
    const pctNum = phase === 'done' ? 100 : totalB > 0 ? Math.min(100, Math.round((doneB / totalB) * 100)) : 0;
    totalFill.style.width = pctNum + '%';
    if (receivedFill && receivedRow) {
      const receivedPct = totalB > 0 ? Math.min(100, Math.round((receivedB / totalB) * 100)) : 0;
      receivedFill.style.width = receivedPct + '%';
      (receivedRow.querySelector('.tfpct') as HTMLElement).textContent = `${receivedPct}%`;
    }
    totalBar.classList.toggle('live', active && (phase === 'sending' || (phase as string) === 'receiving'));
    // 右上角总百分比
    pct.textContent = pctNum + '%';

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
    } else if (dir === 'recv' && (task as RecvTask).savingProgress) {
      const saving = (task as RecvTask).savingProgress!;
      status = `保存中 ${fmtSize(saving.bytes)} / ${fmtSize(saving.totalBytes)}`;
    } else if (phase === 'sending' || (phase as string) === 'receiving') {
      // 字节推进跟踪：3s 无推进速度显示为 0；15s 无推进显示"停滞"
      const sendTask = dir === 'send' ? task as SendTask : undefined;
      const activityBytes = sendTask ? sendTask.peerDoneBytes : doneB;
      if (activityBytes !== lastDoneB) {
        lastDoneB = activityBytes;
        lastAdvanceT = Date.now();
      }
      const idleMs = Date.now() - lastAdvanceT;
      const effSpeed = idleMs > 3000 ? 0 : task.speedBps;
      if (sendTask?.finalizing) {
        status = '对方已接收，正在校验并最终保存…';
      } else if (idleMs > 15000 && pctNum < 100 && !(sendTask && receivedB >= totalB)) {
        status = `⚠ 停滞 ${pctNum}%`;
        statusCls = 'bad';
      } else if (sendTask) {
        const receivedPct = totalB > 0 ? Math.min(100, Math.round((receivedB / totalB) * 100)) : 0;
        status = `写入 ${pctNum}% · 已收 ${receivedPct}%` + (effSpeed > 0 ? ` · ${fmtSpeed(effSpeed)}` : '');
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
