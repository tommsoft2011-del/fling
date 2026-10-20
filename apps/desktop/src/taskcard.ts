// 任务行：紧凑单行渲染（底部抽屉 / 接收中分组用）
// DOM 只建一次，之后原地更新
import type { SendTask, RecvTask } from './tasks.js';
import { fmtSize, fmtSpeed, el } from './ui.js';
import { replaceChildrenPreservingScroll } from './dom-utils.js';
import { distributeSequentialProgress, progressForItem, progressPercent } from './file-progress.js';
import { estimateRemainingSeconds } from './peer-progress.js';

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
  const nativeFile = dir === 'send' ? (task as SendTask).nativeSingleFile : (task as RecvTask).offer?.nativeSingleFile;
  const pathLabel = task.items.some((item) => item.kind !== 'text') ? ` · ${nativeFile ? 'Rust 原生单文件' : '兼容路径'}` : '';
  const sum = el('span', 'tsum', summarizeItems(task) + pathLabel);
  const pct = el('span', 'tpct', '');
  const closeBtn = el('button', 'x', '✕') as HTMLButtonElement;
  closeBtn.title = '取消';
  top.append(arrow, peer, sum, pct, closeBtn);
  // 发送端主进度采用接收端累计 ACK；本机 send() 速率不冒充对端进度。
  const bars = el('div', 'trow-bars');
  const totalRow = el('div', 'tbar-row');
  const totalLabel = el('span', 'tfile', dir === 'send' ? '对端累计确认' : '总计');
  const totalBar = el('div', 'pbar thin');
  const totalFill = el('div', 'pfill');
  totalBar.appendChild(totalFill);
  const eta = el('span', 'teta', '');
  totalRow.append(totalLabel, totalBar, eta);
  bars.append(totalRow);
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
    // 文件按顺序发送：把接收确认和 Rust append 确认独立分配到每一行。
    const sentProgress = dir === 'send'
      ? distributeSequentialProgress(task.items, (task as SendTask).peerDoneBytes, (task as SendTask).peerSavedBytes)
      : undefined;
    for (const [index, it] of task.items.entries()) {
      const r = el('div', 'tdetail-row');
      const progress = sentProgress?.[index] ?? progressForItem(it.totalBytes, it.doneBytes, it.saveBytes ?? 0);
      const fileReceivedB = progress.receivedBytes;
      const fileDoneB = dir === 'send' ? progress.savedBytes : it.kind === 'text' ? it.doneBytes : progress.savedBytes;
      let confirmedDone = it.status === 'done';
      if (dir === 'send') {
        // 只有 Rust append 确认、且文件最终 ACK 完成后才显示完成。
        confirmedDone = progress.savedBytes >= it.totalBytes && it.status === 'done';
      } else {
        confirmedDone = it.status === 'done';
      }
      const st = confirmedDone ? '✓' : it.status === 'error' ? '✗' : it.status === 'active' ? '…' : '○';
      if (it.kind !== 'text') {
        r.classList.add('tdetail-file-row');
        const head = el('div', 'tdetail-file-head');
        const name = el('span', 'tname', it.name);
        name.title = it.name;
        head.append(
          el('span', 'tdot ' + (confirmedDone ? 'done' : it.status), st),
          name,
          el('span', 'tsize', fmtSize(it.size)),
        );
        const progressLine = (label: string, bytes: number, complete: boolean) => {
          const percent = progressPercent(it.totalBytes, bytes, complete);
          const line = el('div', 'tdetail-file-progress');
          const bar = el('div', 'pbar thin');
          const fill = el('div', 'pfill');
          fill.style.width = percent + '%';
          bar.setAttribute('role', 'progressbar');
          bar.setAttribute('aria-label', label);
          bar.setAttribute('aria-valuemin', '0');
          bar.setAttribute('aria-valuemax', '100');
          bar.setAttribute('aria-valuenow', String(percent));
          bar.appendChild(fill);
          line.append(
            el('span', 'tfile', label),
            bar,
            el('span', 'tprogress-text', `${percent}% · ${fmtSize(bytes)} / ${fmtSize(it.totalBytes)}`),
          );
          return line;
        };
        r.append(
          head,
          progressLine(dir === 'send' ? '对端累计确认' : '已接收', dir === 'send' ? fileReceivedB : fileDoneB, confirmedDone),
        );
      } else {
        const p = progressPercent(it.totalBytes, fileDoneB, confirmedDone);
        const ptxt = dir === 'send' ? `写入 ${fmtSize(fileDoneB)} · 收到 ${fmtSize(fileReceivedB)}`
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
      }
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

    // 主进度与 ETA 同用对端累计 ACK，绝不拿本机 send() 速度倒推对端完成时间。
    let doneB = task.doneBytes;
    let totalB = task.totalBytes;
    if (dir === 'send') {
      const sendTask = task as SendTask;
      const pd = sendTask.peerSavedBytes;
      const pt = sendTask.peerTotalBytes;
      doneB = pd;
      if (pt > 0) {
        totalB = pt;
      }
    }
    const pctNum = phase === 'done' ? 100 : totalB > 0 ? Math.min(100, Math.round((doneB / totalB) * 100)) : 0;
    totalFill.style.width = pctNum + '%';
    totalBar.classList.toggle('live', active && (phase === 'sending' || (phase as string) === 'receiving'));
    // 右上角总百分比
    pct.textContent = pctNum + '%';

    const sendTaskForEta = dir === 'send' ? task as SendTask : undefined;
    const etaSpeed = sendTaskForEta ? sendTaskForEta.peerRateBps : task.speedBps;
    const remainingSeconds = estimateRemainingSeconds(totalB, doneB, etaSpeed);
    if (active && remainingSeconds !== null) {
      const sec = remainingSeconds;
      eta.textContent = '剩余 ' + (sec >= 3600 ? `${Math.floor(sec / 3600)}:${String(Math.floor(sec / 60) % 60).padStart(2, '0')}:${String(sec % 60).padStart(2, '0')}`
        : sec >= 60 ? `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, '0')}` : `${sec}秒`);
    } else if (active && sendTaskForEta && sendTaskForEta.peerDoneBytes < totalB) {
      eta.textContent = '等待对端进度';
    } else {
      eta.textContent = '';
    }

    let status: string;
    let statusCls = '';
    const nativeLabels: Record<string, string> = {
      waiting: '等待对方加入…', preparing: 'Rust 正在读取并计算 SHA-256…',
      connecting: '正在建立 P2P 直连…', finalizing: '正在校验、同步磁盘并最终确认…',
    };
    if (active && task.nativeStage && (nativeLabels[task.nativeStage] || (task.nativeStage === 'transferring' && (dir === 'send' ? (task as SendTask).peerDoneBytes : task.doneBytes) === 0))) {
      status = nativeLabels[task.nativeStage] ?? '传输已开始，等待首个对端进度…';
    } else if (phase === 'preparing' || phase === 'waiting') {
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
      const progressSpeed = sendTask ? sendTask.peerRateBps : task.speedBps;
      const effSpeed = idleMs > 3000 ? 0 : progressSpeed;
      if (sendTask?.finalizing) {
        status = '对方已接收，正在校验并最终保存…';
      } else if (sendTask?.directProgressStalled && sendTask.p2p === 'open') {
        status = '直连等待对端确认，尚未切换中继';
        statusCls = 'bad';
      } else if (activityBytes > 0 && idleMs > 15000 && pctNum < 100 && !(sendTask && sendTask.peerDoneBytes >= totalB)) {
        status = `⚠ 停滞 ${pctNum}%`;
        statusCls = 'bad';
      } else if (sendTask) {
        status = `对端累计确认 ${pctNum}%` + (effSpeed > 0 ? ` · ${fmtSpeed(effSpeed)}` : '');
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
