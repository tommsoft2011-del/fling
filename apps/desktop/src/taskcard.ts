// 发送/接收任务卡（DOM 只建一次，之后原地更新）
import type { SendTask, RecvTask, ItemState } from './tasks.js';
import { fmtSize, fmtSpeed, fmtDur, el } from './ui.js';

type AnyTask = SendTask | RecvTask;

function kindBadge(kind: ItemState['kind']): string {
  return kind === 'text' ? 'text' : kind === 'clipboard-image' ? 'clip' : 'file';
}
function kindName(kind: ItemState['kind']): string {
  return kind === 'text' ? '文本' : kind === 'clipboard-image' ? '剪贴板' : '文件';
}

export interface CardHandle {
  root: HTMLElement;
  update: () => void;
  destroy: () => void;
}

export function createTaskCard(task: AnyTask, dir: 'send' | 'recv'): CardHandle {
  const root = el('div', 'task');
  const title = el('div', 'task-t');
  const steps = el('div', 'steps');
  const itemsBox = el('div', 'titems');
  const meta = el('div', 'tmeta');

  const closeBtn = el('button', 'x', '✕');
  closeBtn.title = '关闭';
  closeBtn.onclick = () => task.cancel();
  title.append(
    el('span', '', dir === 'send' ? '发送' : '接收'),
    el('span', 'tpeer', (dir === 'send' ? '→ ' : '← ') + task.peerLabel),
    closeBtn,
  );

  const stepDefs =
    dir === 'send'
      ? [(task as SendTask).waitLabel, '传输中', '完成']
      : ['连接中', '接收中', '完成'];
  const stepEls = stepDefs.map((s) => el('span', 'step', s));
  stepEls.forEach((s) => steps.appendChild(s));

  interface RowRefs {
    state: ItemState;
    row: HTMLElement;
    status: HTMLElement;
    fill: HTMLElement;
    bar: HTMLElement;
  }
  const rows: RowRefs[] = [];
  for (const it of task.items) {
    const row = el('div', 'titem');
    const top = el('div', 'trow');
    const badge = el('span', 'kb ' + kindBadge(it.kind), kindName(it.kind));
    const name = el('span', 'tname', it.name);
    const size = el('span', 'tsize', fmtSize(it.size));
    const status = el('span', '', '');
    top.append(badge, name, size, status);
    const bar = el('div', 'pbar');
    const fill = el('div', 'pfill');
    bar.appendChild(fill);
    row.append(top, bar);
    itemsBox.appendChild(row);
    rows.push({ state: it, row, status, fill, bar });
  }

  root.append(title, steps, itemsBox, meta);

  let dismissTimer: ReturnType<typeof setTimeout> | undefined;
  let gone = false;

  const update = () => {
    if (gone) return;
    const phase = task.phase;
    // 步骤条
    const activeIdx = phase === 'preparing' || phase === 'waiting' ? 0 : phase === 'sending' || (phase as string) === 'receiving' ? 1 : 2;
    stepEls.forEach((s, i) => {
      s.classList.toggle('on', i === activeIdx && (phase === 'preparing' || phase === 'waiting' || phase === 'sending' || (phase as string) === 'receiving'));
      s.classList.toggle('done', phase === 'done' || i < activeIdx);
      s.classList.toggle('pulse', i === activeIdx && task.active);
    });
    if (phase === 'done') {
      stepEls.forEach((s) => {
        s.classList.remove('on', 'pulse');
        s.classList.add('done');
      });
    }
    // 条目
    for (const r of rows) {
      const it = r.state;
      const pct = it.totalBytes > 0 ? Math.min(100, Math.round((it.doneBytes / it.totalBytes) * 100)) : 0;
      r.fill.style.width = pct + '%';
      r.bar.classList.toggle('live', it.status === 'active');
      if (it.status === 'done') {
        r.status.innerHTML = '';
        r.status.appendChild(el('span', 'tok', '✓'));
      } else if (it.status === 'error') {
        r.status.innerHTML = '';
        const b = el('span', 'tbad', '✗');
        b.title = it.error ?? '失败';
        r.status.appendChild(b);
      } else if (it.status === 'active') {
        r.status.textContent = pct + '%';
      } else {
        r.status.textContent = '等待中';
      }
    }
    // 底部信息
    const secs = ((task.endedAt ?? Date.now()) - task.startedAt) / 1000;
    let metaHtml = '';
    if (task.active && task.speedBps > 0) metaHtml += `<span>${fmtSpeed(task.speedBps)}</span>`;
    metaHtml += `<span>共 ${fmtSize(task.totalBytes)}</span><span>用时 ${fmtDur(secs)}</span>`;
    // 发送端：显示对方真实接收进度（而非本地"已发送"，后者只是进了缓冲）
    if (dir === 'send' && task.active) {
      const peerDone = (task as SendTask).peerDoneBytes ?? 0;
      const peerTotal = (task as SendTask).peerTotalBytes ?? 0;
      if (peerTotal > 0) {
        const pct = Math.min(100, Math.round((peerDone / peerTotal) * 100));
        metaHtml += `<span>对方已接收 ${pct}%</span>`;
      }
    }
    // P2P 直连状态（方便确认走的是直连还是中继）
    const p2p = task.p2p;
    if (p2p === 'open') metaHtml += `<span class="done-check">⚡P2P直连</span>`;
    else if (p2p === 'connecting') metaHtml += `<span>正在建立直连…</span>`;
    else if (p2p === 'failed') metaHtml += `<span>中转</span>`;
    if (phase === 'done') metaHtml += `<span class="done-check">✓ 完成</span>`;
    if (phase === 'error') metaHtml += `<span style="color:var(--danger)">✗ ${task.error ?? '失败'}</span>`;
    if (phase === 'cancelled') metaHtml += `<span>已取消</span>`;
    if (phase === 'rejected') metaHtml += `<span style="color:var(--danger)">对方拒绝了传输</span>`;
    meta.innerHTML = metaHtml;

    // 完成/取消后 5s 自动淡出
    if ((phase === 'done' || phase === 'cancelled') && !dismissTimer) {
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
  return { root, update, destroy };
}
