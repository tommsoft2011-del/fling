// 历史页：按传输任务成组，可展开，部分失败可单独重试
import { invoke } from '@tauri-apps/api/core';
import { writeText as writeClipboardText } from '@tauri-apps/plugin-clipboard-manager';
import { $, el, fmtSize, fmtClock, toast } from './ui.js';
import { loadHistory, type HistoryEntry, type HistoryItem } from './store.js';
import { manager } from './tasks.js';
import { setComposeText } from './send.js';

type Filter = 'all' | 'send' | 'recv' | 'clip';
let inited = false;
let filter: Filter = 'all';

export function initHistPage(): void {
  if (inited) return;
  inited = true;
  $('#histFilters').addEventListener('click', (e) => {
    const b = (e.target as HTMLElement).closest('button');
    if (!b) return;
    filter = b.getAttribute('data-f') as Filter;
    const btns = $('#histFilters').querySelectorAll('button');
    btns.forEach((x) => x.classList.toggle('active', x === b));
    renderHist();
  });
  manager.onChange(() => renderHist());
  renderHist();
}

export function refreshHistPage(): void {
  renderHist();
}

function goSend(): void {
  document.dispatchEvent(new CustomEvent('fling:nav', { detail: 'send' }));
}

function renderHist(): void {
  const host = $('#histList');
  host.innerHTML = '';
  const list = loadHistory().filter((e) => {
    if (filter === 'send') return e.dir === 'send';
    if (filter === 'recv') return e.dir === 'recv';
    if (filter === 'clip') return e.items.some((i) => i.kind === 'text' || i.kind === 'clipboard-image');
    return true;
  });
  if (list.length === 0) {
    host.appendChild(el('div', 'empty', '暂无历史记录'));
    return;
  }
  for (const e of list) host.appendChild(renderEntry(e));
}

function kindBadge(kind: HistoryItem['kind']): string {
  const label = kind === 'text' ? '文本' : kind === 'clipboard-image' ? '剪贴板' : '文件';
  return `<span class="kb ${kind === 'text' ? 'text' : kind === 'clipboard-image' ? 'clip' : 'file'}">${label}</span>`;
}

function renderEntry(e: HistoryEntry): HTMLElement {
  const wrap = el('div', 'hitem');
  const main = el('div', 'hmain');
  const dir = el('div', 'hdir ' + e.dir, e.dir === 'send' ? '↑' : '↓');
  const body = el('div', 'hbody');
  const act = el('div', 'hact');

  // ---- 标题行 ----
  const l1 = el('div', 'hl1');
  l1.appendChild(el('b', '', e.peer));
  const counts = new Map<string, number>();
  for (const i of e.items) counts.set(i.kind, (counts.get(i.kind) ?? 0) + 1);
  if (e.items.length === 1) {
    const tmp = el('span', '');
    tmp.innerHTML = kindBadge(e.items[0].kind);
    l1.appendChild(tmp);
  } else {
    for (const [k, n] of counts) {
      const label = k === 'text' ? `${n} 段文本` : k === 'clipboard-image' ? '剪贴板' : `${n} 个文件`;
      const tmp = el('span', '');
      tmp.innerHTML = `<span class="kb ${k === 'text' ? 'text' : k === 'clipboard-image' ? 'clip' : 'file'}">${label}</span>`;
      l1.appendChild(tmp);
    }
  }
  const okCount = e.items.filter((i) => i.status === 'ok').length;
  if (okCount < e.items.length) {
    l1.appendChild(el('span', 'hfail', `${okCount}/${e.items.length} 成功`));
  }
  l1.appendChild(el('span', 'ht', fmtClock(e.time)));

  // ---- 摘要行 ----
  const l2 = el('div', 'hl2');
  if (e.items.length === 1) {
    l2.appendChild(singleSummary(e.items[0], e.dir));
  } else {
    l2.textContent =
      e.dir === 'send'
        ? `共 ${fmtSize(e.totalBytes)} · ${okCount === e.items.length ? '发送成功' : `${e.items.length - okCount} 项失败`}`
        : `共 ${fmtSize(e.totalBytes)} · ${okCount === e.items.length ? '已保存到接收目录' : `${e.items.length - okCount} 项失败`}`;
  }
  body.append(l1, l2);

  // ---- 批次明细（可展开） ----
  if (e.items.length > 1) {
    const batch = el('div', 'hbatch') as HTMLElement;
    batch.hidden = true;
    for (const it of e.items) {
      const row = el('div', 'hbrow');
      const fn = el('span', 'fn', it.name);
      fn.title = it.name;
      row.append(fn, el('span', '', fmtSize(it.size)));
      if (it.status === 'ok') {
        row.appendChild(el('span', 'ok', '✓'));
      } else {
        const bad = el('span', 'bad', '✗');
        bad.title = it.error ?? '失败';
        row.appendChild(bad);
      }
      // 剪贴板/文本条目：行内复制
      if ((it.kind === 'text' || it.kind === 'clipboard-image') && it.status === 'ok') {
        const mini = el('button', 'mini', '复制') as HTMLButtonElement;
        mini.onclick = (ev) => {
          ev.stopPropagation();
          void copyItem(it);
        };
        if (it.kind === 'clipboard-image' && !it.imageB64) mini.remove();
        else row.appendChild(mini);
      }
      batch.appendChild(row);
    }
    body.appendChild(batch);
    const exp = el('button', '', '展开 ▾') as HTMLButtonElement;
    exp.onclick = () => {
      batch.hidden = !batch.hidden;
      exp.textContent = batch.hidden ? '展开 ▾' : '收起 ▴';
    };
    act.appendChild(exp);
  }

  // ---- 操作按钮 ----
  if (e.dir === 'recv') {
    const open = el('button', '', '打开文件夹') as HTMLButtonElement;
    open.onclick = async () => {
      try {
        const dirPath = (await invoke('get_download_dir')) as string;
        await invoke('open_folder', { path: dirPath });
      } catch (err) {
        toast('打开失败：' + (err as Error).message, true);
      }
    };
    act.appendChild(open);
  } else {
    // 发送方：失败项重试（本 session 有源才可点）
    const failed = e.items.filter((i) => i.status === 'fail');
    if (failed.length > 0 && manager.recentSources.has(e.taskId)) {
      const retry = el('button', '', `重试失败项 (${failed.length})`) as HTMLButtonElement;
      retry.onclick = () => manager.retryFailed(e.taskId);
      act.appendChild(retry);
    }
    if (e.items.length === 1 && e.items[0].kind === 'text' && e.items[0].text) {
      const re = el('button', '', '重发') as HTMLButtonElement;
      re.onclick = () => {
        setComposeText(e.items[0].text ?? '');
        goSend();
        toast('已填入发送框，请选择目标后发送');
      };
      act.appendChild(re);
    }
  }
  // 单条剪贴板/文本：复制
  if (e.items.length === 1 && e.items[0].status === 'ok') {
    const it = e.items[0];
    if (it.kind === 'text' || (it.kind === 'clipboard-image' && it.imageB64)) {
      const cp = el('button', '', '复制到剪贴板') as HTMLButtonElement;
      cp.onclick = () => void copyItem(it);
      act.appendChild(cp);
    }
  }

  main.append(dir, body, act);
  wrap.appendChild(main);
  return wrap;
}

function singleSummary(it: HistoryItem, dir: 'send' | 'recv'): HTMLElement {
  const span = el('span', '');
  if (it.kind === 'text') {
    const t = it.text ?? '';
    span.textContent = `“${t.slice(0, 40)}${t.length > 40 ? '…' : ''}” · ${t.length} 字`;
  } else if (it.kind === 'clipboard-image') {
    const th = el('span', 'hthumb');
    if (it.imageB64) {
      const img = document.createElement('img');
      img.src = 'data:image/png;base64,' + it.imageB64;
      th.appendChild(img);
    }
    th.append(`剪贴板图片 · ${fmtSize(it.size)}`);
    span.appendChild(th);
  } else {
    span.textContent = `${it.name} · ${fmtSize(it.size)} · ${dir === 'send' ? '发送成功' : '已保存到接收目录'}`;
  }
  if (it.status === 'fail') {
    span.append(` · `);
    span.appendChild(el('span', 'hfail', `失败：${it.error ?? '未知'}`));
  }
  return span;
}

async function copyItem(it: HistoryItem): Promise<void> {
  try {
    if (it.kind === 'text') {
      await writeClipboardText(it.text ?? '');
    } else if (it.kind === 'clipboard-image' && it.imageB64) {
      await invoke('write_image_clipboard', { data_b64: it.imageB64 });
    } else {
      return;
    }
    toast('已复制到剪贴板');
  } catch (e) {
    toast('复制失败：' + (e as Error).message, true);
  }
}
