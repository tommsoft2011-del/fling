// 远程配对弹窗：只负责收集配对码，等待状态由任务卡承担
import { $ } from './ui.js';
import { normalizeCode } from './net.js';

export type PairMode = 'send' | 'recv';

let onOk: ((code: string) => void) | null = null;

export function openPairDialog(mode: PairMode, ok: (code: string) => void): void {
  onOk = ok;
  $('#pairTitle').textContent = mode === 'send' ? '用配对码发送' : '用配对码接收';
  $('#pairDesc').textContent =
    mode === 'send'
      ? '和对方输入相同的配对码，对方确认后开始传输'
      : '输入对方告诉你的配对码，查看对方发来的传输';
  const input = $('#pairCode') as HTMLInputElement;
  input.value = '';
  $('#pairStatus').textContent = '';
  ($('#pairModal') as HTMLElement).hidden = false;
  setTimeout(() => input.focus(), 50);
}

export function closePairDialog(): void {
  ($('#pairModal') as HTMLElement).hidden = true;
  onOk = null;
}

export function initPairDialog(): void {
  $('#pairCancel').onclick = closePairDialog;
  const submit = () => {
    const code = normalizeCode(($('#pairCode') as HTMLInputElement).value);
    if (code.length < 4) {
      $('#pairStatus').textContent = '配对码至少 4 位';
      return;
    }
    const cb = onOk;
    closePairDialog();
    cb?.(code);
  };
  $('#pairOk').onclick = submit;
  ($('#pairCode') as HTMLInputElement).onkeydown = (e) => {
    if (e.key === 'Enter') submit();
    if (e.key === 'Escape') closePairDialog();
  };
  $('#pairModal').addEventListener('click', (e) => {
    if (e.target === $('#pairModal')) closePairDialog();
  });
}
