import assert from 'node:assert/strict';
import { createTaskRow } from '../src/taskcard.js';
import type { SendTask, RecvTask } from '../src/tasks.js';

// Execute the production task-card renderer with a minimal DOM, not a copy of
// its state logic. This does not claim Windows/WebView visual E2E coverage.
class ElementMock {
  className = ''; textContent = ''; title = ''; hidden = false; scrollTop = 0;
  style: Record<string, string> = {}; children: ElementMock[] = [];
  onclick?: (event: { target: ElementMock }) => void;
  classList = { add: (...names: string[]) => { this.className += ' ' + names.join(' '); }, toggle: (_name: string, _enabled: boolean) => {} };
  append(...children: ElementMock[]) { this.children.push(...children); }
  appendChild(child: ElementMock) { this.children.push(child); return child; }
  replaceChildren(...children: ElementMock[]) { this.children = children; }
  setAttribute(_name: string, _value: string) {}
  remove() {}
  closest(selector: string) { return this.className.split(' ').includes(selector.slice(1)) ? this : null; }
  querySelector(selector: string): ElementMock | null {
    for (const child of this.children) {
      if (child.closest(selector)) return child;
      const nested = child.querySelector(selector); if (nested) return nested;
    }
    return null;
  }
}
Object.assign(globalThis, { document: { createElement: () => new ElementMock() } });
const checks: string[] = [];
function check(value: unknown, message: string) { assert.ok(value, message); checks.push(message); }
for (const direction of ['send', 'recv'] as const) {
  const task = {
    peerLabel: 'Test peer', taskId: 'mock-task', active: true, phase: direction === 'send' ? 'sending' : 'receiving',
    nativeSingleFile: true, offer: { nativeSingleFile: true }, nativeStage: 'fallback_requested', nativeRoute: undefined,
    doneBytes: direction === 'send' ? 900 : 0, totalBytes: 1000, speedBps: 100000,
    peerDoneBytes: 0, peerSavedBytes: 0, peerTotalBytes: 1000, peerRateBps: 100000,
    p2p: 'connecting', items: [{ kind: 'file', name: 'test.bin', size: 1000, totalBytes: 1000, doneBytes: 0, saveBytes: 0, status: 'active' }],
    cancel() {},
  };
  const card = createTaskRow(task as unknown as SendTask | RecvTask, direction);
  const root = card.root as unknown as ElementMock;
  const get = (selector: string) => { const value = root.querySelector(selector); assert.ok(value); return value; };
  try {
    check(get('.tpct').textContent.includes('正在建立安全连接'), `${direction}: fallback request remains connecting`);
    check(!get('.tpeer').textContent.includes('中继'), `${direction}: fallback intent is not a confirmed relay`);
    check(get('.trow-bars').hidden && get('.teta').textContent === '', `${direction}: no progress bar or ETA before real progress`);
    get('.trow-top').onclick!({ target: get('.trow-top') });
    check(!get('.trow-detail').querySelector('.tprogress-text'), `${direction}: expanded file does not invent zero percent`);
    task.nativeStage = 'transferring'; card.update();
    check(get('.trow-bars').hidden && !get('.tpct').textContent.includes('%'), `${direction}: channel establishment alone does not create progress`);
    // The native bridge supplies this only for Rust's relay_established event.
    task.nativeRoute = 'relay' as never; card.update();
    check(get('.tpeer').textContent.includes('TURN'), `${direction}: native-confirmed relay gets a relay label`);
    task.peerDoneBytes = 100; task.peerSavedBytes = 100; task.doneBytes = 100;
    task.items[0]!.doneBytes = 100; task.items[0]!.saveBytes = 100; card.update();
    check(!get('.trow-bars').hidden && get('.tpct').textContent.includes('10%') && get('.teta').textContent.includes('剩余'), `${direction}: actual peer/receive progress enables percent and ETA`);
    Object.assign(task, { active: false, phase: 'error', nativeStage: 'error', error: '连接未建立：安全连接未能建立，请重试。可重试。' });
    card.update();
    check(get('.tpct').textContent.includes('连接未建立') && !get('.tpct').textContent.includes('文件传输失败'), `${direction}: connection error is not mislabeled as file failure`);
    check(get('.retry-failed').hidden === (direction !== 'send'), `${direction}: sender retry remains available; receiver asks sender`);
    if (direction === 'recv') check(get('.tpct').textContent.includes('请让发送方重试'), 'receiver offers actionable sender-retry guidance');
  } finally { card.destroy(); }
}
for (const message of checks) console.log('ok - ' + message);
console.log(`PASS ${checks.length} production-renderer connection UI checks (mock DOM, not Windows E2E)`);
