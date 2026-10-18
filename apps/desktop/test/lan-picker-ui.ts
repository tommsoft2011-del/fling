import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { mock } from 'bun:test';

// DOM + mocked IPC only. This does not replace native-window or spoken-reader tests.
class Element {
  children: Element[] = [];
  private text = '';
  value: any = '';
  max = 0;
  disabled = false;
  hidden = false;
  open = false;
  className = '';
  type = '';
  tabIndex = 0;
  preventedFocusScroll = false;
  lastActivation: Promise<void> | void = undefined;
  onclick: (() => Promise<void> | void) | null = null;
  onchange: (() => void) | null = null;
  attributes = new Map<string, string>();
  private classes = new Set<string>();
  constructor(public tagName = 'div') {}
  classList = {
    toggle: (name: string, force: boolean) => force ? this.classes.add(name) : this.classes.delete(name),
    contains: (name: string) => this.classes.has(name),
  };
  get textContent(): string { return this.text + this.children.map((child) => child.textContent).join(''); }
  set textContent(value: string) { this.text = value; this.children = []; }
  replaceChildren(...children: Element[]): void { this.text = ''; this.children = children; }
  append(...children: Element[]): void { this.children.push(...children); }
  setAttribute(name: string, value: string): void { this.attributes.set(name, value); }
  focus(options?: { preventScroll?: boolean }): void { this.preventedFocusScroll = options?.preventScroll ?? false; dom.activeElement = this; }
  click(): void { this.lastActivation = this.onclick?.(); }
}
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const stylesheet = readFileSync(new URL('../src/style.css', import.meta.url), 'utf8');
const feedback = html.match(/<aside class="lan-feedback"[^>]*>([\s\S]*?)<\/aside>/)![0];
assert.match(feedback, /id="lanStatus"[^>]*role="status"/);
assert.equal((html.match(/id="lanStatus"/g) ?? []).length, 1);
assert.ok(html.indexOf(feedback) < html.indexOf('<main class="lan-content">'));
assert.doesNotMatch(html.match(/<main class="lan-content">([\s\S]*?)<\/main>/)![1], /id="lanStatus"/);
assert.match(stylesheet, /\.lan-feedback\s*\{[^}]*flex:\s*0 0 auto/);
assert.match(stylesheet, /\.lan-content\s*\{[^}]*min-height:\s*0/);
console.log('ok - one live status outside scrollable content; native bounds tested separately (static)');
const elements = new Map([...html.matchAll(/\bid="([^"]+)"/g)].map((match) => [match[1], new Element()]));
const get = (id: string) => elements.get(id)!;
const dom = {
  activeElement: null as Element | null,
  getElementById: (id: string) => elements.get(id) ?? null,
  createElement: (tag: string) => new Element(tag),
};
Object.assign(globalThis, { document: dom });
get('lanPickSource').textContent = '在本机选择文件';
const events = new Map<string, (event: { payload: any }) => void>();
const calls: { command: string; args: any }[] = [];
let nextPick: Promise<unknown> = Promise.resolve(null);
let nextRefresh: Promise<unknown> = Promise.resolve({ validServices: 0, lanEligiblePeers: 0, uiEmittedPeers: 0 });
let nextExport: Promise<unknown> = Promise.resolve({ saved: false });
let nextTask = 0;
let failUiReport = false;
let currentMockPair = '';
const progress = (taskId: string, phase: string, extra: Record<string, unknown> = {}) =>
  events.get('fling://lan/progress')!({ payload: {
    taskId, phase, receivedBytes: 0, savedBytes: 0, totalBytes: 0, speedBps: 0, ...extra,
  } });
mock.module('@tauri-apps/api/core', () => ({
  invoke: async (command: string, args?: any) => {
    calls.push({ command, args });
    if (command === 'native_lan_start_mode') return { deviceId: 'a', deviceName: 'A' };
    if (command === 'native_lan_pick_source') return await nextPick;
    if (command === 'discovery_refresh') return await nextRefresh;
    if (command === 'discovery_export_diagnostics') return await nextExport;
    if (command === 'discovery_report_ui' && failUiReport) throw new Error('diagnostic-write-failed');
    if (command === 'native_lan_start_send') return `test-send-${++nextTask}`;
    if (command === 'native_lan_pair_decision' && (!args.approved || args.peerCode !== '12345678')) {
      progress(currentMockPair, args.approved ? 'interrupted' : 'cancelled',
        args.approved ? { failureKind: 'close', error: 'secondary control close' } : {});
    }
    if (command === 'native_lan_cancel') progress(args.taskId, 'cancelled');
    if (command === 'native_lan_manifest_decision' && args.accepted) {
      progress('accepted-task', 'ready-to-transfer', { name: 'private-meta.bin', totalBytes: 1048576 });
      // Regression: events can arrive before invoke resolves; UI must not overwrite complete.
      progress('accepted-task', 'complete', { name: 'private-meta.bin', totalBytes: 1048576,
        receivedBytes: 1048576, savedBytes: 1048576 });
    }
    if (command === 'native_lan_ipc_counters') return { totalCalls: 0, totalBytes: 0, byType: {} };
    return undefined;
  },
}));
mock.module('@tauri-apps/api/event', () => ({
  listen: async (name: string, callback: (event: { payload: any }) => void) => {
    events.set(name, callback);
    return () => { events.delete(name); };
  },
}));
const { initLanPage } = await import('../src/lan');
await initLanPage();
assert.equal(get('lanSend').disabled, true);
assert.match(get('lanSendReason').textContent, /请选择附近设备和文件/);
assert.match(get('lanSend').attributes.get('aria-label')!, /请求发送，.*请选择附近设备和文件/);
assert.match(html, /id="lanSend"[^>]*aria-describedby="lanSendReason"/);
const detailsTag = html.match(/<details[^>]*id="lanDiagnostics"[^>]*>/)![0];
assert.doesNotMatch(detailsTag, /\bopen(?:\s|=|>)/);
assert.match(html, /<summary[^>]*>诊断信息<\/summary>/);
console.log('ok - no-device disabled reason and diagnostics default collapsed (mock/static HTML)');

await get('lanRefreshPeers').onclick!();
assert.match(get('lanRefreshStatus').textContent, /本轮搜索结束，仍未发现附近设备/);
assert.doesNotMatch(get('lanRefreshStatus').textContent, /已找到|刷新完成/);
assert.match(get('lanPeerStatus').textContent, /尚未发现附近设备/);
assert.equal(get('lanSend').disabled, true);
assert.equal(get('lanRefreshPeers').disabled, false);
console.log('ok - empty-list refresh retries discovery without claiming a peer was found (mock)');
nextRefresh = Promise.resolve({ validServices: 1, lanEligiblePeers: 0, uiEmittedPeers: 0 });
await get('lanRefreshPeers').onclick!();
assert.match(get('lanRefreshStatus').textContent, /仍未发现附近设备/);
assert.match(get('lanDiscoveryPhases').textContent, /有效服务 1；LAN 可用设备 0；成功提交界面事件的设备 0；当前界面可选条目 0/);
assert.equal(get('lanSend').disabled, true);
console.log('ok - protocol-valid service count is not mistaken for a LAN-eligible/rendered peer (mock)');


events.get('fling://lan/peer-up')!({ payload: { id: 'peer-b', name: 'B' } });
get('lanPeerSelect').value = 'peer-b';
get('lanPeerSelect').onchange!();
assert.match(get('lanSendReason').textContent, /请选择文件/);
const button = get('lanPickSource');
const info = get('lanSourceInfo');
const send = get('lanSend');
function beginPick() {
  let resolve!: (value: unknown) => void;
  let reject!: (error: Error) => void;
  nextPick = new Promise((yes, no) => { resolve = yes; reject = no; });
  const pending = button.onclick!() as Promise<void>;
  assert.equal(button.textContent, '正在选择文件…');
  assert.equal(info.textContent, '正在选择文件…');
  assert.equal(button.disabled, true);
  assert.equal(send.disabled, true);
  assert.match(get('lanSendReason').textContent, /正在选择文件/);
  return { pending, resolve, reject };
}
function settled() {
  assert.equal(button.disabled, false);
  assert.equal(button.textContent, '在本机选择文件');
  assert.equal(dom.activeElement, button);
  assert.equal(button.preventedFocusScroll, false);
  assert.doesNotMatch(info.textContent, /正在选择文件/);
}
let pick = beginPick();
pick.resolve(null);
await pick.pending;
settled();
assert.match(info.textContent, /已取消/);
assert.equal(get('lanStatus').classList.contains('error'), false);
assert.equal(send.disabled, true);
pick = beginPick();
pick.reject(new Error('raw-protocol-error session-secret transfer-secret'));
await pick.pending;
settled();
assert.match(info.textContent, /无法选择文件.*可重试/);
assert.doesNotMatch(info.textContent, /session-secret|transfer-secret|raw-protocol/);
assert.equal(get('lanStatus').classList.contains('error'), true);
pick = beginPick();
assert.equal(get('lanStatus').classList.contains('error'), false);
pick.resolve({ sourceId: 'opaque-source-A' });
await pick.pending;
settled();
assert.match(info.textContent, /文件已准备/);
assert.equal(send.disabled, false);
pick = beginPick();
pick.resolve(null);
await pick.pending;
settled();
assert.match(info.textContent, /保留原文件/);
await send.onclick!();
assert.deepEqual(calls.find((call) => call.command === 'native_lan_start_send')?.args,
  { peerId: 'peer-b', sourceId: 'opaque-source-A' });
assert.ok(calls.filter((call) => call.command === 'native_lan_pick_source').every((call) => call.args === undefined));
console.log('ok - picker cancel/error/success, opaque source IPC, retry and focus restoration (mock)');

function pair(taskId: string) {
  currentMockPair = taskId;
  events.get('fling://lan/pairing')!({ payload: {
    sessionId: 'pair-session', taskId, peerDeviceId: 'peer-b', peerDeviceLabel: 'B',
    verificationCode: '12345678', role: 'responder',
  } });
}
pair('wrong-code-task');
assert.equal(get('lanPairCode').textContent, '1234 5678');
assert.match(get('lanPairCodeSpoken').textContent, /第一组：1 2 3 4。第二组：5 6 7 8/);
assert.match(get('lanSendReason').textContent, /正在核对短码/);
get('lanPeerCode').value = '0000 0000';
await get('lanPairAccept').onclick!();
assert.equal(get('lanPairModal').hidden, true);
assert.equal(get('lanPeerCode').value, '');
assert.equal(get('lanPairCodeSpoken').textContent, '');
assert.equal(get('lanPairCodeSpoken').attributes.get('aria-label'), '');
assert.equal(dom.activeElement, get('lanPeerSelect'));
assert.equal(get('lanPeerSelect').preventedFocusScroll, false);
assert.equal(get('lanPeerSelect').value, '');
assert.match(get('lanStatus').textContent, /短码不一致/);
assert.match(get('lanPeerStatus').textContent, /发现 1 台/);
assert.match(get('lanHistory').textContent, /历史记录 · 未完成/);
assert.doesNotMatch(get('lanHistory').textContent, /连接中断|secondary control close/);
assert.doesNotMatch(get('lanTasks').textContent, /wrong-code-task|短码不一致/);
console.log('ok - grouped visual/spoken text, wrong-code cleanup, device focus and separate history (mock)');

const wrongCodePrimary = get('lanStatus').textContent;
const wrongCodeHistory = get('lanHistory').textContent;
for (const [phase, error] of [['cancelled', 'cleanup started'], ['interrupted', 'cleanup close EOF'], ['failed', 'cleanup reader error']]) {
  progress('wrong-code-task', phase, { failureKind: 'close', error,
    receivedBytes: 1048576, savedBytes: 1048576, totalBytes: 1048576 });
  assert.equal(get('lanStatus').textContent, wrongCodePrimary);
  assert.equal(get('lanHistory').textContent, wrongCodeHistory);
  assert.equal(dom.activeElement, get('lanPeerSelect'));
}
const wrongCodeRow = get('lanHistory').children.at(-1)!;
assert.equal((wrongCodeRow.textContent.match(/配对失败/g) ?? []).length, 1);
assert.equal(wrongCodeRow.children.filter((item) => item.className === 'err').length, 0);
assert.doesNotMatch(wrongCodeRow.textContent, /连接中断|cleanup|EOF|MiB/);
pair('explicit-rejection-task');
await get('lanPairReject').onclick!();
const rejectionPrimary = get('lanStatus').textContent;
const rejectionHistory = get('lanHistory').textContent;
progress('explicit-rejection-task', 'interrupted', { failureKind: 'close', error: 'cleanup EOF' });
assert.equal(get('lanStatus').textContent, rejectionPrimary);
assert.equal(get('lanHistory').textContent, rejectionHistory);
assert.match(rejectionPrimary, /配对失败：已拒绝/);
assert.equal(get('lanPairModal').hidden, true);
assert.equal(dom.activeElement, get('lanPeerSelect'));
console.log('ok - wrong code / explicit reject -> cleanup close/EOF preserves one primary pairing failure and device focus (mock)');

for (const failureKind of ['close', 'error'] as const) {
  const taskId = `interruption-${failureKind}`;
  progress(taskId, 'paired', { name: 'hidden-before-consent.bin', totalBytes: 1048576 });
  let row = get('lanTasks').children.at(-1)!;
  assert.match(row.textContent, /短码核对通过，尚未开始传输/);
  assert.doesNotMatch(row.textContent, /hidden-before-consent|MiB|发送中|传输完成/);
  progress(taskId, 'channel-ready', { name: 'hidden-before-consent.bin' });
  row = get('lanTasks').children.at(-1)!;
  assert.match(row.textContent, /安全连接已打开，尚未开始传输/);
  assert.equal(row.children.filter((item) => item.tagName === 'progress').length, 0);
  progress(taskId, 'ready-to-transfer', { name: `${failureKind}-case.bin`, totalBytes: 1048576 });
  row = get('lanTasks').children.at(-1)!;
  assert.equal(row.children.filter((item) => item.tagName === 'progress').length, 2);
  progress(taskId, 'interrupted', { failureKind, error: 'manifest/control session-secret transfer-secret',
    receivedBytes: 1048576, savedBytes: 1048576, totalBytes: 1048576, speedBps: 1048576 });
  row = get('lanHistory').children.at(-1)!;
  assert.match(row.textContent, /历史记录 · 未完成/);
  assert.match(row.textContent, /连接中断，可重试/);
  assert.doesNotMatch(row.textContent, /发送中|1\.0 MiB|传输完成|manifest|control|session-secret|transfer-secret/);
  assert.equal(row.children.filter((item) => item.tagName === 'progress').length, 0);
  for (const phase of ['connecting', 'channel-ready', 'sending', 'complete']) progress(taskId, phase);
  assert.match(get('lanHistory').children.at(-1)!.textContent, /连接中断，可重试/);
  const retry = row.children.find((item) => item.className === 'ghostbtn lan-retry')!;
  assert.equal(retry.type, 'button');
  assert.equal(retry.tabIndex, 0);
  retry.focus();
  assert.equal(dom.activeElement, retry);
  let resolveRetry!: (value: unknown) => void;
  nextPick = new Promise((resolve) => { resolveRetry = resolve; });
  const sendsBefore = calls.filter((call) => call.command === 'native_lan_start_send').length;
  retry.click();
  assert.equal(button.disabled, true);
  assert.equal(send.disabled, true);
  resolveRetry(null);
  await button.lastActivation;
  settled();
  assert.equal(calls.filter((call) => call.command === 'native_lan_start_send').length, sendsBefore);
  console.log(`ok - mocked ${failureKind}: history/retry, clear stale progress, hide raw errors, require fresh explicit send`);
}

progress('bad-manifest', 'start-rejected', { error: 'file manifest transfer id mismatch session-secret transfer-secret' });
const rejected = get('lanHistory').children.at(-1)!;
assert.match(rejected.textContent, /无法开始传输，请重新配对并重试/);
assert.doesNotMatch(rejected.textContent, /session-secret|transfer-secret|manifest|0 B/);
assert.equal(rejected.children.filter((item) => item.tagName === 'progress').length, 0);

pair('accepted-task');
get('lanPeerCode').value = '1234 5678';
await get('lanPairAccept').onclick!();
assert.equal(calls.filter((call) => call.command === 'native_lan_pair_decision').at(-1)!.args.peerCode, '12345678');
events.get('fling://lan/manifest')!({ payload: {
  sessionId: 'pair-session', taskId: 'accepted-task', peerDeviceLabel: 'B', name: 'private-meta.bin', sizeBytes: 1048576,
} });
assert.doesNotMatch(get('lanIncomingInfo').textContent, /private-meta|MiB/);
await get('lanFileAccept').onclick!();
assert.equal(get('lanFileModal').hidden, true);
assert.match(get('lanHistory').textContent, /传输完成，文件已确认保存/);
assert.doesNotMatch(get('lanTasks').textContent, /private-meta|accepted/);
assert.match(get('lanStatus').textContent, /已接收 1\.0 MiB，已保存 1\.0 MiB/);
assert.match(html, /id="lanStatus"[^>]*role="status"[^>]*aria-live="polite"[^>]*aria-atomic="true"/);
console.log('ok - consent hides metadata; native durable-complete cannot be overwritten by invoke result (mock)');

const refresh = get('lanRefreshPeers');
const refreshStatus = get('lanRefreshStatus');
assert.match(html, /<button[^>]*id="lanRefreshPeers"[^>]*type="button"[^>]*aria-describedby="lanRefreshStatus"/);
assert.match(html, /id="lanRefreshStatus"[^>]*role="status"[^>]*aria-live="polite"/);
get('lanPeerSelect').value = 'peer-b';
nextPick = Promise.resolve({ sourceId: 'opaque-source-refresh' });
await button.onclick!();
progress('refresh-active', 'sending', { name: 'active.bin', totalBytes: 100, receivedBytes: 50, savedBytes: 0 });
const primaryBeforeRefresh = get('lanStatus').textContent;
const activeBeforeRefresh = get('lanTasks').textContent;
const historyBeforeRefresh = get('lanHistory').textContent;
const sourceBeforeRefresh = info.textContent;
let resolveRefresh!: (result: unknown) => void;
nextRefresh = new Promise<unknown>((resolve) => { resolveRefresh = resolve; });
const refreshCalls = () => calls.filter((call) => call.command === 'discovery_refresh');
const beforeRefreshCalls = refreshCalls().length;
const pendingRefresh = refresh.onclick!();
assert.equal(refresh.disabled, true);
assert.equal(refresh.textContent, '正在搜索…');
assert.match(refreshStatus.textContent, /正在搜索附近设备/);
assert.equal(refresh.attributes.get('aria-busy'), 'true');
await refresh.onclick!(); // Programmatic activation must also respect the in-flight guard.
assert.equal(refreshCalls().length, beforeRefreshCalls + 1);
events.get('fling://lan/peer-up')!({ payload: { id: 'peer-c', name: 'C' } });
events.get('fling://lan/peer-up')!({ payload: { id: 'peer-c', name: 'C' } });
resolveRefresh({ validServices: 3, lanEligiblePeers: 2, uiEmittedPeers: 1 });
await pendingRefresh;
assert.equal(refresh.disabled, false);
assert.equal(refresh.textContent, '重新搜索附近设备');
assert.equal(refresh.attributes.get('aria-busy'), 'false');
assert.match(refreshStatus.textContent, /本轮搜索结束，当前发现 2 台附近设备.*持续更新/);
assert.match(get('lanPeerStatus').textContent, /发现 2 台/);
assert.match(get('lanDiscoveryPhases').textContent, /有效服务 3；LAN 可用设备 2；成功提交界面事件的设备 1；当前界面可选条目 2/);
assert.equal(get('lanPeerSelect').value, 'peer-b');
assert.equal(info.textContent, sourceBeforeRefresh);
assert.equal(send.disabled, false);
assert.equal(get('lanStatus').textContent, primaryBeforeRefresh);
assert.equal(get('lanTasks').textContent, activeBeforeRefresh);
assert.equal(get('lanHistory').textContent, historyBeforeRefresh);
nextRefresh = Promise.reject(new Error('raw-error session-secret'));
await refresh.onclick!();
assert.match(refreshStatus.textContent, /搜索附近设备失败，可重试/);
assert.equal(refreshStatus.classList.contains('error'), true);
assert.doesNotMatch(refreshStatus.textContent, /raw-error|session-secret/);
assert.equal(refresh.disabled, false);
nextRefresh = Promise.resolve({ validServices: 3, lanEligiblePeers: 2, uiEmittedPeers: 1 });
await refresh.onclick!();
assert.equal(refreshStatus.classList.contains('error'), false);
assert.equal(refreshCalls().length, beforeRefreshCalls + 3);
assert.ok(refreshCalls().every((call) => call.args === undefined));
assert.equal(get('lanStatus').textContent, primaryBeforeRefresh);
assert.equal(get('lanTasks').textContent, activeBeforeRefresh);
assert.equal(get('lanHistory').textContent, historyBeforeRefresh);
console.log('ok - repeat refresh, in-flight dedup, error/retry, live peer update; selection/source/active transfer/history preserved (mock)');

const exportButton = get('lanExportDiscoveryLog');
const exportStatus = get('lanExportStatus');
assert.match(html, /id="lanExportDiscoveryLog"[^>]*type="button"[^>]*aria-describedby="lanLogDisclosure lanLogEvidenceNote lanExportStatus"/);
assert.match(html, /仅在本机生成/);
assert.match(html, /不会自动上传/);
assert.match(html, /包含本机和对端电脑名称/);
assert.match(html, /不能证明报文实际进出网卡/);
assert.match(html, /缓存记录不等于本轮收到新的网络响应/);
await exportButton.onclick!();
assert.match(exportStatus.textContent, /已取消导出/);
assert.equal(exportButton.disabled, false);
nextExport = Promise.reject(new Error('private-file-path 192.168.1.1 short-code-secret'));
await exportButton.onclick!();
assert.match(exportStatus.textContent, /导出失败/);
assert.doesNotMatch(exportStatus.textContent, /private-file|192\.168|short-code/);
assert.equal(exportStatus.classList.contains('error'), true);
assert.equal(exportButton.disabled, false);
let resolveExport!: (result: { saved: boolean; savedPath?: string }) => void;
nextExport = new Promise<{ saved: boolean; savedPath?: string }>((resolve) => { resolveExport = resolve; });
const exportCalls = () => calls.filter((call) => call.command === 'discovery_export_diagnostics');
const exportsBefore = exportCalls().length;
const pendingExport = exportButton.onclick!();
assert.equal(exportButton.disabled, true);
assert.equal(exportButton.attributes.get('aria-busy'), 'true');
await exportButton.onclick!();
assert.equal(exportCalls().length, exportsBefore + 1);
resolveExport({ saved: true, savedPath: 'C:\\Users\\tester\\Desktop\\fling-lan-diagnostics.log' });
await pendingExport;
assert.match(exportStatus.textContent, /已保存至：C:/);
assert.ok(exportStatus.textContent.includes('fling-lan-diagnostics.log'));
assert.match(exportStatus.textContent, /未上传/);
assert.equal(exportStatus.classList.contains('error'), false);
assert.equal(exportButton.disabled, false);
assert.equal(exportButton.attributes.get('aria-busy'), 'false');
assert.equal(exportButton.textContent, '导出诊断日志');
assert.ok(exportCalls().every((call) => call.args === undefined));
assert.equal(get('lanStatus').textContent, primaryBeforeRefresh);
assert.equal(get('lanTasks').textContent, activeBeforeRefresh);
assert.equal(get('lanHistory').textContent, historyBeforeRefresh);
assert.equal(info.textContent, sourceBeforeRefresh);
assert.equal(get('lanPeerSelect').value, 'peer-b');
console.log('ok - manual local log export: cancel/error/success, dedup, chosen export location displayed without reading log content in WebView, no task status overwrite (mock)');

const reports = () => calls.filter((call) => call.command === 'discovery_report_ui');
assert.deepEqual(reports()[0].args, { receivedUp: 0, receivedDown: 0, renderedPeers: 0, reason: 'initialized' });
const duplicateReports = reports().filter((call) => call.args.reason === 'peer-up').slice(-2);
assert.equal(duplicateReports[0].args.renderedPeers, 2);
assert.equal(duplicateReports[1].args.renderedPeers, 2);
assert.equal(duplicateReports[1].args.receivedUp, duplicateReports[0].args.receivedUp + 1);
failUiReport = true;
events.get('fling://lan/peer-down')!({ payload: { id: 'peer-c' } });
await Promise.resolve();
assert.equal(reports().at(-1)!.args.reason, 'peer-down');
assert.equal(reports().at(-1)!.args.receivedDown, 1);
assert.equal(reports().at(-1)!.args.renderedPeers, 1);
assert.match(get('lanPeerStatus').textContent, /发现 1 台/);
assert.equal(get('lanPeerSelect').value, 'peer-b');
assert.equal(get('lanStatus').textContent, primaryBeforeRefresh);
assert.equal(get('lanTasks').textContent, activeBeforeRefresh);
assert.equal(get('lanHistory').textContent, historyBeforeRefresh);
for (const { args } of reports()) {
  assert.deepEqual(Object.keys(args).sort(), ['reason', 'receivedDown', 'receivedUp', 'renderedPeers']);
  for (const field of ['receivedUp', 'receivedDown', 'renderedPeers']) {
    assert.ok(Number.isInteger(args[field]) && args[field] >= 0 && args[field] <= 0xffffffff);
  }
  assert.ok(['initialized', 'peer-up', 'peer-down', 'refresh-completed'].includes(args.reason));
}
console.log('ok - aggregate-only UI receipt/render snapshots distinguish duplicates; diagnostic failure does not affect discovery/transfer (mock)');
console.log('LAN picker/state/accessibility-contract harness passed. Native window and spoken-reader results remain separate.');
