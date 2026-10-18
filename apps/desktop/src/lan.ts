import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { startFailureFeedback } from './lan-start-error';

interface Peer {
  id: string;
  name: string;
}

interface SourceMetadata {
  sourceId: string;
}

interface LanIdentity {
  deviceId: string;
  deviceName: string;
}

interface DiscoveryResult {
  validServices: number;
  lanEligiblePeers: number;
  uiEmittedPeers: number;
}

interface PairRequest {
  sessionId: string;
  taskId: string;
  peerDeviceId: string;
  peerDeviceLabel: string;
  verificationCode: string;
  role: 'initiator' | 'responder';
  diagnosticAttempt?: number;
}

interface IncomingManifest {
  sessionId: string;
  taskId: string;
  peerDeviceLabel: string;
  name: string;
  sizeBytes: number;
}

interface LanProgress {
  taskId: string;
  phase: string;
  route?: string;
  name?: string;
  receivedBytes: number;
  savedBytes: number;
  totalBytes: number;
  speedBps: number;
  error?: string;
  failureKind?: 'close' | 'error';
}

interface IpcEntry {
  calls: number;
  bytes: number;
}

interface IpcCounters {
  byType: Record<string, IpcEntry>;
  totalCalls: number;
  totalBytes: number;
}

interface TaskView extends LanProgress {
  name: string;
  direction: 'send' | 'receive';
}

const peers = new Map<string, Peer>();
const tasks = new Map<string, TaskView>();
let source: SourceMetadata | null = null;
let pickingSource = false;
let startingSend = false;
let uiSendAttempt = 0;
const pairingFailures = new Map<string, string>();
let selectedPair: PairRequest | null = null;
let selectedManifest: IncomingManifest | null = null;
let unlisten: UnlistenFn[] = [];
let initialized = false;
let receivedPeerUp = 0;
let receivedPeerDown = 0;
let lastDiscoveryResult: DiscoveryResult | null = null;

function reportDiscoveryUi(reason: 'initialized' | 'peer-up' | 'peer-down' | 'refresh-completed'): void {
  // Counts only: never send names, addresses, identifiers or log content through this IPC.
  void invoke('discovery_report_ui', {
    receivedUp: receivedPeerUp,
    receivedDown: receivedPeerDown,
    renderedPeers: selectablePeerCount(),
    reason,
  }).catch(() => {}); // Diagnostics must not break discovery or transfers.
}

const byId = <T extends HTMLElement>(id: string): T => {
  const element = document.getElementById(id);
  if (!element) throw new Error(`Missing LAN UI element: ${id}`);
  return element as T;
};

function selectablePeerCount(): number {
  return Array.from(byId<HTMLSelectElement>('lanPeerSelect').children).filter((child) => {
    const option = child as HTMLOptionElement;
    return Boolean(option.value) && !option.disabled;
  }).length;
}

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function setStatus(message: string, error = false): void {
  const status = byId<HTMLDivElement>('lanStatus');
  status.textContent = message;
  status.classList.toggle('error', error);
}

function renderDiscoveryPhases(): void {
  if (!lastDiscoveryResult) return;
  const result = lastDiscoveryResult;
  byId<HTMLElement>('lanDiscoveryPhases').textContent = `最近一轮应用层观察：有效服务 ${result.validServices}；LAN 可用设备 ${result.lanEligiblePeers}；成功提交界面事件的设备 ${result.uiEmittedPeers}；当前界面可选条目 ${selectablePeerCount()}。提交事件不等于界面已收到，也不代表网卡报文。`;
}

function renderPeers(): void {
  const select = byId<HTMLSelectElement>('lanPeerSelect');
  const previous = select.value;
  select.replaceChildren();
  const placeholder = document.createElement('option');
  placeholder.value = '';
  placeholder.textContent = peers.size ? '选择附近设备…' : '等待附近设备…';
  select.append(placeholder);
  for (const peer of peers.values()) {
    const option = document.createElement('option');
    option.value = peer.id;
    option.textContent = `${peer.name} · 可发现，等待配对`;
    select.append(option);
  }
  if (previous && peers.has(previous)) select.value = previous;
  const selectableCount = selectablePeerCount();
  byId<HTMLElement>('lanPeerStatus').textContent = selectableCount ? `发现 ${selectableCount} 台附近设备，可选择后请求配对` : '尚未发现附近设备；请确认两台设备在同一局域网并允许本地设备发现';
  renderDiscoveryPhases();
  updateSendEnabled();
}

function updateSendEnabled(): void {
  const peerSelected = Boolean(byId<HTMLSelectElement>('lanPeerSelect').value);
  const reason = pickingSource ? '正在选择文件，请完成或取消选择'
    : selectedPair ? '正在核对短码，请完成或取消配对'
    : startingSend ? '正在请求配对，请稍候'
    : !peerSelected && !source ? '请选择附近设备和文件'
    : !peerSelected ? '请选择附近设备'
    : !source ? '请选择文件'
    : '已选择设备和文件，点击请求发送并核对短码';
  byId<HTMLButtonElement>('lanSend').disabled = pickingSource || startingSend || Boolean(selectedPair) || !source || !peerSelected;
  byId<HTMLButtonElement>('lanPickSource').disabled = pickingSource || startingSend;
  byId<HTMLElement>('lanSendReason').textContent = reason;
  byId<HTMLElement>('lanSendReason').setAttribute('aria-label', reason);
  byId<HTMLButtonElement>('lanSend').setAttribute('aria-label', byId<HTMLButtonElement>('lanSend').disabled ? `请求发送，${reason}` : '请求发送');
}

const terminalPhases = new Set(['complete', 'failed', 'cancelled', 'interrupted', 'start-rejected']);

function phaseLabel(phase: string): string {
  return ({
    pairing: '等待双方核对短码',
    paired: '短码核对通过，尚未开始传输',
    connecting: '正在建立安全连接',
    'channel-ready': '安全连接已打开，尚未开始传输',
    'waiting-recipient': '等待接收方接受文件',
    'ready-to-transfer': '文件已被接受，准备传输',
    sending: '发送中',
    receiving: '接收中',
    finalizing: '正在校验并保存；尚未完成',
    complete: '传输完成，文件已确认保存',
    failed: '传输失败，可重试',
    cancelled: '已取消，可重试',
    interrupted: '连接中断，可重试',
    reconnecting: '连接中断，可重试',
    'start-rejected': '无法开始传输，请重新配对并重试',
  } as Record<string, string>)[phase] ?? '正在准备传输';
}

function renderTasks(): void {
  const host = byId<HTMLDivElement>('lanTasks');
  const history = byId<HTMLDivElement>('lanHistory');
  host.replaceChildren();
  history.replaceChildren();
  let activeCount = 0;
  let historyCount = 0;
  for (const task of tasks.values()) {
    const row = document.createElement('article');
    row.className = 'lan-task';
    const historical = terminalPhases.has(task.phase);
    if (historical) {
      const marker = document.createElement('div');
      marker.className = 'lan-history-label';
      marker.textContent = task.phase === 'complete' ? '历史记录 · 已完成' : '历史记录 · 未完成';
      row.append(marker);
    }
    const title = document.createElement('div');
    title.className = 'lan-task-title';
    title.textContent = `${task.direction === 'send' ? '发送' : '接收'}：${task.name}`;
    const phase = document.createElement('div');
    phase.className = 'hint';
    phase.textContent = historical && pairingFailures.has(task.taskId) ? pairingFailures.get(task.taskId)! : phaseLabel(task.phase);
    const numbers = document.createElement('div');
    numbers.className = 'hint';
    const failed = terminalPhases.has(task.phase) && task.phase !== 'complete';
    const progressVisible = ['ready-to-transfer', 'sending', 'receiving', 'finalizing', 'complete'].includes(task.phase);
    numbers.textContent = failed
      ? '本次传输未完成；请重新选择文件重试'
      : progressVisible
        ? `已接收 ${formatBytes(task.receivedBytes)} · 已保存 ${formatBytes(task.savedBytes)} · 共 ${formatBytes(task.totalBytes)}${task.speedBps > 0 ? ` · ${formatBytes(task.speedBps)}/s` : ''}`
        : '尚未开始传输';
    row.append(title, phase, numbers);
    if (progressVisible && task.totalBytes > 0) {
      for (const [label, value] of [['已接收', task.receivedBytes], ['已保存', task.savedBytes]] as const) {
        const bar = document.createElement('progress');
        bar.max = task.totalBytes;
        bar.value = Math.min(task.totalBytes, Math.max(0, value));
        bar.setAttribute('aria-label', `${label} ${formatBytes(value)}，共 ${formatBytes(task.totalBytes)}`);
        row.append(bar);
      }
    }
    if (['interrupted', 'start-rejected', 'failed', 'cancelled'].includes(task.phase)) {
      const retry = document.createElement('button');
      retry.className = 'ghostbtn lan-retry';
      retry.type = 'button';
      retry.tabIndex = 0;
      retry.textContent = '重新选择文件并重试';
      retry.onclick = () => {
        source = null;
        updateSendEnabled();
        const picker = byId<HTMLButtonElement>('lanPickSource');
        picker.focus();
        picker.click();
      };
      row.append(retry);
    }
    if (!terminalPhases.has(task.phase)) {
      const cancel = document.createElement('button');
      cancel.className = 'ghostbtn lan-cancel';
      cancel.textContent = '取消';
      cancel.onclick = () => {
        void invoke('native_lan_cancel', { taskId: task.taskId }).catch(() => {});
      };
      row.append(cancel);
    }
    if (historical) {
      history.append(row);
      historyCount += 1;
    } else {
      host.append(row);
      activeCount += 1;
    }
  }
  if (activeCount === 0) {
    const empty = document.createElement('div');
    empty.className = 'empty';
    empty.textContent = '当前没有进行中的传输';
    host.append(empty);
  }
  byId<HTMLElement>('lanHistorySection').hidden = historyCount === 0;
}

function reportPairingUi(phase: 'button-clicked' | 'prompt-received', uiAttempt: number | null, attempt: number | null): void {
  void invoke('pairing_report_ui', { phase, uiAttempt, attempt }).catch(() => {});
}

function showPair(request: PairRequest): void {
  selectedPair = request;
  if (!tasks.has(request.taskId)) {
    tasks.set(request.taskId, {
      taskId: request.taskId,
      name: '文件传输',
      direction: request.role === 'initiator' ? 'send' : 'receive',
      phase: 'pairing',
      receivedBytes: 0,
      savedBytes: 0,
      totalBytes: 0,
      speedBps: 0,
    });
    renderTasks();
  }
  byId<HTMLParagraphElement>('lanPairPeer').textContent = `${request.peerDeviceLabel} · ${request.role === 'initiator' ? '发起配对' : '请求配对'}`;
  byId<HTMLElement>('lanPairCode').textContent = request.verificationCode.replace(/^(\d{4})(\d{4})$/, '$1 $2');
  byId<HTMLElement>('lanPairCodeSpoken').textContent = `短码分两组。第一组：${request.verificationCode.slice(0, 4).split('').join(' ')}。第二组：${request.verificationCode.slice(4).split('').join(' ')}`;
  byId<HTMLElement>('lanPairCodeSpoken').setAttribute('aria-label', byId<HTMLElement>('lanPairCodeSpoken').textContent ?? '');
  byId<HTMLInputElement>('lanPeerCode').value = '';
  byId<HTMLDivElement>('lanPairModal').hidden = false;
  updateSendEnabled();
  setStatus('等待双方核对短码，尚未开始传输');
  byId<HTMLInputElement>('lanPeerCode').focus();
  if (Number.isSafeInteger(request.diagnosticAttempt) && request.diagnosticAttempt! > 0) {
    reportPairingUi('prompt-received', null, request.diagnosticAttempt!);
  }
}

function hidePair(): void {
  selectedPair = null;
  byId<HTMLElement>('lanPairCode').textContent = '••••••••';
  byId<HTMLElement>('lanPairCodeSpoken').textContent = '';
  byId<HTMLElement>('lanPairCodeSpoken').setAttribute('aria-label', '');
  byId<HTMLInputElement>('lanPeerCode').value = '';
  byId<HTMLDivElement>('lanPairModal').hidden = true;
  updateSendEnabled();
}

function returnToDeviceSelection(): void {
  source = null;
  byId<HTMLSelectElement>('lanPeerSelect').value = '';
  byId<HTMLElement>('lanSourceInfo').textContent = '请重新选择设备和文件';
  updateSendEnabled();
  byId<HTMLSelectElement>('lanPeerSelect').focus();
}

function showManifest(manifest: IncomingManifest): void {
  selectedManifest = manifest;
  byId<HTMLElement>('lanIncomingInfo').textContent = `${manifest.peerDeviceLabel} 想向你发送文件。接受后显示文件详情，并保存到本机配置的接收目录。`;
  byId<HTMLDivElement>('lanFileModal').hidden = false;
}

function hideManifest(): void {
  selectedManifest = null;
  byId<HTMLDivElement>('lanFileModal').hidden = true;
}

async function refreshCounters(): Promise<void> {
  const counters = await invoke<IpcCounters>('native_lan_ipc_counters');
  byId<HTMLSpanElement>('lanCounterSummary').textContent = `${counters.totalCalls} 次调用 · ${counters.totalBytes} B（累计）`;
  const pre = byId<HTMLPreElement>('lanCounters');
  pre.textContent = Object.entries(counters.byType)
    .map(([kind, value]) => `${kind}: ${value.calls} 次 / ${value.bytes} B`)
    .join('\n') || '尚无计数';
}

function applyProgress(progress: LanProgress): void {
  const current = tasks.get(progress.taskId);
  if (current && terminalPhases.has(current.phase)) return;
  const direction = current?.direction ?? (progress.phase === 'receiving' ? 'receive' : 'send');
  const metadataAllowed = ['ready-to-transfer', 'sending', 'receiving', 'finalizing', 'complete'].includes(progress.phase);
  const name = metadataAllowed ? progress.name ?? current?.name ?? '文件传输' : current?.name ?? '文件传输';
  const failed = terminalPhases.has(progress.phase) && progress.phase !== 'complete';
  tasks.set(progress.taskId, { ...current, ...progress, name, direction,
    ...(!metadataAllowed || failed ? { receivedBytes: 0, savedBytes: 0, totalBytes: 0, speedBps: 0 } : {}),
  });
  renderTasks();
  if (failed && pairingFailures.has(progress.taskId)) {
    setStatus(`${pairingFailures.get(progress.taskId)}。请选择设备重新配对。`, true);
  } else if (progress.phase === 'interrupted') {
    setStatus('连接中断，可重试。请重新选择文件；新任务会重新核对短码。', true);
  } else if (progress.phase === 'start-rejected') {
    setStatus('无法开始传输，请重新配对并重试', true);
  } else {
    const counts = ['sending', 'receiving', 'finalizing', 'complete'].includes(progress.phase)
      ? `；已接收 ${formatBytes(progress.receivedBytes)}，已保存 ${formatBytes(progress.savedBytes)}，共 ${formatBytes(progress.totalBytes)}` : '';
    setStatus(`${phaseLabel(progress.phase)}${counts}`, ['failed', 'cancelled'].includes(progress.phase));
  }
  if (terminalPhases.has(progress.phase)) {
    if (selectedPair?.taskId === progress.taskId) {
      hidePair();
      if (failed) returnToDeviceSelection();
    }
    if (selectedManifest?.taskId === progress.taskId) hideManifest();
    void refreshCounters().catch(() => {});
  }
}

function recordPairingFailure(request: PairRequest, reason: string): void {
  const current = tasks.get(request.taskId);
  if (current && terminalPhases.has(current.phase)) return;
  pairingFailures.set(request.taskId, reason);
  // This is a local rejection only; positive connection/progress still requires Native events.
  applyProgress({ taskId: request.taskId, phase: 'cancelled', receivedBytes: 0,
    savedBytes: 0, totalBytes: 0, speedBps: 0 });
}

export async function initLanPage(): Promise<LanIdentity> {
  if (initialized) throw new Error('LAN page already initialized');

  unlisten.push(await listen<Peer>('fling://lan/peer-up', ({ payload }) => {
    receivedPeerUp = Math.min(0xffffffff, receivedPeerUp + 1);
    peers.set(payload.id, payload);
    renderPeers();
    reportDiscoveryUi('peer-up');
  }));
  unlisten.push(await listen<{ id: string }>('fling://lan/peer-down', ({ payload }) => {
    receivedPeerDown = Math.min(0xffffffff, receivedPeerDown + 1);
    peers.delete(payload.id);
    renderPeers();
    reportDiscoveryUi('peer-down');
  }));
  unlisten.push(await listen<PairRequest>('fling://lan/pairing', ({ payload }) => showPair(payload)));
  unlisten.push(await listen<IncomingManifest>('fling://lan/manifest', ({ payload }) => showManifest(payload)));
  unlisten.push(await listen<LanProgress>('fling://lan/progress', ({ payload }) => applyProgress(payload)));

  const refreshButton = byId<HTMLButtonElement>('lanRefreshPeers');
  refreshButton.onclick = async () => {
    if (refreshButton.disabled) return;
    const status = byId<HTMLElement>('lanRefreshStatus');
    refreshButton.disabled = true;
    refreshButton.textContent = '正在搜索…';
    refreshButton.setAttribute('aria-busy', 'true');
    status.classList.toggle('error', false);
    status.textContent = '正在搜索附近设备，请稍候…';
    lastDiscoveryResult = null;
    byId<HTMLElement>('lanDiscoveryPhases').textContent = '正在收集本轮应用层阶段统计…';
    try {
      // Native owns the bounded search round; a browse-start acknowledgment is not completion.
      const result = await invoke<DiscoveryResult>('discovery_refresh');
      lastDiscoveryResult = result;
      renderDiscoveryPhases();
      const selectableCount = selectablePeerCount();
      status.textContent = selectableCount
        ? `本轮搜索结束，当前发现 ${selectableCount} 台附近设备；列表仍会持续更新。`
        : '本轮搜索结束，仍未发现附近设备；可再次搜索，请检查同一局域网及防火墙。';
    } catch {
      byId<HTMLElement>('lanDiscoveryPhases').textContent = '本轮搜索未正常完成；请导出诊断日志查看超时或失败类别。';
      status.textContent = '搜索附近设备失败，可重试；请检查本地网络和防火墙是否允许设备发现。';
      status.classList.toggle('error', true);
    } finally {
      reportDiscoveryUi('refresh-completed');
      refreshButton.disabled = false;
      refreshButton.textContent = '重新搜索附近设备';
      refreshButton.setAttribute('aria-busy', 'false');
    }
  };

  byId<HTMLSelectElement>('lanPeerSelect').onchange = updateSendEnabled;
  const pickButton = byId<HTMLButtonElement>('lanPickSource');
  pickButton.onclick = async () => {
    if (startingSend) return;
    if (pickButton.disabled) return;
    const previousSource = source;
    const sourceInfo = byId<HTMLSpanElement>('lanSourceInfo');
    const previousLabel = pickButton.textContent ?? '在本机选择文件';
    pickingSource = true;
    pickButton.disabled = true;
    pickButton.textContent = '正在选择文件…';
    sourceInfo.textContent = '正在选择文件…';
    setStatus('正在准备文件，尚未开始新传输');
    renderPeers();
    try {
      const picked = await invoke<SourceMetadata | null>('native_lan_pick_source');
      if (picked) {
        source = picked;
        sourceInfo.textContent = '文件已准备；对方接受后显示详情';
      } else {
        source = previousSource;
        sourceInfo.textContent = previousSource
          ? '已取消选择；保留原文件，可重新选择'
          : '已取消选择；可重新选择文件';
      }
    } catch (error) {
      source = previousSource;
      const message = String(error).includes('无法打开所选文件')
        ? '无法读取所选文件，请检查读取权限' : '无法选择文件，请重试';
      sourceInfo.textContent = previousSource
        ? `选择失败：${message}（已保留原文件，可重试）`
        : `选择失败：${message}（可重试）`;
      setStatus(`选择文件失败：${message}（可重试）`, true);
    } finally {
      pickingSource = false;
      pickButton.disabled = false;
      pickButton.textContent = previousLabel;
      updateSendEnabled();
      pickButton.focus();
    }
  };
  byId<HTMLButtonElement>('lanSend').onclick = async () => {
    const peerId = byId<HTMLSelectElement>('lanPeerSelect').value;
    if (!source || !peerId || pickingSource || startingSend || selectedPair) return;
    const requestedSource = source;
    startingSend = true;
    uiSendAttempt = uiSendAttempt >= 0xffffffff ? 1 : uiSendAttempt + 1;
    reportPairingUi('button-clicked', uiSendAttempt, null);
    updateSendEnabled();
    try {
      const taskId = await invoke<string>('native_lan_start_send', {
        peerId,
        sourceId: requestedSource.sourceId,
        uiAttempt: uiSendAttempt,
      });
      const peer = peers.get(peerId);
      tasks.set(taskId, {
        taskId,
        name: '文件传输',
        direction: 'send',
        phase: 'pairing',
        receivedBytes: 0,
        savedBytes: 0,
        totalBytes: 0,
        speedBps: 0,
      });
      renderTasks();
      if (source?.sourceId === requestedSource.sourceId) {
        source = null;
        byId<HTMLSpanElement>('lanSourceInfo').textContent = '文件已准备；对方接受后显示详情';
      }
      updateSendEnabled();
      setStatus(`已请求与 ${peer?.name ?? '目标设备'} 配对；双方都需要核对短码并确认`);
    } catch (error) {
      const failure = startFailureFeedback(error);
      if (failure.sourceRetained === false && source?.sourceId === requestedSource.sourceId) {
        source = null;
        byId<HTMLSpanElement>('lanSourceInfo').textContent = '所选文件已失效，请重新选择文件';
      }
      const sourceNote = failure.sourceRetained === true
        ? ' 已保留所选文件，可直接重试。'
        : failure.sourceRetained === undefined ? ' 文件选择未改动。' : '';
      setStatus(failure.message + sourceNote, true);
    } finally {
      startingSend = false;
      updateSendEnabled();
    }
  };
  byId<HTMLButtonElement>('lanPairAccept').onclick = async () => {
    if (!selectedPair) return;
    const request = selectedPair;
    const peerCode = byId<HTMLInputElement>('lanPeerCode').value.replace(/\s/g, '');
    const mismatch = peerCode !== request.verificationCode;
    if (mismatch) recordPairingFailure(request, '配对失败：短码不一致，已取消配对');
    byId<HTMLButtonElement>('lanPairAccept').disabled = true;
    try {
      await invoke('native_lan_pair_decision', {
        sessionId: request.sessionId,
        approved: true,
        peerCode,
      });
      hidePair();
      if (mismatch) {
        returnToDeviceSelection();
        setStatus(`${pairingFailures.get(request.taskId)}。请选择设备重新配对。`, true);
      }
    } catch {
      if (!pairingFailures.has(request.taskId)) recordPairingFailure(request, '配对失败，请重新配对并重试');
      await invoke('native_lan_cancel', { taskId: request.taskId }).catch(() => {});
      hidePair();
      returnToDeviceSelection();
      const primary = pairingFailures.get(request.taskId);
      if (primary) setStatus(`${primary}。请选择设备重新配对。`, true);
    } finally {
      byId<HTMLButtonElement>('lanPairAccept').disabled = false;
    }
  };
  byId<HTMLButtonElement>('lanPairReject').onclick = async () => {
    if (!selectedPair) return;
    const request = selectedPair;
    recordPairingFailure(request, '配对失败：已拒绝本次配对');
    try {
      await invoke('native_lan_pair_decision', {
        sessionId: request.sessionId,
        approved: false,
        peerCode: '',
      });
    } catch {
      await invoke('native_lan_cancel', { taskId: request.taskId }).catch(() => {});
    } finally {
      hidePair();
    }
  };
  byId<HTMLButtonElement>('lanFileAccept').onclick = async () => {
    if (!selectedManifest) return;
    const manifest = selectedManifest;
    byId<HTMLButtonElement>('lanFileAccept').disabled = true;
    try {
      await invoke('native_lan_manifest_decision', {
        sessionId: manifest.sessionId,
        accepted: true,
      });
      // Only native events may advance progress or declare durable completion.
      hideManifest();
    } catch {
      setStatus('无法接收文件，请重新配对并重试', true);
    } finally {
      byId<HTMLButtonElement>('lanFileAccept').disabled = false;
    }
  };
  byId<HTMLButtonElement>('lanFileReject').onclick = async () => {
    if (!selectedManifest) return;
    const manifest = selectedManifest;
    try {
      await invoke('native_lan_manifest_decision', {
        sessionId: manifest.sessionId,
        accepted: false,
      });
    } finally {
      hideManifest();
    }
  };
  byId<HTMLButtonElement>('lanRefreshCounters').onclick = () => {
    void refreshCounters().catch(() => setStatus('诊断计数读取失败，请重试', true));
  };
  const exportButton = byId<HTMLButtonElement>('lanExportDiscoveryLog');
  exportButton.onclick = async () => {
    if (exportButton.disabled) return;
    const status = byId<HTMLElement>('lanExportStatus');
    exportButton.disabled = true;
    exportButton.textContent = '正在导出…';
    exportButton.setAttribute('aria-busy', 'true');
    status.classList.toggle('error', false);
    status.textContent = '请选择本地保存位置…';
    try {
      const result = await invoke<{ saved: boolean; savedPath?: string }>('discovery_export_diagnostics');
      status.textContent = result.saved
        ? `诊断日志已保存至：${result.savedPath ?? '所选位置'}。未上传。`
        : '已取消导出，可随时重试。';
    } catch {
      status.textContent = '导出失败，请检查保存位置后重试。';
      status.classList.toggle('error', true);
    } finally {
      exportButton.disabled = false;
      exportButton.textContent = '导出诊断日志';
      exportButton.setAttribute('aria-busy', 'false');
    }
  };
  byId<HTMLButtonElement>('lanExit').onclick = async () => {
    try {
      await invoke('native_lan_stop_mode');
    } finally {
      unlisten.forEach((dispose) => dispose());
      unlisten = [];
      window.location.reload();
    }
  };

  let identity: LanIdentity;
  try {
    identity = await invoke<LanIdentity>('native_lan_start_mode');
  } catch (error) {
    unlisten.forEach((dispose) => dispose());
    unlisten = [];
    throw error;
  }
  initialized = true;
  byId<HTMLDivElement>('lanDeviceLine').textContent = `本机：${identity.deviceName} · 仅本地网络，不连接云端`;
  renderPeers();
  reportDiscoveryUi('initialized');
  return identity;
}
