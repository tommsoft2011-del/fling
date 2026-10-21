// Fling v0.3 桌面端入口：传输工具（发送 / 接收 / 历史 / 设置）
import './style.css';
// 禁用 WebView 右键菜单，但编辑框（input/textarea）保留原生菜单（复制/粘贴等）
document.addEventListener('contextmenu', (e) => {
  const t = e.target as HTMLElement | null;
  if (t && t.closest('input, textarea, [contenteditable="true"]')) return;
  e.preventDefault();
});
// 全局错误兜底：未捕获的异常/拒绝不再静默（v0.5.8 的 init 中断就是静默失败的教训）
// 只做 toast + 日志，不干扰正常流程；toast 节流避免刷屏。
let lastErrToast = 0;
function reportGlobalError(where: string, err: unknown): void {
  const msg = err instanceof Error ? err.message : String(err);
  const errorType = err instanceof Error ? err.name : typeof err;
  try {
    // log 是同步入队，flush 在后台；这里直接记
    import('./log.js').then((m) => m.log('ui', 'global error', { where, errorType })).catch(() => {});
  } catch { /* ignore */ }
  const now = Date.now();
  if (now - lastErrToast > 5000) {
    lastErrToast = now;
    try {
      import('./ui.js').then((m) => m.toast('出错：' + msg.slice(0, 120), true)).catch(() => {});
    } catch { /* ignore */ }
  }
}
window.addEventListener('unhandledrejection', (e) => reportGlobalError('unhandledrejection', e.reason));
window.addEventListener('error', (e) => reportGlobalError('window.onerror', e.error ?? e.message));
import { invoke } from '@tauri-apps/api/core';
import { listen } from './ui-events.js';
import { $, toast } from './ui.js';
import { initNet, getDeviceName } from './net.js';
import { manager } from './tasks.js';
import { applyTheme, hydrateHistory } from './store.js';
import { initSendPage } from './send.js';
import { initRecvPage, blurRecvPage } from './receive.js';
import { initHistPage, refreshHistPage } from './history.js';
import { initSettingsPage } from './settings.js';
import { log } from './log.js';

/** 从服务器拉取加密的远程配置并应用（静默失败：用内置/本地配置继续） */
async function refreshRemoteConfig(): Promise<void> {
  try {
    const url = await invoke<string | null>('get_remote_config_url');
    if (!url) return;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 8000);
    try {
      const resp = await fetch(url, { signal: ctrl.signal, cache: 'no-store' });
      if (!resp.ok) return;
      const j = await resp.json();
      if (j.v !== 1 || typeof j.nonce !== 'string' || typeof j.data !== 'string') return;
      await invoke('apply_remote_config', { nonceB64: j.nonce, dataB64: j.data });
      log('net', 'remote config applied');
    } finally {
      clearTimeout(timer);
    }
  } catch {
    /* 离线或服务器不可用：静默忽略 */
  }
}

type Page = 'send' | 'recv' | 'hist' | 'settings';
let current: Page = 'send';
let started = false;
let nativeDiagnosticListenerReady = false;

async function installNativeDiagnosticListener(): Promise<void> {
  if (nativeDiagnosticListenerReady) return;
  try {
    await listen<Record<string, unknown>>('fling:diagnostic', (event) => {
      log('xfer', 'native diagnostic', event.payload);
    });
    nativeDiagnosticListenerReady = true;
  } catch {
    log('app', 'native diagnostic listener unavailable');
  }
}

function switchPage(p: Page): void {
  if (current === 'recv' && p !== 'recv') blurRecvPage();
  current = p;
  document.querySelectorAll('#sideNav button[data-page]').forEach((b) =>
    b.classList.toggle('active', (b as HTMLElement).dataset.page === p),
  );
  document.querySelectorAll('.page').forEach((s) =>
    s.classList.toggle('active', s.id === 'page-' + p),
  );
  if (p === 'hist') refreshHistPage();
}

function showSetup(prefill: boolean): void {
  $('#mainScreen').classList.remove('active');
  $('#setupScreen').classList.add('active');
  if (prefill) {
    invoke('get_config')
      .then((cfg) => {
        const c = cfg as { server_url?: string; api_key?: string } | null;
        if (c) {
          ($('#cfgUrl') as HTMLInputElement).value = c.server_url ?? '';
          ($('#cfgKey') as HTMLInputElement).value = c.api_key ?? '';
        }
      })
      .catch(() => {});
  }
}

function showMain(): void {
  $('#setupScreen').classList.remove('active');
  $('#mainScreen').classList.add('active');
}

function updateBadges(): void {
  const s = $('#badgeSend');
  const r = $('#badgeRecv');
  const ns = manager.sendActiveCount;
  const nr = manager.recvBadgeCount;
  s.hidden = ns === 0;
  s.textContent = String(ns);
  r.hidden = nr === 0;
  r.textContent = String(nr);
}

async function startMain(): Promise<void> {
  if (started) {
    showMain();
    return;
  }
  started = true;
  try {
    await installNativeDiagnosticListener();
    await initNet();
  } catch (e) {
    started = false;
    showSetup(false);
    ($('#setupErr') as HTMLElement).textContent = '读取配置失败：' + (e as Error).message;
    return;
  }

  $('#myDeviceLine').textContent = getDeviceName();
  try {
    await hydrateHistory();
  } catch (e) {
    log('app', 'disk history unavailable; showing local cache', { error: String(e) });
    toast('完整传输历史暂时无法读取，将显示本地缓存', true);
  }
  showMain();

  document.addEventListener('fling:devicename', (e) => {
    $('#myDeviceLine').textContent = (e as CustomEvent).detail;
  });
  document.addEventListener('fling:nav', (e) => {
    const d = (e as CustomEvent).detail as string;
    if (d === 'setup') {
      showSetup(true);
      return;
    }
    if (d === 'send' || d === 'recv' || d === 'hist' || d === 'settings') switchPage(d);
  });
  document.querySelectorAll('#sideNav button[data-page]').forEach((b) => {
    (b as HTMLButtonElement).onclick = () => switchPage((b as HTMLElement).dataset.page as Page);
  });

  initSendPage();
  initRecvPage();
  initHistPage();
  initSettingsPage();

  // 远程配置：启动时尝试从服务器拉取加密配置（失败则静默用内置/本地）
  refreshRemoteConfig().catch(() => {});

  manager.onChange(updateBadges);
  try {
    await manager.init();
  } catch (e) {
    const msg = typeof e === 'string' ? e : (e as Error)?.message ?? String(e);
    toast('传输服务初始化失败：' + msg + '（可先检查服务器配置）', true);
  }
  try {
    const ver = (await invoke('app_version')) as string;
    log('app', 'fling started', { version: ver });
    log('app', 'diagnostic build identifier', {
      buildId: await invoke<string>('app_build_identity'),
      frontendBuildId: `v0.6.34+${__FLING_BUILD_COMMIT__}`,
      commit: __FLING_BUILD_COMMIT__,
    });
  } catch {
    /* ignore */
  }
  updateBadges();
  switchPage('send');
}

async function boot(): Promise<void> {
  applyTheme();
  // v0.6.9+: 移除模式选择，直接进入主界面（局域网发现常驻，信令优先走局域网）
  $('#saveCfg').onclick = async () => {
    const cfg = {
      server_url: (($('#cfgUrl') as HTMLInputElement).value || '').trim(),
      api_key: (($('#cfgKey') as HTMLInputElement).value || '').trim(),
      api_secret: (($('#cfgSecret') as HTMLInputElement).value || '').trim(),
    };
    if (!cfg.server_url || !cfg.api_key || !cfg.api_secret) {
      $('#setupErr').textContent = '三项都要填';
      return;
    }
    try {
      await invoke('save_config', { cfg });
      $('#setupErr').textContent = '';
      if (started) {
        // 服务器配置变更：重载生效最干净
        window.location.reload();
      } else {
        await startMain();
      }
    } catch (e) {
      $('#setupErr').textContent = '保存失败：' + (e as Error).message;
    }
  };

  let cfg: unknown = null;
  try {
    cfg = await invoke('get_config');
  } catch {
    cfg = null;
  }
  // v0.6.9+: 有配置直接进主界面，无配置显示设置页（不再经过模式选择）
  if (cfg) {
    await startMain();
  } else {
    showSetup(false);
  }
}

boot();
