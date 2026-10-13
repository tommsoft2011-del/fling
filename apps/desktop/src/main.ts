// Fling v0.3 桌面端入口：传输工具（发送 / 接收 / 历史 / 设置）
import './style.css';
// 禁用 WebView 右键菜单（桌面端不需要浏览器上下文菜单）
document.addEventListener('contextmenu', (e) => e.preventDefault());
import { invoke } from '@tauri-apps/api/core';
import { $, toast } from './ui.js';
import { initNet, getDeviceName } from './net.js';
import { manager } from './tasks.js';
import { applyTheme } from './store.js';
import { initSendPage } from './send.js';
import { initRecvPage, blurRecvPage } from './receive.js';
import { initHistPage, refreshHistPage } from './history.js';
import { initSettingsPage } from './settings.js';
import { log } from './log.js';

type Page = 'send' | 'recv' | 'hist' | 'settings';
let current: Page = 'send';
let started = false;

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
    await initNet();
  } catch (e) {
    started = false;
    showSetup(false);
    ($('#setupErr') as HTMLElement).textContent = '读取配置失败：' + (e as Error).message;
    return;
  }

  showMain();
  $('#myDeviceLine').textContent = getDeviceName();

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

  manager.onChange(updateBadges);
  try {
    await manager.init();
  } catch (e) {
    toast('传输服务初始化失败：' + (e as Error).message + '（可先检查服务器配置）', true);
  }
  try {
    const ver = (await invoke('app_version')) as string;
    log('app', 'fling started', { version: ver, device: getDeviceName() });
  } catch {
    /* ignore */
  }
  updateBadges();
  switchPage('send');
}

async function boot(): Promise<void> {
  applyTheme();
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
  if (cfg) {
    await startMain();
  } else {
    showSetup(false);
  }
}

boot();
