// 设置页：设备名称 / 接收目录 / 界面风格 / 关于
import { invoke } from '@tauri-apps/api/core';
import { $, toast } from './ui.js';
import { settings, type ThemeMode } from './store.js';
import { renameDevice } from './net.js';
import { manager } from './tasks.js';

let inited = false;

export function initSettingsPage(): void {
  if (inited) return;
  inited = true;

  // ---- 设备名称 ----
  invoke('get_device_info')
    .then((info) => {
      ($('#deviceNameInput') as HTMLInputElement).value = (info as { device_name: string }).device_name;
    })
    .catch(() => {});
  $('#saveDeviceName').onclick = async () => {
    const name = (($('#deviceNameInput') as HTMLInputElement).value || '').trim();
    if (!name) {
      toast('设备名称不能为空', true);
      return;
    }
    try {
      await renameDevice(name);
      toast('设备名称已更新，附近设备将看到新名字');
      document.dispatchEvent(new CustomEvent('fling:devicename', { detail: name }));
    } catch (e) {
      toast('保存失败：' + (e as Error).message, true);
    }
  };

  // ---- 接收目录 ----
  const loadDir = async () => {
    try {
      const dir = (await invoke('get_download_dir')) as string;
      ($('#downloadDirInput') as HTMLInputElement).value = dir;
    } catch (e) {
      toast('读取接收目录失败：' + (e as Error).message, true);
    }
  };
  void loadDir();
  $('#pickDownloadDir').onclick = async () => {
    try {
      const picked = (await invoke('pick_download_dir')) as string | null;
      if (!picked) return;
      await invoke('set_download_dir', { path: picked });
      ($('#downloadDirInput') as HTMLInputElement).value = picked;
      manager.setDownloadDirCache(picked);
      toast('接收目录已更新');
    } catch (e) {
      toast('设置失败：' + (e as Error).message, true);
    }
  };
  $('#openDownloadDir').onclick = async () => {
    const dir = ($('#downloadDirInput') as HTMLInputElement).value;
    if (!dir) return;
    try {
      await invoke('open_folder', { path: dir });
    } catch (e) {
      toast('打开失败：' + (e as Error).message, true);
    }
  };

  // ---- 允许被发现（从接收页搬过来） ----
  const tgl = $('#discoverToggle') as HTMLInputElement;
  tgl.checked = settings.discoverable;
  tgl.onchange = async () => {
    settings.discoverable = tgl.checked;
    try {
      await manager.setDiscoverable(tgl.checked);
    } catch (e) {
      toast('切换失败：' + (e as Error).message, true);
      tgl.checked = !tgl.checked;
      settings.discoverable = tgl.checked;
    }
  };

  // ---- 传输完成系统通知 ----
  const ntgl = $('#notifyToggle') as HTMLInputElement;
  ntgl.checked = settings.notifyOnDone;
  ntgl.onchange = () => {
    settings.notifyOnDone = ntgl.checked;
  };

  // ---- 界面风格 ----
  const seg = $('#themeSeg');
  const paint = () => {
    seg.querySelectorAll('button').forEach((b) =>
      b.classList.toggle('active', (b as HTMLElement).dataset.theme === settings.theme),
    );
  };
  seg.addEventListener('click', (e) => {
    const b = (e.target as HTMLElement).closest('button');
    if (!b) return;
    settings.theme = (b as HTMLElement).dataset.theme as ThemeMode;
    paint();
  });
  paint();

  // ---- 关于 ----
  invoke('app_version')
    .then((ver) => {
      $('#aboutLine').textContent = `Fling v${ver} · 端到端加密的多任务传输`;
    })
    .catch(() => {
      $('#aboutLine').textContent = 'Fling · 端到端加密的多任务传输';
    });

  // ---- 诊断日志 ----
  $('#openLogDir').onclick = async () => {
    try {
      const dir = (await invoke('get_log_dir')) as string;
      await invoke('open_folder', { path: dir });
    } catch (e) {
      toast('打开失败：' + (e as Error).message, true);
    }
  };

  // ---- 修改服务器设置 ----
  $('#editServerBtn').onclick = () => {
    document.dispatchEvent(new CustomEvent('fling:nav', { detail: 'setup' }));
  };
}
