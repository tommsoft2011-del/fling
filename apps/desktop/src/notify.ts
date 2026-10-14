// 系统通知：传输完成/失败时弹 OS 级通知（切到别的窗口也能看到）
// 纯加法，不影响现有传输逻辑。失败不抛错。
import {
  sendNotification,
  isPermissionGranted,
  requestPermission,
} from '@tauri-apps/plugin-notification';
import { settings } from './store.js';

let permChecked = false;

async function ensurePerm(): Promise<boolean> {
  if (permChecked) return true;
  permChecked = true;
  try {
    if (await isPermissionGranted()) return true;
    return (await requestPermission()) === 'granted';
  } catch {
    return false;
  }
}

/** 任务结束时调用：dir=send/recv，ok=是否成功 */
export function notifyTaskDone(dir: 'send' | 'recv', ok: boolean, summary: string): void {
  if (!settings.notifyOnDone) return;
  void (async () => {
    try {
      if (!(await ensurePerm())) return;
      sendNotification({
        title: ok
          ? dir === 'send' ? 'Fling 发送完成' : 'Fling 接收完成'
          : dir === 'send' ? 'Fling 发送失败' : 'Fling 接收失败',
        body: summary,
      });
    } catch {
      /* 通知失败不影响主流程 */
    }
  })();
}
