// Clipboard access.
// Linux/macOS/Windows via clipboardy; Android (Termux) via termux-clipboard-*
// (requires the Termux:API app).

import { execFileSync } from 'node:child_process';

function isTermux(): boolean {
  const prefix = process.env.PREFIX || '';
  return prefix.includes('com.termux');
}

export async function readClipboard(): Promise<string> {
  if (isTermux()) {
    return execFileSync('termux-clipboard-get', { encoding: 'utf8' });
  }
  const { read } = await import('clipboardy');
  return read();
}

export async function writeClipboard(text: string): Promise<void> {
  if (isTermux()) {
    execFileSync('termux-clipboard-set', { input: text });
    return;
  }
  const { write } = await import('clipboardy');
  await write(text);
}
