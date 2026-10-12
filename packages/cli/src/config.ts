// Fling CLI config: server URL + LiveKit API key/secret.
// The user fills these in themselves; nothing secret ever enters the repo.
//
//   Linux:   ~/.config/fling/config.json
//   macOS:   ~/Library/Application Support/fling/config.json
//   Windows: %APPDATA%\fling\config.json

import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';

export interface FlingConfig {
  serverUrl: string;
  apiKey: string;
  apiSecret: string;
}

export const DEFAULT_SERVER_URL = 'wss://rtc.syncpage.app';

export function configPath(): string {
  if (process.platform === 'win32') {
    const base = process.env.APPDATA || join(homedir(), 'AppData', 'Roaming');
    return join(base, 'fling', 'config.json');
  }
  if (process.platform === 'darwin') {
    return join(homedir(), 'Library', 'Application Support', 'fling', 'config.json');
  }
  const base = process.env.XDG_CONFIG_HOME || join(homedir(), '.config');
  return join(base, 'fling', 'config.json');
}

export function loadConfig(): FlingConfig {
  const p = configPath();
  if (!existsSync(p)) {
    throw new Error(`config not found: ${p}\nRun "fling init" first and fill in your LiveKit key/secret.`);
  }
  let cfg: FlingConfig;
  try {
    cfg = JSON.parse(readFileSync(p, 'utf8'));
  } catch {
    throw new Error(`config is not valid JSON: ${p}`);
  }
  if (!cfg.serverUrl || !cfg.apiKey || !cfg.apiSecret) {
    throw new Error(`incomplete config: ${p}\nRun "fling init" to fill in serverUrl, apiKey, apiSecret.`);
  }
  return cfg;
}

export function saveConfig(cfg: FlingConfig): string {
  const p = configPath();
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(cfg, null, 2) + '\n');
  try {
    chmodSync(p, 0o600);
  } catch {
    /* Windows: best effort */
  }
  return p;
}
