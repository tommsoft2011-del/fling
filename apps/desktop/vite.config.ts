import { execFileSync } from 'node:child_process';
import { defineConfig } from 'vite';

let buildCommit = 'unknown';
try {
  buildCommit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
} catch {
  // Keep non-git preview builds usable; diagnostic artifacts are built from Git.
}

export default defineConfig({
  define: { __FLING_BUILD_COMMIT__: JSON.stringify(buildCommit) },
  // Tauri dev server port must match tauri.conf.json devUrl
  server: { port: 1420, strictPort: true },
  build: { outDir: 'dist' },
  clearScreen: false,
});
