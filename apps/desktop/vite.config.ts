import { defineConfig } from 'vite';

const buildCommit = process.env.GITHUB_SHA ?? 'local';

export default defineConfig({
  define: {
    __FLING_BUILD_COMMIT__: JSON.stringify(buildCommit),
  },
  // Tauri dev server port must match tauri.conf.json devUrl
  server: { port: 1420, strictPort: true },
  build: { outDir: 'dist' },
  clearScreen: false,
});
