import { defineConfig } from 'vite';

export default defineConfig({
  // Tauri dev server port must match tauri.conf.json devUrl
  server: { port: 1420, strictPort: true },
  build: { outDir: 'dist' },
  clearScreen: false,
});
