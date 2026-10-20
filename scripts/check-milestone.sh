#!/usr/bin/env bash
# Called by Tauri beforeBuildCommand: failure prevents packaging this milestone.
set -euo pipefail
cd "$(dirname "$0")/.."
echo '=== milestone: all TypeScript regressions including WebView boundary ==='
bash scripts/test-typescript.sh
echo '=== milestone: frontend type check ==='
bun apps/desktop/node_modules/typescript/bin/tsc --noEmit --target ES2022 --module ESNext --moduleResolution bundler --skipLibCheck --lib ES2022,DOM apps/desktop/src/*.ts
echo '=== milestone: native lint ==='
cargo clippy --locked --all-targets --manifest-path apps/desktop/src-tauri/Cargo.toml
echo '=== milestone: production frontend ==='
bun run --cwd apps/desktop build
