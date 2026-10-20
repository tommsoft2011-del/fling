#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for test in packages/core/test/*.ts apps/desktop/test/*.ts; do
  echo "=== $test ==="
  bun "$test"
done
