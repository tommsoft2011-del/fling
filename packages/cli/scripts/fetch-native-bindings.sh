#!/bin/bash
# Fetch platform-specific native bindings for @livekit/rtc-node.
# Needed because `bun install` on the build machine skips packages whose
# os/cpu don't match, and `bun build --compile --target=...` would otherwise
# produce a binary that dies at startup with "Cannot find native binding".
#
# Usage: bash fetch-native-bindings.sh [win32|x64|arm64|all]
# Run on the Linux build machine before cross-compiling.
set -euo pipefail

VERSION="0.12.73"  # must match @livekit/rtc-node's @livekit/rtc-ffi-bindings dep
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)/node_modules/@livekit"

fetch_one() {
  local pkg="$1" file="$2"
  local dest="$ROOT/$pkg"
  if [ -f "$dest/$file" ]; then
    echo "already present: $pkg"
    return
  fi
  echo "downloading ${pkg}@${VERSION} ..."
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  curl -sL -o "$tmp/binding.tgz" \
    "https://registry.npmjs.org/${pkg}/-/${pkg##*/}-${VERSION}.tgz"
  mkdir -p "$dest"
  tar xzf "$tmp/binding.tgz" -C "$tmp"
  cp -r "$tmp"/package/* "$dest/"
  rm -rf "$tmp"
  trap - EXIT
  echo "installed: $dest/$file"
}

WANT="${1:-all}"
case "$WANT" in
  win32) fetch_one "@livekit/rtc-ffi-bindings-win32-x64-msvc" "rtc-node.win32-x64-msvc.node" ;;
  arm64) fetch_one "@livekit/rtc-ffi-bindings-linux-arm64-gnu" "rtc-node.linux-arm64-gnu.node" ;;
  x64)   echo "linux x86_64 binding comes with bun install, nothing to do" ;;
  all)
    fetch_one "@livekit/rtc-ffi-bindings-win32-x64-msvc" "rtc-node.win32-x64-msvc.node"
    fetch_one "@livekit/rtc-ffi-bindings-linux-arm64-gnu" "rtc-node.linux-arm64-gnu.node"
    ;;
  *) echo "usage: $0 [win32|arm64|x64|all]" >&2; exit 1 ;;
esac
