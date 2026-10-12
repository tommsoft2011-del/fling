#!/bin/bash
# Fetch the Windows x64 native binding for @livekit/rtc-node.
# Needed because `bun install` on Linux skips win32-only optional packages,
# and `bun build --compile --target=bun-windows-x64` would otherwise produce
# an exe that fails at startup with "Cannot find native binding".
#
# Run this on the Linux build machine before compiling the Windows binary.
set -euo pipefail

VERSION="0.12.73"  # must match @livekit/rtc-node's @livekit/rtc-ffi-bindings dep
PKG="@livekit/rtc-ffi-bindings-win32-x64-msvc"
DEST="$(cd "$(dirname "$0")/../../../.." && pwd)/node_modules/@livekit/rtc-ffi-bindings-win32-x64-msvc"

if [ -f "$DEST/rtc-node.win32-x64-msvc.node" ]; then
  echo "win32 binding already present: $DEST"
  exit 0
fi

echo "downloading ${PKG}@${VERSION} ..."
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
curl -sL -o "$TMP/binding.tgz" \
  "https://registry.npmjs.org/${PKG}/-/${PKG##*/}-${VERSION}.tgz"
mkdir -p "$DEST"
tar xzf "$TMP/binding.tgz" -C "$TMP"
cp -r "$TMP/package/"* "$DEST/"
echo "installed to $DEST"
ls -la "$DEST"
