# Fling

Fling text (and soon files) between your devices. Fast, end-to-end encrypted, powered by LiveKit.

One shared TypeScript core (`packages/core`) drives the CLI today and the web / GUI clients tomorrow.

## Download

Prebuilt binaries are published on the
[Releases page](https://github.com/tommsoft2011-del/fling/releases):
`fling-linux-x64`, `fling-linux-arm64`, `fling-windows-x64.exe`
(each release ships a `checksums.txt` — verify with `sha256sum -c`).

## Setup: where to put your key & secret

Fling reads **`config.json`** — you create it yourself, the key/secret never
touches the repo or the chat:

| Platform | Path |
|---|---|
| Windows | `%APPDATA%\fling\config.json`  (e.g. `C:\Users\你\AppData\Roaming\fling\config.json`) |
| Linux | `~/.config/fling/config.json` |
| macOS | `~/Library/Application Support/fling/config.json` |

Format:

```json
{
  "serverUrl": "wss://rtc.syncpage.app",
  "apiKey": "YOUR_LIVEKIT_API_KEY",
  "apiSecret": "YOUR_LIVEKIT_API_SECRET"
}
```

Or run `fling init` and type the values interactively. `fling config` prints
the exact path on your machine.

## Usage

Device A (show a pairing code and listen):

```
fling host
  Pairing code:  K7Q2M9
```

Device B (join with the code, then type lines to send):

```
fling join K7Q2M9
```

One-shot send (or pipe stdin):

```
fling send K7Q2M9 "hello from the other side"
echo "paste me" | fling send K7Q2M9
```

Received text is printed **and** copied to the local clipboard automatically.

## How it works

- The 6-letter pairing code derives both the LiveKit room name
  (`SHA-256 → fling-<16 hex>`) and an AES-GCM-256 key (PBKDF2, 100k rounds).
- Messages are encrypted **before** hitting the LiveKit data channel —
  the server only ever sees ciphertext.
- The API secret stays on your machine; the CLI mints its own join tokens
  locally. Nothing is uploaded anywhere.

## Building

```bash
bun install
# Windows binary (run on the Linux build machine first):
bash packages/cli/scripts/fetch-native-bindings.sh all
bun build --compile --target=bun-windows-x64 --outfile dist/fling-windows-x64.exe packages/cli/src/main.ts
bun build --compile --target=bun-linux-x64   --outfile dist/fling-linux-x64   packages/cli/src/main.ts
bun build --compile --target=bun-linux-arm64 --outfile dist/fling-linux-arm64 packages/cli/src/main.ts
```

## Project layout

```
packages/core   shared logic: pairing, E2E crypto, protocol, LiveKit room wrapper
packages/cli    bun-compiled CLI (linux-x64 / windows-x64 single binaries)
apps/web        (v3) browser client — reuses packages/core
dist/           prebuilt binaries
```

## Roadmap

- v1: CLI text send ✅
- v2: file transfer (chunked, checksummed)
- v3: web client
- v4: desktop / mobile GUI
