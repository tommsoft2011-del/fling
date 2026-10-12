# Fling 需求文档（v1 收集，2026-10-12）

跨设备剪贴板同步工具。LiveKit 只做信令 + NAT 打洞，不经手明文内容。

## 阶段划分

- **v1（本期）**：CLI，发送文本；Linux + Windows 二进制；目标：我和老板两人先联通。
- **v2**：文件传输（分片 + 校验 + 重组）。
- **v3**：Web 版（浏览器打开即用）。
- **v4**：各平台 GUI 客户端（桌面 + Android/iOS）。

## 架构：一个核心，多端复用

TypeScript monorepo（npm workspaces），所有端共用 `packages/core`：

```
fling/
├── packages/
│   ├── core/      # 纯 TS：配对码、房间派生、E2E 加解密、LiveKit 房间封装、消息信封
│   └── cli/       # Bun 编译的命令行（linux-x64 / windows-x64 单二进制）
├── apps/
│   ├── web/       # v3：浏览器版，直接复用 core（livekit-client 是官方一等 SDK）
│   └── desktop/   # v4：Tauri 套壳 web UI，复用 core
└── mobile/        # v4：Tauri mobile / Capacitor，复用 core（待定）
```

选 TS 而不用 Go：Web 端必须用 JS 生态（Go→WASM 跑 WebRTC 很痛苦），
只有 TS 能让 CLI / Web / GUI 真正共用同一份核心。CLI 用 `bun build --compile`
打出单文件二进制；Android 的 CLI 先走 Termux + node 跑脚本版。

## 传输设计（LiveKit）

- Server：`wss://rtc.syncpage.app`（老板自建）。
- 文本走 DataChannel 可靠消息，topic `clip-v1`，JSON 信封：
  `{ v:1, kind:"text"|"hello"|"bye", ... }`。
- v2 文件：按 32KB 分片，`file-manifest` + `file-chunk{fileId,seq,total,sha256}`，
  接收端重组后校验 sha256。

## 配对与安全

- A 端 `host` 生成 6 位配对码（Crockford base32，去掉易混字符）。
- 房间名 = `clip-` + sha256(配对码) 前 16 位 hex；双方输同一码即进同一房间。
- Token 由 CLI 本地用 API secret 签发（livekit-server-sdk 的 AccessToken），
  secret 只存本机 `~/.config/fling/config.json`（0600），不进 git、不进二进制。
- **E2E 加密默认开**：配对码经 PBKDF2 派生 AES-GCM 密钥，消息体加密后才进
  DataChannel，server 端也看不到剪贴板内容。配对码即口令（6 位约 30 bit 熵，
  短时配对可接受；文档注明风险）。
- 防输错码：加入后双方发加密 `hello` 握手，N 秒无对端则提示"等待对方加入，
  请检查配对码"。
- 公开发布 Web 版之前，必须把"本地签 token"换成 token server（小服务代签），
  否则 secret 会随前端分发。这是 v3 的前置门槛。

## CLI v1 命令

- `fling init` — 交互式写入配置（server URL 默认 wss://rtc.syncpage.app、api key、api secret）。
- `fling host` — 生成配对码并监听；收到文本自动写入本机剪贴板并打印。
- `fling join <code>` — 加入房间；stdin 逐行发送（`/` 开头为命令，如 `/quit`）；收到同上。
- `fling send <code> <文本>` / 管道输入 — 一次性发送后退出。
- v1 默认手动发送；`--watch` 监听本机剪贴板变化自动发送列为 v1.1（默认关，避免误发密码）。

剪贴板实现：Linux（wl-copy / xclip / xsel）、Windows（clipboardy）、
Android Termux（termux-clipboard-get/set，需装 Termux:API）。

## 里程碑

- **M1**：VM 上双进程联调（两个 CLI 连 wss://rtc.syncpage.app 互发文本），验证 server 真的是 LiveKit。
- **M2**：打出 linux + windows 二进制，老板 Windows 端实测联通。
- **M3/M4/M5**：文件、Web、GUI。

## 待确认（2026-10-12）

1. LiveKit API key + secret 如何交接（安全录入卡一次只能收一个值；secret 只在 CLI 本地签 token 用）。
2. GitHub 仓库名（提议 `clipsync`，放老板账号下新建）。
