# 最小 Rust Data Plane POC（v0.6.3）

> **仅供实验测试，不是生产替代版。** 本分支从公开 v0.6.3 commit `9cb4e3602a9e8921a53c5a9630a1f96b57e1307c` 派生。它使用独立的 Windows 应用标识和配置目录，与生产版隔离。

## POC 范围

- Rust（`webrtc-rs`）负责 WebRTC `RTCPeerConnection` 与可靠、有序的 DataChannel（`fling-file`），包括 DTLS/SCTP 数据传输；**不引入自定义 UDP/QUIC 传输**。
- 信令/控制仍走现有加密 LiveKit room；Rust 桥接原有 `p2p-offer`、`p2p-answer`、`p2p-ice` 信令及 FLNG 二进制帧。
- 继续使用 v0.6.3 的 manifest、32 KiB 逻辑分块、直连窗口/进度 ACK、SHA-256 校验、`sync_all`、不覆盖既有文件的原子提交、最终 `task-ack` 与 relay fallback。
- Rust 接收端最多保留一帧（最大为现有 128 KiB 批帧加 30 字节头），交付 WebView 后等待该帧的 handoff ACK 再读取下一帧；WebView 发往 Rust 的待发队列上限为 1 MiB。现有应用 ACK、窗口和恢复逻辑仍负责端到端流控。
- 接收文件仍由既有 TypeScript `FileReceiver` 组装，再顺序流式写入 Rust 临时文件。因此，**本 POC 尚未证明接收端堆内存下降**；目前只改变 DataChannel 所有权，并测量已有的流式写盘/校验路径。
- Tauri app identifier 为 `app.fling.desktop.rust-poc`，产品名/窗口标题为 `Fling Rust Data Plane POC (EXPERIMENTAL)`；设置配置与生产版 `app.fling.desktop` 分开。

## 当前互操作结论

- **实现/编译：** Rust/Tauri 与 TypeScript 均可编译；现有协议回归测试通过，包括 32 KiB chunk 重组、流式保存/哈希、ACK/最终确认和 relay 修复。
- **真实 v0.6.3 WebView ↔ Rust DataChannel 两机互操作：尚未实测确认。** 编译与内存模拟测试不能证明两个物理设备间 SDP/ICE 一定互通。请按下方流程实测并记录是否完成直连协商、双向数据传输及文件哈希校验。失败本身就是 POC 结果；不要改成另一种自定义传输。
- **尚无速度提升结论。** 现有内存测试不是物理网络、真实磁盘或 Rust POC 的性能数据，不可据此声称提速。

## Windows EXE 与首次启动

专用 GitHub Actions workflow 从 `experiment/rust-data-plane-poc` 分支构建 Windows 测试产物。下载该次运行的 `fling-desktop-Windows` artifact，解压后运行其中独立的 `.exe`。**不要卸载或覆盖现有 v0.6.3 安装。** 该 workflow 在实验分支上只上传 Actions artifact；不创建版本 tag，也不发布 GitHub Release。

首次启动时，在 POC 中填写与 v0.6.3 测试端相同的 Fling Server URL/API 配置。因为应用标识不同，POC 不会读取或覆盖生产版本地配置。

## 可复现的双设备 A/B 测试

两轮都使用相同两台 Windows 设备、相同网络路径、相同保存目录和完全相同的文件集合。两轮之间不要改变 MTU、防火墙、VPN 或电源设置。

1. **基线轮：** 两台设备都运行未修改的 v0.6.3。按 A→B、B→A 各传一次选定文件集合，保存两端诊断日志并记下目标文件。
2. **POC 轮：** 只把其中一台替换为实验 EXE，另一台仍运行 v0.6.3；为 POC 配置相同服务器设置，再按相同方向、相同文件重测。如果未能建立直连，保留日志，并记录现有 relay fallback 是否完成传输。
3. **完整性：** 对源文件和接收文件逐个核对大小与 SHA-256。PowerShell 示例：
   ```powershell
   Get-Item .\path\to\file | Select-Object Name,Length
   Get-FileHash .\path\to\file -Algorithm SHA256
   ```
4. 每个方向记录：路线（direct/relay/mixed）、建链结果、端到端耗时、接收 payload 耗时、保存 IPC、Rust 写入/哈希/sync/commit 阶段耗时、最终 ACK 耗时、CPU 与峰值内存、完整性结果及恢复/relay 修复情况。
5. 若首轮遇到瞬时网络或磁盘事件，建议每轮至少重复 3 次；失败样本也要保留，不能只挑成功数据。

诊断日志在应用的**设置页 →「打开日志目录」**。重点字段：

- `send end.elapsedMs`、`recv end.elapsedMs`：任务端到端墙钟时间。
- `receive payload phase.elapsedMs`：每个文件从首个正向接收进度到最后一段 payload 回调的时间。
- `Tauri streaming save IPC end.elapsedMs`：接收端保存管线总耗时。
- `Rust write_all stage.elapsedMs`、`Rust incremental SHA-256 update stage.elapsedMs`、`Rust SHA-256 final verify stage.elapsedMs`、`Rust sync_all stage.elapsedMs`、`Rust atomic commit stage.elapsedMs`：写盘、增量/最终校验、持久化和原子提交分段耗时。
- `task-ack sent.receiverFinalizationMs`：接收端收到 `task-done` 后，等待分块/保存完成直到发出首个最终 ACK 的时间。
- `final task-ack phase.elapsedMs`：发送端从发出 `task-done` 到收到最终 ACK（或超时）的时间。
- `file route selected`、direct frame、fallback/replay 日志：传输路线与恢复证据。

CPU 和峰值内存请在 Windows Task Manager 中分别记录 POC 与 v0.6.3 进程，并在相同文件集合/方向下比较。不要把现有内存模拟测试的耗时或吞吐当成局域网结果。

## 验收与恢复检查

- 网络允许直连时，两端都应协商出直接 DataChannel；若只能走 relay，则记录为“未实现直连互操作”或“relay-only”，不能算作直连成功。
- 每个完成文件在接收端大小、SHA-256 均与源文件一致。
- 接收方仍完成原有 SHA-256 校验、`sync_all`、不覆盖文件的原子提交和最终 `task-ack`，之后发送方才报告成功。
- 原生直连失败时应走原有 relay fallback；若发生部分文件修复，最后仍须通过原有大小与哈希校验。
- 任一检查失败时，保留两端日志并记录确切阶段/错误；不得为通过 POC 而移除或弱化生产安全、耐久性和恢复保证。

## 已完成的构建/回归检查

- `apps/desktop/src-tauri` 下 `cargo check`
- 桌面 Vite production build，以及对 `p2p.ts` / `tasks.ts` 的 TypeScript 类型检查
- Core P2P-routing、file-stream、direct-window 回归测试

这些检查验证了编译与既有协议语义，不代替上面的物理双设备互操作测试。
