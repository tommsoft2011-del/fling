# Fling v0.6.3 启动看门狗测试版说明

## 根因与修复

诊断任务 `task-mvaotjaf-1`、传输 `d63fa73f` 中，512,126,585 字节文件的来源/manifest 准备耗时 12,394 ms。原 8 秒 peer-progress watchdog 在准备开始前就已启动；到 8,003 ms 时，DataChannel 与 host-UDP 仍处于 open，但首个 direct 数据帧尚未被接受，已发送字节和接收端字节都为 0，因而过早判成无进展。

修复版将来源/manifest 启动阶段设为有界期限；manifest 确认后，首个 direct 数据帧有单独的 30 秒启动期限。8 秒接收端进度 watchdog 只在 DataChannel 接受首个数据帧后启动；显式 native send 或连接失败仍立即走 relay，不等待计时器。首帧期限耗尽时会开始 relay 恢复，并阻止迟到的 direct 首帧继续发送。

ACK、同文件 relay 修复/重放、有界背压、最终 hash 校验、接收端 sync、原子提交及持久化完成 ACK 均保留。

## 两个 120 秒 deadline 的准确关系

**后台预哈希**和**下一文件的 source/manifest 启动总预算**是两个不同的绝对期限；它们在墙钟上可以重叠，但下一文件选中后不会串行追加两个完整 120 秒。

1. 前一文件收到 `onFileFinalizing` 时，会调用 `prepareNextBlobManifest()` 启动下一份已在内存中的 Blob 预哈希。此处创建 `preparedManifest.deadlineAt = Date.now() + 120_000`。预哈希与前一文件 finalizing 并行运行；懒加载的 native-path 文件不会提前预哈希。
2. 下一文件进入发送循环时，任务先设置 `currentFileStartedAt`，并创建 `currentFileStartupDeadlineAt = currentFileStartedAt + 120_000`，然后才会 `await preparedManifest.promise`。如果预哈希尚未完成，await 确实会先串行等待它；但预哈希自己的 `deadlineAt` 更早，因为它在前一文件 finalizing 时已启动，所以它可能先到期。它不会在下一文件 120 秒预算之后再获得新的完整 120 秒。
3. 预哈希完成（或无需预哈希）后，`sendFile` 收到同一个绝对 `startupDeadlineAt`。来源读取、需要时的 manifest hash、manifest 发布与 ACK 都使用这条 deadline 的剩余时间；不会因进入 `sendFile` 或 await 结束而重置为 120 秒。若预哈希超时，当前任务报对应错误，不会再重新启动一个完整 hash。

换言之：预哈希的 120 秒从**前一文件 finalizing**时开始；下一文件的 120 秒从**该文件开始发送**时开始。两个 deadline 可在这段时间里重叠；若 await 发生，它是顺序上的等待，但其余量受更早的预哈希 deadline 限制，同时后续 `sendFile` 仍受从文件开始发送时创建的绝对总预算约束。不会从“下一文件被选中”开始等待 120 秒预哈希、再等待另一个 120 秒 source/manifest。

## 从下一文件开始发送到首帧的上限

这里的“首帧”按路线区分，避免把“开始回退”误写成“relay 数据已发布”：

- **Direct 可用时：**从下一文件的 120 秒启动预算起点，到首个 direct 帧被 DataChannel 的 `send()` 接受，最长为 **120 秒 source/manifest 总预算 + 30 秒 direct 首帧期限 = 150 秒**。30 秒期限从 manifest ACK 后首次 direct chunk 准备/提交时开始，包含首块读取和必要的 channel drain；`send()` 返回即表示本地接受该帧并停止该 timer。若 30 秒内未接受，期限到时会立即开始 relay 回退；显式 native send/连接错误会更早回退。8 秒 peer-progress watchdog 在首帧被接受前尚未启动。
- **150 秒只保证到 direct 首帧被接受，或 direct 首帧期限触发并开始回退；它不是 relay 首帧的上限。**若等到 30 秒期限才转 relay，代码还可能重发 relay manifest（publish/ACK 并行，最多 8 秒），再等待 relay 首块准备（最多 120 秒）和 relay publish（最多 15 秒）。若这些阶段都在期限边界前成功，从下一文件开始发送到首个 relay chunk 的本地 publish 返回，保守上界约为 **293 秒（120 + 30 + 8 + 120 + 15）**。relay publish 返回只表示本地发送队列接受，不代表接收端已确认。Relay 从一开始就是选定路线时，manifest/ACK 在 120 秒总预算内；首个 relay chunk 准备与 publish 另最多 135 秒，保守上界约 **255 秒**。

## 计时器速查

| 阶段 | 上限 | 起点与到期含义 |
| --- | ---: | --- |
| 后台 Blob manifest 预哈希 | 120 秒 | 前一文件 finalizing 时由 `prepareNextBlobManifest()` 启动；`preparedManifest.deadlineAt` 独立计时，可能在下一文件开始发送后的启动 deadline 前到期。 |
| 下一文件 source/manifest 总预算 | 120 秒 | 每个文件开始发送时创建 `currentFileStartupDeadlineAt`；source read、manifest 准备、发布和 ACK 共用同一个绝对预算。 |
| manifest publish / ACK | 各最多 8 秒，且受 source/manifest 总预算约束 | publish stage 和 ACK waiter 并行启动，因此不是两个串行 8 秒。 |
| 首个 direct 数据帧启动 | 30 秒 | manifest ACK 后，首次 direct chunk 准备/提交时开始；`DataChannel.send()` 返回代表本地接受该帧。到期时开始 relay 恢复。 |
| direct peer-progress | 8 秒 | 仅在首个 direct 数据帧被 DataChannel 接受后启动；接收端进度会刷新静默时长。检查轮询间隔为 1 秒。 |
| 首个 relay chunk 准备 / publish | 各最多 120 / 15 秒 | direct 首帧回退后，relay manifest 恢复完成后开始；不属于 150 秒 direct 上界。 |
| direct window ACK / relay window ACK | 8 / 5 秒 | 等待相应数据窗口 ACK；超时后执行已有的有界重放/恢复。 |
| 文件最终持久化完成 ACK | 120 秒 | 等待接收端完成校验、sync 与原子提交后的 durable completion ACK。 |
| direct channel drain | 3 秒 | 等待有界 DataChannel 背压排空。 |
| relay manifest publish / ACK after fallback | 最多 8 秒 | publish deadline 与 ACK waiter 并行计时。 |
| P2P 连接建立（既有） | 12 秒 | 连接阶段既有期限；明确连接错误会直接进入回退。 |

### 重要：15 秒“停滞 0%”提示

v0.6.3 taskcard 现有逻辑会在 sending 阶段 15 秒无字节推进时显示“停滞 0%”。本修复**没有改 UI**，所以来源/哈希准备尚未完成时仍可能看到该提示。首个 direct 帧 accepted 之前，提示本身不代表 8 秒 peer-progress watchdog 已启动或已回退；请看分阶段日志并等待相应的有界期限，不要仅因该提示取消。

- `peer progress watchdog deferred until first accepted frame`：仍在等首个 direct 数据帧，8 秒 peer-progress watchdog 尚未启动。
- `transfer timer stage fired: stage=first_direct_frame_startup`：30 秒 direct 首帧期限到，接下来检查 relay 恢复；这不是“relay 首帧已到达”。
- `timerStage=source_manifest_startup`、`file_source_read`、`manifest_preparation`、`manifest_publish` 或 `manifest_ack`：对应来源/manifest 阶段到期。
- `timerStage=peer_progress_after_first_frame`：首帧已被接受后，接收端 8 秒无进度，才属于真正的 peer-progress watchdog 回退。
- `stage=direct_window_ack`、`relay_window_ack`、`durable_completion_ack`：分别表示数据窗口 ACK 或最终持久化 ACK 阶段触发期限。

## 验证结果

以下代码验证在本次文档修订前已通过；本次只改文档，没有改传输行为：

- Core TypeScript 类型检查；desktop `tasks.ts` 直接类型检查；Vite production 构建成功（只有既有分块告警）。
- `test:startup`：12,394 ms 慢准备、30 秒首帧边界、8 秒 peer-progress 精确边界、首帧接受前不启动 watchdog、准备阶段超时、stage 标签及取消清理。
- `test:p2p`：首帧超时 relay 恢复、迟到 direct 帧抑制、ACK stall、native send 失败立即回退、durable final ACK，以及 8/16 MiB 有界窗口与 16 MiB 部分丢失修复；最终 hash/size 校验通过。
- `test:stream`：123 MB 有界流式传输、SHA-256、ACK/finalization 及中断清理通过。
- `cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml`：15/15 通过，含“不覆盖既有文件”和接收端保存/原子提交相关测试。编译有一条既有 unused-variable 警告。
- P2P 大文件用例为合成数据的内存测试，不是实际局域网传输或磁盘 fsync 性能测试。

## 本次文档与构建

本次修订仅改变计时说明文档，不改 UI 或发送行为。文档会提交到隔离测试分支 `test/v063-startup-watchdog-20261017`；Windows EXE 会从该提交对应的 branch HEAD 通过现有 workflow 重建。具体 commit/head SHA、workflow run 和 EXE SHA-256 以本次交付消息及该 workflow run 为准。没有创建 tag 或公开 release；应用版本仍显示 v0.6.3。