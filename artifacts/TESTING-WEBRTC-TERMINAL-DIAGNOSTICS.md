# WebRTC 确认后失败：诊断候选

本分支以 `b2f2e4fd38c50de54351b9cb2c93b9232c930cf2` 为唯一父提交。本包用于 Server 2022 发送到 Win10 接收的下一次取证，**不是已证实根因的修复包**。旧诊断包和旧工作树保留，不更新 main、正式 tag 或 Release。

## 当前能确定什么

原双端日志的两次尝试均记录了双方短码确认与鉴权确认，PC 创建成功，offer/answer 控制通道有收发记录。没有 DataChannel open、manifest 或 first-frame。发送端的 `answer_received` 不能证明 answer 已解析或应用；原来的 `data_channel_close state=ok reason=none` 也不能表示连接成功或说明谁先关闭。缺少候选汇总不等于没有候选。

本轮自行核对了生产源码中这些位置及生命周期边界。提供的归档审查任务 ID 在当前执行环境没有可读取入口，未声称重新打开该任务；采用交接结论定位源码后自行验证。没有把时间相邻、候选、防火墙或生命周期瑕疵判为本次唯一根因。

## 本包增加的观测

所有传输详情沿用同一个配对 `attempt`（即 UI 的 `diagnosticAttempt`），放在原 pairing 事件的 `transport` 对象中。

- `answer_received` 仍只表示收到控制消息。随后单独记录 `answer_validate`、`answer_peer_lookup`、`answer_parse`、`answer_set_remote`，以及实际原生调用 `set_remote_description` 的 start/ok/error 和闭合错误枚举。offer 同样观测。
- `pending_ice_apply` 记录起始数量、应用成功数量和失败枚举；汇总记录 taken/applied/abandoned。PC 初始化时既有 pending 数量另记 `pending_at_peer_install`。
- 候选只累计 generated、filtered 各理由、queued、written、queue/write failed、received、pending、added、rejected。拒绝使用单一枚举分类：载荷超限/格式、候选行格式、非 host、非数字地址、非 LAN、与发现端点不符、pending 上限、缺少 mid/mline、原生 parse/add、状态锁。计数饱和到 u32 上限；固定枚举 map 有界。gathering complete、首个终态、cleanup 开始及 close 返回后做低频累计快照，不逐候选或逐帧打印。
- ICE connection/gathering 保存初始状态和实际转换；重复相邻状态去重，但 checking→connected→checking 不会被全局去重吞掉。DC 区分 initial 与 transition。
- 旧 `data_channel_close` 改为 `state=closed, reason=not_reported`。实际 Closed 回调的 `closeObserved=true, closeOrigin=unknown`；即使此前有本地 close 请求，也不推断回调原因。
- 显式本地 close 调用前另发 `local_close_requested`，该事件 `closeObserved=false, closeOrigin=local_cleanup`，表示已知的本地调用，不表示回调因果。Closed 事件附带 `localCloseRequested` 布尔值供区分顺序。不编造 remote/transport 底层原因。
- `first_terminal` 在同一诊断锁内只写一次且不可覆盖；`cleanup` 单独记录清理理由并保留首因。因此可以区分 answer/ICE 错误后本地清理与通道先关闭。

新增结构只接收阶段、枚举、布尔、计数；没有原始短码/输入、密钥、IP/MAC/port、SDP、candidate、session/task/transfer/device/service ID、文件路径/名称/内容或 OS 错误字符串。现有进程内盐化来源指纹语义不变。

## 测试的准确边界

新增原生回归复用生产的配对确认/解密函数、PC 初始化、transport dispatch、Answer/Offer/ICE handler、DC 等待 helper、reader 读取观测及 registry cleanup。两个真实 libwebrtc PC 完成 offer/answer 应用，并测试 native set-remote 失败、parse/policy 失败、pending 和延迟候选、唯一拒绝计数、队列写入/写失败、EOF 清理、ICE/DC 状态及超时、并发首因保护。隐私测试逐项覆盖新阶段/结果/原因/计数枚举，并通过真正的手动 export_text 路径检查隐私哨兵。

**这不是完整生产入口端到端通过。** 夹具在内存中交换真实加密控制帧；不创建两个 Tauri AppHandle，不启动生产 TCP reader/writer/router 任务、UI command 和完整文件传输链。DC open/close/timeout 的新等待测试使用状态注入；既有真实 DataChannel 数据往返测试也绕过完整生产 router。前端/IPC/UI和生产 handler 接线另有静态/DOM mock 契约，不等于原生窗口验收。尚未在 Server2022→Win10 双机器、真实网卡/防火墙环境复现本次失败，因此不能据此宣称故障已修复。

一个注入排序测试记录了既有行为：PC 尚未安装时排队的候选会在初始化时被清空。本轮仅把数量暴露出来，**没有改变该行为**；该注入未证明真实 Windows router 发生此排序，也未证明与本次日志故障相关。router/worker 生命周期、取消及引用环不在本轮做架构改写。

## 本地检查

```bash
CC=clang-21 CXX=clang++-21 CARGO_TARGET_DIR="$PWD/validation/target" \
  cargo test --locked --manifest-path apps/desktop/src-tauri/Cargo.toml
(cd apps/desktop && for t in test/*.ts; do bun "$t" || exit; done && bun run build)
git diff --check
```

隔离分支使用现有 `test/pairing-confirm-observability-` 前缀门禁，workflow 文件与父提交完全一致（无需扩大令牌 workflow 权限）。手动触发 release workflow 时，仅 Windows x64 desktop job 构建 portable EXE，CLI build 和正式 Release job 应为 skipped。下载后必须核对 workflow head SHA、包内 `diagnostic-build.txt` commit、SHA-256 文件与实际 EXE 一致，再交付；绝不复用旧 EXE。

## 一步复测

**两端完全退出旧进程，均启动本轮同 SHA-256 的新 EXE；按原路径 Server 2022→Win10 发送一个小文件，双方确认短码一致后等到成功或明确失败，再立即分别手动导出局域网诊断日志并回传。** 不需要把短码、文件或候选原文另行提供。优先查看发送端 answer 原生应用结果、双方最后的候选累计快照，以及各自 `first_terminal → cleanup → local_close_requested/Closed` 的记录顺序；不按跨机器壁钟顺序推断因果。
