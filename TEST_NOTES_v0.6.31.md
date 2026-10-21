# Fling 0.6.31 路径可验证诊断测试版

这是 TEST-only 里程碑，不是正式发布，也不宣称已经修复跨网连接问题。保留 0.6.30 原包，两个 Windows 测试端都换成本次同一构建；不要混用版本。

## 这次改了什么

只新增路径观测、低敏感度阶段日志和准确标签，不改变 STUN/TURN 配置、ICE 策略、连接超时、传输窗口、ACK 协议或落盘顺序。

界面区分“已建立直连”“已通过 TURN 中继连接”和“连接已建立，路径暂不可确认”。只有 Rust 统计里 transport 指向的成功所选候选对才可证明路径。配置了 TURN、出现 relay 候选、请求 fallback、DataChannel 打开，都不能单独证明已走 TURN。统计不可用时为 unknown；历史页也保留未知路径，不把它记为直连。

TURN 中继的是加密的 WebRTC DataChannel 数据包，不是 LiveKit 应用层文件中继。原生普通单文件路径仍不自动切换到 LiveKit 文件中继。

## 跨网络测试

1. 两台 Windows 使用不同网络，保留原有 TURN 配置，发送一个数 MiB 的普通文件；先测 A→B，再交换方向。
2. 观察连接标签、真实接收进度和最终完成状态。DataChannel 打开不代表已有字节；100% 字节也不代表校验和落盘确认完成。
3. 两端保留同一次测试的完整应用日志。搜索 `path diagnostic`：应能看到角色、阶段、`route`、`selected`、`pair_state`、`nominated`、两端候选类型、两端 UDP/TCP、耗时和统计快照年龄。
4. 只有 `route=turn_relay`、`selected=true`、`pair_state=succeeded` 且至少一端类型为 `relay`，才算实际走 TURN。若走直连，这是有效直连结果，不能算 TURN 已验证；若 unknown，也不能推断中继失败或成功。
5. 正常接收应分别出现 `first_real_bytes`、`disk_synced`、`hash_verified`、`atomic_commit`、`durable_ack_sent`；发送端应出现 `durable_ack_received`，原生任务最终还需任务 ACK。失败时核对 `connection_failed` / `transfer_failed` / `connection_closed` 与 `task_end`，不要只看单端进度截图。

`transfer_no` 只是单个本地会话内的文件序号，不能跨设备关联身份；日志不记录原始任务/会话/传输 ID、IP、端口、原始候选/SDP、TURN 地址/凭证、文件名/路径、哈希或内容。日志只在关键变化时产生，不逐包输出。

## 断网局域网测试

1. 两台机器保留同一局域网连接，仅切断外网；不要禁用承载局域网的网卡。
2. 通过附近设备/LAN 路径双向发送普通文件，确认发现、确认、进度与保存结果正常。
3. 比较发送源和接收文件 SHA-256，确认一致；原有同名文件不应被覆盖。
4. 该路径不应依赖在线房间或 TURN。这里的 `path diagnostic` 是在线 WebRTC 观测，LAN 测试没有这类在线日志并不等于失败。

## 构建与验证边界

启动日志 `buildId` 使用 EXE 内嵌的 `v0.6.31+完整提交 SHA`；另记前端构建身份供对照。交付时核对 Git 提交、Windows workflow 的 head SHA、artifact sidecar、EXE 内嵌身份及 EXE SHA-256。

CI、本地自动化和引擎测试都不等于真实两台 Windows 的跨网/TURN 或离线 LAN 验证。实际网络结果需要这轮双机测试确认。本次不创建正式 tag/Release，不移动 main、稳定版或其他原有引用。
