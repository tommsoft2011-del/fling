# 0.6.32 直连优先：安全门禁记录

**状态以文末最新节点为准。** 下文旧设计、旧失败与停止记录保留，不是对后续已修复源码的结论；任何尚未通过的当前门禁仍阻止 Windows 出包。

本轮只实现了供测试调用的双通道选择处理器，未接入应用运行时。八项处理器测试实际运行结果为 **6 通过、2 失败**；有限状态探索深度为 6，共访问 12,349 个状态。这不是完整的应用协议或原生双端验收。

## 两个可重现的反例

测试直接调用 `online_route_choice.rs` 的同一个 `Choice::handle` 实现，不是另写的 Python 简化模型。双端信令由处理器产生的 Effect 驱动。

共同前置过程：发送端窗口到期 → 产生 BackupRequest → 接收端产生 BackupAck → 发送端产生 BackupCommit → 双端开始独立备用候选 → 双端备用候选 ready → 发送端提前写入 `reserved=Backup` 并产生 Select(Backup)。此时尚无文件发送权，也没有文件帧。

1. **Select 在途时直连成功。** 暂不投递 Select；让双端 Direct 都 ready 并交换 Ready，然后投递原 Select 和返回的 Selected。实际双方激活 Backup，验收要求是 Direct。测试 `acceptance_direct_ready_while_backup_select_is_in_flight` 失败。
2. **Selected 在途时直连成功。** 接收端已处理 Select，但先扣留返回的 Selected。此时发送端仍没有文件发送权。让双端 Direct ready 并交换 Ready，再投递 Selected。实际双方激活 Backup，验收要求是 Direct。测试 `acceptance_direct_ready_while_backup_selected_is_in_flight` 失败。

根因是不可逆 reservation 放在发送端生成 Select 的位置。它有利于证明单路发送，却把直连可胜出的窗口提前关闭到了双方确认之前。不能将要求悄悄改成“仅在 sender reservation 前直连优先”来宣布通过。也不能直接清空或改写 reservation：旧 Select/Selected 仍可能迟到，必须先设计并证明带代次栅栏的撤销/替代协议，且证明不存在旧路径文件发送权。

这两个反例没有证明发生双发或文件损坏；它们证明本候选方案尚未满足获批实验的完整边界条件，不能出近似通过的测试包。也不据此宣称所有双 PC 方案都不可能。

## 已实现但不足以放行的选择状态机

候选 Direct/Backup 分离，candidate-ready 不等于 active。固定 sender，receiver 永无文件发送权。sender 仅在收到与 write-once reservation 相同的 Selected 后获得 send permit。receiver 首次接受 Select 时锁定接收路径，重复同代消息返回同代 Selected。冲突选择不改写，Abort/Timeout 为吸收态；发送状态模糊时只重放同一事务，不自动换路或新开文件发送任务。

五项场景正控制覆盖直连确认、备用就绪不等于激活、有限的选择前直连边界、发送状态模糊时重放、各等待态超时及迟到事件屏蔽。第六项正控制在深度 6 内枚举本地 ready、窗口、重传、超时及已产生控制消息的任意再投递/乱序，检查 write-once reservation 一致和至多一个 sender permit。消息池不消耗已产生消息，因此允许重复投递；它不是所有深度、所有网络故障的完整证明，也不覆盖未实现的原生 offer/answer 副作用。

## 停止边界与未执行项目

`main.rs` 仅增加 `#[cfg(test)] mod online_route_choice;`。实验处理器明确标注 BLOCKED，不进入正常应用编译路径。原始在线运行时、文件引擎、UI、LAN 和版本号均未改动；未使用旧 unsafe WIP patch。

安全门失败后没有继续运行 0.6.32 Linux 双实例/文件测试，没有把基线探针当作本版证据，没有声称 SHA/落盘/最终 ACK、TURN 回退或 Windows 跨网络验收通过。完整 Rust/frontend/type/lint/build 放行流程未完成；未触发 Windows CI，未生成/交付 EXE，未提交或推送，未创建 tag/release。

唯一工作树 `/workspace/fling-v0.6.32-direct-first`，唯一分支 `test/v0.6.32-direct-first`，HEAD 保持 `0034bcd2c867f043c7041b051ceccee4dcd3cf17`。所有 Git 引用以本轮前后快照对比复核。

## 复现

原生工程测试入口（Linux 使用已安装的 clang 21）：

```sh
CC=clang-21 CXX=clang++-21 CARGO_TARGET_DIR=/tmp/fling-native-lan-target \
  cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml online_route_choice::tests -- --nocapture
```

本轮最终八项结果通过直接 `rustc --test` 编译同一源文件并链接缓存中的 serde 获得，无应用运行时启动。完整输出保存到 `test-results/direct-first-safety/choice-final-gate.log`；最小两个反例的输出保存到 `test-results/direct-first-safety/direct-boundary-counterexamples.log`。两者应返回 101，禁止把失败标为 ignored 或 should_panic 来绕过门槛。

随后已通过上面的原生工程 Cargo 标准入口复核：测试程序成功编译，实际运行 8 项，6 通过、2 失败、0 ignored，159 项既有测试未在本次筛选中运行；同样探索 12,349 状态，退出码 101。证据为 `test-results/direct-first-safety/cargo-choice-gate.log`。因此本次阻断不是编译器缺失或构建错误，而是两个真实断言失败。Git 全部引用前后逐行一致，`git diff --check` 通过。

## 继续审查：START_ACK 前全局直连优先仍有不可观测窗口

本次保持全部处理器代码与两个必需断言不变，再次从原生 Cargo 入口运行：8 项中 6 通过、2 失败、0 ignored；探索 12,349 状态，159 项其他测试被筛选。输出：`test-results/direct-first-safety/resume-choice-gate.log`。HEAD 与分支不变，`git diff --check` 通过，未接入运行时、未构建/推送/触发 Windows CI。

对拟议 PREPARE/PREPARED/COMMIT/COMMIT_ACK/START/START_ACK + REVOKE/ABORT 的独立理论审查发现：若要求是任一端实际 Direct-ready 发生于 sender 接受 START_ACK/授予 permit 之前均必须阻止 Backup，那么仅凭异步两端消息和有限确认轮次无法在正常无故障 Backup 情况仍可完成的同时保证此要求。

最小时间线：

1. receiver 发送 Backup 的 START_ACK；此时 sender 无 permit。
2. receiver 观察到 Direct-ready 并发送 REVOKE，REVOKE 被网络延迟。
3. sender 先收到旧 START_ACK；其本地历史与“receiver 从未 Direct-ready”的正常 Backup 执行完全一致。
4. 若 sender 授予 Backup permit，违反第 2 步先于 permit 的严格优先要求；若为了排除此可能永不授予，则同样不可区分的正常 Backup 执行也无法成功。加有限轮确认只把窗口推至最后一次远端确认之后。

这不是声称所有双 PC 设计或一般直连优先不可实现；阻断的是上述异步假设下的精确全局时间边界。sender executor 串行化只能排序已送达事件，不能排序远端尚未送达的事实。attempt/seq/path/generation 栅栏、双方 abort 和未知送达时 fail-closed 对安全仍必要，但不能让在途撤销先于已到达 ACK 被观察。普通额外查询或共享协调服务也不能自动消除此窗口：除非把 ready 的定义/发生纳入同一原子仲裁，仍有本地事实先于提交而通知在途的问题。

按交接中的明确停止条件，本次停在设计门。没有降低验收要求、改为 Backup 期望、忽略断言，亦未以基线包冒充 0.6.32。后续需要明确授权新的可实现语义（例如只保证 coordinator 已观测且在 permit 前排队的 Direct-ready，或明确较早双端冻结点），或给出能覆盖实际 ready 与 permit 并发窗口的可证明同步机制，再开发；这些都不是本次已获批变更。

## 2026-10-21：采用已明确接受的 coordinator 截止语义后续开发

以下更新取代上文关于旧优先边界未获授权的继续开发限制；上文及历史失败日志作为历史证据保留。新授权边界已经可实现，不再以“未报告的远端物理就绪也必须获胜”作为要求。等于截止时间的 READY 视为迟到，只有 coordinator 在严格 `<2500ms` 观察到的自身和远端 READY 才能选择 Direct。

已在同一工作树中重写 Rust `online_route_choice.rs`。包含带 attempt/generation/decision_seq/path 的事务、严格候选对证明、sender 唯一仲裁、request/prepare/commit/start 确认、START_ACK 唯一首帧许可、旧事件隔离、确认字节单调保留、候选缓冲上限/ufrag 绑定与去重，以及关闭未选 PC 的时序保护。保留八个原测试名字和场景（直连获胜窗口按新授权截止规则调整），历史失败日志未删除。十九项处理器测试通过；双端效果/消息池探索深度 12，访问 288 个状态，允许重复和重排。此有限探索不是完整证明，也不是实际 WebRTC 双端传输验收。日志：`test-results/direct-first-safety/cargo-dual-peer-final2.log`。

新增 `online_route_runtime.rs` 单 Tokio executor 候选适配层。其最初完整 Rust 测试曾为 179 passed，但该结果不足以放行；随后补入两个必须成立的生产适配门禁，发现两个真实失败：

1. `initial_effect_error_must_terminally_fail_before_route_selection`：初始副作用执行失败会向调用方返回错误，但由于用未选中的 PathFailed 代替 attempt 失败，handler 实际仍是 Gathering，而非 Failed，违背初始失败统一收敛要求。
2. `cutoff_must_include_time_since_actual_set_local_description_not_dispatch`：executor 把源 GatheringStarted 时间戳重写为事件被取出时的当前时间。模拟 setLocalDescription 在原点成功、启动事件延后 2700ms 入队后，截止仍未决策（None），而要求已锁 TURN。这会延长实际 2.5 秒窗口。

复现命令：

```sh
CC=clang-21 CXX=clang++-21 CARGO_TARGET_DIR=/tmp/fling-native-lan-target \
  cargo test --locked --manifest-path apps/desktop/src-tauri/Cargo.toml online_route_ -- --nocapture
```

真实结果退出码 101：22 项中 20 passed、2 failed、0 ignored，159 项其他测试被筛选。证据：`test-results/direct-first-safety/production-adapter-required-gate.log`。两个失败测试保留为正常断言，没有 ignored/should_panic 绕过。

按已确认的停止条件，本轮在这一生产适配门禁停止。两个新增模块均重新置于 `main.rs` 的 `#[cfg(test)]` 下，原生产 OnlineSession/OnlineP2p/信令/文件引擎、LAN-only、TS/WebView 和版本号均不变。不存在真实 direct/relay-only TURN PC 副作用接线、setLocalDescription 源 Instant 接线、能力协商与原生帧/ACK/UI 代次绑定的完整适配器，不能把该 executor 接口声称为完成的 0.6.32。

未执行 0.6.32 实际 Linux 双进程文件传输或 TURN 回退；未声称 SHA-256、落盘、最终 ACK、性能或 Windows 跨网络验收通过。未执行失败后的完整 frontend/type/lint/build 放行流程，未触发 Windows CI，未生成 EXE，未 commit/push/tag/release。后续必须先修复上述两项并完成生产适配与实际双进程安全/完整性门禁，再走最终构建及 Windows artifact 校验；不能交付旧包替代。

最终仍使用 `/workspace/fling-v0.6.32-direct-first`、`test/v0.6.32-direct-first`，HEAD `0034bcd2c867f043c7041b051ceccee4dcd3cf17`。没有新增工作树/分支，没有修改 0.6.31/0.6.30/稳定 0.6.29 等其他工作树或任何 Git 引用。

## 继续开发：源 Instant 与本地失败清理已修复，初始消息丢失收敛仍阻断

本次仍在指定唯一工作树和分支开发，两个模块继续由 `#[cfg(test)]` 隔离。以下为本轮更新，不代表完整生产集成，亦不替换历史失败日志。

已修复此前两项适配器门禁：启动入口现在显式接受成功原生副作用源点的 `std::time::Instant`，通过队列原样传递，拒绝用数值事件猜测源点；重复或旧代启动事件不能改写时间原点。事件处理使用该源点的实际 elapsed，包含排队延迟。重试通过只允许 Idle/匹配 epoch 的源时钟绑定入口建立新 attempt 的原点。尚未从实际生产 `set_local_description` 成功点调用这一入口，不能将 API 修复说成生产源时间已接线。

副作用错误现在先把整个 attempt 转入 Failed，再有界地执行全部清理效果；发送终止通知失败不会跳过 CloseAll/Failed。每个副作用最长等待 2 秒；模糊 publish 一律本地失败，后续原始副作用停止，不授予首帧许可。这只建立本地 fail-closed，不证明对端已收到终止消息，也不能证明取消一个 publish future 会取消底层已经发生的发布。

新增明确 `AttemptAbort`，使用同 epoch 的规范 Direct/seq=0 标识整个 attempt，允许双方决策阶段不同或 Decision 丢失时终止；原来的路径级 `Abort` 栅栏和原断言保持不变。新增跨阶段终止、规范终止栅栏、旧代终止隔离、重试源时钟、阻塞副作用超时、部分创建失败及终止发布失败仍执行清理的正常测试。

原有两个失败门禁及其断言保留，测试仅将数值 GatheringStarted 调用改成显式源 Instant 入口；延迟测试依旧真实等待 2700ms，仍要求锁 TURN，初始错误测试仍要求 Failed。首次修复后的筛选门禁曾为 27 passed、0 failed、0 ignored，日志为 `test-results/direct-first-safety/source-instant-adapter-gate.log`。随后扩展测试后的完整原生 Cargo 门禁结果为 **189 passed、1 failed、0 ignored、0 filtered out**（退出 101），日志为 `test-results/direct-first-safety/initial-offer-convergence-full-gate.log`。

### 新的真实阻断：初始 offer 和终止通知均丢失

正常失败测试 `initial_offer_and_abort_loss_must_have_bounded_remote_failure` 对应以下时序：接收端已确认 manifest 并等待初始 offer；发送端 createOffer/初始 offer 发布失败，进入 Failed；终止通知同样未送达。接收端未成功执行 setLocalDescription，因此没有 gathering 起点。对接收端推进至 15 秒并处理 Tick，实际仍为 Idle，断言要求 Failed。当前 Idle Tick 没有 setup 看门狗；不是放宽 READY 或换一个 Direct 决策起点能够解决的问题。

实际生产代码检查也显示对应缺口：`online_room.rs` 的 reliable publish 只有本地 Result，不能代表远端已消费；`online_session.rs` 的接收端 manifest ACK 后没有初始 offer/attempt setup 看门狗，等待 DC 的超时要收到 offer 并创建 PC 后才开始；发送 `send_source` 的错误结尾只更新本地状态，不保证向对端送达终止。生产 offer/answer/ICE 也仍缺少本实验所需 attempt/generation/decision_seq/path/ufrag 封装和确认。索引证据为 `test-results/direct-first-safety/initial-offer-production-timer-audit.txt`。这不是声称异步信令下永远无法实现有界失败，而是**当前实现尚未建立这项保证**。

下一步必须在真实 Rust signaling/manifest 接入显式 attempt setup 生命周期、不可重标的源代次、独立于 2.5 秒 Direct 决策时钟的有界失败看门狗，以及幂等终止/确认。该 setup 时钟只允许失败，不能替代 successful setLocalDescription 的 Direct 起点，也不能让双方独立选择路径。新失败断言不得 ignored/should_panic 或改成 Idle 来绕过。

按安全门停止：未将测试状态机当生产集成；没有修改生产 OnlineSession/OnlineP2p、TS/WebView、LAN-only、版本号或正式发布流程。未运行 0.6.32 实际 Linux 双进程在线文件验收、TURN 回退、性能验收或最终 frontend/type/lint/build 放行。完整 Rust 测试包含的既有文件/SHA/sync 和 LAN WebRTC 用例不等于本版本在线双进程验收。未触发 Windows CI、未生成或交付 0.6.32 EXE、未 commit/push/tag/release。交付测试包仍被阻断。

## 最新继续开发：真实 setup 看门狗已接入，完整 direct-first 仍不得出包

本节更新上文“生产 OnlineSession/OnlineP2p 未修改”的历史状态；保留全部旧失败证据。仍使用唯一工作树 `/workspace/fling-v0.6.32-direct-first` 和分支 `test/v0.6.32-direct-first`，HEAD 为 `0034bcd2c867f043c7041b051ceccee4dcd3cf17`。版本字段仍为 0.6.31，没有把不完整实现改名为完成的 0.6.32。

### 已接入正常生产编译的修复

新增 `online_setup.rs`，由 `OnlineSession` 直接调用。原生单文件接收端仅在 Rust 已接受 receiving task 并验证 manifest 名称、大小、种类后启动独立 15 秒源 Instant 期限，且在 manifest ACK 发布前启动。生产与测试共享 `wait_for_timeout` 自动等待路径；初始 offer、终止通知均丢失也能失败，不依赖后续消息或手工 Tick。该时钟只允许失败，不选路，不是也不提前启动 2.5 秒 gathering 时钟。

超时先栅栏 session 并发出 Rust native error 状态，再回收 receiver/ACK/PC；TaskCancel 是清理后的最多 500ms 最佳努力通知。offer 接纳是原子的，重复 offer 只忽略、不创建第二个 PC、不杀死正常连接。超时后 ICE/offer 不能重启 setup。accept_offer 受原绝对期限和本地取消约束，成功返回（其内部已成功 setLocalDescription(answer)）后才撤销 setup。取消/超时后 receiver pump 增加保护。完整 AppHandle、网络和磁盘并发仍未被集成验收，不能据这些局部保护宣称全部旧回调都已安全。

原生初始 createOffer/setLocalDescription、manifest ACK、offer/answer 发布使用共享 2 秒副作用失败上界；明确错误或模糊发布超时不视为成功，不触发原生初始错误的 fallback_requested 流程。sender 错误结尾关闭已登记 PC 并清空 pending ICE；新增 OnlineP2p Drop 关闭尚未登记的部分创建 PC。真实原生 PC 测试验证取消挂起 future 后 PC 关闭，不代表文件端到端验收。

相同完整 manifest 重投只重发 ACK，不重建文件、不重置看门狗。比较覆盖完整元数据；新增 PartialEq 不改变 JSON 格式。冲突 manifest 仍 fail-closed。比较单测不等于包含 AppHandle/room 的真实重投集成测试。

### 测试处理器与保留的反例

test-gated route handler 新增可信 SetupExpected；原缺失 offer/abort 失败测试只补上原注释已有的“manifest 已确认”触发事件，15 秒必须 Failed 的原断言保留。新增迟到启动、重复 setup、重试 epoch 隔离及 setup 不选 TURN 的测试。runtime setup/gathering 均保留源 Instant；普通事件入口不能猜测源点；切换到 gathering 时钟前先检查旧 setup 时钟。单串行 executor pulse 驱动 Tick，接收端无独立选路权。

生产 handler 第一版存在真实竞争：ICE active 或迟到 offer 先置 TimedOut 后，后台 timeout 返回 false，可能漏掉 native error/清理。新增两个正常断言实际失败，`setup-notification-race-required-gate.log` 为 4 passed、2 failed、退出 101。通过单次 cleanup claim 修复：已 TimedOut 但未领取通知仍必须通知一次。修复后的 `setup-notification-race-fixed-gate.log` 为 7 passed。失败日志、原断言均保留，没有 ignored/should_panic 绕过。

`setup-watchdog-route-gate.log` 保留了一次并行编辑中模块尚未写完的编译失败，不是最终源码的门禁结果。后续完整 Cargo 201/204/206 passed 分别对应新增回归前的源码快照，不覆盖后加测试。

### 最终完成的验证

**完整标准 Cargo：210 passed、0 failed、0 ignored、0 filtered out，退出 0。** 日志 `test-results/direct-first-safety/setup-production-cleanup-full-gate.log`。命令：

```sh
CC=clang-21 CXX=clang++-21 CARGO_TARGET_DIR=/tmp/fling-native-lan-target \
  cargo test --locked --manifest-path apps/desktop/src-tauri/Cargo.toml -- --nocapture
```

非测试生产 `cargo check --locked --manifest-path apps/desktop/src-tauri/Cargo.toml` 退出 0，日志 `test-results/direct-first-safety/setup-production-cargo-check.log`；存在工程 warnings，不能说 lint 全通过。`git diff --check` 通过；`refs-before-setup-watchdog.txt` 和 `refs-after-setup-watchdog.txt` 逐字相同。

### 仍禁止交付 EXE 的具体阻断

1. 路由事务与 executor 仍 test-gated。生产 offer/answer/ICE 没有本实验要求的 attempt/generation/decision_seq/path、SDP ufrag 封套、能力协商和确认，不能把 210 项通过当作真实 direct-first 集成。
2. 实际 OnlineSession 仍是一个 PC，OnlineP2p 仍使用 All ICE policy；没有完成生产 direct-only 初始 PC、截止后独立 relay-only TURN PC、严格 READY 采样及 successful setLocalDescription 源 Instant 的唯一 coordinator 截止接线。
3. 真实 sender 仍以现有 DC Open 路径进入发送，receiver 仍使用原 FLNG pump；没有把双方 START_ACK、唯一 selected PC/epoch 首帧许可和真实 frame/ACK/UI 代次贯通。测试 FirstFramePermit 不是生产发送门。
4. 尚无目标版本 Linux 两进程在线 direct/TURN、取消/重试/重复、源与目标 SHA、sync/原子发布和 durable final ACK 全链路证据。既有 Cargo 文件/SHA/落盘与 LAN WebRTC 用例不是 0.6.32 双进程或 WAN 验收。未声称存在有效 TURN 测试配置，也未把未经核实的凭据缺失当作外部阻断。

按授权的“生产集成范围不能安全验证则停止，不做 Windows 构建”边界，保留这些局部生产改进，**不放行完整 direct-first，不触发 Windows CI，不生成或交付 0.6.32 EXE**。未运行完整集成后的 frontend/type/lint/build 和在线双进程放行流程，未以旧包或基线代替。未 commit/push/tag/release，未修改 Git 引用、LAN-only 代码、TS/WebView 数据路径、旧包或其他工作树。真实双 Windows 跨网络最终验收仍由 owner 完成。

## 当前节点：真实双 PC 生产集成完成，目标版本最终门禁待放行

本节点取代上文关于“路由仅 test-gated、生产仍单 PC”的历史结论。`online_route_choice`、`online_route_runtime`、`online_direct` 已进入正常 Rust 编译，OnlineSession 实际 manifest/SDP/ICE/控制消息/FLNG 帧/文件 ACK 走同一适配器。初始 Direct PC 过滤为 host+STUN 并移除 TURN 凭据；截止后才创建独立 Relay policy PC。原生成功 setLocalDescription 捕获 Instant，coordinator 使用其真实 elapsed（包括入队延迟），只认严格 `<2500ms` 的双 READY；相等、迟到和未知证明不能获得 Direct。READY、提交确认和首帧许可检查真实 DC Open、PC Connected、selected/nominated/succeeded/已知候选对。接收端不选路；首帧依赖 PREPARE/COMMIT/START/START_ACK。未选帧不会进入文件引擎，选中路径失败整个 attempt 失败，首帧后无 Direct/TURN 迁移。

生产协议增加显式能力与 attempt/generation/decision_seq/path 围栏及 ufrag 绑定，ICE 缓冲按代次分桶、去重并限制数量和长度。旧版缺失能力明确失败，不静默回到 TS/LiveKit 文件中继。WebView 通用入口也拒绝 SDP/ICE/路由事务/帧/文件 ACK；原生单文件任务只接收高层 sourceId 意图。既有 legacy Blob/batch 兼容范围未伪称已全部迁移。

真实文件流继续复用原 Rust FLNG、源句柄、SHA、flush/sync_all、磁盘回读哈希、原子不覆盖提交与持久化最终 ACK。增加累计 ACK 几何校验和所有早期退出清理。最终确认连续发布失败仍 fail-closed：UI 明确区分“文件已保存但确认失败”，不得冒充传输成功，也不在已终止 attempt 内复活。协议发布有明确超时；接收端缺失初始 offer 的 15s setup 看门狗保持独立。

### 本轮实际失败与修复（旧日志不删除）

- 终止 Abort 发布失败会跳过 CloseAll：新增正常断言先实际失败，随后修复终止状态的全部清理效果执行，证据 `production-terminal-cleanup-counterexample.log` 与后续完整门禁。
- 首次两应用测试发现后加入端不见已在房间中的对端，等待阶段超时：补齐 LiveKit `remote_participants()` 初始快照。证据 `app-preflight.log`。
- 小文件首次完成后重复断连被误报错误：断连改为对真实 Disconnected 状态幂等，未吞掉其他错误。证据 `app-preflight-2.log`。
- 前端静态契约中的旧方法/旧代码切片已对准当前生产边界，并加强存在性、顺序、两端角色、selected fence、Direct 凭据剥离等断言，没有 ignored/should_panic 或删除实质要求。
- 最终加固：满数据帧队列不能丢失终止事实（独立 sticky watch）；取消先关闭发送门再采集诊断；旧路径异步统计在发出 UI 前持有当前连接身份围栏。

### 已完成的开发预验收，不冒充 0.6.32 最终构建

最后完整预验收 Rust 为 **218 passed / 0 failed / 0 ignored / 0 filtered out**，`production-dual-full-gate-5.log`。真实应用两进程七项全部符合预期：小文件、64MiB+19 大文件、同名原文件严格不覆盖并双方明确失败、空文件、128MiB 传输途中取消且清理 part、新任务重试、4MiB+13 TURN 文件。所有成功文件实际源/目标 SHA 一致，成功均等待 durable 文件 ACK 和 TaskAck。证据为 `app-preflight-3.log` 与 `app-preflight-3/summary.json` 及两端 JSONL。

上述开发二进制当时版本仍为 **0.6.31**，因此仅证明真实生产接线里程碑，**不是目标版本 0.6.32 的验收结果**。实际 Direct 两端都是 nominated/succeeded host-host；TURN 两端都是 nominated/succeeded relay-relay。TURN 使用已有嵌入配置，没有新增或向 WebView 暴露凭据。通过仅 Linux debug 内部验收入口故意丢弃 Direct READY 触发截止，随后是实际 TURN-only PC 与文件数据；不是伪造 READY 或绕过选择。内部入口不会编译进 Windows 或 release。所有两进程测试是同一 Linux 宿主，不是 WAN 或双 Windows。

在真实接线里程碑完成后，隔离分支版本字段已同步为 0.6.32。接下来必须在最终源码上通过全部 Rust（包括最后新增终止队列回归）、全部 TS 回归、类型、Clippy、前端/应用构建，再运行带最终 commit 身份的 **实际 0.6.32** 双进程七项验收，并分别提取数据窗口、保存/最终 ACK 与到 durable ACK 总耗时。全部通过后才可推送本测试分支、触发现有 Windows 工作流；仍不创建 tag/release、不改 main/稳定/回滚引用或其他工作树。交付还须校验 workflow head=最终 commit=EXE 内嵌身份，以及 EXE SHA-256。

最终源码检查已完成：`v0632-final-rust-observed.log` 为 219 passed、0 failed、0 ignored、0 filtered out；`v0632-final-clippy-observed.log` 退出 0（保留工程 warnings，非零警告承诺）；全部前端 `v0632-final-typescript.log` 与类型 `v0632-final-typecheck.log` 均退出 0。Linux 内部观测日志增加独立单调事件时间戳和实际编译身份，验收脚本对两进程版本 0.6.32、最终 buildIdentity、真实 relay-relay 证明做硬断言。后续应用/Windows 结果保存在未跟踪验收输出中，不以旧版本预验收替代。
