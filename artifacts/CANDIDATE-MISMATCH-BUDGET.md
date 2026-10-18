# 候选拒绝后的有界继续读取：隔离测试版

这不是已经验证成功的 Windows 修复。仍须用同一构建在 myserver ↔ tomm10 真实传输验证；不能据此认定此前第二条候选一定匹配。

## 已确认的问题

基线 `eb6696afcc42bd55da82e3f859e9407df4b6b864` 中，`receive_ice_candidate` 对单个 `candidate_peer_mismatch` 调用 `rejected`，既写入首终态又返回错误。该错误经 transport router / authenticated handler 返回给 `run_control_reader`，reader 退出并执行原有会话清理。因此第一条未公告 link-local 候选确实可以阻止后续候选被处理。

## 本轮只改变什么

已认证控制流中的单条候选，只有在原 host、numeric、LAN 地址筛选之后，且通过完整候选语法与 mid/m-line 校验、仅因不匹配已认证设备当前发现地址集合时，才可丢弃后继续读。该候选不进入 pending、不调用 native add、不扩张地址集合，不占用 `firstTerminal`。后续候选仍按同一 `peer_device_id` 查询当前 mDNS 地址集合；保留既有 IPv4-mapped 地址比较兼容，不放宽更早的 LAN 地址 eligibility gate。

每会话最多容忍 15 条 mismatch；第 16 条以 `candidate_mismatch_budget` 终止。第一次 mismatch 建立固定 30 秒绝对期限，后续 mismatch、空 ICE 帧和其他控制帧不会续期。只有 native add 实际成功才解除该等待期限；添加成功不补回拒绝次数。全候选不匹配时，次数或期限先到者触发失败与原路径清理，不能成功。

另有每会话 **256 条 remote ICE 输入总预算**：已匹配、重复、remote description 设置前后及空 end-of-candidates 帧全部计入。第 257 条在解析/queue/add 前以 `candidate_input_budget` 终止。该计数不因 remote SDP、成功添加或清空 pending 重置，并固定封顶，不会无限增长。原 pending 队列 128、每条 payload 4 KiB 和控制帧大小限制仍保留。

期限检查覆盖无输入、只发半帧和持续可读流。read helper 在计时器首次 poll 前显式检查绝对期限，避免已到期但 timer-driver 尚未唤醒时被 ready frame 抢先；到期后使用 `candidate_mismatch_timeout`，不会把先前已丢弃候选误记成导致会话结束的原因。

非 host、非 LAN、畸形/超界候选与帧、pending 上限、native add 错误、未认证/身份不匹配/解密失败以及其他控制协议处理错误仍失败，reader 的终止和清理策略没有改成忽略通用错误。LAN-only、host-only、端到端加密、已认证设备身份边界不变。

## 诊断与自动化证据边界

诊断只沿用固定类别计数、第一条拒绝的 address family / LAN class / endpointMatched 摘要，增加固定枚举终态；不写候选原文、IP、端口、SDP、密钥或原始设备/会话/服务 ID，不增加逐 mismatch 日志。

原生回归通过生产的加密帧读取、认证解码、候选路由及 native libwebrtc add 边界，覆盖陌生 link-local 后合法 private 候选可添加、已设置 remote SDP 后重复候选总预算、全 mismatch 次数耗尽与清理、固定绝对期限的 silent/partial/ready 流、身份/解密/语法边界、当前发现集合变化与 mapped 地址比较、计数封顶和其他 handler 错误。时限测试使用同一个生产绝对期限字段将测试等待缩短，不把缩短测试宣称为真实等待 30 秒的 Windows 验证。

这些测试不等于 Tauri GUI/router E2E，也不等于跨 Windows 网络传输成功。最终以本轮 full commit 对应的完整 Windows x64 workflow 和下载 artifact 校验为构建交付门槛。仅发布隔离分支 artifact；不修改 main、正式 tag 或正式 Release。

## 双机验证步骤

1. myserver 和 tomm10 都完全退出旧 Fling 进程，启动本轮同 SHA-256 的 portable EXE，不混用旧版。
2. 保持原操作顺序：myserver 发送小文件到 tomm10；两端输入并核对短码，先点接收端确定，再点发送端确定，不需要改变用户习惯或先测反向。
3. 等到文件成功并确认落盘，或出现明确失败；双方立即手动导出局域网诊断日志。记录发送端与接收端各自导出文件，不提供短码、原始候选或文件内容。
4. 检查同一构建下发送端是否在 mismatch 后继续 received 后续候选，是否有 added，以及实际首终态/cleanup。单纯 mismatch 不应出现为 `firstTerminal`；若无匹配候选，应看到预算/超时或更早的真实错误并清理，不能出现假成功。
5. 如仍失败，保留双方日志供进一步定位；不能仅根据接收端先点确定就判定为用户操作问题，也不能仅凭一次候选添加认定文件传输已经修复。

## 本地验证记录

- 完整 Rust/native/privacy 回归：111 通过、0 失败、0 ignored；包括原有 DataChannel 双向传输与文件持久化回归。
- 全部 8 个前端测试脚本通过，frontend production build 通过；保留既有 Vite 动态导入/包体积警告。
- 本轮首轮原生测试为 107 通过、3 断言失败：发现并修正了已过期期限与 ready frame 的首次 poll 竞态；测试夹具保留真实 reader 会持有的 watch 订阅以验证取消；mapped 兼容测试改为验证既有地址比较归一化，而非错误假定 IPv6 mapped 候选可绕过既有 LAN eligibility。没有删除这些回归或跳过负例。新增静态契约曾因 rustfmt 换行不匹配，已修正为空白兼容匹配。
- 本轮未观察到 native abort；此前基线同提交负例测试的 abort 原因仍未知，不能据本轮通过宣称风险消失。
