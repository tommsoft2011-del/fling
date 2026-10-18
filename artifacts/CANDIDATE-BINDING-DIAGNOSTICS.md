# 候选地址绑定：仅诊断候选

## 目前能够确定

7044c3be 双端日志中，发送端两次尝试的首个终态都是 `candidate_peer_mismatch`，其 Answer validate/parse/set-remote/pending apply 已成功；被收取的首个远端候选尚未 added 就被拒，随后本地 cleanup。接收端各自 added 一个对方候选后以 control_read 终止，与对端 cleanup 后 EOF 相容，但不把跨机器时间邻近当作独立因果证明。

该错误来自 numeric host、LAN 范围检查之后的当前 peer-directory 精确地址匹配，不能凭此判定候选来自另一网卡、合法次级接口、mDNS 缺漏或系统防火墙。

## 地址集合的源码边界

注册使用 `ServiceInfo::new` 的空地址加 `.enable_addr_auto()`。mdns-sd 0.13.11 自动填入接口策略所选地址，逐接口公告/应答按该接口子网投影；接收端 cache 按 hostname 聚合当前尚有效的 A/AAAA，不能把每次逐接口公告直接当成一个独立、互不聚合的 peer 全集。

Fling 将 `ServiceResolved.info.get_addresses()` 经过 `eligible_lan_endpoints` 过滤，并对当前 fullname 整项更新。同一 device 的多个当前 fullname 在 endpoint 查询和匹配时都会聚合。只有该 device 对应的精确 IP，或既有 IPv4-mapped IPv6 等价，才被认为命中；同一私网范围、同一子网本身均不会被当作归属证明。

本地 WebRTC 配置不指定某一张网卡；不配置 STUN/TURN，保留 host/LAN 过滤。项目源码与日志不足以恢复 Windows 当时的接口选择、路由和被拒候选的真实来源，不能断言具体 adapter。

## 本轮新增的唯一生产观测

在已有 `candidate_summary` 中新增可选 `firstRejectedCandidate`：

```json
{"family":"ipv4","lanClass":"private_ipv4","endpointMatched":false}
```

只包含 family 枚举、LAN class 枚举、当次原有 endpoint 校验的布尔结果。早期 host/LAN/格式门禁已拒绝、原有 endpoint lookup 尚未执行时，`endpointMatched` 缺省，而不是伪造 false。整个尝试只保存第一个拒绝分类，之后不能覆盖；不逐 candidate 新增日志事件，继续随已有终态快照输出。

不记录候选原文、IP、端口、接口名、地址散列、SDP、短码、密钥、原始 ID、文件路径或 OS 错误。地址匹配纯函数的提取与旧精确/mapped 逻辑等价，不增加任何允许地址。

## 没有做的修改

未放开任意 RFC1918/ULA 或“同子网即可信”，未忽略 mismatch，未把拒绝降为继续等待，未改变 first-terminal/cleanup。保留配对认证控制流、host-only、LAN-only、DTLS 与精确地址归属门禁。workflow、main、tags/releases 和旧工件不动。

## 回归能够证明与不能证明的内容

新增纯地址集合边界测试：公告 A 不能证明 B 属于同一设备；B 出现在同 device 的另一 fullname 时才能命中；其他 device 的公告无效；移除或空快照按原行为失去匹配；保留 mapped-IP 等价，不添加同子网 fallback。

新增回归复用双方确认、真实加密控制帧、native PC/Offer/Answer 和生产 transport handler：Answer 成功后注入未公告的 LAN host 候选，验证原有 mismatch→首终态→registry cleanup 路径不变，并在终态汇总中得到三个安全字段。还有门禁排序、首分类不覆盖/万次调用无逐候选日志，以及全枚举手动导出隐私哨兵。

这些是合成、受控的输入边界，不证明现场 B 是合法次级候选；也不是 Tauri/TCP router 或 Server2022→Win10 双机端到端验收。

## 还缺什么

缺少被拒候选的 family/class，以及它与当时该 peer 已宣告地址集合、该 peer 自己的真实本地接口/路由之间的可验证关系。新增三个字段可以缩小类别，但单凭类别仍不能认证地址所有权。因此本轮只能交付诊断候选，不能声称根因修复，也不能借现有证据调整网络接纳策略。

此前 7044 首轮 Windows CI 的负例 abort/后续通过仍是未解释的测试稳定性风险；本轮不把它关联到实机 mismatch，也不偷偷忽略相关测试。
