#!/bin/bash
# Fling TURN 服务器部署脚本（coturn）
# 安全原则：只做增量安装，不停止/不修改任何现有服务，只追加防火墙放行规则
# 适用：Ubuntu / Debian
set -euo pipefail

TURN_USER="flingturn"
TURN_REALM="fling"
TURN_PORT=3478
RELAY_MIN=49160
RELAY_MAX=49170

echo "=== 1. 检查现有服务（只读，不做任何改动）==="
echo "--- 已监听端口 ---"
(ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null) | head -30
echo ""
echo "--- 3478 端口是否被占用 ---"
if (ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null) | grep -q ":3478 "; then
  echo "警告：3478 已被占用，请手动改 TURN_PORT 后再跑"
  exit 1
else
  echo "3478 空闲，可用"
fi

echo ""
echo "=== 2. 安装 coturn（仅 apt 安装，不改现有配置）==="
sudo apt-get update -qq
sudo apt-get install -y -qq coturn

echo ""
echo "=== 3. 生成随机密码 ==="
TURN_PASS=$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)
echo "TURN 用户名: $TURN_USER"
echo "TURN 密码: $TURN_PASS"
echo "请妥善保存上面两行（后面要填进 Fling 配置）"

echo ""
echo "=== 4. 写 coturn 配置（独立文件，不碰系统默认）==="
EXT_IP=$(curl -s --max-time 10 https://api.ipify.org || curl -s --max-time 10 https://ifconfig.me)
echo "本机公网 IP: $EXT_IP"
sudo tee /etc/turnserver.conf > /dev/null <<EOF
# Fling TURN server (generated $(date -u +%Y-%m-%dT%H:%M:%SZ))
listening-port=$TURN_PORT
external-ip=$EXT_IP
realm=$TURN_REALM
lt-cred-mech
user=$TURN_USER:$TURN_PASS
min-port=$RELAY_MIN
max-port=$RELAY_MAX
no-tcp-relay
no-tls
no-dtls
log-file=/var/log/turnserver.log
simple-log
EOF
sudo chmod 600 /etc/turnserver.conf

echo ""
echo "=== 5. 启动 coturn（独立 systemd 服务，不影响其他服务）==="
sudo systemctl enable --now coturn
sleep 2
sudo systemctl is-active coturn && echo "coturn 运行中"

echo ""
echo "=== 6. 防火墙放行（只追加 ALLOW，不删除任何规则）==="
if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | grep -q "Status: active"; then
  sudo ufw allow $TURN_PORT/udp comment 'fling turn'
  sudo ufw allow $TURN_PORT/tcp comment 'fling turn'
  sudo ufw allow $RELAY_MIN:$RELAY_MAX/udp comment 'fling turn relay'
  echo "ufw 已放行"
elif command -v firewall-cmd >/dev/null 2>&1 && sudo firewall-cmd --state 2>/dev/null | grep -q running; then
  sudo firewall-cmd --permanent --add-port=$TURN_PORT/udp
  sudo firewall-cmd --permanent --add-port=$TURN_PORT/tcp
  sudo firewall-cmd --permanent --add-port=$RELAY_MIN-$RELAY_MAX/udp
  sudo firewall-cmd --reload
  echo "firewalld 已放行"
else
  echo "未检测到活动的 ufw/firewalld，请手动在云厂商安全组放行："
  echo "  $TURN_PORT/udp, $TURN_PORT/tcp, $RELAY_MIN-$RELAY_MAX/udp"
fi

echo ""
echo "=== 完成 ==="
echo "TURN 地址: turn:$EXT_IP:$TURN_PORT"
echo "用户名: $TURN_USER"
echo "密码: $TURN_PASS（见上方，只显示一次）"
echo ""
echo "卸载（回滚）命令：sudo systemctl disable --now coturn && sudo apt-get remove -y coturn"
