#!/usr/bin/env bash
# CQ-Accel 一键构建 + 部署 + 重启 + 自检
# 用法： sudo bash deploy.sh          （构建、部署、重启服务、探活）
#        SKIP_RESTART=1 bash deploy.sh （只构建、部署，不重启）
set -euo pipefail

SRC="/root/.openclaw/workspace/projects/cq-accel"
DST="/opt/cq-accel"
BIN="cq-accel"

cd "$SRC"
echo "[1/4] 构建 ($(go version | awk '{print $3}')) ..."
go build -o "$BIN" .

echo "[2/4] 部署二进制 -> $DST/"
install -m 0755 "$BIN" "$DST/$BIN"
# upstreams.json：源码即权威，先备份线上再覆盖
[ -f "$DST/upstreams.json" ] && cp -a "$DST/upstreams.json" "$DST/upstreams.json.bak-$(date +%Y%m%d-%H%M%S)" || true
install -m 0644 upstreams.json "$DST/upstreams.json"

if [ "${SKIP_RESTART:-0}" != "1" ]; then
  echo "[3/4] 重启 cq-accel.service ..."
  systemctl restart cq-accel.service
  sleep 1
  systemctl is-active cq-accel.service
else
  echo "[3/4] SKIP_RESTART=1，跳过重启"
fi

echo "[4/4] 探活 ..."
curl -fsS -m 10 http://127.0.0.1:8890/api/health && echo
echo "完成 OK"
