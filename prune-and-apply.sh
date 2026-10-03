#!/usr/bin/env bash
# CQ-Accel 只做「修剪」：删掉连续不可用的上游 -> 有变化才上线（重启服务）
# 由 systemd cq-accel-prune.timer 定时调用；也可手动跑。
set -euo pipefail

DIR="/root/.openclaw/workspace/projects/cq-accel"
DST="/opt/cq-accel"
cd "$DIR"

before="$(md5sum upstreams.json | awk '{print $1}')"
echo "== $(date '+%F %T') 修剪上游 =="
python3 prune.py 2>&1 | sed 's/^/  /'
after="$(md5sum upstreams.json | awk '{print $1}')"

if [ "$before" != "$after" ]; then
  [ -f "$DST/upstreams.json" ] && cp -a "$DST/upstreams.json" "$DST/upstreams.json.bak-$(date +%Y%m%d-%H%M%S)"
  install -m 0644 upstreams.json "$DST/upstreams.json"
  systemctl restart cq-accel.service
  sleep 1
  curl -fsS -m 10 http://127.0.0.1:8890/api/health && echo
  echo "== 已应用（有删除）=="
else
  echo "== 无变化，不动服务 =="
fi
