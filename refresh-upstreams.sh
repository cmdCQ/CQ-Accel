#!/usr/bin/env bash
# CQ-Accel 上游池自动刷新：自动发现 -> 合并 -> 上线（重启服务）
# 由 systemd timer 定时调用，也可手动跑。
set -euo pipefail

DIR="/root/.openclaw/workspace/projects/cq-accel"
DST="/opt/cq-accel"
cd "$DIR"

# GitHub token（用于搜索 API）
if [ -f "$DIR/.gh_env" ]; then
  set -a; . "$DIR/.gh_env"; set +a
fi

echo "== $(date '+%F %T') 自动发现 =="
echo "-- 修剪不可用节点 --"
python3 prune.py 2>&1 | sed 's/^/  /'
echo "-- 发现 --"
python3 -u discover.py 2>&1 | sed 's/^/  /'
echo "-- 合并 --"
python3 merge_discovered.py 2>&1 | sed 's/^/  /'

echo "-- 上线 --"
[ -f "$DST/upstreams.json" ] && cp -a "$DST/upstreams.json" "$DST/upstreams.json.bak-$(date +%Y%m%d-%H%M%S)"
install -m 0644 "$DIR/upstreams.json" "$DST/upstreams.json"
systemctl restart cq-accel.service
sleep 1
curl -fsS -m 10 http://127.0.0.1:8890/api/health && echo
echo "== 完成 =="
