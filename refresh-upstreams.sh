#!/usr/bin/env bash
# CQ-Accel 上游池自动刷新：自动发现 -> 合并 -> 上线（仅在有变更时重启）
# 由 systemd timer 定时调用，也可手动跑。
set -euo pipefail

DIR="/root/.openclaw/workspace/projects/cq-accel"
DST="/opt/cq-accel"
cd "$DIR"

# H3: 与 prune 脚本互斥，避免并发写同一文件 / 重复重启
exec 9>/run/cq-accel.lock
flock -n 9 || { echo "$(date '+%F %T') 已有另一轮在跑，跳过"; exit 0; }

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

# H1: 上线前校验 JSON 合法，损坏就不动线上
python3 -c "import json;json.load(open('$DIR/upstreams.json'))" || { echo "upstreams.json 校验失败，放弃上线"; exit 1; }

# H2: 仅当内容有变更时才 install + restart
if [ -f "$DST/upstreams.json" ] && cmp -s "$DIR/upstreams.json" "$DST/upstreams.json"; then
  echo "== 无变更，不动服务 =="
  exit 0
fi

echo "-- 上线 --"
[ -f "$DST/upstreams.json" ] && cp -a "$DST/upstreams.json" "$DST/upstreams.json.bak-$(date +%Y%m%d-%H%M%S)"
install -m 0644 "$DIR/upstreams.json" "$DST/upstreams.json"
systemctl restart cq-accel.service
sleep 1
curl -fsS -m 10 http://127.0.0.1:8890/api/health && echo
echo "== 完成 =="
