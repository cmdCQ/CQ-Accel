#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CQ-Accel · 上游池修剪器
------------------------------------------------
读服务实时健康（/api/upstreams），连续 N 轮不可用的节点从 upstreams.json 里删掉。
用「连击计数」避免单次抖动误删：可用即清零，不可用累计，达到阈值才删。

安全阀：最少保留 MIN_KEEP 条；单轮最多删 MAX_DROP_PER_RUN 条。

用法：
  python3 prune.py            # 默认 2 连击删除
  python3 prune.py --strikes 3
  python3 prune.py --dry      # 只报告不写盘
"""
from __future__ import annotations
import json, os, sys, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
UP = os.path.join(HERE, "upstreams.json")
STATE = os.path.join(HERE, "prune_state.json")
API = "http://127.0.0.1:8890/api/upstreams"
MIN_KEEP = 12
MAX_DROP_PER_RUN = 40


def atomic_json(path, obj, indent=2):
    """先写 tmp 再 os.replace，避免断写损坏文件（H1）"""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def fetch_health():
    req = urllib.request.Request(API, headers={"User-Agent": "cq-accel-prune/0.1"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def main():
    args = sys.argv[1:]
    dry = "--dry" in args
    strikes_limit = 2
    if "--strikes" in args:
        strikes_limit = int(args[args.index("--strikes") + 1])

    ups = json.load(open(UP))
    try:
        state = json.load(open(STATE))
    except Exception:
        state = {}

    try:
        health = {u["id"]: u for u in fetch_health().get("upstreams", [])}
    except Exception as e:
        print(f"读取服务健康失败，跳过本轮修剪：{e}")
        return

    drop, keep = [], []
    for u in ups:
        h = health.get(u.get("id"))
        ok = bool(h and h.get("ok"))
        sid = u.get("id")
        if ok:
            state[sid] = 0
            keep.append(u)
        else:
            state[sid] = state.get(sid, 0) + 1
            if state[sid] >= strikes_limit:
                drop.append(u)
            else:
                keep.append(u)

    # 安全阀
    if len(drop) > MAX_DROP_PER_RUN:
        drop = drop[:MAX_DROP_PER_RUN]
    while len(ups) - len(drop) < MIN_KEEP and drop:
        drop.pop()
    drop_ids = {u["id"] for u in drop}
    keep = [u for u in ups if u["id"] not in drop_ids]

    print(f"上游 {len(ups)} 条 -> 保留 {len(keep)}，删除 {len(drop)}")
    for u in drop:
        print(f"  - {u['name']} ({u.get('kind')}) 连击 {state.get(u['id'])}")

    if dry:
        print("(--dry，不写盘)")
        return
    atomic_json(UP, keep)
    atomic_json(STATE, {k: v for k, v in state.items() if k not in drop_ids})
    print("已更新 upstreams.json")


if __name__ == "__main__":
    main()
