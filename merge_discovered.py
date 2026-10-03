#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 discover.py 产出的 discovered.json 合并进 upstreams.json（按 id/host 去重）。"""
import json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
UP = os.path.join(HERE, "upstreams.json")
DISC = os.path.join(HERE, "discovered.json")


def host_of(u):
    return (u.get("base") or u.get("domain") or "").replace("https://", "").replace("http://", "").strip("/").lower()


def main():
    cur = json.load(open(UP))
    try:
        disc = json.load(open(DISC))
    except FileNotFoundError:
        print("discovered.json 不存在，先跑 discover.py"); sys.exit(1)

    have_id = {u.get("id") for u in cur}
    have_host = {host_of(u) for u in cur}
    added = 0
    for d in disc:
        d = {k: v for k, v in d.items() if not k.startswith("_")}
        if d.get("id") in have_id or host_of(d) in have_host:
            continue
        cur.append(d)
        have_id.add(d.get("id"))
        have_host.add(host_of(d))
        added += 1

    json.dump(cur, open(UP, "w"), ensure_ascii=False, indent=2)
    print(f"已合并 {added} 条，upstreams.json 现有 {len(cur)} 条上游")


if __name__ == "__main__":
    main()
