#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CQ-Accel · P2 采集器（自动发现）
------------------------------------------------
从 GitHub 搜索 + 公开加速站清单里挖候选加速域名，逐个实测（前缀/参数两种形态），
把**实测可用**的写成 discovered.json（cq-accel upstreams 的结构，enabled=true）。

发现≠可用：所有候选都必须过实测才进池。

用法：
  GITHUB_TOKEN=ghp_xxx python3 discover.py            # 全流程
  python3 discover.py --probe-only base1 base2 ...    # 只测指定 base
  python3 discover.py --no-net                        # 只做候选收集，不实测
"""
from __future__ import annotations
import os, re, sys, json, time, urllib.request, urllib.parse, urllib.error, concurrent.futures as cf
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "discovered.json")
TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
UA = "cq-accel-discover/0.1 (+https://gh.somtfly.com)"
TEST_URL = "https://raw.githubusercontent.com/twbs/bootstrap/main/dist/css/bootstrap.css"
TEST_NEEDLE = b"Bootstrap"  # 正确内容里应含这个词
TIMEOUT = 15

# 已知/官方域名，不作为候选
BLOCK = {
    "github.com", "raw.githubusercontent.com", "codeload.github.com", "gist.github.com",
    "gist.githubusercontent.com", "objects.githubusercontent.com", "api.github.com",
    "jsdelivr.net", "unpkg.com", "npmjs.com", "google.com", "bing.com", "baidu.com",
    "githubusercontent.com", "githubassets.com", "w3.org", "schema.org", "gnu.org",
    "opensource.org", "apache.org", "mit.edu", "creativecommons.org", "shields.io",
    "badgen.net", "travis-ci.org", "travis-ci.com", "github.io", "cloudflare.com",
    "docker.com", "python.org", "nodejs.org", "npmjs.org", "stackoverflow.com",
}
# 域名里出现这些片段才当"像加速站"
HINTS = ("gh", "proxy", "accel", "mirror", "git", "down", "hub", "fast", "node")

DOMAIN_RE = re.compile(r"https?://([a-zA-Z0-9][a-zA-Z0-9.\-]{1,80}\.[a-zA-Z]{2,})")


def log(*a):
    print(*a, flush=True)


def gh_get(url: str):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Accept": "application/vnd.github+json",
        **({"Authorization": "Bearer " + TOKEN} if TOKEN else {}),
    })
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


# ---------- 1. 候选收集 ----------

def search_repos(queries):
    repos = {}
    for q in queries:
        try:
            d = gh_get("https://api.github.com/search/repositories?per_page=20&sort=stars&q=" + urllib.parse.quote(q))
        except urllib.error.HTTPError as e:
            log(f"  [repo] {q} -> HTTP {e.code}")
            continue
        except Exception as e:
            log(f"  [repo] {q} -> {e}")
            continue
        for it in d.get("items", []):
            repos[it["full_name"]] = it.get("default_branch", "main")
        log(f"  [repo] {q} -> {len(d.get('items', []))} hits")
        time.sleep(1.5)
    return repos


def readme_text(full_name: str, branch: str) -> str:
    # 走 api.github.com/repos/{full}/readme（带 token，返回体直接是 README 原文）
    url = f"https://api.github.com/repos/{full_name}/readme"
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/vnd.github.raw",
        **({"Authorization": "Bearer " + TOKEN} if TOKEN else {}),
    })
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return ""


def http_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return r.read().decode("utf-8", "replace")


def collect_from_aggregators() -> set[str]:
    """从公开的、定时更新的加速站聚合源拉候选域名（比扫 README 准）。"""
    hosts: set[str] = set()
    # 1) xiake.pro —— 定时统计的节点 JSON（带 latency/speed）
    try:
        d = json.loads(http_text("https://xiake.pro/static/node.json"))
        for n in d.get("data", []):
            h = re.sub(r"^https?://", "", n.get("url", "")).strip("/").lower()
            if h:
                hosts.add(h)
        log(f"  [聚合] xiake.pro -> {len(hosts)} 累计")
    except Exception as e:
        log(f"  [聚合] xiake.pro 失败: {e}")
    # 2) moretools.app —— 页面内嵌节点列表
    try:
        html = http_text("https://www.moretools.app/zh-CN/github-proxy")
        for h in DOMAIN_RE.findall(html):
            hosts.add(h.lower())
        log(f"  [聚合] moretools.app -> {len(hosts)} 累计")
    except Exception as e:
        log(f"  [聚合] moretools.app 失败: {e}")
    # 清洗
    bad = {"example.com", "schema.org"}
    return {h for h in hosts if h and "." in h and not h.endswith(".") and h not in bad}


def collect_candidates() -> set[str]:
    queries = [
        "github accelerator proxy", "github 加速", "github proxy mirror",
        "topic:github-proxy", "topic:github-accelerate", "topic:github-mirror",
        "ghproxy", "github release 加速下载", "github download accelerate",
    ]
    log("[1] GitHub 搜索候选仓库 ...")
    repos = search_repos(queries)
    log(f"    命中仓库 {len(repos)} 个，抓 README 抽域名 ...")

    items = list(repos.items())
    cands: set[str] = set()
    done = 0

    def work(full_branch):
        full, branch = full_branch
        txt = readme_text(full, branch)
        found = set()
        for h in DOMAIN_RE.findall(txt):
            h = h.lower()
            if h in BLOCK or h.endswith(".github.io") or h.count(".") < 1:
                continue
            if any(k in h for k in HINTS):
                found.add(h)
        return found

    with cf.ThreadPoolExecutor(max_workers=12) as ex:
        for found in ex.map(work, items):
            cands |= found
            done += 1
            if done % 20 == 0:
                log(f"    ... {done}/{len(items)}，候选 {len(cands)}")
    log(f"    README 收集到候选域名 {len(cands)}")
    log("[1b] 聚合已有公开加速站列表 ...")
    cands |= collect_from_aggregators()
    log(f"    合并后候选域名 {len(cands)}")
    return cands


# ---------- 2. 实测 ----------

def http_get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        body = r.read(4096)
        return r.status, body, int((time.time() - t0) * 1000)


def probe(host: str):
    """返回 (ok, kind, base, ttfb_ms) 或 None"""
    for kind, url in (
        ("prefix", f"https://{host}/{TEST_URL}"),
        ("param",  f"https://{host}/?url={urllib.parse.quote(TEST_URL, safe='')}"),
    ):
        try:
            st, body, ms = http_get(url)
        except Exception:
            continue
        if 200 <= st < 400 and TEST_NEEDLE in body:
            return (True, kind, f"https://{host}", ms)
    return None


def probe_all(hosts: list[str]):
    log(f"[2] 实测 {len(hosts)} 个候选（前缀 + 参数两种形态）...")
    good = []
    with cf.ThreadPoolExecutor(max_workers=32) as ex:
        futs = {ex.submit(probe, h): h for h in hosts}
        for f in cf.as_completed(futs):
            h = futs[f]
            try:
                r = f.result()
            except Exception:
                r = None
            if r:
                log(f"    ✓ {h}  [{r[1]}]  {r[3]}ms")
                good.append((h, r[1], r[3]))
    log(f"    可用 {len(good)} 个")
    return good


# ---------- 3. 输出 ----------

def atomic_json(path, obj, indent=2):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def make_id(host: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", host.lower()).strip("-")


def emit(good, existing_ids: set[str]):
    out = []
    for host, kind, ms in sorted(good, key=lambda x: x[2]):
        uid = make_id(host)
        if uid in existing_ids:
            continue
        region = "cn" if any(s in host for s in ("cn", "china", "233", "top", "xyz")) else "global"
        out.append({"id": uid, "name": host, "kind": kind, "base": f"https://{host}",
                    "region": region, "enabled": True, "_ttfb_ms": ms})
    atomic_json(OUT, out)
    log(f"[3] 写入 {OUT}（{len(out)} 条新上游）")
    return out


def main():
    args = sys.argv[1:]
    existing = []
    try:
        existing = json.load(open(os.path.join(HERE, "upstreams.json")))
    except Exception:
        pass
    existing_ids = {u.get("id") for u in existing}
    known_hosts = {urllib.parse.urlparse(u.get("base", "")).hostname for u in existing
                   if u.get("base")} | {u.get("domain") for u in existing if u.get("domain")}

    if args and args[0] == "--probe-only":
        hosts = [a.strip().lower() for a in args[1:] if a.strip()]
    else:
        hosts = sorted(collect_candidates())
        hosts = [h for h in hosts if h not in known_hosts]

    if "--no-net" in args:
        log("候选域名："); [log("  " + h) for h in hosts]
        return
    good = probe_all(hosts)
    emit(good, existing_ids)


if __name__ == "__main__":
    main()
