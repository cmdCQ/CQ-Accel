# CQ-Accel

> GitHub 加速聚合中转站 —— 不自己加速，而是**聚合全网现成加速站当上游**，动态择优选路，对外提供网页 + API 两种入口。
>
> Public · 不限速 · 定时更新节点 · 纯流式中转（无缓存、不绕本地代理）。

在线体验：<https://gh.somtfly.com>

---

## 特性

- **聚合中转**：粘贴任意 GitHub 链接，自动选最快上游中转下载；支持 `Range` 断点续传、`HEAD`、大文件流式转发（不落盘）。
- **形态适配**：兼容现有加速站的多种 URL 形态 —— `prefix`（前缀拼）、`param`（`?url=`）、`replace`（域名整体替换）、`jsdelivr`（raw 走 jsDelivr）。
- **自动发现**：定时从 GitHub 搜索 + 公开聚合源（如 `xiake.pro`、`moretools.app`）挖候选域名，实测可用才入池。
- **健康测速**：定时探活 + TTFB/吞吐测速打分，请求失败自动降权、切次优、摘除。
- **自动修剪**：连续多轮不可用的节点自动从池中删除。
- **流量监控**：页面展示今日请求 / 流量 / 独立 IP / 累计与热门线路。
- **安全**：只允许 GitHub 域名；重定向做最终域名白名单校验；防 SSRF/开放代理滥用。

## 架构

```
用户/代码 ──► CQ-Accel(API/网页) ──► 选路引擎 ──► 上游加速站 A/B/C… ──► GitHub
                     ▲                                  │
                     └──── 采集器 + 健康测速引擎 ────────┘
                          (定时扫源、打分、摘除)
```

- 后端：Go 单二进制，高并发流式转发
- 前端：纯静态（vanilla，零构建）
- 部署：systemd 常驻 + nginx 反代

## 用法

### 1）直接用（路径即原 GitHub 路径）

把 `https://github.com/...` 的域名换成站点域名即可：

```bash
# release 资源
curl -L -O "https://gh.somtfly.com/github.com/owner/repo/releases/download/v1.0/x.zip"

# raw / archive / codeload / gist 同理
curl -L -O "https://gh.somtfly.com/raw.githubusercontent.com/owner/repo/main/file.txt"

# git clone
git clone https://gh.somtfly.com/github.com/owner/repo.git
```

### 2）参数式

```
GET /fetch?u=<urlencode(github_url)>
```

### 3）元数据 API

| 接口 | 说明 |
| --- | --- |
| `/api/health` | 服务存活 |
| `/api/upstreams` | 上游池 + 健康（延迟 / 速度 / 可用） |
| `/api/stats` | 上游池统计汇总 |
| `/api/traffic` | 流量监控（今日 / 累计 / 热门线路） |
| `/api/resolve?u=<url>` | 解析可选线路列表 |
| `/api/best?u=<url>` | 当前最优上游 |

每次中转响应带 `X-Upstream: <上游名>`，便于排查线路。

## 目录结构

```
cq-accel/
├── main.go                  # HTTP 路由 / 中转 / 元数据 API
├── upstream.go              # 上游模型、形态适配、健康测速、选路
├── traffic.go               # 流量监控
├── static/index.html        # 网页（贴链接→选路中转），含版心/响应式
├── static/docs.html         # API 文档页
├── discover.py              # 采集器：GitHub 搜索 + 公开聚合源 → 实测 → discovered.json
├── merge_discovered.py      # 合并去重进 upstreams.json
├── prune.py                 # 修剪：连续不可用节点删除
├── deploy.sh                # 构建 + 部署 + 重启 + 探活
├── refresh-upstreams.sh     # 定时：prune → discover → merge → 上线
├── prune-and-apply.sh       # 定时：只修剪，有变化才重启
└── upstreams.json           # 上游池配置
```

## 上游池配置 `upstreams.json`

```json
[
  { "id": "ghfast", "name": "ghfast.top", "kind": "prefix", "base": "https://ghfast.top", "region": "cn", "enabled": true },
  { "id": "jsdelivr", "name": "jsDelivr", "kind": "jsdelivr", "region": "global", "enabled": true }
]
```

`kind` 可选：`prefix` / `param` / `replace`（配 `domain`）/ `jsdelivr`。

## 快速部署

```bash
# 1) 构建 & 部署（默认到 /opt/cq-accel，重启 cq-accel.service）
sudo bash deploy.sh

# 2) 自动发现需要 GitHub token（可选，提高搜索配额）
echo 'export GITHUB_TOKEN=ghp_xxx' > .gh_env && chmod 600 .gh_env

# 3) 定时任务（systemd）
#    cq-accel-discover.timer  每天 03/09/15/21:20 自动发现刷新
#    cq-accel-prune.timer     每天 01/04/07/10/13/16/19/22:40 修剪不可用节点
```

环境变量：`LISTEN`（默认 `:8890`）、`UPSTREAMS_FILE`、`TRAFFIC_FILE`。

## 风险与合规

1. **上游封禁/限速**：依赖他人免费服务，随时可能被掐 —— 靠「多上游 + 自动摘除」兜底。
2. **被打爆**：出圈后流量暴涨需自行限流。
3. **版权**：仅做转发中转，内容版权归原作者；尊重上游 ToS。

## License

MIT
