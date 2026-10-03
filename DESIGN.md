# CQ-Accel · GitHub 加速聚合中转站 · 方案设计 v0.1

> 定位：**不为自己做加速，而是聚合全网现成加速站作为上游**，动态择优选路，对外提供「API 调用」+「网页」两种入口。
> 状态：规划稿（2026-10-03）

---

## 1. 一句话架构

```
用户/代码 ──► 你的中转站(API/网页) ──► 选路引擎 ──► 上游加速站A/B/C… ──► GitHub
                     ▲                                     │
                     └──── 采集器 + 健康测速引擎 ───────────┘
                            (定时扫全网、打分、摘除坏上游)
```

核心两件事：
1. **上游池**：持续发现 + 测速 + 评分 + 淘汰。
2. **中转转发**：按请求把 GitHub URL 经最优上游流式转发回用户。

---

## 2. 模块拆解

| # | 模块 | 职责 |
|---|------|------|
| ① | **采集器 Discoverer** | 定时扫全网，发现新的加速站端点（域名/前缀/?url= 形态） |
| ② | **健康测速 Health** | 对上游池批量探活 + 测 TTFB/速度/成功率，滑动窗口打分 |
| ③ | **选路 Router** | 每次请求选当前最优上游，失败自动 fallback |
| ④ | **代理核心 Relay** | 流式转发（支持 Range/大文件/重定向/四种 URL 形态） |
| ⑤ | **API 层** | 面向代码的接口（转发 + 元数据） |
| ⑥ | **网页 Web** | 面向人的页面：贴链接→取加速下载/复制 API |
| ⑦ | **缓存 Cache(可选)** | 热门文件本地 LRU，减负上游、加速重复下载 |
| ⑧ | **管理后台 Admin** | 看上游健康、手动增删、看流量统计 |

---

## 3. 上游加速站：形态与适配

现有加速站 URL 形态不统一，**必须适配器模式**，每个上游一个 adapter：

| 形态 | 例子（示意） | 映射规则 |
|------|------|------|
| 前缀型 | `https://ghproxy.com/https://github.com/...` | 直接拼前缀 |
| 前缀型(域名替换) | `https://mirror.ghproxy.com/...` | 拼前缀 |
| 参数型 | `https://x.com/?url=https://github.com/...` | urlencode 拼参 |
| jsDelivr 型 | raw/`@tag` 走 `cdn.jsdelivr.net/gh/...` | 仅 raw，规则不同 |
| 反代型 | `https://kkgithub.com/owner/repo` | 域名整体替换 |

**适配器接口**（伪代码）：
```go
type Upstream interface {
    // CanHandle 判断该上游是否能处理这类 github 请求
    CanHandle(kind Kind) bool
    // BuildURL 把原始 github url 映射成该上游的请求 url
    BuildURL(raw string) (string, error)
    // Meta 上游元信息（前缀、地区、限速、是否需鉴权）
    Meta() Meta
}
```
覆盖资源类型：**release 下载、raw、archive(zip/tar)、codeload、gist、clone(info/refs)**。

---

## 4. 采集器（Discoverer）：怎么"扫全网"

分三层，从易到难：

1. **种子库（人工维护）**：已知加速站清单，放 SQLite，随时扩。
2. **主动发现（自动）**：
   - GitHub Code Search API：搜 README/文档里出现 `ghproxy`、`github.*加速`、`mirror` 等关键词的仓库 → 正则抽域名。
   - 抓 awesome 列表 / 相关博客 / issue 里提到的地址。
   - 监控 GitHub topic：`github-proxy`、`github-accelerate`。
   - （进阶）抓搜素引擎结果里的域名。
3. **去重 + 入库待测**：抽出候选域名 → 加入"待测池"。

> 注意：**发现≠可用**，所有候选必须先过体检才进正式池。

---

## 5. 健康测速引擎（Health）

- **探活**：HEAD/GET 一个固定小文件，判定 2xx/3xx 可用。
- **真实测速**：固定测试对象（如某稳定项目的 ~1MB 与 ~50MB release 资源），测：成功率、TTFB、吞吐、重定向链。
- **打分**：`score = w1*可用性 + w2*速度 + w3*稳定性(滑动窗口)`，可按 **客户端地区 × 上游地区** 分桶（国内/海外）。
- **调度**：低频全量体检（如每 10~30min）+ 请求时实时反馈（失败即降权/摘除）。
- **摘除**：连续失败 N 次 → 标记 disabled，进入观察期。

---

## 6. 选路（Router）

- 默认：选当前 bucket 内 score 最高者。
- 失败：立即 fallback 到次优，并回写失败。
- 可选策略：轮询/随机（分摊上游压力）、就近、固定线路。
- 记录每次选路的耗时，持续修正权重。

---

## 7. 代理核心（Relay）

- **流式转发**，默认不落盘（除非命中缓存），避免大文件占内存。
- 必须支持：`Range`（断点/多线程下载）、`HEAD`、`Content-Disposition` 透传、跟随且校验重定向（防上游劫持到恶意地址 → 白名单最终域名）。
- 超时/重试：连接超时→换上游；传输中断→按 Range 续传或换源。
- 防滥用：单 IP 限速/限流、可选 Token、Referer 校验、并发上限、大小限制。

---

## 8. 对外两种入口

### 8.1 API（给代码用）
```
# 直接用（路径即原始 github 路径）
GET https://acc.你的域名/github.com/owner/repo/releases/download/v1.0/x.zip
# 或参数式
GET https://acc.你的域名/fetch?u=<urlencode(github_url)>
# 元数据/上游状态
GET https://acc.你的域名/api/upstreams         # 上游列表+健康分
GET https://acc.你的域名/api/best?kind=release # 当前最优上游
GET https://acc.你的域名/api/health            # 健康检查
```
支持 `HEAD`/`Range`；返回带 `X-Upstream: <name>`、`X-Cache: HIT/MISS` 便于调试。

### 8.2 网页（给人用）
- 输入框：粘贴任意 GitHub 链接 → 输出**加速下载按钮 + 多条可选线路**（显示各自延迟）。
- 一键复制：对应 API 调用 / curl / git config 片段。
- 上游状态面板：实时显示各上游延迟/可用率。
- （进阶）「我的加速站 API 文档」页面。

---

## 9. 数据模型（SQLite）

```sql
upstream(id, name, kind, base_url_or_tmpl, region, enabled, score,
         success_rate, ttfb_ms, speed_kbps, last_checked, note)
probe(upstream_id, ts, ok, ttfb_ms, speed_kbps, http_code)
request_log(ts, raw_url, upstream_id, bytes, ms, ok)   -- 可采样
cache(key, path, size, hits, last_access)               -- 可选
```

---

## 10. 技术选型建议

- **后端**：Go（单二进制、高并发流式转发强，复用你 FreeP 的经验）。备选 Node/Python（开发快但并发/流式弱些）。
- **存储**：SQLite。
- **前端**：纯静态（Svelte/vanilla + Tailwind），复用你的 nginx 静态托管习惯。
- **部署**：systemd 常驻 + nginx 反代 + Let's Encrypt；入口走你已有的 47.83.171.151 隧道；域名可挂 `somtfly.com` 子域（如 `gh.somtfly.com`，你有 CF 管理权）。
- **定时任务**：内置 ticker 或 systemd timer 跑采集/体检。

---

## 11. 里程碑

- **P0 MVP**：手填上游 + 简单选路 + `/<path>` 流式转发 + 网页输入框。
- **P1**：健康测速 + 评分 + fallback + 上游状态页。
- **P2**：全网自动发现 + 缓存 + 分地区调度 + 防滥用 + 开放 API 文档。
- **P3**：数据可视化、社区共建上游清单。

---

## 12. 风险 & 合规（必读）

1. **上游封禁/限速/加鉴权**：你依赖别人的免费服务，随时可能被掐。→ 靠"多上游+自动摘除"兜底，别单点依赖。
2. **被你的站打爆**：出圈后流量暴涨 → 必须限流/限速，否则上游先把你封了。
3. **合规/版权**：只做转发中转，缓存要注意不长期囤受版权保护内容；尊重上游 ToS；页面注明"仅供参考、加速内容版权归原作者"。
4. **安全**：严防上游重定向到恶意地址（最终域名白名单校验）；防止被当 SSRF/开放代理滥用（限制只允许 github 域）。
5. **可持续**：带宽/服务器成本自担 → 限速 + 可选缓存，避免成为无底洞。

---

## 13. 待你拍板

- [ ] 域名/子域（`gh.somtfly.com`？）
- [ ] 技术栈（Go 默认？）
- [ ] 是否要本地缓存（吃磁盘换速度）
- [ ] 开放程度（完全公开 / 需 Token / 限速阈值）
