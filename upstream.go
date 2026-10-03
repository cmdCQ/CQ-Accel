package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------- 上游模型 ----------

// 支持的加速站形态：
//
//	prefix   : {base}/{原始URL}           例 https://ghproxy.com/https://github.com/...
//	param    : {base}/?url={urlencode}    例 https://x.com/?url=https%3A%2F%2Fgithub.com%2F...
//	replace  : 把 github 主机名替换为 domain，路径不变
//	jsdelivr : raw / repos/raw 走 cdn.jsdelivr.net
type Upstream struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Base    string `json:"base,omitempty"`
	Domain  string `json:"domain,omitempty"`
	Region  string `json:"region,omitempty"`
	Enabled bool   `json:"enabled"`

	// 运行时指标（不写回配置文件）
	OK      bool    `json:"ok"`
	Score   float64 `json:"score"`
	TTFBms  int64   `json:"ttfb_ms"`
	SpeedKB float64 `json:"speed_kbps"`
	Checked string  `json:"checked,omitempty"`

	SpeedAt time.Time
	ttfbTmp int64
	fails   int
	mu      sync.Mutex
}

var ghHosts = []string{
	"github.com",
	"raw.githubusercontent.com",
	"codeload.github.com",
	"gist.githubusercontent.com",
	"gist.github.com",
	"objects.githubusercontent.com",
	"release-assets.githubusercontent.com",
}

func isGHHost(h string) bool {
	h = strings.ToLower(h)
	for _, x := range ghHosts {
		if x == h {
			return true
		}
	}
	return false
}

// 校验：只允许公网 http(s) 的 GitHub 域
func isGHURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	return isGHHost(u.Hostname())
}

func (up *Upstream) CanHandle(raw string) bool {
	if up.Kind == "jsdelivr" {
		_, ok := jsdelivrBuild(raw)
		return ok
	}
	return isGHURL(raw)
}

func (up *Upstream) BuildURL(raw string) (string, bool) {
	switch up.Kind {
	case "prefix":
		return strings.TrimRight(up.Base, "/") + "/" + raw, true
	case "param":
		return strings.TrimRight(up.Base, "/") + "/?url=" + url.QueryEscape(raw), true
	case "replace":
		u, err := url.Parse(raw)
		if err != nil {
			return "", false
		}
		u.Host = up.Domain
		return u.String(), true
	case "jsdelivr":
		return jsdelivrBuild(raw)
	}
	return "", false
}

// raw.githubusercontent.com/user/repo/branch/path  ->  cdn.jsdelivr.net/gh/user/repo@branch/path
// github.com/user/repo/raw/branch/path             ->  同上
func jsdelivrBuild(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	h := strings.ToLower(u.Hostname())
	segs := strings.Split(strings.Trim(u.Path, "/"), "/")
	switch {
	case h == "raw.githubusercontent.com" && len(segs) >= 3:
		return "https://cdn.jsdelivr.net/gh/" + segs[0] + "/" + segs[1] + "@" + segs[2] + "/" + strings.Join(segs[3:], "/"), true
	case h == "github.com" && len(segs) >= 5 && segs[2] == "raw":
		return "https://cdn.jsdelivr.net/gh/" + segs[0] + "/" + segs[1] + "@" + segs[3] + "/" + strings.Join(segs[4:], "/"), true
	}
	return "", false
}

// ---------- 上游池 ----------

type Store struct {
	mu  sync.RWMutex
	ups []*Upstream
}

func LoadStore(path string) (*Store, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ups []*Upstream
	if err := json.Unmarshal(b, &ups); err != nil {
		return nil, err
	}
	for _, u := range ups {
		if u.Region == "" {
			u.Region = "global"
		}
	}
	return &Store{ups: ups}, nil
}

func (s *Store) snapshot() []*Upstream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Upstream, len(s.ups))
	copy(out, s.ups)
	return out
}

// Candidates 返回能处理 raw 的上游，按分数从高到低
func (s *Store) Candidates(raw string) []*Upstream {
	var out []*Upstream
	for _, u := range s.snapshot() {
		if u.Enabled && u.CanHandle(raw) {
			out = append(out, u)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

func (up *Upstream) markFail() {
	up.mu.Lock()
	up.fails++
	up.Score = -1
	up.OK = false
	up.mu.Unlock()
}

func (up *Upstream) markOK(ttfb int64, kbps float64) {
	up.mu.Lock()
	up.fails = 0
	up.OK = true
	up.TTFBms = ttfb
	up.SpeedKB = kbps
	// 评分：以**实测吞吐**为主（KB/s 量级），延迟为辅；200 封顶
	sc := kbps/100.0 + 1000.0/float64(ttfb+1)
	if sc > 200 {
		sc = 200
	}
	up.Score = sc
	up.Checked = time.Now().Format("15:04:05")
	up.mu.Unlock()
}

// ---------- 健康测速 ----------

// 小文件测 TTFB（便宜、高频）；大文件测**持续吞吐**（贵、低频）
var testRawURL = "https://raw.githubusercontent.com/twbs/bootstrap/main/dist/css/bootstrap.css"
var speedTestURL = "https://github.com/cli/cli/releases/download/v2.62.0/gh_2.62.0_linux_amd64.tar.gz"

const (
	speedEvery     = 6 * time.Hour // 同一上游多久重新测一次吞吐
	speedReadBytes = 2 << 20       // 每次最多读 2MB
	speedParallel  = 4             // 吞吐测试并发上限（高了测的是总带宽而非单上游）
	speedTopN      = 24            // 只给 TTFB 最优的前 N 个测吞吐（省流量）
)

var speedSem = make(chan struct{}, speedParallel)

func (s *Store) HealthLoop(interval time.Duration) {
	s.HealthOnce()
	for {
		time.Sleep(interval)
		s.HealthOnce()
	}
}

func (s *Store) HealthOnce() {
	ups := s.snapshot()

	// 第一层：所有启用上游并发测 TTFB（只看首字节，几乎不耗流量）
	var wg sync.WaitGroup
	for _, up := range ups {
		if !up.Enabled {
			continue
		}
		wg.Add(1)
		go func(u *Upstream) {
			defer wg.Done()
			ttfb, ok := probeTTFB(u)
			if !ok {
				u.markFail()
				return
			}
			u.mu.Lock()
			u.ttfbTmp = ttfb
			u.mu.Unlock()
		}(up)
	}
	wg.Wait()

	// 选出 TTFB 最优的候选
	type cand struct {
		up   *Upstream
		ttfb int64
	}
	var cs []cand
	for _, up := range ups {
		up.mu.Lock()
		if up.ttfbTmp > 0 {
			cs = append(cs, cand{up, up.ttfbTmp})
		}
		up.mu.Unlock()
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].ttfb < cs[j].ttfb })

	// 先立即用已有吞吐标记 OK，避免被后面的测速阻塞
	for _, c := range cs {
		c.up.mu.Lock()
		sp := c.up.SpeedKB
		c.up.mu.Unlock()
		c.up.markOK(c.ttfb, sp)
	}

	// 第二层：只给 TTFB 最优的前 speedTopN 个测吞吐（并发，限速）
	var wg2 sync.WaitGroup
	for i, c := range cs {
		if i >= speedTopN {
			break
		}
		c.up.mu.Lock()
		stale := time.Since(c.up.SpeedAt) > speedEvery
		c.up.mu.Unlock()
		if !stale {
			continue
		}
		wg2.Add(1)
		go func(u *Upstream, ttfb int64) {
			defer wg2.Done()
			if sp := probeSpeed(u); sp > 0 {
				u.mu.Lock()
				u.SpeedAt = time.Now()
				u.mu.Unlock()
				u.markOK(ttfb, sp)
			}
		}(c.up, c.ttfb)
	}
	wg2.Wait()
}

func probe(up *Upstream) {
	ttfb, ok := probeTTFB(up)
	if !ok {
		up.markFail()
		return
	}
	speed := up.SpeedKB
	if time.Since(up.SpeedAt) > speedEvery {
		if s := probeSpeed(up); s > 0 {
			speed = s
			up.SpeedAt = time.Now()
		}
	}
	up.markOK(ttfb, speed)
}

// 小文件：测首字节延迟
func probeTTFB(up *Upstream) (int64, bool) {
	target, ok := up.BuildURL(testRawURL)
	if !ok {
		return 0, false
	}
	start := time.Now()
	resp, err := probeClient.Get(target)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return 0, false
	}
	ttfb := time.Since(start).Milliseconds()
	return ttfb, true // 只看首字节，不读 body（几乎不耗流量）
}

// 大文件：测持续吞吐（限字节数 + 超时，避免拖垮上游/本地）
func probeSpeed(up *Upstream) float64 {
	target, ok := up.BuildURL(speedTestURL)
	if !ok {
		return 0
	}
	speedSem <- struct{}{}
	defer func() { <-speedSem }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("User-Agent", "cq-accel/0.1 (+https://gh.somtfly.com)")
	resp, err := streamClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return 0
	}
	// 先读掉开头一小段（含 TTFB），再开始计时
	first := make([]byte, 64<<10)
	n0, _ := io.ReadFull(resp.Body, first)
	start := time.Now()
	n, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, speedReadBytes))
	el := time.Since(start).Seconds()
	if int64(n0)+n < 512<<10 || el <= 0 {
		return 0
	}
	return float64(int64(n0)+n) / 1024 / el
}

// ---------- 定时发现（占位，P2 扩展） ----------

// DiscoverOnce 从 GitHub Code Search 抓含加速关键词的仓库，正则抽域名 -> 待测池。
// P0 先留桩，人工种子为主。
func (s *Store) DiscoverLoop(interval time.Duration) {
	_ = fmt.Sprint // keep import
	for {
		time.Sleep(interval)
		// TODO(P2): code search + 页面抓取 + 去重入池
	}
}
