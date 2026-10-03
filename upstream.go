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
	"sync/atomic"
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
	fails   int
	mu      sync.Mutex
}

// UpstreamView 是无锁视图：API 只读它的拷贝，避免与健康循环数据竞争
func (up *Upstream) metrics() (ok bool, score float64, ttfb int64, speed float64, checked string) {
	up.mu.Lock()
	defer up.mu.Unlock()
	return up.OK, up.Score, up.TTFBms, up.SpeedKB, up.Checked
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
	mu         sync.RWMutex
	ups        []*Upstream
	healthPath string
	speedBusy  atomic.Bool
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
	s := &Store{ups: ups, healthPath: getenv("HEALTH_FILE", "health.json")}
	s.loadHealth() // 启动时先回填上次的健康快照，避免重启后前端一片“不可用”
	return s, nil
}

// ---------- 健康快照持久化（覆盖式：每轮覆盖写入，启动时回填） ----------

type healthSnap struct {
	OK      bool    `json:"ok"`
	Score   float64 `json:"score"`
	TTFBms  int64   `json:"ttfb_ms"`
	SpeedKB float64 `json:"speed_kbps"`
	Checked string  `json:"checked,omitempty"`
}

func (s *Store) loadHealth() {
	b, err := os.ReadFile(s.healthPath)
	if err != nil {
		return
	}
	var m map[string]healthSnap
	if json.Unmarshal(b, &m) != nil {
		return
	}
	for _, u := range s.ups {
		if h, ok := m[u.ID]; ok {
			u.OK, u.Score, u.TTFBms, u.SpeedKB, u.Checked = h.OK, h.Score, h.TTFBms, h.SpeedKB, h.Checked
		}
	}
}

func (s *Store) saveHealth() {
	m := map[string]healthSnap{}
	for _, u := range s.snapshot() {
		u.mu.Lock()
		m[u.ID] = healthSnap{u.OK, u.Score, u.TTFBms, u.SpeedKB, u.Checked}
		u.mu.Unlock()
	}
	if b, err := json.Marshal(m); err == nil {
		_ = os.WriteFile(s.healthPath+".tmp", b, 0o644)
		_ = os.Rename(s.healthPath+".tmp", s.healthPath)
	}
}

func (s *Store) snapshot() []*Upstream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Upstream, len(s.ups))
	copy(out, s.ups)
	return out
}

// UpstreamView 对外只读快照（在锁内拷贝，供 JSON 序列化）
type UpstreamView struct {
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	Kind    string  `json:"kind"`
	Region  string  `json:"region,omitempty"`
	Enabled bool    `json:"enabled"`
	OK      bool    `json:"ok"`
	Score   float64 `json:"score"`
	TTFBms  int64   `json:"ttfb_ms"`
	SpeedKB float64 `json:"speed_kbps"`
	Checked string  `json:"checked,omitempty"`
}

// View 返回全部上游的只读快照（消除 API 无锁读的竞态）
func (s *Store) View() []UpstreamView {
	ups := s.snapshot()
	out := make([]UpstreamView, 0, len(ups))
	for _, u := range ups {
		ok, sc, ttfb, sp, ck := u.metrics()
		out = append(out, UpstreamView{u.ID, u.Name, u.Kind, u.Region, u.Enabled, ok, sc, ttfb, sp, ck})
	}
	return out
}

// Candidates 返回能处理 raw 的上游，按分数从高到低
func (s *Store) Candidates(raw string) []*Upstream {
	type sc struct {
		u *Upstream
		s float64
	}
	var arr []sc
	for _, u := range s.snapshot() {
		if u.Enabled && u.CanHandle(raw) {
			u.mu.Lock()
			arr = append(arr, sc{u, u.Score})
			u.mu.Unlock()
		}
	}
	sort.SliceStable(arr, func(i, j int) bool { return arr[i].s > arr[j].s })
	out := make([]*Upstream, len(arr))
	for i, x := range arr {
		out[i] = x.u
	}
	return out
}

// markProbeFail 健康探测失败：先降权；连续 2 次才标不可用（避免扫描瞬间闪成不可用）
func (up *Upstream) markProbeFail() {
	up.mu.Lock()
	up.fails++
	if up.fails >= 2 {
		up.OK = false
		up.Score = -1
	}
	up.mu.Unlock()
}

// markRelayFail 用户请求中转失败：温和降权（不立即拉黑），连续多次才标不可用。
// 避免“一次性抖动”把好节点拉到下一轮健康检查前都不能用（性能优先）。
func (up *Upstream) markRelayFail() {
	up.mu.Lock()
	up.fails++
	up.Score *= 0.5
	if up.fails >= 3 {
		up.OK = false
	}
	up.mu.Unlock()
}

// setLatency 只更新延迟（不动吞吐）；score 用当前 SpeedKB 重算
func (up *Upstream) setLatency(ttfb int64) {
	up.mu.Lock()
	up.fails = 0
	up.OK = true
	up.TTFBms = ttfb
	up.Score = calcScore(up.SpeedKB, up.TTFBms)
	up.Checked = time.Now().Format("15:04:05")
	up.mu.Unlock()
}

// setSpeed 只更新吞吐（不动延迟）；避免用陈旧 TTFB 覆盖较新值（M2）
func (up *Upstream) setSpeed(kbps float64) {
	up.mu.Lock()
	up.fails = 0
	up.OK = true
	up.SpeedKB = kbps
	up.Score = calcScore(up.SpeedKB, up.TTFBms)
	up.Checked = time.Now().Format("15:04:05")
	up.mu.Unlock()
}

// calcScore：以实测吞吐为主（KB/s 量级），延迟为辅；200 封顶
func calcScore(kbps float64, ttfb int64) float64 {
	sc := kbps/100.0 + 1000.0/float64(ttfb+1)
	if sc > 200 {
		sc = 200
	}
	return sc
}

// ---------- 健康测速 ----------

// 小文件测 TTFB（便宜、高频）；大文件测**持续吞吐**（贵、低频）
var testRawURL = "https://raw.githubusercontent.com/twbs/bootstrap/main/dist/css/bootstrap.css"
var speedTestURL = "https://github.com/cli/cli/releases/download/v2.62.0/gh_2.62.0_linux_amd64.tar.gz"

const (
	speedEvery     = 6 * time.Hour // 同一上游多久重新测一次吞吐
	speedSkipBytes = 1 << 20       // 先丢掉前 1MB（冷启动/回源阶段，测的是慢速）
	speedReadBytes = 3 << 20       // 再测接下来的 3MB（稳态吞吐）
	speedParallel  = 6             // 吞吐测试并发上限（高了测的是总带宽而非单上游）
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

	// 第一层：TTFB 并发探测；**每探测完一个立即覆盖式更新那一条**，不重置整池、不等整批
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
				u.markProbeFail()
				return
			}
			u.setLatency(ttfb) // 只更新延迟，不用旧吞吐覆盖
		}(up)
	}
	wg.Wait()

	s.saveHealth() // TTFB 阶段完就落盘（不等慢吞吞的吞吐测试）

	// 第二层：吞吐测试放到后台异步跑，不阻塞健康循环，也不再重叠
	if s.speedBusy.CompareAndSwap(false, true) {
		go func() {
			defer s.speedBusy.Store(false)
			s.speedSweep(ups)
			s.saveHealth()
		}()
	}
}

func (s *Store) speedSweep(ups []*Upstream) {
	var wg sync.WaitGroup
	for _, up := range ups {
		if !up.Enabled {
			continue
		}
		up.mu.Lock()
		okNow := up.OK
		stale := time.Since(up.SpeedAt) > speedEvery
		up.mu.Unlock()
		if !okNow || !stale {
			continue
		}
		wg.Add(1)
		go func(u *Upstream) {
			defer wg.Done()
			if sp := probeSpeed(u); sp > 0 {
				u.mu.Lock()
				u.SpeedAt = time.Now()
				u.mu.Unlock()
				u.setSpeed(sp) // 只更新吞吐，不动延迟（M2）
			}
		}(up)
	}
	wg.Wait()
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	// 跳过前 1MB 的冷启动/回源阶段，再测接下来的稳态吞吐
	if _, err := io.CopyN(io.Discard, resp.Body, speedSkipBytes); err != nil {
		return 0
	}
	start := time.Now()
	n, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, speedReadBytes))
	el := time.Since(start).Seconds()
	if n < speedReadBytes/2 || el <= 0 {
		return 0
	}
	return float64(n) / 1024 / el
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
