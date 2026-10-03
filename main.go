package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//go:embed static/index.html
var indexHTML string

//go:embed static/docs.html
var docsHTML string

//go:embed static/favicon.ico
var faviconICO []byte

var (
	store        *Store
	traffic      *Traffic
	streamClient *http.Client
	probeClient  *http.Client
)

func main() {
	var err error
	store, err = LoadStore(getenv("UPSTREAMS_FILE", "upstreams.json"))
	if err != nil {
		log.Fatalf("load upstreams: %v", err)
	}

	traffic = LoadTraffic(getenv("TRAFFIC_FILE", "traffic.json"))
	go traffic.SaverLoop(60 * time.Second)
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		_ = traffic.Save()
		os.Exit(0)
	}()

	tr := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		ResponseHeaderTimeout: 30 * time.Second,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   50,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true, // 原样透传
	}
	streamClient = &http.Client{
		Transport: tr,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 6 {
				return fmt.Errorf("too many redirects")
			}
			if !isSafeRedirect(req.URL) {
				return fmt.Errorf("unsafe redirect -> %s", req.URL.Host)
			}
			return nil
		},
	}
	probeClient = &http.Client{Timeout: 15 * time.Second}

	go store.HealthLoop(15 * time.Minute)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"ok": true, "service": "cq-accel", "time": time.Now().Format(time.RFC3339)})
	})
	mux.HandleFunc("/api/stats", handleStats)
	mux.HandleFunc("/api/traffic", handleTraffic)
	mux.HandleFunc("/api/upstreams", handleUpstreams)
	mux.HandleFunc("/api/resolve", handleResolve)
	mux.HandleFunc("/api/best", handleBest)
	mux.HandleFunc("/", handleRoot)

	addr := getenv("LISTEN", ":8890")
	log.Printf("cq-accel listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// ---------- 路由 ----------

func handleRoot(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "favicon.ico" {
		w.Header().Set("Content-Type", "image/x-icon")
		w.Header().Set("Cache-Control", "public, max-age=604800")
		_, _ = w.Write(faviconICO)
		return
	}
	if p == "docs" || p == "docs/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, docsHTML)
		return
	}
	if p == "" || p == "index.html" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, indexHTML)
		return
	}
	raw, ok := resolveRaw(r)
	if !ok {
		http.Error(w, "只用 GitHub 链接，例：/github.com/owner/repo/releases/download/v1/x.zip", http.StatusBadRequest)
		return
	}
	relay(w, r, raw)
}

func resolveRaw(r *http.Request) (string, bool) {
	if u := r.URL.Query().Get("u"); u != "" {
		return normalizeGH(u), true
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "fetch" {
		return "", false
	}
	host := strings.SplitN(p, "/", 2)[0]
	if isGHHost(host) {
		u := "https://" + p
		if r.URL.RawQuery != "" {
			u += "?" + r.URL.RawQuery
		}
		return normalizeGH(u), true
	}
	return "", false
}

// normalizeGH 把 github.com/<owner>/<repo>/blob/... 重写为 /raw/...，
// 保证“分支文件”一定拿到原文（而不是依赖上游自己转换）
func normalizeGH(u string) string {
	pu, err := url.Parse(u)
	if err != nil || pu.Hostname() != "github.com" {
		return u
	}
	if strings.Contains(pu.Path, "/blob/") {
		pu.Path = strings.Replace(pu.Path, "/blob/", "/raw/", 1)
		return pu.String()
	}
	return u
}

func relay(w http.ResponseWriter, r *http.Request, raw string) {
	// CORS 预检：让浏览器/XHR 跨域直接调用中转地址
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,HEAD,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Max-Age", "1728000")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
	case http.MethodPost:
		// 只放行 git 智能 HTTP 的 POST（git-upload-pack / git-receive-pack），防开放代理滥用
		if !isGitRPCPath(r.URL.Path) {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !isGHURL(raw) {
		http.Error(w, "only GitHub URLs allowed", http.StatusBadRequest)
		return
	}
	cands := store.Candidates(raw)
	if len(cands) == 0 {
		http.Error(w, "no available upstream", http.StatusBadGateway)
		return
	}

	// 默认：直接 302 重定向到最优上游——流量不经过本机（也就绕开了香港隧道），又快又省带宽。
	// 例外：git 的 POST（无法用 302 重定向，会丢方法/请求体）、HEAD（部分镜像不支持 HEAD）、以及显式 ?proxy=1。
	isGit := r.Method == http.MethodPost || r.Method == http.MethodHead || isGitRPCPath(r.URL.Path)
	if !isGit && r.URL.Query().Get("proxy") != "1" {
		if upName, target, size := pickRedirect(raw, cands); target != "" {
			traffic.Record(clientIP(r), raw, upName, size, true)
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Location", target)
			w.WriteHeader(http.StatusFound) // 302
			return
		}
	}
	var lastErr error
	lastStatus := 0
	attempts := 0
	const maxRelayAttempts = 25
	const relayBudget = 15 * time.Second
	deadline := time.Now().Add(relayBudget)
	for _, up := range cands {
		if attempts >= maxRelayAttempts || time.Now().After(deadline) {
			break
		}
		target, ok := up.BuildURL(raw)
		if !ok {
			continue
		}
		attempts++
		if n, err := streamThrough(w, r, target, up); err == nil {
			traffic.Record(clientIP(r), raw, up.Name, n, true)
			return
		} else {
			lastErr = err
			var se *upstreamStatusError
			if errors.As(err, &se) {
				lastStatus = se.code
			}
			log.Printf("upstream %s failed: %v", up.Name, err)
		}
	}
	traffic.Record(clientIP(r), raw, "", 0, false)
	if lastStatus != 0 {
		http.Error(w, "upstream error: "+errStr(lastErr), lastStatus)
		return
	}
	http.Error(w, "all upstreams failed: "+errStr(lastErr), http.StatusBadGateway)
}

// isRenderableType 判断 Content-Type 是否属于“浏览器可内联渲染/执行”的危险类型
func isRenderableType(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "text/html", "application/xhtml+xml", "image/svg+xml", "application/xml",
		"text/xml", "application/javascript", "text/javascript", "application/ecmascript",
		"text/ecmascript", "application/json", "text/vtt", "application/rss+xml", "application/atom+xml":
		return true
	}
	return false
}

// isGitRPCPath 判断是否 git 智能 HTTP 的 RPC 端点
func isGitRPCPath(p string) bool {
	return strings.Contains(p, "/git-upload-pack") || strings.Contains(p, "/git-receive-pack")
}

// upstreamStatusError 携带上游返回的 HTTP 状态码，便于全失败时透传给客户端
// （比如资源确实不存在 → 最终回 404）
type upstreamStatusError struct {
	code int
	msg  string
}

func (e *upstreamStatusError) Error() string { return e.msg }

func streamThrough(w http.ResponseWriter, r *http.Request, target string, up *Upstream) (int64, error) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, nil)
	if err != nil {
		return 0, err
	}
	// git RPC（POST）需要带请求体与相关头
	if r.Method == http.MethodPost {
		req.Body = r.Body
		req.ContentLength = r.ContentLength
		for _, h := range []string{"Content-Type", "Content-Encoding", "Accept", "Accept-Encoding"} {
			if v := r.Header.Get(h); v != "" {
				req.Header.Set(h, v)
			}
		}
	}
	for _, h := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set("User-Agent", "cq-accel/0.1 (+https://gh.somtfly.com)")
	resp, err := streamClient.Do(req)
	if err != nil {
		up.markRelayFail()
		return 0, err
	}
	defer resp.Body.Close()
	// 4xx 多为“该上游不支持这个形态/方法”（403/404/405…）→ 换下一家再试，不罚健康分
	// 429/5xx 为上游过载或自身故障 → 罚分
	if resp.StatusCode >= 400 {
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			up.markRelayFail()
		}
		return 0, &upstreamStatusError{resp.StatusCode, fmt.Sprintf("upstream status %d", resp.StatusCode)}
	}
	for _, h := range []string{
		"Content-Type", "Content-Length", "Content-Range",
		"Accept-Ranges", "Content-Disposition", "ETag", "Last-Modified", "Content-Encoding",
	} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.Header().Set("X-Upstream", up.Name)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "*")
	// H2-安全：剔除上游可内联/执行的内容类型，强制当附件下载，防止 gh 域内联 XSS
	if isRenderableType(w.Header().Get("Content-Type")) {
		w.Header().Set("Content-Type", "application/octet-stream")
		if w.Header().Get("Content-Disposition") == "" {
			w.Header().Set("Content-Disposition", "attachment")
		}
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return 0, nil
	}
	n, err := io.Copy(w, resp.Body)
	return n, err
}

// ---------- 重定向选路 ----------

var redirectCheckClient = &http.Client{Timeout: 8 * time.Second}

// pickRedirect 从按分数排好的候选里挑第一个“真正能服务”的上游，返回名字/直链/文件大小。
// 用 Range 探测做轻量校验（兼容性比 HEAD 好），失败就换下一个；最多试 3 个。
func pickRedirect(raw string, cands []*Upstream) (name, target string, size int64) {
	tries := 0
	for _, up := range cands {
		if !up.isOK() {
			continue
		}
		t, ok := up.BuildURL(raw)
		if !ok {
			continue
		}
		tries++
		if sz, ok := quickCheck(t); ok {
			return up.Name, t, sz
		}
		if tries >= 3 {
			break
		}
	}
	return "", "", 0
}

// quickCheck 用 Range: bytes=0-0 轻探一下，返回总大小（可能为 0）
func quickCheck(u string) (int64, bool) {
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return 0, false
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", "cq-accel/0.1 (+https://gh.somtfly.com)")
	resp, err := redirectCheckClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, false
	}
	var size int64
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			size, _ = strconv.ParseInt(strings.TrimSpace(cr[i+1:]), 10, 64)
		}
	}
	if size == 0 {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			size, _ = strconv.ParseInt(cl, 10, 64)
		}
	}
	return size, true
}

// ---------- API ----------

func handleStats(w http.ResponseWriter, r *http.Request) {
	ups := store.View()
	var ok, sumTTFB int
	var sumSpeed float64
	kinds := map[string]int{}
	regions := map[string]int{}
	for _, u := range ups {
		kinds[u.Kind]++
		regions[u.Region]++
		if u.OK {
			ok++
			sumTTFB += int(u.TTFBms)
			sumSpeed += u.SpeedKB
		}
	}
	avgTTFB, avgSpeed := 0, 0.0
	if ok > 0 {
		avgTTFB = sumTTFB / ok
		avgSpeed = sumSpeed / float64(ok)
	}
	writeJSON(w, map[string]any{
		"total": len(ups), "ok": ok, "down": len(ups) - ok,
		"avg_ttfb_ms": avgTTFB, "avg_speed_kbps": avgSpeed,
		"kinds": kinds, "regions": regions,
		"updated": time.Now().Format(time.RFC3339),
	})
}

func handleTraffic(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, traffic.snapshot())
}

func handleUpstreams(w http.ResponseWriter, r *http.Request) {
	v := store.View()
	writeJSON(w, map[string]any{"count": len(v), "upstreams": v})
}

func handleResolve(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("u")
	raw = normalizeGH(raw)
	if !isGHURL(raw) {
		http.Error(w, "invalid github url", http.StatusBadRequest)
		return
	}
	var out []map[string]any
	for _, up := range store.Candidates(raw) {
		t, _ := up.BuildURL(raw)
		_, sc, ttfb, sp, _ := up.metrics()
		out = append(out, map[string]any{
			"name": up.Name, "region": up.Region,
			"score": sc, "ttfb_ms": ttfb, "speed_kbps": sp,
			"url": t,
		})
	}
	writeJSON(w, map[string]any{"raw": raw, "self": selfURL(r, raw), "upstreams": out})
}

func handleBest(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("u")
	raw = normalizeGH(raw)
	if !isGHURL(raw) {
		http.Error(w, "invalid github url", http.StatusBadRequest)
		return
	}
	cands := store.Candidates(raw)
	if len(cands) == 0 {
		http.Error(w, "no upstream", http.StatusBadGateway)
		return
	}
	t, _ := cands[0].BuildURL(raw)
	_, sc, _, _, _ := cands[0].metrics()
	writeJSON(w, map[string]any{
		"raw": raw, "self": selfURL(r, raw),
		"best": cands[0].Name, "upstream_url": t, "score": sc,
	})
}

func selfURL(r *http.Request, raw string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	}
	return scheme + "://" + r.Host + "/" + strings.TrimPrefix(raw, "https://")
}

// ---------- helpers ----------

func isSafeRedirect(u *url.URL) bool {
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	host := u.Hostname()
	if host == "localhost" || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func errStr(err error) string {
	if err == nil {
		return "unknown"
	}
	return err.Error()
}
