package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

//go:embed static/index.html
var indexHTML string

//go:embed static/docs.html
var docsHTML string

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
		return u, true
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
		return u, true
	}
	return "", false
}

func relay(w http.ResponseWriter, r *http.Request, raw string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
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
	var lastErr error
	for _, up := range cands {
		target, ok := up.BuildURL(raw)
		if !ok {
			continue
		}
		if n, err := streamThrough(w, r, target, up); err == nil {
			traffic.Record(clientIP(r), raw, up.Name, n, true)
			return
		} else {
			lastErr = err
			log.Printf("upstream %s failed: %v", up.Name, err)
		}
	}
	traffic.Record(clientIP(r), raw, "", 0, false)
	http.Error(w, "all upstreams failed: "+errStr(lastErr), http.StatusBadGateway)
}

func streamThrough(w http.ResponseWriter, r *http.Request, target string, up *Upstream) (int64, error) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, nil)
	if err != nil {
		return 0, err
	}
	for _, h := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set("User-Agent", "cq-accel/0.1 (+https://gh.somtfly.com)")
	resp, err := streamClient.Do(req)
	if err != nil {
		up.markFail()
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		up.markFail()
		return 0, fmt.Errorf("upstream status %d", resp.StatusCode)
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
	w.WriteHeader(resp.StatusCode)
	if r.Method == http.MethodHead {
		return 0, nil
	}
	n, err := io.Copy(w, resp.Body)
	return n, err
}

// ---------- API ----------

func handleStats(w http.ResponseWriter, r *http.Request) {
	ups := store.snapshot()
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
	writeJSON(w, map[string]any{"count": len(store.ups), "upstreams": store.snapshot()})
}

func handleResolve(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("u")
	if !isGHURL(raw) {
		http.Error(w, "invalid github url", http.StatusBadRequest)
		return
	}
	var out []map[string]any
	for _, up := range store.Candidates(raw) {
		t, _ := up.BuildURL(raw)
		out = append(out, map[string]any{
			"name": up.Name, "region": up.Region,
			"score": up.Score, "ttfb_ms": up.TTFBms, "speed_kbps": up.SpeedKB,
			"url": t,
		})
	}
	writeJSON(w, map[string]any{"raw": raw, "self": selfURL(r, raw), "upstreams": out})
}

func handleBest(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("u")
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
	writeJSON(w, map[string]any{
		"raw": raw, "self": selfURL(r, raw),
		"best": cands[0].Name, "upstream_url": t, "score": cands[0].Score,
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
