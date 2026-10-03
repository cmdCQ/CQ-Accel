package main

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------- 流量监控 ----------

type DayStat struct {
	Req   int64           `json:"req"`
	Bytes int64           `json:"bytes"`
	IPs   map[string]bool `json:"ips"`
}

type Traffic struct {
	mu         sync.Mutex
	TotalReq   int64               `json:"total_req"`
	TotalBytes int64               `json:"total_bytes"`
	Days       map[string]*DayStat `json:"days"`
	TopUp      map[string]int64    `json:"top_upstreams"`
	TopPath    map[string]int64    `json:"top_paths"`
	LastSeen   string              `json:"last_seen"`
	LastIP     string              `json:"last_ip"`
	LastPath   string              `json:"last_path"`
	path       string
}

const trafficMaxKeys = 300

func LoadTraffic(path string) *Traffic {
	t := &Traffic{path: path, Days: map[string]*DayStat{}, TopUp: map[string]int64{}, TopPath: map[string]int64{}}
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, t)
	}
	if t.Days == nil {
		t.Days = map[string]*DayStat{}
	}
	if t.TopUp == nil {
		t.TopUp = map[string]int64{}
	}
	if t.TopPath == nil {
		t.TopPath = map[string]int64{}
	}
	for _, d := range t.Days {
		if d.IPs == nil {
			d.IPs = map[string]bool{}
		}
	}
	t.path = path
	return t
}

func (t *Traffic) Record(ip, path, up string, n int64, ok bool) {
	now := time.Now()
	day := now.Format("2006-01-02")
	t.mu.Lock()
	defer t.mu.Unlock()
	d := t.Days[day]
	if d == nil {
		d = &DayStat{IPs: map[string]bool{}}
		t.Days[day] = d
	}
	d.Req++
	d.Bytes += n
	if ip != "" && len(d.IPs) < 20000 {
		d.IPs[ip] = true
	}
	t.TotalReq++
	t.TotalBytes += n
	if up != "" {
		t.TopUp[up]++
	}
	if path != "" {
		t.TopPath[path]++
	}
	t.LastSeen = now.Format(time.RFC3339)
	t.LastIP = ip
	t.LastPath = path
	trim(t.TopUp)
	trim(t.TopPath)
}

func trim(m map[string]int64) {
	if len(m) <= trafficMaxKeys {
		return
	}
	type kv struct {
		k string
		v int64
	}
	arr := make([]kv, 0, len(m))
	for k, v := range m {
		arr = append(arr, kv{k, v})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
	for _, x := range arr[trafficMaxKeys/2:] {
		delete(m, x.k)
	}
}

func (t *Traffic) Save() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	tmp := t.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, t.path)
}

func (t *Traffic) SaverLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		_ = t.Save()
	}
}

func (t *Traffic) snapshot() map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()

	days := make([]map[string]any, 0, len(t.Days))
	keys := make([]string, 0, len(t.Days))
	for k := range t.Days {
		keys = append(keys, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	for _, k := range keys {
		d := t.Days[k]
		days = append(days, map[string]any{"date": k, "req": d.Req, "bytes": d.Bytes, "ips": len(d.IPs)})
		if len(days) >= 14 {
			break
		}
	}
	today := t.Days[time.Now().Format("2006-01-02")]

	return map[string]any{
		"total_req":     t.TotalReq,
		"total_bytes":   t.TotalBytes,
		"last_seen":     t.LastSeen,
		"last_ip":       t.LastIP,
		"last_path":     t.LastPath,
		"today":         dayView(today),
		"days":          days,
		"top_upstreams": topN(t.TopUp, 8),
		"top_paths":     topN(t.TopPath, 8),
	}
}

func dayView(d *DayStat) map[string]any {
	if d == nil {
		return map[string]any{"req": 0, "bytes": 0, "ips": 0}
	}
	return map[string]any{"req": d.Req, "bytes": d.Bytes, "ips": len(d.IPs)}
}

func topN(m map[string]int64, n int) []map[string]any {
	type kv struct {
		k string
		v int64
	}
	arr := make([]kv, 0, len(m))
	for k, v := range m {
		arr = append(arr, kv{k, v})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
	if len(arr) > n {
		arr = arr[:n]
	}
	out := make([]map[string]any, 0, len(arr))
	for _, x := range arr {
		out = append(out, map[string]any{"name": x.k, "req": x.v})
	}
	return out
}

func clientIP(r *http.Request) string {
	if v := r.Header.Get("X-Real-IP"); v != "" {
		return v
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		return strings.TrimSpace(strings.Split(v, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
