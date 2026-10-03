package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Host      string
	Port      int
	APIKey    string
	Source    string
	CacheTTL  time.Duration
	Timeout   time.Duration
	RateLimit int
	LiveMax   time.Duration
	RecentMax time.Duration
	CORS      map[string]bool
}
type Cache struct {
	Data      map[string]any
	Retrieved time.Time
}

var cfg Config
var cacheMu sync.RWMutex
var cached *Cache
var rateMu sync.Mutex
var rate = map[string][]time.Time{}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt(k string, d int) int {
	v, err := strconv.Atoi(env(k, strconv.Itoa(d)))
	if err != nil {
		return d
	}
	return v
}
func main() {
	cfg = Config{Host: env("HOST", "0.0.0.0"), Port: envInt("PORT", 8080), APIKey: os.Getenv("METALS_DEV_API_KEY"), Source: strings.ToLower(env("MARKET_SOURCE", "spot")), CacheTTL: time.Duration(envInt("CACHE_TTL_SECONDS", 60)) * time.Second, Timeout: time.Duration(envInt("UPSTREAM_TIMEOUT_SECONDS", 8)) * time.Second, RateLimit: envInt("RATE_LIMIT_PER_MINUTE", 60), LiveMax: time.Duration(envInt("LIVE_MAX_AGE_SECONDS", 180)) * time.Second, RecentMax: time.Duration(envInt("RECENT_MAX_AGE_SECONDS", 900)) * time.Second, CORS: parseCORS(os.Getenv("CORS_ORIGINS"))}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", health)
	mux.HandleFunc("/api/gold", gold)
	mux.HandleFunc("/", notFound)
	srv := &http.Server{Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), Handler: securityHeaders(mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		log.Printf("[gold-rate-api] listening on %s source=%s", srv.Addr, cfg.Source)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
func parseCORS(s string) map[string]bool {
	m := map[string]bool{}
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			m[v] = true
		}
	}
	return m
}
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		origin := r.Header.Get("Origin")
		if origin != "" && (cfg.CORS["*"] || cfg.CORS[origin]) {
			if cfg.CORS["*"] {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET,OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Accept,Content-Type")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func rateAllowed(ip string) bool {
	now := time.Now()
	rateMu.Lock()
	defer rateMu.Unlock()
	arr := rate[ip]
	i := 0
	for _, t := range arr {
		if now.Sub(t) < time.Minute {
			arr[i] = t
			i++
		}
	}
	arr = arr[:i]
	if len(arr) >= cfg.RateLimit {
		return false
	}
	arr = append(arr, now)
	rate[ip] = arr
	if len(rate) > 5000 {
		for k := range rate {
			delete(rate, k)
			if len(rate) <= 4000 {
				break
			}
		}
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]any{"success": false, "code": "METHOD_NOT_ALLOWED", "message": "GET required"})
		return
	}
	writeJSON(w, 200, map[string]any{"success": true, "status": "ok", "service": "gold-rate-api", "timezone": "Asia/Kolkata", "providerConfigured": cfg.APIKey != "", "marketSource": cfg.Source, "serverTime": time.Now().UTC().Format(time.RFC3339Nano)})
}
func notFound(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 404, map[string]any{"success": false, "code": "NOT_FOUND", "message": "Route not found."})
}
func classify(p, r time.Time) string {
	age := r.Sub(p)
	if age < 0 {
		age = 0
	}
	if age <= cfg.LiveMax {
		return "LIVE"
	}
	if age <= cfg.RecentMax {
		return "RECENT"
	}
	return "STALE"
}
func fresh(ctx context.Context) (map[string]any, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("MISSING_PROVIDER_KEY")
	}
	endpoint := "https://api.metals.dev/v1/metal/spot"
	q := url.Values{}
	q.Set("api_key", cfg.APIKey)
	q.Set("currency", "INR")
	q.Set("unit", "g")
	if cfg.Source == "ibja" {
		endpoint = "https://api.metals.dev/v1/metal/authority"
		q.Set("authority", "ibja")
	} else {
		q.Set("metal", "gold")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "GoldRateIndia/1.0")
	hc := http.Client{Timeout: cfg.Timeout}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("UPSTREAM_NETWORK_ERROR")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, fmt.Errorf("UPSTREAM_READ_ERROR")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("UPSTREAM_HTTP_%d", resp.StatusCode)
	}
	var d struct {
		Status    string `json:"status"`
		Timestamp string `json:"timestamp"`
		Currency  string `json:"currency"`
		Rate      struct {
			Price float64 `json:"price"`
		} `json:"rate"`
		Rates map[string]float64 `json:"rates"`
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, fmt.Errorf("UPSTREAM_INVALID_JSON")
	}
	if d.Status != "success" {
		return nil, fmt.Errorf("UPSTREAM_REPORTED_FAILURE")
	}
	pTs, err := time.Parse(time.RFC3339Nano, d.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("UPSTREAM_TIMESTAMP_MISSING_OR_INVALID")
	}
	if d.Currency != "" && d.Currency != "INR" {
		return nil, fmt.Errorf("UPSTREAM_CURRENCY_NOT_INR")
	}
	price := d.Rate.Price
	source, cat := "Metals.Dev — Gold spot", "GLOBAL GOLD SPOT PRICE"
	if cfg.Source == "ibja" {
		price = d.Rates["ibja_gold"]
		source = "Metals.Dev — IBJA authority feed"
		cat = "INDIAN AUTHORITY / REFERENCE FEED"
	}
	if !(price > 0) {
		return nil, fmt.Errorf("UPSTREAM_PRICE_MISSING_OR_INVALID")
	}
	r := time.Now().UTC()
	p24 := price
	p22 := p24 * (916.0 / 999.0)
	p18 := p24 * (750.0 / 999.0)
	return map[string]any{"success": true, "source": source, "marketCategory": cat, "providerTimestamp": pTs.UTC().Format(time.RFC3339Nano), "retrievedAt": r.Format(time.RFC3339Nano), "serverNow": r.Format(time.RFC3339Nano), "timezone": "Asia/Kolkata", "currency": "INR", "status": classify(pTs, r), "cacheStatus": "MISS", "methodology": "24K direct from upstream; 22K and 18K derived by fineness ratio only; excludes GST, duty, making charges, wastage, premiums and jeweller margin.", "providerDelaySeconds": r.Sub(pTs).Seconds(), "marketState": nil, "rates": map[string]any{"24k_999": map[string]any{"perGram": p24, "per10g": p24 * 10, "type": "DIRECT"}, "22k_916": map[string]any{"perGram": p22, "per10g": p22 * 10, "type": "DERIVED"}, "18k_750": map[string]any{"perGram": p18, "per10g": p18 * 10, "type": "DERIVED"}}}, nil
}
func gold(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, 405, map[string]any{"success": false, "code": "METHOD_NOT_ALLOWED", "message": "GET required"})
		return
	}
	ip := strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0]
	if ip == "" {
		ip = r.RemoteAddr
	}
	if !rateAllowed(ip) {
		writeJSON(w, 429, map[string]any{"success": false, "code": "RATE_LIMITED", "message": "Too many requests. Retry later."})
		return
	}
	cacheMu.RLock()
	c := cached
	cacheMu.RUnlock()
	if c != nil {
		retr, err := time.Parse(time.RFC3339Nano, c.Data["retrievedAt"].(string))
		if err == nil && time.Since(retr) <= cfg.CacheTTL {
			copy := cloneMap(c.Data)
			copy["cacheStatus"] = "HIT"
			copy["serverNow"] = time.Now().UTC().Format(time.RFC3339Nano)
			writeJSON(w, 200, copy)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), cfg.Timeout)
	defer cancel()
	data, err := fresh(ctx)
	if err == nil {
		cacheMu.Lock()
		cached = &Cache{Data: data, Retrieved: time.Now()}
		cacheMu.Unlock()
		writeJSON(w, 200, data)
		return
	}
	log.Printf("gold request failed: %v", err)
	if c != nil {
		copy := cloneMap(c.Data)
		copy["cacheStatus"] = "STALE_FALLBACK"
		copy["status"] = "STALE"
		copy["serverNow"] = time.Now().UTC().Format(time.RFC3339Nano)
		writeJSON(w, 200, copy)
		return
	}
	status := 502
	if err.Error() == "MISSING_PROVIDER_KEY" {
		status = 503
	}
	writeJSON(w, status, map[string]any{"success": false, "code": err.Error(), "message": map[bool]string{true: "Provider API key is not configured on the backend.", false: "Gold provider request failed."}[err.Error() == "MISSING_PROVIDER_KEY"]})
}
func cloneMap(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
