// Package admin 提供管理 API（号池增删/启停、配置修改、记录查询）。
//
// 鉴权双通道：浏览器走 /admin/login 服务端会话（httpOnly cookie，24h），
// 脚本/curl 走 Authorization: Bearer <token>。
//
// 所有账号变更（增删/启停/默认 bucket）都会写回 config.json 并落盘 key 文件，
// 重启后保持。
package admin

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"gcs-pool/internal/pool"
	"gcs-pool/internal/records"
)

const (
	sessionCookie = "gcs_pool_session"
	sessionTTL    = 24 * time.Hour
	keyDirName    = "keys"
)

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// Handler 管理 API
type Handler struct {
	pool    *pool.Pool
	recs    *records.Store
	token   string
	cfgPath string
	baseDir string
	keysDir string

	mu       sync.Mutex // 保护 sessions 与 config 文件读-改-写
	sessions map[string]time.Time
}

// New 创建管理 handler。cfgPath 为 config.json 路径（写回用）。
func New(p *pool.Pool, recs *records.Store, token, cfgPath string) *Handler {
	baseDir := filepath.Dir(cfgPath)
	return &Handler{
		pool:     p,
		recs:     recs,
		token:    token,
		cfgPath:  cfgPath,
		baseDir:  baseDir,
		keysDir:  filepath.Join(baseDir, keyDirName),
		sessions: map[string]time.Time{},
	}
}

// Auth 鉴权中间件：cookie 会话优先，其次 Bearer token
func (h *Handler) Auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if sid, err := r.Cookie(sessionCookie); err == nil {
			h.mu.Lock()
			exp, ok := h.sessions[sid.Value]
			if ok && time.Now().Before(exp) {
				h.sessions[sid.Value] = time.Now().Add(sessionTTL) // 滑动刷新
				h.mu.Unlock()
				next(w, r)
				return
			}
			delete(h.sessions, sid.Value)
			h.mu.Unlock()
		}
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if tok == "" {
			tok = r.Header.Get("X-Admin-Token")
		}
		if h.token != "" && subtle.ConstantTimeCompare([]byte(tok), []byte(h.token)) == 1 {
			next(w, r)
			return
		}
		writeErr(w, http.StatusUnauthorized, "unauthorized")
	}
}

// Login POST /admin/login  body: {token}。成功下发 httpOnly cookie。
func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if h.token == "" || subtle.ConstantTimeCompare([]byte(req.Token), []byte(h.token)) != 1 {
		writeErr(w, http.StatusUnauthorized, "invalid token")
		return
	}
	sid := randomHex(32)
	h.mu.Lock()
	h.sessions[sid] = time.Now().Add(sessionTTL)
	h.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    sid,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(sessionTTL.Seconds()),
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// Logout POST /admin/logout 清除会话 cookie
func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		h.mu.Lock()
		delete(h.sessions, c.Value)
		h.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// poolInfo GET /admin/pool
func (h *Handler) PoolInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"default_bucket":  h.pool.DefaultBucket(),
		"max_size_mb":     h.pool.MaxSizeMB(),
		"retry":           h.pool.Retry(),
		"max_concurrent":  h.pool.MaxConcurrent(),
		"request_timeout": int(h.pool.RequestTimeout().Seconds()),
		"stats":           h.pool.Stats(),
		"accounts":        h.pool.Snapshot(),
	})
}

// addAccount POST /admin/pool/accounts  body: {name, bucket?, key_json}
func (h *Handler) AddAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Bucket  string `json:"bucket"`
		KeyJSON string `json:"key_json"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	st, err := h.addAccount(req.Name, req.Bucket, []byte(req.KeyJSON))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	slog.Info("admin: account added", "name", req.Name, "bucket", st.Bucket)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "account": st})
}

// addAccount 内部实现（管理页 API 与 keys 目录扫描共用）：
// 校验 → 落盘 key → 进内存池 → 写回 config.json（任一步失败回滚）
func (h *Handler) addAccount(name, bucket string, keyJSON []byte) (*pool.State, error) {
	if !nameRe.MatchString(name) {
		return nil, fmt.Errorf("name must be 1-64 chars of [a-zA-Z0-9_-]")
	}
	if len(keyJSON) == 0 {
		return nil, errors.New("key_json is required")
	}
	if _, err := validateKeyJSON(keyJSON); err != nil {
		return nil, fmt.Errorf("invalid service account key: %w", err)
	}

	// key 落盘（0600，仅属主可读）
	if err := os.MkdirAll(h.keysDir, 0o700); err != nil {
		return nil, fmt.Errorf("create keys dir: %w", err)
	}
	keyPath := filepath.Join(h.keysDir, name+".json")
	if err := os.WriteFile(keyPath, keyJSON, 0o600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}
	rollback := func() { os.Remove(keyPath) }

	// 进内存池（bucket 为空时走三级解析：default_bucket → 自动探测）
	st, err := h.pool.AddAccount(name, bucket, keyJSON)
	if err != nil {
		rollback()
		return nil, err
	}
	// 立即探活：新号加入后不等 60s tick 立即确认健康，否则健康检查过滤会把它挡在调度外
	h.pool.ProbeAccount(name)

	// 写回 config.json（config 里存相对路径 keys/{name}.json）
	h.mu.Lock()
	err = h.updateConfig(func(cfg *pool.Config) {
		cfg.Accounts = append(cfg.Accounts, pool.AccountConfig{
			Name:    name,
			KeyFile: filepath.Join(keyDirName, name+".json"),
			Bucket:  bucket,
		})
	})
	h.mu.Unlock()
	if err != nil {
		h.pool.RemoveAccount(name) // 内存回滚
		rollback()
		return nil, fmt.Errorf("persist config: %w", err)
	}
	return st, nil
}

// ScanKeys 扫描 keys/ 目录，自动注册未上线的 key JSON 文件。
// 返回 map：文件名(去 .json) → 结果（"ok" 或错误原因）。
// bucket 规则：文件对应的号没配 bucket → 用 default_bucket → 无则自动探测项目第一个桶。
func (h *Handler) ScanKeys() map[string]string {
	result := map[string]string{}
	files, err := os.ReadDir(h.keysDir)
	if err != nil {
		if !os.IsNotExist(err) {
			result["_scan_error"] = err.Error()
		}
		return result
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(f.Name(), ".json")
		if !nameRe.MatchString(name) {
			continue
		}
		if h.pool.HasAccount(name) {
			continue // 已注册，跳过
		}
		content, err := os.ReadFile(filepath.Join(h.keysDir, f.Name()))
		if err != nil {
			result[name] = "读取失败: " + err.Error()
			continue
		}
		if _, err := h.addAccount(name, "", content); err != nil {
			result[name] = err.Error()
			continue
		}
		result[name] = "ok"
		slog.Info("admin: auto-registered key from keys/", "name", name)
	}
	return result
}

// removeAccount DELETE /admin/pool/accounts/{name}
func (h *Handler) RemoveAccount(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := h.pool.RemoveAccount(name); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}

	h.mu.Lock()
	err := h.updateConfig(func(cfg *pool.Config) {
		out := cfg.Accounts[:0]
		for _, a := range cfg.Accounts {
			if a.Name != name {
				out = append(out, a)
			}
		}
		cfg.Accounts = out
	})
	h.mu.Unlock()
	if err != nil {
		slog.Error("admin: remove persisted failed", "name", name, "err", err)
		writeErr(w, http.StatusInternalServerError, "persist config: "+err.Error())
		return
	}

	// 清理落盘的 key 文件（仅 keys 目录下且文件名匹配，防误删）
	keyPath := filepath.Join(h.keysDir, name+".json")
	if _, err := os.Stat(keyPath); err == nil {
		if err := os.Remove(keyPath); err != nil {
			slog.Warn("admin: remove key file failed", "path", keyPath, "err", err)
		}
	}

	slog.Info("admin: account removed", "name", name)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "removed": name})
}

// toggleAccount POST /admin/pool/accounts/{name}/{action}  action: enable|disable
func (h *Handler) ToggleAccount(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	action := r.PathValue("action")
	if action != "enable" && action != "disable" {
		writeErr(w, http.StatusBadRequest, "action must be enable or disable")
		return
	}
	enabled := action == "enable"
	if err := h.pool.SetEnabled(name, enabled); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}

	h.mu.Lock()
	err := h.updateConfig(func(cfg *pool.Config) {
		for i := range cfg.Accounts {
			if cfg.Accounts[i].Name == name {
				e := enabled
				cfg.Accounts[i].Enabled = &e
				break
			}
		}
	})
	h.mu.Unlock()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "persist config: "+err.Error())
		return
	}

	slog.Info("admin: account set", "name", name, "enabled", enabled)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": name, "enabled": enabled})
}

// updateConfig POST /admin/config  body: {default_bucket?}
func (h *Handler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DefaultBucket string `json:"default_bucket"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.DefaultBucket != "" {
		h.pool.SetDefaultBucket(req.DefaultBucket)
		h.mu.Lock()
		err := h.updateConfig(func(cfg *pool.Config) { cfg.DefaultBucket = req.DefaultBucket })
		h.mu.Unlock()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "persist config: "+err.Error())
			return
		}
		slog.Info("admin: default_bucket changed", "bucket", req.DefaultBucket)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "default_bucket": h.pool.DefaultBucket()})
}

// records GET /admin/records?limit=10
func (h *Handler) Records(w http.ResponseWriter, r *http.Request) {
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := parseLimit(v); err == nil {
			limit = n
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": h.recs.Recent(limit)})
}

// ProbeAccount POST /admin/pool/accounts/{name}/probe  立即探活单个号
func (h *Handler) ProbeAccount(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ok := h.pool.ProbeAccount(name)
	if !ok {
		writeErr(w, http.StatusNotFound, "account not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": name})
}

// ListAPIKeys GET /admin/api-keys
func (h *Handler) ListAPIKeys(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"keys": h.pool.APIKeySnapshot()})
}

// CreateAPIKey POST /admin/api-keys  生成并返回明文（仅创建时一次性返回）
func (h *Handler) CreateAPIKey(w http.ResponseWriter, r *http.Request) {
	var b [16]byte
	rand.Read(b[:])
	key := "gcs-" + hex.EncodeToString(b[:])
	h.pool.AddAPIKey(key)

	h.mu.Lock()
	err := h.updateConfig(func(cfg *pool.Config) {
		cfg.APIKeys = append(cfg.APIKeys, key)
	})
	h.mu.Unlock()
	if err != nil {
		h.pool.RemoveAPIKey(key)
		writeErr(w, http.StatusInternalServerError, "persist config: "+err.Error())
		return
	}
	slog.Info("admin: api key created")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": key})
}

// DeleteAPIKey DELETE /admin/api-keys/{key}  key 走 URL path value，注意需做轻度解码
func (h *Handler) DeleteAPIKey(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("key")
	key, err := url.PathUnescape(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid key")
		return
	}
	h.pool.RemoveAPIKey(key)

	h.mu.Lock()
	err = h.updateConfig(func(cfg *pool.Config) {
		out := cfg.APIKeys[:0]
		for _, k := range cfg.APIKeys {
			if k != key {
				out = append(out, k)
			}
		}
		cfg.APIKeys = out
	})
	h.mu.Unlock()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "persist config: "+err.Error())
		return
	}
	slog.Info("admin: api key deleted")
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": key})
}

// updateConfig 读-改-写 config.json（调用方需持有 h.mu）
func (h *Handler) updateConfig(mutate func(*pool.Config)) error {
	cfg, err := h.readConfig()
	if err != nil {
		return err
	}
	mutate(cfg)
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	// 直接覆盖写：容器里 config.json 是单文件 bind mount，rename 会报 device or resource busy
	if err := os.WriteFile(h.cfgPath, b, 0o644); err != nil {
		return err
	}
	return nil
}

func (h *Handler) readConfig() (*pool.Config, error) {
	b, err := os.ReadFile(h.cfgPath)
	if err != nil {
		return nil, err
	}
	var cfg pool.Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// validateKeyJSON 校验是合法的 service account key JSON（导出给外部使用）
func validateKeyJSON(b []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("not valid JSON")
	}
	if m["type"] != "service_account" || m["client_email"] == nil || m["private_key"] == nil {
		return nil, fmt.Errorf("not a service account key (missing type/client_email/private_key)")
	}
	return m, nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func parseLimit(s string) (int, error) {
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, err
	}
	if n > 100 {
		n = 100
	}
	return n, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
