package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"gcs-pool/internal/pool"
	"gcs-pool/internal/records"
)

// 格式合法但私钥无效的假 key（storage.NewClient 是 lazy 的，不会因凭证无效而失败）
const fakeKey = `{"type":"service_account","project_id":"t","private_key_id":"k","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEvQ==\n-----END PRIVATE KEY-----\n","client_email":"a@t.iam.gserviceaccount.com","client_id":"1","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token"}`

func newTestHandler(t *testing.T) (*Handler, *pool.Pool, string) {
	t.Helper()
	dir := t.TempDir()
	key1 := filepath.Join(dir, "k1.json")
	if err := os.WriteFile(key1, []byte(fakeKey), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config{
		AdminToken:    "test-token",
		DefaultBucket: "bkt",
		Accounts:      []pool.AccountConfig{{Name: "a", KeyFile: key1}},
	}
	b, _ := json.Marshal(cfg)
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := pool.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return New(p, records.New(10), "test-token", cfgPath), p, cfgPath
}

func doReq(h *Handler, method, path string, body []byte, cookie *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rec := httptest.NewRecorder()
	// 直接路由到对应 handler（mux 未参与，需手工注入 path value）
	switch {
	case method == "POST" && path == "/admin/login":
		h.Login(rec, req)
	case method == "POST" && path == "/admin/logout":
		h.Logout(rec, req)
	case method == "GET" && path == "/admin/pool":
		h.Auth(h.PoolInfo)(rec, req)
	case method == "POST" && path == "/admin/pool/accounts":
		h.Auth(h.AddAccount)(rec, req)
	case method == "DELETE" && path == "/admin/pool/accounts/x":
		req.SetPathValue("name", "x")
		h.Auth(h.RemoveAccount)(rec, req)
	case method == "POST" && path == "/admin/pool/accounts/a/disable":
		req.SetPathValue("name", "a")
		req.SetPathValue("action", "disable")
		h.Auth(h.ToggleAccount)(rec, req)
	case method == "POST" && path == "/admin/config":
		h.Auth(h.UpdateConfig)(rec, req)
	default:
		rec.Code = http.StatusNotFound
	}
	return rec
}

// normalizedCfg 模拟 main.go 的路径归一化：相对 key_file 按 config 目录解析
func normalizedCfg(t *testing.T, cfgPath string) pool.Config {
	t.Helper()
	cfg := readCfg(t, cfgPath)
	base := filepath.Dir(cfgPath)
	for i := range cfg.Accounts {
		if cfg.Accounts[i].KeyFile != "" && !filepath.IsAbs(cfg.Accounts[i].KeyFile) {
			cfg.Accounts[i].KeyFile = filepath.Join(base, cfg.Accounts[i].KeyFile)
		}
	}
	return cfg
}

func readCfg(t *testing.T, cfgPath string) pool.Config {
	t.Helper()
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg pool.Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestLoginAndAuth(t *testing.T) {
	h, _, _ := newTestHandler(t)

	// 错误 token
	rec := doReq(h, "POST", "/admin/login", []byte(`{"token":"wrong"}`), nil)
	if rec.Code != 401 {
		t.Fatalf("wrong token: got %d want 401", rec.Code)
	}
	// 正确 token → 拿 cookie
	rec = doReq(h, "POST", "/admin/login", []byte(`{"token":"test-token"}`), nil)
	if rec.Code != 200 {
		t.Fatalf("login: got %d want 200", rec.Code)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 || cookies[0].Name != sessionCookie {
		t.Fatal("expected session cookie")
	}
	c := cookies[0]

	// 带 cookie 访问管理 API
	rec = doReq(h, "GET", "/admin/pool", nil, c)
	if rec.Code != 200 {
		t.Fatalf("cookie auth: got %d want 200", rec.Code)
	}
	// 无凭证访问 → 401
	rec = doReq(h, "GET", "/admin/pool", nil, nil)
	if rec.Code != 401 {
		t.Fatalf("no auth: got %d want 401", rec.Code)
	}
	// Bearer token 访问
	req := httptest.NewRequest("GET", "/admin/pool", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec2 := httptest.NewRecorder()
	h.Auth(h.PoolInfo)(rec2, req)
	if rec2.Code != 200 {
		t.Fatalf("bearer auth: got %d want 200", rec2.Code)
	}
}

func TestAddAccountPersists(t *testing.T) {
	h, p, cfgPath := newTestHandler(t)
	body := []byte(`{"name":"x","bucket":"bkt2","key_json":` + string(fakeKeyJSON()) + `}`)
	rec := doReq(h, "POST", "/admin/pool/accounts", body, loginCookie(t, h))
	if rec.Code != 200 {
		t.Fatalf("add: got %d want 200, body=%s", rec.Code, rec.Body.String())
	}
	if p.AccountCount() != 2 {
		t.Fatalf("pool count: got %d want 2", p.AccountCount())
	}
	// config.json 更新
	cfg := readCfg(t, cfgPath)
	found := false
	for _, a := range cfg.Accounts {
		if a.Name == "x" {
			found = true
			if a.KeyFile != filepath.Join("keys", "x.json") {
				t.Fatalf("key_file: got %s want keys/x.json", a.KeyFile)
			}
			if a.Bucket != "bkt2" {
				t.Fatalf("bucket: got %s want bkt2", a.Bucket)
			}
		}
	}
	if !found {
		t.Fatal("account x not persisted in config.json")
	}
	// key 文件落盘
	if _, err := os.Stat(filepath.Join(filepath.Dir(cfgPath), "keys", "x.json")); err != nil {
		t.Fatalf("key file not on disk: %v", err)
	}
	// 重载验证：重启后新号仍在
	reloaded, err := pool.New(context.Background(), normalizedCfg(t, cfgPath))
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	if reloaded.AccountCount() != 2 {
		t.Fatalf("reloaded count: got %d want 2", reloaded.AccountCount())
	}
}

func TestRemoveAccountPersists(t *testing.T) {
	h, p, cfgPath := newTestHandler(t)
	// 先加
	if rec := doReq(h, "POST", "/admin/pool/accounts", []byte(`{"name":"x","key_json":`+string(fakeKeyJSON())+`}`), loginCookie(t, h)); rec.Code != 200 {
		t.Fatalf("add: %d", rec.Code)
	}
	// 再删
	if rec := doReq(h, "DELETE", "/admin/pool/accounts/x", nil, loginCookie(t, h)); rec.Code != 200 {
		t.Fatalf("remove: %d", rec.Code)
	}
	if p.AccountCount() != 1 {
		t.Fatalf("pool count: got %d want 1", p.AccountCount())
	}
	cfg := readCfg(t, cfgPath)
	for _, a := range cfg.Accounts {
		if a.Name == "x" {
			t.Fatal("account x still in config")
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(cfgPath), "keys", "x.json")); !os.IsNotExist(err) {
		t.Fatal("key file should be removed")
	}
}

func TestToggleAccountPersists(t *testing.T) {
	h, _, cfgPath := newTestHandler(t)
	if rec := doReq(h, "POST", "/admin/pool/accounts/a/disable", nil, loginCookie(t, h)); rec.Code != 200 {
		t.Fatalf("disable: %d, body=%s", rec.Code, rec.Body.String())
	}
	cfg := readCfg(t, cfgPath)
	disabled := false
	for _, a := range cfg.Accounts {
		if a.Name == "a" && a.Enabled != nil && !*a.Enabled {
			disabled = true
		}
	}
	if !disabled {
		t.Fatal("account a should be persisted as disabled")
	}
	// 重启后该号处于禁用状态
	reloaded, err := pool.New(context.Background(), normalizedCfg(t, cfgPath))
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	for _, st := range reloaded.Snapshot() {
		if st.Name == "a" && st.Enabled {
			t.Fatal("account a should be disabled after reload")
		}
	}
}

func TestUpdateDefaultBucketPersists(t *testing.T) {
	h, p, cfgPath := newTestHandler(t)
	if rec := doReq(h, "POST", "/admin/config", []byte(`{"default_bucket":"new-bkt"}`), loginCookie(t, h)); rec.Code != 200 {
		t.Fatalf("update: %d", rec.Code)
	}
	if p.DefaultBucket() != "new-bkt" {
		t.Fatalf("pool bucket: got %s want new-bkt", p.DefaultBucket())
	}
	if readCfg(t, cfgPath).DefaultBucket != "new-bkt" {
		t.Fatal("default_bucket not persisted")
	}
}

func TestAddAccountValidation(t *testing.T) {
	h, p, _ := newTestHandler(t)
	// 非法名称（路径注入）
	if rec := doReq(h, "POST", "/admin/pool/accounts", []byte(`{"name":"../../etc/x","key_json":`+string(fakeKeyJSON())+`}`), loginCookie(t, h)); rec.Code != 400 {
		t.Fatalf("bad name: got %d want 400", rec.Code)
	}
	// 非法 key
	if rec := doReq(h, "POST", "/admin/pool/accounts", []byte(`{"name":"y","key_json":"not-a-key"}`), loginCookie(t, h)); rec.Code != 400 {
		t.Fatalf("bad key: got %d want 400", rec.Code)
	}
	if p.AccountCount() != 1 {
		t.Fatal("pool should be unchanged after invalid adds")
	}
}

func TestScanKeysAutoRegister(t *testing.T) {
	h, p, cfgPath := newTestHandler(t)
	keysDir := filepath.Join(filepath.Dir(cfgPath), "keys")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// 两个合法 key + 一个非法文件
	if err := os.WriteFile(filepath.Join(keysDir, "scan-1.json"), []byte(fakeKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keysDir, "scan-2.json"), []byte(fakeKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keysDir, "bad.json"), []byte("not-a-key"), 0o600); err != nil {
		t.Fatal(err)
	}

	res := h.ScanKeys()
	if res["scan-1"] != "ok" {
		t.Fatalf("scan-1: got %q want ok", res["scan-1"])
	}
	if res["scan-2"] != "ok" {
		t.Fatalf("scan-2: got %q want ok", res["scan-2"])
	}
	if res["bad"] == "ok" {
		t.Fatal("bad key should not register")
	}
	if p.AccountCount() != 3 { // a + scan-1 + scan-2
		t.Fatalf("pool count: got %d want 3", p.AccountCount())
	}

	// 二次扫描：全部已注册，应无新结果
	res2 := h.ScanKeys()
	if _, ok := res2["scan-1"]; ok {
		t.Fatal("scan-1 should be skipped on second scan")
	}

	// 写回 config 验证
	cfg := readCfg(t, cfgPath)
	found := map[string]bool{}
	for _, a := range cfg.Accounts {
		found[a.Name] = true
	}
	if !found["scan-1"] || !found["scan-2"] {
		t.Fatalf("scanned accounts not persisted to config: %v", found)
	}
}

func loginCookie(t *testing.T, h *Handler) *http.Cookie {
	t.Helper()
	rec := doReq(h, "POST", "/admin/login", []byte(`{"token":"test-token"}`), nil)
	if rec.Code != 200 {
		t.Fatal("login failed")
	}
	cs := rec.Result().Cookies()
	if len(cs) == 0 {
		t.Fatal("no cookie")
	}
	return cs[0]
}

// fakeKeyJSON 返回带引号包裹的 key JSON 字符串（可直接拼进 JSON body）
func fakeKeyJSON() []byte {
	b, _ := json.Marshal(fakeKey)
	return b
}
