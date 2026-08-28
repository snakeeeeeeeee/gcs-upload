package pool

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakePool 构造只有名字的假账号，仅测调度逻辑（默认标记为健康，老测试不需要显式探测）
func fakePool(n int) *Pool {
	p := &Pool{retry: 3, defaultBucket: "bkt",
		apiKeys: make(map[string]struct{}), sem: make(chan struct{}, 2)}
	for i := 0; i < n; i++ {
		acc := &Account{Name: string(rune('a' + i)), Bucket: "bkt"}
		acc.Enabled.Store(true)
		acc.markHealth(true)
		p.accounts = append(p.accounts, acc)
	}
	return p
}

func TestRoundRobin(t *testing.T) {
	p := fakePool(3)
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i, w := range want {
		if got := p.Next().Name; got != w {
			t.Fatalf("round %d: got %s want %s", i, got, w)
		}
	}
}

func TestRoundRobinSingle(t *testing.T) {
	p := fakePool(1)
	for i := 0; i < 5; i++ {
		if got := p.Next().Name; got != "a" {
			t.Fatalf("single account: got %s want a", got)
		}
	}
}

func TestAccountCount(t *testing.T) {
	p := fakePool(4)
	if p.AccountCount() != 4 {
		t.Fatalf("count: got %d want 4", p.AccountCount())
	}
	if p.MaxSize() != 0 {
		t.Fatalf("default maxsize: got %d want 0", p.MaxSize())
	}
}

func TestNextSkipsDisabled(t *testing.T) {
	p := fakePool(3)
	// 禁用中间的 b
	if err := p.SetEnabled("b", false); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for i := 0; i < 6; i++ {
		acc := p.Next()
		if acc == nil {
			t.Fatal("unexpected nil account")
		}
		seen[acc.Name]++
	}
	if seen["b"] != 0 {
		t.Fatalf("disabled account b was selected: %v", seen)
	}
	if seen["a"] != 3 || seen["c"] != 3 {
		t.Fatalf("expected a=3 c=3, got %v", seen)
	}
}

func TestNextAllDisabled(t *testing.T) {
	p := fakePool(2)
	for _, n := range []string{"a", "b"} {
		if err := p.SetEnabled(n, false); err != nil {
			t.Fatal(err)
		}
	}
	if acc := p.Next(); acc != nil {
		t.Fatalf("expected nil when all disabled, got %s", acc.Name)
	}
}

func TestAddRemoveAccount(t *testing.T) {
	p := fakePool(1)
	// 添加需要真实 key，这里只测不存在的 key 会报错且不影响现有池
	if _, err := p.AddAccount("x", "", []byte("not-a-key")); err == nil {
		t.Fatal("expected error for invalid key json")
	}
	if p.AccountCount() != 1 {
		t.Fatalf("pool count changed after failed add: %d", p.AccountCount())
	}
	// 移除不存在的号
	if err := p.RemoveAccount("zzz"); err == nil {
		t.Fatal("expected error removing nonexistent account")
	}
}

func TestIsRateLimit(t *testing.T) {
	if !isRateLimit(&googleapi.Error{Code: 429}) {
		t.Fatal("HTTP 429 should be rate limit")
	}
	if !isRateLimit(fmt.Errorf("wrapped: %w", &googleapi.Error{Code: 429})) {
		t.Fatal("wrapped 429 should be rate limit")
	}
	if isRateLimit(&googleapi.Error{Code: 500}) {
		t.Fatal("HTTP 500 is not rate limit")
	}
	if isRateLimit(errors.New("boom")) {
		t.Fatal("plain error is not rate limit")
	}
	if !isRateLimit(status.Error(codes.ResourceExhausted, "quota exceeded")) {
		t.Fatal("gRPC ResourceExhausted should be rate limit")
	}
	if isRateLimit(status.Error(codes.Unavailable, "down")) {
		t.Fatal("gRPC Unavailable is not rate limit")
	}
}

func TestLimiter(t *testing.T) {
	p := fakePool(1) // sem 容量 2
	ctx := context.Background()
	if err := p.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	// 第 3 个应排队超时
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := p.Acquire(cctx); err == nil {
		t.Fatal("expected timeout waiting for slot")
	}
	p.Release()
	// 释放后用新 context 应能获取
	cctx2, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	if err := p.Acquire(cctx2); err != nil {
		t.Fatalf("expected acquire after release: %v", err)
	}
}

// genTestKeyPEM 生成测试用 RSA 私钥 PEM
func genTestKeyPEM(t *testing.T) []byte {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
}

func TestSignURL(t *testing.T) {
	acc := &Account{saEmail: "svc@test.iam.gserviceaccount.com", saKey: genTestKeyPEM(t)}
	u, err := acc.signURL("my-bucket", "dir/obj.txt", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("signURL: %v", err)
	}
	for _, want := range []string{"X-Goog-Algorithm=", "X-Goog-Credential=", "X-Goog-Date=", "X-Goog-Signature=", "my-bucket/dir/obj.txt"} {
		if !strings.Contains(u, want) {
			t.Fatalf("signed url missing %q: %s", want, u)
		}
	}
}

// acc markHealth 直接给 account 标记健康状态（绕开真实 Client 探活）
func (a *Account) markHealth(ok bool) {
	a.healthAt.Store(time.Now().Unix())
	a.healthOK.Store(ok)
	if ok {
		a.healthErr.Store("")
	} else {
		a.healthErr.Store("test")
	}
}

func TestNextSkipsUntested(t *testing.T) {
	// 自行构造不调 markHealth 的池（fakePool 默认健康）
	p := &Pool{retry: 3, defaultBucket: "bkt",
		apiKeys: make(map[string]struct{}), sem: make(chan struct{}, 2)}
	for _, n := range []string{"a", "b"} {
		acc := &Account{Name: n, Bucket: "bkt"}
		acc.Enabled.Store(true)
		p.accounts = append(p.accounts, acc)
	}
	// 两号都未测（health_at=0）→ Next 返回 nil（绝不 fallback 到未测号）
	if acc := p.Next(); acc != nil {
		t.Fatalf("expected nil when all untested, got %s", acc.Name)
	}
	// 标 a 健康，b 仍未测 → Next 只回 a（轮询跳过 b）
	p.accounts[0].markHealth(true)
	for i := 0; i < 4; i++ {
		if got := p.Next(); got == nil || got.Name != "a" {
			t.Fatalf("iter %d: want a, got %v", i, got)
		}
	}
	// 两号都健康 → 轮询正常
	p.accounts[1].markHealth(true)
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		if a := p.Next(); a != nil {
			seen[a.Name]++
		}
	}
	if seen["a"] != 2 || seen["b"] != 2 {
		t.Fatalf("round robin after both healthy: %v", seen)
	}
	// 一号 healthy 一号坏 → 只返回 healthy 号
	p.accounts[1].markHealth(false)
	for i := 0; i < 4; i++ {
		if got := p.Next(); got == nil || got.Name != "a" {
			t.Fatalf("only-healthy iter %d: got %v", i, got)
		}
	}
}

func TestAPIKeyLifecycle(t *testing.T) {
	p := fakePool(1)
	p.AddAPIKey("gcs-abc")
	p.AddAPIKey("gcs-xyz")
	if !p.ValidateAPIKey("gcs-abc") {
		t.Fatal("expected gcs-abc valid")
	}
	if !p.ValidateAPIKey("gcs-xyz") {
		t.Fatal("expected gcs-xyz valid")
	}
	if p.ValidateAPIKey("gcs-bad") {
		t.Fatal("expected gcs-bad invalid")
	}
	if p.ValidateAPIKey("") {
		t.Fatal("empty should be invalid")
	}
	p.RemoveAPIKey("gcs-abc")
	if p.ValidateAPIKey("gcs-abc") {
		t.Fatal("expected invalid after remove")
	}
	got := p.APIKeySnapshot()
	if len(got) != 1 || got[0] != "gcs-xyz" {
		t.Fatalf("snapshot: %v", got)
	}
}

func TestConfigValidation(t *testing.T) {
	// 空号池允许启动（账号通过管理页/keys 扫描在线添加）
	p, err := New(t.Context(), Config{AdminToken: "t"})
	if err != nil {
		t.Fatalf("expected empty pool to start, got: %v", err)
	}
	p.Close()
	// 有账号但缺 admin_password/token
	if _, err := New(t.Context(), Config{DefaultBucket: "b", Accounts: []AccountConfig{{Name: "x", KeyFile: "nope.json"}}}); err == nil {
		t.Fatal("expected error for missing admin_token")
	}
	// key 文件不存在
	if _, err := New(t.Context(), Config{AdminToken: "t", DefaultBucket: "b", Accounts: []AccountConfig{{Name: "x", KeyFile: "nope.json"}}}); err == nil {
		t.Fatal("expected error for missing key file")
	}
}

// ---------- 桶解析（新增：自动创建） ----------

func TestResolveBucketPlan(t *testing.T) {
	cases := []struct {
		name      string
		explicit  string
		def       string
		listed    []string
		wantBkt   string
		wantAuto  bool
	}{
		{"显式配置优先", "explicit-bkt", "def-bkt", []string{"z-bkt"}, "explicit-bkt", false},
		{"显式空则用 default", "", "def-bkt", []string{"z-bkt"}, "def-bkt", false},
		{"无显式无 default 取项目第一个(按名排序)", "", "", []string{"z-bkt", "a-bkt"}, "a-bkt", false},
		{"无任何桶则自动创建", "", "", nil, "", true},
		{"显式空列表也自动创建", "", "", []string{}, "", true},
	}
	for _, c := range cases {
		bkt, auto := resolveBucketPlan(c.explicit, c.def, c.listed)
		if bkt != c.wantBkt || auto != c.wantAuto {
			t.Fatalf("%s: got (%q, %v) want (%q, %v)", c.name, bkt, auto, c.wantBkt, c.wantAuto)
		}
	}
}

func TestGenBucketName(t *testing.T) {
	names := map[string]bool{}
	for i := 0; i < 100; i++ {
		n := genBucketName("project-123")
		if len(n) < 3 || len(n) > 63 {
			t.Fatalf("bucket name length out of range: %q (%d)", n, len(n))
		}
		for _, r := range n {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				t.Fatalf("bucket name has invalid char %q in %q", r, n)
			}
		}
		if names[n] {
			t.Fatalf("duplicate bucket name: %q", n)
		}
		names[n] = true
	}
}

// TestAutoCreateBucketIntegration 真实创建桶的集成测试：
// 设置 GCS_PROBE_KEY=<key 文件路径> 且网络可达时执行（自动创建→验证→删除，无残留）。
func TestAutoCreateBucketIntegration(t *testing.T) {
	keyFile := os.Getenv("GCS_PROBE_KEY")
	if keyFile == "" {
		t.Skip("set GCS_PROBE_KEY=<sa key json> to run real create test")
	}
	cred, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	var kj struct{ ProjectID string `json:"project_id"` }
	if err := json.Unmarshal(cred, &kj); err != nil {
		t.Fatal(err)
	}
	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(keyFile))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	p := &Pool{bucketLocation: "US", ttlDays: atomic.Int32{}}
	p.ttlDays.Store(7)
	name, err := p.autoCreateBucket(client, kj.ProjectID, "itest")
	if err != nil {
		t.Fatalf("autoCreateBucket: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := client.Bucket(name).Delete(ctx); err != nil {
			t.Logf("cleanup delete failed (manual cleanup %s): %v", name, err)
		}
	}()
	t.Logf("auto-created %s for project %s", name, kj.ProjectID)

	// 验证创建者能写对象（owner 语义）
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w := client.Bucket(name).Object("probe.txt").NewWriter(ctx)
	if _, err := w.Write([]byte("x")); err != nil {
		t.Fatalf("write to auto-created bucket: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	// 验证 TTL 生命周期规则已带
	attrs, err := client.Bucket(name).Attrs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs.Lifecycle.Rules) != 1 || attrs.Lifecycle.Rules[0].Condition.AgeInDays != 7 {
		t.Fatalf("lifecycle rules: %+v, want one 7d rule", attrs.Lifecycle.Rules)
	}
	// 清理对象
	if err := client.Bucket(name).Object("probe.txt").Delete(ctx); err != nil {
		t.Logf("cleanup object failed: %v", err)
	}
}
