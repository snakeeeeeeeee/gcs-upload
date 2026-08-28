// Package pool 实现 GCS 号池：多 service account key 轮询上传。
//
// 核心设计：
//   - 每个 key 常驻一个 storage.Client（client 线程安全，可复用，不能按请求新建）
//   - 原子计数器做 round-robin 选号，跳过已禁用/熔断中的号
//   - 上传失败（429/5xx/网络错误）自动换下一个号重试，最多 retry 次
//   - 某号连续失败达到阈值自动熔断，冷却后自动恢复
//   - 支持运行时热插拔：添加/移除/启停号，无需重启服务
//   - 默认 bucket + 每号可选覆盖
package pool

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxSizeMB      = 1024               // 1GB（单位 MB）
	defaultRetry          = 3
	defaultCircuit        = 5                 // 连续失败次数阈值
	defaultCoolDown       = 60 * time.Second
	defaultMaxConcurrent  = 1024              // 全局并发上限
	defaultRequestTimeout = 30 * time.Minute  // 上传请求超时
	defaultRetry429Base   = 1 * time.Second   // 429 退避基础时长
	defaultRetry429Max    = 5                  // 429 最大退避次数
	defaultSignedURLTTL   = 7 * 24 * time.Hour // 签名 URL 默认 7 天（v4 官方上限）
	maxSignedURLTTL       = 7 * 24 * time.Hour // v4 签名 URL 过期时间不能超过 7 天
	defaultHealthInterval = 5 * time.Minute    // 健康检查间隔
	healthProbeTimeout    = 5 * time.Second    // 单次探活超时
)

// AccountConfig 单个号（service account key）的配置
type AccountConfig struct {
	Name    string `json:"name"`             // 号名，日志/监控用
	KeyFile string `json:"key_file"`         // SA key JSON 文件路径
	Bucket  string `json:"bucket,omitempty"` // 可选：覆盖默认 bucket
	Enabled *bool  `json:"enabled,omitempty"` // 可选：初始禁用（nil=启用）
}

// Config 服务配置
type Config struct {
	Listen          string          `json:"listen"`                // 监听地址，默认 :8080
	DefaultBucket   string          `json:"default_bucket"`        // 默认 bucket（可选，未配的号自动探测项目第一个桶）
	MaxSize         int64           `json:"max_size"`              // 单文件上限（MB），默认 1024（1GB）
	Retry           int             `json:"retry"`                 // 普通失败换号重试次数，默认 3
	AdminPassword   string          `json:"admin_password"`        // 管理登录密码（必填）
	AdminToken      string          `json:"admin_token,omitempty"` // 兼容旧字段名
	MaxConcurrent   int             `json:"max_concurrent"`        // 全局并发上传上限，默认 1024
	RequestTimeout  int             `json:"request_timeout"`       // 上传请求超时（秒），默认 1800（30 分钟）
	Retry429Base    int             `json:"retry_429_base"`        // 429 退避基础秒数（指数增长），默认 1
	Retry429Max     int             `json:"retry_429_max"`         // 429 最大退避次数，默认 5
	SignedURLTTL    int             `json:"signed_url_ttl"`        // 返回签名 URL 有效期（秒），默认 2592000（30 天）；<=0 返回原生地址
	TTLDays         int             `json:"ttl_days"`              // 存储期限（天），默认 7；全部文件到期自动删除，客户不可改；<=0 永久存储
	HealthInterval  int             `json:"health_check_interval"` // 健康检查间隔（秒），默认 300（5 分钟）；0 关闭
	ScanInterval    int             `json:"keys_scan_interval"`    // keys/ 目录自动扫描间隔（秒），默认 60；0 关闭
	BucketLocation  string          `json:"bucket_location"`       // 自动创建桶的区域（默认 "US"）
	APIKeys         APIKeyList       `json:"api_keys"`              // 客户端 API Key 列表（Authorization: Bearer <key>）
	Accounts        []AccountConfig  `json:"accounts"`               // 号池
}

// APIKey 客户端 API Key 及其备注名（name 可空）
type APIKey struct {
	Key  string `json:"key"`
	Name string `json:"name,omitempty"`
}

// APIKeyList []*APIKey 列表类型，提供自定义 JSON 解码（兼容旧 []string 配置）
type APIKeyList []APIKey

// UnmarshalJSON 优先解析新格式 []APIKey；旧 config 中是 []string 时，逐项当作 {Key: s} 处理
func (l *APIKeyList) UnmarshalJSON(b []byte) error {
	var ll []APIKey
	if err := json.Unmarshal(b, &ll); err == nil {
		*l = ll
		return nil
	}
	var ss []string
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}
	for _, s := range ss {
		*l = append(*l, APIKey{Key: s})
	}
	return nil
}

// Account 运行时的"号"
type Account struct {
	Name       string
	Client     *storage.Client
	Bucket     string
	BucketAuto bool // 桶是程序自动创建的
	KeyFile    string
	Enabled    atomic.Bool

	saEmail string // 签名 URL 用：service account email
	saKey   []byte // 签名 URL 用：private key PEM

	uploads   atomic.Int64
	failures  atomic.Int64
	lastErr   atomic.Value // string
	lastTime  atomic.Int64 // unix 秒，最近一次上传尝试
	circuit   atomic.Bool  // 熔断中
	coolTill  atomic.Int64 // unix 秒，熔断冷却到何时
	healthOK  atomic.Bool  // 最近一次探活结果
	healthErr atomic.Value // string，探活错误
	healthAt  atomic.Int64 // unix 秒，最近一次探活时间
	thresh    int          // 熔断阈值（连续失败次数）
	coolDown  time.Duration // 熔断冷却时长
}

// State 管理页展示用的号状态
type State struct {
	Name       string `json:"name"`
	KeyFile    string `json:"key_file"`
	Bucket     string `json:"bucket"`
	BucketAuto bool   `json:"bucket_auto,omitempty"` // 桶是程序自动创建的
	Enabled    bool   `json:"enabled"`
	Circuit   bool   `json:"circuit"`
	CoolTill  int64  `json:"cool_till"`
	Uploads   int64  `json:"uploads"`
	Failures  int64  `json:"failures"`
	LastError string `json:"last_error,omitempty"`
	LastTime  int64  `json:"last_time"`
	Health    string `json:"health"`             // ok / fail / unknown
	HealthAt  int64  `json:"health_at,omitempty"` // 最近探活时间
	HealthErr string `json:"health_error,omitempty"`
}

// Stats 汇总统计
type Stats struct {
	Accounts     int    `json:"accounts"`
	Enabled      int    `json:"enabled"`
	CircuitOpen  int    `json:"circuit_open"`
	HealthOK     int    `json:"health_ok"`
	HealthFail   int    `json:"health_fail"`
	TotalUploads int64  `json:"total_uploads"`
	TotalFail    int64  `json:"total_fail"`
	SuccessRate  string `json:"success_rate"`
	InFlight     int    `json:"in_flight"`   // 当前正在处理的请求数（占用并发名额）
	MaxConcurrent int    `json:"max_concurrent"` // 全局并发上限（热更新值）
	RPM          int    `json:"rpm"`        // 最近 60 秒完成的上传数
	MemMB        int    `json:"mem_mb"`     // 进程堆分配 MB
}

// Pool GCS 号池
type Pool struct {
	mu             sync.RWMutex
	accounts       []*Account
	defaultBucket  string
	bucketLocation string
	maxSize        atomic.Int64 // 单文件上限（字节），可热更新
	retry          atomic.Int32 // 换号重试次数，可热更新
	circuitThresh  int
	coolDown       time.Duration
	maxConcurrent  atomic.Int32 // 全局并发上限（可热更新，由 sem 兜底）
	requestTimeout atomic.Int64 // 上传请求超时（纳秒），可热更新
	retry429Base   time.Duration
	retry429Max    int
	signedTTL      time.Duration
	ttlDays        atomic.Int32
	healthInterval time.Duration
	healthStop     chan struct{}
	healthDone     chan struct{}
	apiKeysMu      sync.RWMutex
	apiKeys        map[string]string // key → name（空字符串表示无备注）
	sem            *dynSem      // 动态并发信号量（容量可热更新）
	counter        atomic.Uint64
	// RPM ring：固定槽位，存最近完成时间（纳秒）；写指针原子递增取模，无需锁。
	// 1024 槽 ≈ 极限 1024/min 即 ~17 RPS；超过会被覆盖（覆盖前会先被读取统计，覆盖导致的是短时间略偏低，对监控够用）
	rpmBuf [1024]int64
	rpmIdx atomic.Uint64
}

// UploadResult 上传成功后的返回信息
type UploadResult struct {
	URL    string `json:"url"`    // 最终地址：签名 URL（signed_url_ttl>0）或原生地址
	Bucket string `json:"bucket"`
	Object string `json:"object"`
	Size   int64  `json:"size"`
	Used   string `json:"used"` // 实际使用哪个号
}

// NewReaderFunc 每次重试时重新打开文件流（流式源无法重放，必须可重建）
type NewReaderFunc func() (io.ReadCloser, error)

// New 加载所有 key，构建号池。任一 key 加载失败则整体失败并清理已建 client。
// 允许空号池启动（accounts 可为空数组）：可通过管理页添加号或 keys/ 目录自动扫描注册。
func New(ctx context.Context, cfg Config) (*Pool, error) {
	if len(cfg.Accounts) == 0 {
		slog.Warn("pool: no accounts configured, starting with empty pool; add accounts via admin UI or drop key files into keys/ dir")
	}
	// admin_password（兼容旧字段 admin_token）必填，否则管理 API 裸奔
	if cfg.AdminPassword == "" && cfg.AdminToken == "" {
		return nil, errors.New("pool: admin_password is required (management API would be exposed)")
	}
	if cfg.Retry <= 0 {
		cfg.Retry = defaultRetry
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = defaultMaxSizeMB
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = defaultMaxConcurrent
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = int(defaultRequestTimeout.Seconds())
	}
	if cfg.Retry429Base <= 0 {
		cfg.Retry429Base = int(defaultRetry429Base.Seconds())
	}
	if cfg.Retry429Max <= 0 {
		cfg.Retry429Max = defaultRetry429Max
	}
	if cfg.SignedURLTTL <= 0 {
		cfg.SignedURLTTL = int(defaultSignedURLTTL.Seconds())
	}
	if cfg.TTLDays == 0 {
		cfg.TTLDays = 7 // 默认存储 7 天
	}
	if time.Duration(cfg.SignedURLTTL)*time.Second > maxSignedURLTTL {
		slog.Warn("pool: signed_url_ttl exceeds 7-day v4 limit, clamping to 7 days",
			"configured_seconds", cfg.SignedURLTTL)
		cfg.SignedURLTTL = int(maxSignedURLTTL.Seconds())
	}
	if cfg.HealthInterval <= 0 {
		cfg.HealthInterval = int(defaultHealthInterval.Seconds())
	}
	if cfg.BucketLocation == "" {
		cfg.BucketLocation = "US"
	}

	p := &Pool{
		defaultBucket:  cfg.DefaultBucket,
		bucketLocation: cfg.BucketLocation,
		retry:          atomic.Int32{},
		circuitThresh:  defaultCircuit,
		coolDown:       defaultCoolDown,
		maxConcurrent:  atomic.Int32{},
		requestTimeout: atomic.Int64{},
		retry429Base:   time.Duration(cfg.Retry429Base) * time.Second,
		retry429Max:    cfg.Retry429Max,
		signedTTL:      time.Duration(cfg.SignedURLTTL) * time.Second,
		ttlDays:        atomic.Int32{},
		healthInterval: time.Duration(cfg.HealthInterval) * time.Second,
		healthStop:     make(chan struct{}),
		healthDone:     make(chan struct{}),
		apiKeys:        make(map[string]string),
		sem:            newDynSem(cfg.MaxConcurrent),
	}
	p.maxSize.Store(cfg.MaxSize * 1024 * 1024)   // MB → 字节
	p.retry.Store(int32(cfg.Retry))
	p.maxConcurrent.Store(int32(cfg.MaxConcurrent))
	p.requestTimeout.Store(int64(time.Duration(cfg.RequestTimeout) * time.Second))

	for _, k := range cfg.APIKeys {
		if k.Key != "" {
			p.apiKeys[k.Key] = k.Name
		}
	}
	p.ttlDays.Store(int32(cfg.TTLDays))

	for _, ac := range cfg.Accounts {
		if ac.Name == "" {
			ac.Name = ac.KeyFile
		}
		cred, err := os.ReadFile(ac.KeyFile)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("pool: read key file %s: %w", ac.KeyFile, err)
		}
		acc, err := p.buildAccount(ac.Name, ac.KeyFile, ac.Bucket, cred)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.accounts = append(p.accounts, acc)
		if ac.Enabled != nil && !*ac.Enabled {
			acc.Enabled.Store(false)
			slog.Info("pool: account loaded (disabled)", "name", acc.Name, "bucket", acc.Bucket)
		} else {
			slog.Info("pool: account loaded", "name", acc.Name, "bucket", acc.Bucket)
		}
	}

	if cfg.HealthInterval > 0 {
		go p.runHealthChecker()
	}
	return p, nil
}

func (p *Pool) buildAccount(name, keyFile, bucket string, cred []byte) (*Account, error) {
	if err := validateKeyJSON(cred); err != nil {
		return nil, fmt.Errorf("pool: invalid key for %s: %w", name, err)
	}
	client, err := storage.NewClient(context.Background(), option.WithCredentialsJSON(cred))
	if err != nil {
		return nil, fmt.Errorf("pool: create client for %s: %w", name, err)
	}
	// 提取签名 URL 所需的凭证（client_email + private_key）与探测所需 project_id
	var kj struct {
		ProjectID   string `json:"project_id"`
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
	}
	json.Unmarshal(cred, &kj)

	// bucket 解析：号级配置 → default_bucket → 项目第一个桶 → 自动创建
	// （自动创建后创建者即 owner，上传/配 TTL/删除都无需额外授权）
	bucketAuto := false
	if bucket == "" {
		bucket = p.defaultBucket
	}
	if bucket == "" {
		names, err := p.listProjectBuckets(client, kj.ProjectID)
		if err != nil {
			// list 失败（无权限/网络）→ 直接尝试自动创建（create 权限可能独立存在）
			created, cerr := p.autoCreateBucket(client, kj.ProjectID, name)
			if cerr != nil {
				client.Close()
				return nil, fmt.Errorf(
					"pool: resolve bucket for %s: list failed: %v; auto-create failed: %v (hint: grant the SA Storage Admin roles/storage.admin, or set bucket/default_bucket in config)",
					name, err, cerr)
			}
			bucket, bucketAuto = created, true
		} else {
			bkt, needCreate := resolveBucketPlan("", "", names)
			if needCreate {
				created, cerr := p.autoCreateBucket(client, kj.ProjectID, name)
				if cerr != nil {
					client.Close()
					return nil, fmt.Errorf(
						"pool: no bucket in project %s and auto-create failed: %v (hint: grant the SA storage.buckets.create / Storage Admin roles/storage.admin)",
						kj.ProjectID, cerr)
				}
				bucket, bucketAuto = created, true
			} else {
				bucket = bkt
				slog.Info("pool: auto-detected bucket", "name", name, "bucket", bucket)
			}
		}
	}

	acc := &Account{Name: name, Client: client, Bucket: bucket, BucketAuto: bucketAuto, KeyFile: keyFile,
		saEmail: kj.ClientEmail, saKey: []byte(kj.PrivateKey),
		thresh: p.circuitThresh, coolDown: p.coolDown}
	acc.Enabled.Store(true)
	return acc, nil
}

// resolveBucketPlan 桶选择优先级（纯逻辑，便于单测）：
// 显式配置 > default_bucket > 项目第一个桶（按名排序） > 自动创建
func resolveBucketPlan(explicit, def string, listed []string) (bucket string, autoCreate bool) {
	if explicit != "" {
		return explicit, false
	}
	if def != "" {
		return def, false
	}
	if len(listed) > 0 {
		sort.Strings(listed)
		return listed[0], false
	}
	return "", true
}

// listProjectBuckets 列出项目下所有 bucket 名（需要 storage.buckets.list 权限）
func (p *Pool) listProjectBuckets(client *storage.Client, projectID string) ([]string, error) {
	if projectID == "" {
		return nil, errors.New("key missing project_id")
	}
	// 探测必须有超时：网络不通/权限不足时快速失败，避免卡死扫描器与添加号接口
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	it := client.Buckets(ctx, projectID)
	var names []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		names = append(names, attrs.Name)
	}
	return names, nil
}

// autoCreateBucket 项目下没有任何桶（或 list 无权限）时自动创建一个，
// 并直接套用全局 TTL 生命周期规则（ttl_days > 0 时）。
// 桶名全局唯一，极小概率撞名（409）时自动换名重试，最多 3 次。
func (p *Pool) autoCreateBucket(client *storage.Client, projectID, accountName string) (string, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		name := genBucketName(projectID)
		attrs := &storage.BucketAttrs{Location: p.bucketLocation}
		if ttl := p.ttlDays.Load(); ttl > 0 {
			attrs.Lifecycle = storage.Lifecycle{Rules: []storage.LifecycleRule{
				{Action: storage.LifecycleAction{Type: storage.DeleteAction}, Condition: storage.LifecycleCondition{AgeInDays: int64(ttl)}},
			}}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := client.Bucket(name).Create(ctx, projectID, attrs)
		cancel()
		if err == nil {
			slog.Info("pool: auto-created bucket", "account", accountName, "bucket", name, "location", p.bucketLocation, "ttl_days", p.ttlDays.Load())
			return name, nil
		}
		lastErr = err
		if !isAlreadyExists(err) {
			return "", fmt.Errorf("create bucket %s: %w", name, err)
		}
		slog.Warn("pool: bucket name collision, retrying", "name", name, "attempt", attempt+1)
	}
	return "", fmt.Errorf("create bucket: %w", lastErr)
}

func isAlreadyExists(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == http.StatusConflict
}

// genBucketName 生成全局唯一的桶名：gcs-pool-<project hash>-<随机>
// GCS 桶名规则：3-63 字符，小写字母/数字/连字符，不能以 goog 开头，不能以 - 结尾
func genBucketName(projectID string) string {
	h := fnv.New32a()
	h.Write([]byte(projectID))
	var b [4]byte
	if _, err := rand.Read(b[:]); err == nil {
		return fmt.Sprintf("gcs-pool-%x-%x", h.Sum32(), b)
	}
	// 熵源异常时兜底：时间戳（并发撞名概率升高，但 autoCreateBucket 会换名重试）
	return fmt.Sprintf("gcs-pool-%x-%x", h.Sum32(), time.Now().UnixNano()&0xFFFFFF)
}

// signURL 生成 v4 签名 URL（私有 bucket 也能匿名访问）
func (a *Account) signURL(bucket, object string, expires time.Time) (string, error) {
	return storage.SignedURL(bucket, object, &storage.SignedURLOptions{
		GoogleAccessID: a.saEmail,
		PrivateKey:     a.saKey,
		Method:         "GET",
		Expires:        expires,
		Scheme:         storage.SigningSchemeV4,
	})
}

// validateKeyJSON 校验是合法的 service account key JSON
func validateKeyJSON(b []byte) error {
	var m struct {
		Type        string `json:"type"`
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
		ProjectID   string `json:"project_id"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return errors.New("not valid JSON")
	}
	if m.Type != "service_account" || m.ClientEmail == "" || m.PrivateKey == "" {
		return errors.New("not a service account key")
	}
	return nil
}

// Next 按 round-robin 取下一个可用号（跳过禁用/熔断/未测/探活失败）。全部不可用返回 nil。
// 注意：不会 fallback 到未测/失败号——未测不参与调度是硬规则，避免把不确定的号暴露给上传。
func (p *Pool) Next() *Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := len(p.accounts)
	if n == 0 {
		return nil
	}
	for i := 0; i < n; i++ {
		acc := p.accounts[(p.counter.Add(1)-1)%uint64(n)]
		if acc.available() {
			return acc
		}
	}
	return nil
}

// ProbeAccount 立即对单个号做一次探活（用于添加新号后立即确认 / 手动按钮触发）
func (p *Pool) ProbeAccount(name string) bool {
	p.mu.RLock()
	var target *Account
	for _, a := range p.accounts {
		if a.Name == name {
			target = a
			break
		}
	}
	p.mu.RUnlock()
	if target == nil {
		return false
	}
	p.checkAccountHealth(target)
	return target.healthOK.Load()
}

func (a *Account) available() bool {
	if !a.Enabled.Load() {
		return false
	}
	if a.circuit.Load() {
		if time.Now().Unix() >= a.coolTill.Load() {
			a.circuit.Store(false)
			a.failures.Store(0)
			return true
		}
		return false
	}
	// 未测（health_at=0）或探活失败的号不参与调度——避免首请求踩雷
	if at := a.healthAt.Load(); at == 0 {
		return false
	}
	if !a.healthOK.Load() {
		return false
	}
	return true
}

func (a *Account) recordSuccess(size int64) {
	a.failures.Store(0)
	a.circuit.Store(false)
	a.lastErr.Store("")
	a.lastTime.Store(time.Now().Unix())
	a.uploads.Add(1)
}

func (a *Account) recordFailure(err error) {
	a.failures.Add(1)
	a.lastErr.Store(err.Error())
	a.lastTime.Store(time.Now().Unix())
	if a.failures.Load() >= int64(a.thresh) {
		a.circuit.Store(true)
		a.coolTill.Store(time.Now().Add(a.coolDown).Unix())
	}
}

// Upload 选号上传：round-robin 选号，失败自动换下一个号，最多 retry 次。
func (p *Pool) Upload(ctx context.Context, object string, newReader NewReaderFunc, contentType string) (*UploadResult, error) {
	return p.UploadWith(ctx, object, newReader, contentType, "", "")
}

// UploadWith 支持指定号/指定 bucket 上传（管理页诊断单个 key 用）。
// preferredAccount 非空时严格使用该号：不可用或失败直接返回错误，不换号。
// preferredBucket 非空时临时覆盖该号的 bucket。
func (p *Pool) UploadWith(ctx context.Context, object string, newReader NewReaderFunc, contentType, preferredAccount, preferredBucket string) (*UploadResult, error) {
	if preferredAccount != "" {
		acc := p.find(preferredAccount)
		if acc == nil {
			return nil, fmt.Errorf("pool: account %q not found", preferredAccount)
		}
		if !acc.Enabled.Load() {
			return nil, fmt.Errorf("pool: account %q is disabled", preferredAccount)
		}
		if acc.circuit.Load() {
			return nil, fmt.Errorf("pool: account %q is cooling down", preferredAccount)
		}
		res, err := p.uploadOne(ctx, acc, object, newReader, contentType, preferredBucket)
		if err != nil {
			acc.recordFailure(err)
			return nil, err
		}
		acc.recordSuccess(res.Size)
		return res, nil
	}

	var lastErr error
	var tried []string
	attempts, backoffs := 0, 0
	for attempts < p.Retry() && backoffs <= p.retry429Max {
		acc := p.Next()
		if acc == nil {
			if p.AccountCount() == 0 {
				return nil, errors.New("pool: no accounts configured, add one via admin UI or keys/ scan first")
			}
			return nil, errors.New("pool: no available account (all disabled, unhealthy, or cooling down)")
		}
		attempts++
		tried = append(tried, acc.Name)
		res, err := p.uploadOne(ctx, acc, object, newReader, contentType, preferredBucket)
		if err == nil {
			acc.recordSuccess(res.Size)
			return res, nil
		}
		acc.recordFailure(err)
		lastErr = err

		if isRateLimit(err) {
			backoffs++
			if backoffs > p.retry429Max {
				slog.Warn("pool: giving up after rate-limit backoffs",
					"backoffs", backoffs, "object", object)
				break
			}
			wait := p.retry429Base * time.Duration(1<<(backoffs-1)) // 1s, 2s, 4s, 8s, 16s
			slog.Warn("pool: rate limited, backing off",
				"account", acc.Name, "n", backoffs, "wait", wait.String())
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, fmt.Errorf("pool: upload aborted during backoff: %w", ctx.Err())
			}
			continue // 退避后换号继续，不消耗普通重试次数
		}
		slog.Warn("pool: upload failed, switching account",
			"account", acc.Name, "attempt", attempts, "err", err)
	}
	return nil, fmt.Errorf("pool: upload failed after %d attempt(s) (tried %v): %w", len(tried), tried, lastErr)
}

// dynSem 动态容量信号量：容量可运行时调整（热更新 max_concurrent）
// 实现：atomic 计数（CAS 抢占，无超额）+ 唤醒 channel（容量变大/有释放时通知等待者）
type dynSem struct {
	cap   atomic.Int64
	inUse atomic.Int64
	wake  chan struct{} // cap=1，用作"有名事件"唤醒；满则丢
}

func newDynSem(initial int) *dynSem {
	s := &dynSem{wake: make(chan struct{}, 1)}
	if initial > 0 {
		s.cap.Store(int64(initial))
	}
	return s
}

func (s *dynSem) Acquire(ctx context.Context) error {
	for {
		cap := s.cap.Load()
		cur := s.inUse.Load()
		if cur < cap {
			if s.inUse.CompareAndSwap(cur, cur+1) {
				return nil
			}
			continue // CAS 失败（被并发抢走）重试
		}
		// 容量已满：等待释放或扩容
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.wake:
			// 被唤醒，重试
		}
	}
}

func (s *dynSem) Release() {
	s.inUse.Add(-1)
	select {
	case s.wake <- struct{}{}: // 唤醒一个等待者
	default: // 已有信号待消费，丢
	}
}

func (s *dynSem) SetCap(n int64) {
	s.cap.Store(n)
	select {
	case s.wake <- struct{}{}: // 容量变大可能解锁等待者
	default:
	}
}

// Acquire 获取一个并发上传名额（全局信号量，超限阻塞排队；ctx 取消则返回错误）
func (p *Pool) Acquire(ctx context.Context) error {
	return p.sem.Acquire(ctx)
}

// Release 释放并发上传名额，并记录一次"完成事件"用于 RPM 统计
func (p *Pool) Release() {
	p.sem.Release()
	// 无锁写 ring：写指针原子递增，写入纳秒时间戳（0 表示空槽）
	idx := p.rpmIdx.Add(1) & 1023
	p.rpmBuf[idx] = time.Now().UnixNano()
}

// RequestTimeout 上传请求超时时长
func (p *Pool) RequestTimeout() time.Duration {
	return time.Duration(p.requestTimeout.Load())
}

// MaxConcurrent 全局并发上限
func (p *Pool) MaxConcurrent() int {
	return int(p.maxConcurrent.Load())
}

// Retry 普通失败换号重试次数
func (p *Pool) Retry() int { return int(p.retry.Load()) }

// SetMaxConcurrent 热更新全局并发上限
func (p *Pool) SetMaxConcurrent(n int) {
	p.maxConcurrent.Store(int32(n))
	p.sem.SetCap(int64(n))
	slog.Info("pool: max_concurrent changed", "value", n)
}

// SetRequestTimeout 热更新上传请求超时（秒）
func (p *Pool) SetRequestTimeout(seconds int) {
	d := time.Duration(seconds) * time.Second
	p.requestTimeout.Store(int64(d))
	slog.Info("pool: request_timeout changed", "seconds", seconds)
}

// SetRetry 热更新换号重试次数
func (p *Pool) SetRetry(n int) {
	p.retry.Store(int32(n))
	slog.Info("pool: retry changed", "value", n)
}

// isRateLimit 判断是否为 GCS 限流错误（HTTP 429 / gRPC ResourceExhausted）
func isRateLimit(err error) bool {
	var ge *googleapi.Error
	if errors.As(err, &ge) && ge.Code == 429 {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.ResourceExhausted {
		return true
	}
	return false
}

// runHealthChecker 定期探活所有号（启动即探一轮，之后按间隔）
func (p *Pool) runHealthChecker() {
	defer close(p.healthDone)
	ticker := time.NewTicker(p.healthInterval)
	defer ticker.Stop()
	p.checkAllHealth() // 启动立即探一轮
	for {
		select {
		case <-p.healthStop:
			return
		case <-ticker.C:
			p.checkAllHealth()
		}
	}
}

// checkAllHealth 对池中每个号做一次探活
func (p *Pool) checkAllHealth() {
	p.mu.RLock()
	accounts := make([]*Account, len(p.accounts))
	copy(accounts, p.accounts)
	p.mu.RUnlock()

	var wg sync.WaitGroup
	for _, acc := range accounts {
		if !acc.Enabled.Load() {
			continue
		}
		wg.Add(1)
		go func(a *Account) {
			defer wg.Done()
			p.checkAccountHealth(a)
		}(acc)
	}
	wg.Wait()
}

// checkAccountHealth 探活单个号：列对象取 1 条（验证连通 + 权限）。
// 空桶（iterator.Done）也算健康——list 请求本身成功执行了。
func (p *Pool) checkAccountHealth(acc *Account) {
	ctx, cancel := context.WithTimeout(context.Background(), healthProbeTimeout)
	defer cancel()

	it := acc.Client.Bucket(acc.Bucket).Objects(ctx, nil)
	_, err := it.Next()
	if err == nil || errors.Is(err, iterator.Done) {
		acc.healthOK.Store(true)
		acc.healthErr.Store("")
		acc.healthAt.Store(time.Now().Unix())
		slog.Debug("pool: health ok", "account", acc.Name, "bucket", acc.Bucket)
		return
	}
	acc.healthOK.Store(false)
	acc.healthErr.Store(err.Error())
	acc.healthAt.Store(time.Now().Unix())
	slog.Warn("pool: health check failed", "account", acc.Name, "bucket", acc.Bucket, "err", err)
}

func (p *Pool) find(name string) *Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, a := range p.accounts {
		if a.Name == name {
			return a
		}
	}
	return nil
}

// HasAccount 判断号是否已注册（按名称）
func (p *Pool) HasAccount(name string) bool {
	return p.find(name) != nil
}

func (p *Pool) uploadOne(ctx context.Context, acc *Account, object string, newReader NewReaderFunc, contentType, bucketOverride string) (*UploadResult, error) {
	r, err := newReader()
	if err != nil {
		return nil, fmt.Errorf("pool: open source: %w", err)
	}
	defer r.Close()

	bucket := acc.Bucket
	if bucketOverride != "" {
		bucket = bucketOverride
	}

	// NewWriter 在文件 >16MiB 时自动走可续传上传，断点自动重试
	w := acc.Client.Bucket(bucket).Object(object).NewWriter(ctx)
	w.ContentType = contentType
	w.ChunkTransferTimeout = 2 * time.Minute

	n, err := io.Copy(w, r)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("pool: write with %s: %w", acc.Name, err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("pool: finalize with %s: %w", acc.Name, err)
	}

	res := &UploadResult{
		URL:    "https://storage.googleapis.com/" + bucket + "/" + url.PathEscape(object),
		Bucket: bucket,
		Object: object,
		Size:   n,
		Used:   acc.Name,
	}
	// 配置了 signed_url_ttl（默认 30 天）则返回签名 URL，私有 bucket 也能匿名访问
	if p.signedTTL > 0 {
		signed, err := acc.signURL(bucket, object, time.Now().Add(p.signedTTL))
		if err != nil {
			return nil, fmt.Errorf("pool: sign url with %s: %w", acc.Name, err)
		}
		res.URL = signed
	}
	return res, nil
}

// AddAccount 热加载一个号（key JSON 内容），失败不影响现有池。
func (p *Pool) AddAccount(name, bucket string, keyJSON []byte) (*State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, a := range p.accounts {
		if a.Name == name {
			return nil, fmt.Errorf("pool: account %q already exists", name)
		}
	}
	acc, err := p.buildAccount(name, "(runtime)", bucket, keyJSON)
	if err != nil {
		return nil, err
	}
	p.accounts = append(p.accounts, acc)
	slog.Info("pool: account added", "name", name, "bucket", acc.Bucket)
	st := acc.State()
	return &st, nil
}

// RemoveAccount 移除并关闭一个号
func (p *Pool) RemoveAccount(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, a := range p.accounts {
		if a.Name == name {
			a.Client.Close()
			p.accounts = append(p.accounts[:i], p.accounts[i+1:]...)
			slog.Info("pool: account removed", "name", name)
			return nil
		}
	}
	return fmt.Errorf("pool: account %q not found", name)
}

// SetEnabled 启用/禁用一个号
func (p *Pool) SetEnabled(name string, enabled bool) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, a := range p.accounts {
		if a.Name == name {
			a.Enabled.Store(enabled)
			if !enabled {
				a.circuit.Store(false)
			}
			slog.Info("pool: account set", "name", name, "enabled", enabled)
			return nil
		}
	}
	return fmt.Errorf("pool: account %q not found", name)
}

// ConfigureLifecycle 对指定 bucket 配置前缀生命周期规则（{n}d/ 前缀对象 n 天后自动删除）。
// 用池内每个号的 client 依次尝试（读现有规则+合并新增，不覆盖已有规则）。
// 返回 map：号名 → "ok" 或错误原因（权限不足的号会记录 403）。
func (p *Pool) ConfigureLifecycle(bucket string, ttlDays []int) map[string]string {
	result := map[string]string{}
	p.mu.RLock()
	accounts := make([]*Account, len(p.accounts))
	copy(accounts, p.accounts)
	p.mu.RUnlock()

	for _, acc := range accounts {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

		attrs, err := acc.Client.Bucket(bucket).Attrs(ctx)
		if err != nil {
			result[acc.Name] = "读 bucket 失败: " + err.Error()
			cancel()
			continue
		}
		rules := attrs.Lifecycle.Rules
		for _, d := range ttlDays {
			if d < 1 {
				continue
			}
			prefix := fmt.Sprintf("%dd/", d)
			if hasLifecycleRule(rules, prefix) {
				continue
			}
			rules = append(rules, storage.LifecycleRule{
				Action: storage.LifecycleAction{Type: storage.DeleteAction},
				Condition: storage.LifecycleCondition{
					AgeInDays:     int64(d),
					MatchesPrefix: []string{prefix},
				},
			})
		}
		_, err = acc.Client.Bucket(bucket).Update(ctx, storage.BucketAttrsToUpdate{
			Lifecycle: &storage.Lifecycle{Rules: rules},
		})
		cancel()
		if err != nil {
			result[acc.Name] = "设置失败: " + err.Error()
		} else {
			result[acc.Name] = "ok"
		}
	}
	return result
}

// ConfigureLifecycleAll 对号池内所有去重 bucket 配置前缀生命周期规则
// 返回 map[bucket]map[account]result（按桶+号分组，方便前端展示）
func (p *Pool) ConfigureLifecycleAll(ttlDays []int) map[string]map[string]string {
	buckets := map[string]struct{}{}
	p.mu.RLock()
	for _, a := range p.accounts {
		if a.Bucket != "" {
			buckets[a.Bucket] = struct{}{}
		}
	}
	p.mu.RUnlock()

	results := map[string]map[string]string{}
	for b := range buckets {
		results[b] = p.ConfigureLifecycle(b, ttlDays)
	}
	return results
}

// hasLifecycleRule 判断规则列表里是否已有匹配该前缀的删除规则
func hasLifecycleRule(rules []storage.LifecycleRule, prefix string) bool {
	for _, r := range rules {
		if r.Action.Type == storage.DeleteAction {
			for _, m := range r.Condition.MatchesPrefix {
				if m == prefix {
					return true
				}
			}
		}
	}
	return false
}

// SetDefaultBucket 运行时修改默认 bucket（影响后续添加的号）
func (p *Pool) SetDefaultBucket(bucket string) {
	p.mu.Lock()
	p.defaultBucket = bucket
	p.mu.Unlock()
}

// ValidateAPIKey 校验客户端 API Key（常量时间比较防时序攻击）
func (p *Pool) ValidateAPIKey(key string) bool {
	if key == "" {
		return false
	}
	p.apiKeysMu.RLock()
	defer p.apiKeysMu.RUnlock()
	for k := range p.apiKeys {
		if subtle.ConstantTimeCompare([]byte(key), []byte(k)) == 1 {
			return true
		}
	}
	return false
}

// AddAPIKey 注册新的 API Key（name 可空）
func (p *Pool) AddAPIKey(key, name string) {
	if key == "" {
		return
	}
	p.apiKeysMu.Lock()
	p.apiKeys[key] = name
	p.apiKeysMu.Unlock()
}

// RemoveAPIKey 删除 API Key
func (p *Pool) RemoveAPIKey(key string) {
	p.apiKeysMu.Lock()
	delete(p.apiKeys, key)
	p.apiKeysMu.Unlock()
}

// SetAPIKeyName 更新 API Key 的备注名（key 不存在返回 false）
func (p *Pool) SetAPIKeyName(key, name string) bool {
	p.apiKeysMu.Lock()
	defer p.apiKeysMu.Unlock()
	if _, ok := p.apiKeys[key]; !ok {
		return false
	}
	p.apiKeys[key] = name
	return true
}

// APIKeySnapshot 返回当前所有 API Key 的副本（管理页展示用）
func (p *Pool) APIKeySnapshot() []APIKey {
	p.apiKeysMu.RLock()
	defer p.apiKeysMu.RUnlock()
	out := make([]APIKey, 0, len(p.apiKeys))
	for k, n := range p.apiKeys {
		out = append(out, APIKey{Key: k, Name: n})
	}
	return out
}

// Snapshot 返回所有号的状态（管理页用）
func (p *Pool) Snapshot() []State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]State, 0, len(p.accounts))
	for _, a := range p.accounts {
		out = append(out, a.State())
	}
	return out
}

// Stats 汇总统计
func (p *Pool) Stats() Stats {
	states := p.Snapshot()
	var s Stats
	s.Accounts = len(states)
	var ok int64
	for _, st := range states {
		if st.Enabled {
			s.Enabled++
		}
		if st.Circuit {
			s.CircuitOpen++
		}
		switch st.Health {
		case "ok":
			s.HealthOK++
		case "fail":
			s.HealthFail++
		}
		s.TotalUploads += st.Uploads
		s.TotalFail += st.Failures
		ok += st.Uploads - st.Failures
	}
	if s.TotalUploads > 0 {
		s.SuccessRate = fmt.Sprintf("%.1f%%", float64(ok)/float64(s.TotalUploads)*100)
	} else {
		s.SuccessRate = "-"
	}

	// 当前并发（直接读信号量 inUse，零成本）
	s.InFlight = int(p.sem.inUse.Load())
	s.MaxConcurrent = int(p.maxConcurrent.Load())

	// RPM：遍历 ring 统计 60s 内完成数
	cutoff := time.Now().Add(-60 * time.Second).UnixNano()
	rpm := 0
	for _, ts := range p.rpmBuf {
		if ts > cutoff {
			rpm++
		}
	}
	s.RPM = rpm

	// 进程堆分配 MB（runtime.ReadMemStats 不加锁，但会 STW 极短，开销可忽略）
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s.MemMB = int(ms.Alloc / 1024 / 1024)

	return s
}

// State 导出单个号状态
func (a *Account) State() State {
	lastErr, _ := a.lastErr.Load().(string)
	healthErr, _ := a.healthErr.Load().(string)
	health := "unknown"
	if at := a.healthAt.Load(); at > 0 {
		if a.healthOK.Load() {
			health = "ok"
		} else {
			health = "fail"
		}
	}
	return State{
		Name:       a.Name,
		KeyFile:    a.KeyFile,
		Bucket:     a.Bucket,
		BucketAuto: a.BucketAuto,
		Enabled:    a.Enabled.Load(),
		Circuit:   a.circuit.Load(),
		CoolTill:  a.coolTill.Load(),
		Uploads:   a.uploads.Load(),
		Failures:  a.failures.Load(),
		LastError: lastErr,
		LastTime:  a.lastTime.Load(),
		Health:    health,
		HealthAt:  a.healthAt.Load(),
		HealthErr: healthErr,
	}
}

// AccountCount 号池规模
func (p *Pool) AccountCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.accounts)
}

// DefaultBucket 默认 bucket
func (p *Pool) DefaultBucket() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.defaultBucket
}

// MaxSize 单文件上限（字节）
func (p *Pool) MaxSize() int64 { return p.maxSize.Load() }

// MaxSizeMB 单文件上限（MB，管理页展示用）
func (p *Pool) MaxSizeMB() int64 {
	return p.maxSize.Load() / 1024 / 1024
}

// SetMaxSizeMB 运行时修改单文件上限（MB，热生效，后续上传按新值校验）
func (p *Pool) SetMaxSizeMB(mb int64) {
	p.maxSize.Store(mb * 1024 * 1024)
	slog.Info("pool: max_size changed", "mb", mb)
}

// TTLDays 全局存储期限（天）；<=0 表示永久存储
func (p *Pool) TTLDays() int { return int(p.ttlDays.Load()) }

// SetTTLDays 运行时修改全局存储期限（热生效，后续上传按新值加前缀）
func (p *Pool) SetTTLDays(days int) {
	p.ttlDays.Store(int32(days))
	slog.Info("pool: ttl_days changed", "days", days)
}
// Close 关闭所有 client 并停止健康检查
func (p *Pool) Close() {
	select {
	case <-p.healthStop:
	default:
		close(p.healthStop)
		// 等待 checker 退出；若从未启动（如 New 失败路径），超时兜底防死锁
		select {
		case <-p.healthDone:
		case <-time.After(2 * time.Second):
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, a := range p.accounts {
		if a.Client != nil {
			a.Client.Close()
		}
	}
	p.accounts = nil
}
