package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"gcs-pool/internal/admin"
	"gcs-pool/internal/handler"
	"gcs-pool/internal/pool"
	"gcs-pool/internal/records"
)

//go:embed web
var webFS embed.FS

func main() {
	cfgPath := flag.String("config", "config.json", "path to config json")
	flag.Parse()

	b, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	var cfg pool.Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}

	// key_file 相对路径统一按 config.json 所在目录解析（相对路径可移植）
	baseDir := filepath.Dir(*cfgPath)
	for i := range cfg.Accounts {
		if cfg.Accounts[i].KeyFile != "" && !filepath.IsAbs(cfg.Accounts[i].KeyFile) {
			cfg.Accounts[i].KeyFile = filepath.Join(baseDir, cfg.Accounts[i].KeyFile)
		}
	}

	ctx := context.Background()
	p, err := pool.New(ctx, cfg)
	if err != nil {
		log.Fatalf("init pool: %v", err)
	}
	defer p.Close()

	recs := records.New(10) // 上传记录仅保留最近 10 条（内存）
	up := handler.New(p, recs)
	adminPass := cfg.AdminPassword
	if adminPass == "" {
		adminPass = cfg.AdminToken // 兼容旧字段名
	}
	adm := admin.New(p, recs, adminPass, *cfgPath)

	// keys/ 目录自动扫描：发现新 key JSON 自动注册（默认 60s 一次）
	scanInterval := time.Duration(cfg.ScanInterval) * time.Second
	if scanInterval <= 0 {
		scanInterval = 60 * time.Second
	}
	go func() {
		scan := func() {
			res := adm.ScanKeys()
			added, failed := 0, 0
			for _, st := range res {
				if st == "ok" {
					added++
				} else {
					failed++
					slog.Warn("admin: keys scan skipped", "reason", st)
				}
			}
			if added > 0 || failed > 0 {
				slog.Info("admin: keys scan done", "added", added, "failed", failed)
			}
		}
		scan() // 启动立即扫一次
		ticker := time.NewTicker(scanInterval)
		defer ticker.Stop()
		for range ticker.C {
			scan()
		}
	}()

	webSub, _ := fs.Sub(webFS, "web")

	// 独立登录页（嵌入内容直接输出，避免 FileServer 文件名匹配问题）
	loginHTML, err := webFS.ReadFile("web/login.html")
	if err != nil {
		log.Fatalf("read login page: %v", err)
	}

	mux := http.NewServeMux()
	// 页面：/ 管理台，/login 独立登录页（公开）
	mux.Handle("GET /{$}", http.FileServer(http.FS(webSub)))
	mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(loginHTML)
	})
	// 上传与健康检查（公开）
	mux.HandleFunc("POST /upload", up.Upload)
	mux.HandleFunc("GET /healthz", up.Healthz)
	// 登录/登出（公开；内部校验 token）
	mux.HandleFunc("POST /admin/login", adm.Login)
	mux.HandleFunc("POST /admin/logout", adm.Logout)
	// 管理 API（会话 cookie / Bearer token 鉴权）
	mux.HandleFunc("GET /admin/pool", adm.Auth(adm.PoolInfo))
	mux.HandleFunc("POST /admin/pool/accounts", adm.Auth(adm.AddAccount))
	mux.HandleFunc("DELETE /admin/pool/accounts/{name}", adm.Auth(adm.RemoveAccount))
	mux.HandleFunc("POST /admin/pool/accounts/{name}/{action}", adm.Auth(adm.ToggleAccount))
	mux.HandleFunc("POST /admin/pool/accounts/{name}/probe", adm.Auth(adm.ProbeAccount))
	mux.HandleFunc("POST /admin/config", adm.Auth(adm.UpdateConfig))
	mux.HandleFunc("GET /admin/records", adm.Auth(adm.Records))
	mux.HandleFunc("GET /admin/api-keys", adm.Auth(adm.ListAPIKeys))
	mux.HandleFunc("POST /admin/api-keys", adm.Auth(adm.CreateAPIKey))
	mux.HandleFunc("DELETE /admin/api-keys/{key}", adm.Auth(adm.DeleteAPIKey))

	addr := cfg.Listen
	if addr == "" {
		addr = ":8080"
	}
	slog.Info("gcs-pool started",
		"addr", addr,
		"accounts", p.AccountCount(),
		"default_bucket", p.DefaultBucket(),
		"max_size", p.MaxSize(),
		"retry", p.Retry(),
		"max_concurrent", p.MaxConcurrent(),
		"request_timeout", p.RequestTimeout().String(),
	)
	// ReadHeaderTimeout 防慢连接攻击；大文件上传不能设全局 ReadTimeout，
	// 上传耗时由业务层 ctx（request_timeout）控制
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
