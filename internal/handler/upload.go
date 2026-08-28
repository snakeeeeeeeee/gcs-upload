// Package handler 提供 HTTP 上传接口
package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"gcs-pool/internal/pool"
	"gcs-pool/internal/records"
)

// UploadHandler 上传接口
type UploadHandler struct {
	pool *pool.Pool
	recs *records.Store
}

// New 创建上传 handler
func New(p *pool.Pool, recs *records.Store) *UploadHandler {
	return &UploadHandler{pool: p, recs: recs}
}

// Upload POST /upload，multipart 表单，字段名 file
func (h *UploadHandler) Upload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// 客户端鉴权：Authorization: Bearer <api_key>（管理后台走 admin_session，与此隔离）
	authKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if authKey == "" {
		writeErr(w, http.StatusUnauthorized, "Authorization: Bearer <api_key> required")
		return
	}
	if !h.pool.ValidateAPIKey(authKey) {
		writeErr(w, http.StatusUnauthorized, "invalid api key")
		return
	}

	// 全局并发限流：超出上限排队，排队超时（受请求超时约束）返回 503
	ctx, cancel := context.WithTimeout(r.Context(), h.pool.RequestTimeout())
	defer cancel()
	if err := h.pool.Acquire(ctx); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "server busy, retry later: "+err.Error())
		return
	}
	defer h.pool.Release()

	// 限制请求体大小（multipart 开销 + 文件本体；上限实时从池读取，后台改 max_size 立即生效）
	r.Body = http.MaxBytesReader(w, r.Body, h.pool.MaxSize()+(16<<20))
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid multipart form: "+err.Error())
		return
	}
	defer r.MultipartForm.RemoveAll()

	file, header, err := r.FormFile("file")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "missing 'file' field: "+err.Error())
		return
	}
	defer file.Close()

	// 落临时文件：一是避免大文件占内存，二是失败换号重试时需要可重放的流
	tmp, err := os.CreateTemp("", "gcs-upload-*")
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "create temp file: "+err.Error())
		return
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	// 精确单文件上限：流式截断读取 maxSize+1 字节，超出立即拒绝（无需先落盘整个文件）
	// 上限实时从池读取，后台改 max_size 立即生效
	maxSize := h.pool.MaxSize()
	size, err := io.Copy(tmp, io.LimitReader(file, maxSize+1))
	if err != nil {
		tmp.Close()
		writeErr(w, http.StatusBadRequest, "read upload body: "+err.Error())
		return
	}
	if size > maxSize {
		tmp.Close()
		writeErr(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("file too large: %d bytes, limit is %d MB", size, maxSize/1024/1024))
		return
	}
	if err := tmp.Close(); err != nil {
		writeErr(w, http.StatusInternalServerError, "close temp file: "+err.Error())
		return
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = mime.TypeByExtension(path.Ext(header.Filename))
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// 存储期限：统一取服务端配置的 TTL（后台统一设置，客户端不可改），object 名加 {n}d/ 前缀由生命周期规则到期删除
	ttlDays := h.pool.TTLDays()

	object := newObjectName(header.Filename, ttlDays)
	// 每次重试重新打开临时文件，从 0 开始读
	newReader := func() (io.ReadCloser, error) {
		return os.Open(tmpName)
	}

	slog.Info("upload start", "filename", header.Filename, "size", size,
		"object", object, "content_type", contentType)

	// 可选：指定号/指定 bucket（管理页诊断单个 key 用）
	prefAccount := r.Header.Get("X-GCS-Account")
	prefBucket := r.Header.Get("X-GCS-Bucket")

	res, err := h.pool.UploadWith(ctx, object, newReader, contentType, prefAccount, prefBucket)
	if err != nil {
		slog.Error("upload failed", "object", object, "err", err)
		h.recs.Add(records.Record{
			Filename: header.Filename,
			Size:     size,
			Object:   object,
			Bucket:   h.pool.DefaultBucket(),
			Success:  false,
			Error:    err.Error(),
			RemoteIP: clientIP(r),
		})
		writeErr(w, http.StatusBadGateway, "upload failed: "+err.Error())
		return
	}

	slog.Info("upload ok", "object", object, "used", res.Used, "url", res.URL)
	h.recs.Add(records.Record{
		Filename: header.Filename,
		Size:     res.Size,
		Object:   res.Object,
		Bucket:   res.Bucket,
		Account:  res.Used,
		URL:      res.URL,
		Success:  true,
		RemoteIP: clientIP(r),
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"uri":       res.URL,         // 签名 URL（客户直接用）
		"bucket":    res.Bucket,
		"object":    res.Object,
		"mimeType":  contentType,     // 上传时的 Content-Type
		"name":      header.Filename, // 原始文件名（display_name）
		"size":      res.Size,
	})
}

// clientIP 提取客户端 IP（去掉端口）
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// newObjectName 生成唯一对象路径：可选 {n}d/ TTL 前缀 + 日期目录 + 时间戳 + 随机后缀 + 原扩展名
// 随机后缀避免同秒并发写同一 object（GCS 同 object 限 1 写/秒）；ttlDays>0 时加前缀供生命周期规则匹配
func newObjectName(filename string, ttlDays int) string {
	ext := safeExt(filename)
	var b [6]byte
	rand.Read(b[:])
	now := time.Now()
	prefix := ""
	if ttlDays > 0 {
		prefix = fmt.Sprintf("%dd/", ttlDays)
	}
	return fmt.Sprintf("%s%s/%s-%s%s",
		prefix,
		now.Format("2006/01/02"),
		now.Format("150405"),
		hex.EncodeToString(b[:]),
		ext,
	)
}

// safeExt 提取安全的扩展名（只保留字母数字，最长 16），防止路径注入
func safeExt(filename string) string {
	ext := path.Ext(filename)
	if len(ext) > 17 { // 含点，最长 16 字符
		return ""
	}
	for _, c := range ext {
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '.' {
			return ""
		}
	}
	return ext
}

// Healthz GET /healthz 健康检查
func (h *UploadHandler) Healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"pool":        h.pool.AccountCount(),
		"bucket":      h.pool.DefaultBucket(),
		"max_size_mb": h.pool.MaxSizeMB(),
		"time":        time.Now().Format(time.RFC3339),
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
