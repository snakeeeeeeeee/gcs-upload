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
	pool    *pool.Pool
	recs    *records.Store
	maxSize int64
}

// New 创建上传 handler
func New(p *pool.Pool, recs *records.Store) *UploadHandler {
	return &UploadHandler{pool: p, recs: recs, maxSize: p.MaxSize()}
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

	// 限制请求体大小（multipart 开销 + 文件本体）
	r.Body = http.MaxBytesReader(w, r.Body, h.maxSize+(16<<20))
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

	size, err := io.Copy(tmp, file)
	if err != nil {
		tmp.Close()
		writeErr(w, http.StatusBadRequest, "read upload body: "+err.Error())
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

	object := newObjectName(header.Filename)
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

// newObjectName 生成唯一对象路径：日期目录 + 时间戳 + 随机后缀 + 原扩展名
// 随机后缀避免同秒并发写同一 object（GCS 同 object 限 1 写/秒）
func newObjectName(filename string) string {
	ext := safeExt(filename)
	var b [6]byte
	rand.Read(b[:])
	now := time.Now()
	return fmt.Sprintf("%s/%s-%s%s",
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
