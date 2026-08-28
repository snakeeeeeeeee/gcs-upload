package handler

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"gcs-pool/internal/pool"
	"gcs-pool/internal/records"
)

// TestUploadRejectsOversize 单文件上限精确生效：超过 max_size 的 multipart 上传被 413 拒绝
// （无需真实 GCS——校验发生在落盘/上传之前）
func TestUploadRejectsOversize(t *testing.T) {
	p, err := pool.New(context.Background(), pool.Config{
		AdminToken: "t",
		MaxSize:    5, // 5 MB
		APIKeys:    pool.APIKeyList{{Key: "k1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	h := New(p, records.New(10))

	// 构造 6MB multipart body
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("file", "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 6*1024*1024)
	fw.Write(chunk)
	w.Close()

	req := httptest.NewRequest(http.MethodPost, "/upload", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer k1")
	rec := httptest.NewRecorder()
	h.Upload(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize upload: got %d want 413, body=%s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("too large")) {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}
