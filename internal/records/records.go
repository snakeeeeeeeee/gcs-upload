// Package records 内存环形缓冲存储上传记录（重启清空）
package records

import (
	"sync"
	"time"
)

// Record 一条上传记录
type Record struct {
	ID       int64  `json:"id"`
	Time     string `json:"time"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	Object   string `json:"object"`
	Bucket   string `json:"bucket"`
	Account  string `json:"account"`
	URL      string `json:"url,omitempty"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
	RemoteIP string `json:"remote_ip"`
}

// Store 环形缓冲
type Store struct {
	mu     sync.Mutex
	buf    []Record
	max    int
	nextID int64
}

// New 创建环形缓冲，max 为保留的最大记录数
func New(max int) *Store {
	if max <= 0 {
		max = 500
	}
	return &Store{max: max}
}

// Add 追加一条记录（自动裁剪最旧的）
func (s *Store) Add(r Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	r.ID = s.nextID
	if r.Time == "" {
		r.Time = time.Now().Format("2006-01-02 15:04:05")
	}
	s.buf = append(s.buf, r)
	if len(s.buf) > s.max {
		s.buf = s.buf[len(s.buf)-s.max:]
	}
}

// Recent 返回最近 n 条（新在前）
func (s *Store) Recent(n int) []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || n > len(s.buf) {
		n = len(s.buf)
	}
	out := make([]Record, n)
	for i := 0; i < n; i++ {
		out[i] = s.buf[len(s.buf)-1-i]
	}
	return out
}
