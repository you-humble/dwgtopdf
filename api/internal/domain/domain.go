package domain

import (
	"errors"
	"io"
	"time"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusDone       TaskStatus = "done"
	StatusFailed     TaskStatus = "failed"
	StatusExpired    TaskStatus = "expired"
)

type Task struct {
	ID string `json:"id"`

	Status TaskStatus `json:"status"`

	OriginalName  string `json:"original_name"`
	InputFilename string `json:"input_filename"`

	ResultFilename string `json:"result_filename"`

	// meta
	FileSize       int64     `json:"file_size"`
	FileHashSHA    string    `json:"file_hash_sha"`
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	Error          string    `json:"error"`
}

type CreateTaskParams struct {
	OriginalName   string
	InputFilename  string
	FileSize       int64
	FileHashSHA    string
	IdempotencyKey string

	TTL time.Duration
}

type ConvertResponse struct {
	ID string `json:"id"`
}

type StatusResponse struct {
	ID          string     `json:"id"`
	Status      TaskStatus `json:"status"`
	DownloadURL string     `json:"download_url,omitempty"`
	FileName    string     `json:"file_name,omitempty"`
	Error       string     `json:"error,omitempty"`
}

type DownloadResult struct {
	FileName string
	Size     int64
	Content  io.ReadCloser
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

var (
	ErrTaskNotFound = errors.New("task not found")
	ErrTaskFailed   = errors.New("task failed")
	ErrTaskExpired  = errors.New("task expired")
	ErrTaskNotReady = errors.New("task not ready")
)
