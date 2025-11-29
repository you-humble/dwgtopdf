package domain

import (
	"errors"
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

var (
	ErrTaskNotFound = errors.New("task not found")
	ErrTaskExpired  = errors.New("task expired")
)
