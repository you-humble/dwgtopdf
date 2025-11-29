package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/you-humble/dwgtopdf/api/internal/domain"

	"github.com/google/uuid"
)

type FileStore interface {
	Save(ctx context.Context, reader io.Reader, filename string, size int64) (int64, string, error)
	Open(ctx context.Context, filename string) (io.ReadCloser, int64, error)
	Delete(ctx context.Context, filename string) error
}

type TaskStore interface {
	CreateTask(p domain.CreateTaskParams) (string, error)
	Task(id string) (domain.Task, bool)
	UpdateStatus(id string, newStatus domain.TaskStatus, errReason string)
	ByIdempotencyKey(key string) (domain.Task, bool)
}

type TaskQueue interface {
	Enqueue(ctx context.Context, taskID string) error
}

type usecase struct {
	taskTTL   time.Duration
	taskStore TaskStore
	fileStore FileStore
	queue     TaskQueue
}

func New(
	taskTTL time.Duration,
	taskStore TaskStore,
	fileStore FileStore,
	queue TaskQueue,
) *usecase {
	return &usecase{
		taskTTL:   taskTTL,
		taskStore: taskStore,
		fileStore: fileStore,
		queue:     queue,
	}
}

func (uc *usecase) Convert(ctx context.Context, file io.Reader, filename, idempotencyKey string, size int64) (string, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	if ext != ".dwg" {
		return "", errors.New("supported only .dwg files")
	}

	if idempotencyKey != "" {
		if existingTask, ok := uc.taskStore.ByIdempotencyKey(idempotencyKey); ok {
			switch existingTask.Status {
			case domain.StatusFailed, domain.StatusExpired:
				return "", fmt.Errorf("task status: %s", existingTask.Status)
			default:
				return existingTask.ID, nil
			}
		}
	}

	fileID := uuid.NewString()
	inputFilename := fileID + ext
	writen, hash, err := uc.fileStore.Save(ctx, file, inputFilename, size)
	if err != nil {
		return "", fmt.Errorf("save file: %w", err)
	}

	taskID, err := uc.taskStore.CreateTask(
		domain.CreateTaskParams{
			OriginalName:   filename,
			InputFilename:  inputFilename,
			FileSize:       writen,
			FileHashSHA:    hash,
			IdempotencyKey: idempotencyKey,
			TTL:            uc.taskTTL,
		})
	if err != nil {
		_ = uc.fileStore.Delete(ctx, inputFilename)
		return "", fmt.Errorf("create task: %w", err)
	}

	if existingTask, ok := uc.taskStore.Task(taskID); ok {
		if existingTask.InputFilename != inputFilename {
			if err := uc.fileStore.Delete(ctx, inputFilename); err != nil {
				slog.Warn("delete duplicated file", slog.String("error", err.Error()))
			}
		}
	}

	slog.Debug("Enqueue task", slog.String("task_id", taskID))
	if err := uc.queue.Enqueue(ctx, taskID); err != nil {
		slog.Error("Enqueue failed",
			slog.String("task_id", taskID),
			slog.String("error", err.Error()),
		)
		uc.taskStore.UpdateStatus(taskID, domain.StatusFailed, err.Error())
		return "", fmt.Errorf("enqueue: %w", err)
	}

	return taskID, nil
}

func (uc *usecase) GetStatus(ctx context.Context, taskID string) (domain.StatusResponse, error) {
	task, ok := uc.taskStore.Task(taskID)
	if !ok {
		return domain.StatusResponse{}, domain.ErrTaskNotFound
	}

	resp := domain.StatusResponse{
		ID:     task.ID,
		Status: task.Status,
	}

	switch task.Status {
	case domain.StatusDone:
		resp.DownloadURL = fmt.Sprintf("/download/%s", task.ID)
		resp.FileName = task.ResultFilename
	case domain.StatusFailed, domain.StatusExpired:
		resp.Error = task.Error
	}

	return resp, nil
}

func (uc *usecase) GetResultFile(ctx context.Context, taskID string) (domain.DownloadResult, error) {
	task, ok := uc.taskStore.Task(taskID)
	if !ok {
		return domain.DownloadResult{}, domain.ErrTaskNotFound
	}

	switch task.Status {
	case domain.StatusDone:
		if task.ResultFilename == "" {
			return domain.DownloadResult{}, fmt.Errorf("empty result path")
		}

		f, size, err := uc.fileStore.Open(ctx, task.ResultFilename)
		if err != nil {
			return domain.DownloadResult{}, fmt.Errorf("open result: %w", err)
		}

		return domain.DownloadResult{
			FileName: task.ResultFilename,
			Size:     size,
			Content:  f,
		}, nil

	case domain.StatusFailed:
		return domain.DownloadResult{}, domain.ErrTaskFailed

	case domain.StatusExpired:
		return domain.DownloadResult{}, domain.ErrTaskExpired

	default:
		return domain.DownloadResult{}, domain.ErrTaskNotReady
	}
}
