package filestore

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/you-humble/dwgtopdf/api/internal/infra/store/file/replicator"

	"golang.org/x/sync/errgroup"
)

type FileStore interface {
	Save(ctx context.Context, reader io.Reader, filename string, size int64) (int64, string, error)
	Open(ctx context.Context, filename string) (io.ReadCloser, int64, error)
	Delete(ctx context.Context, filename string) error
	CleanupOlderThan(ctx context.Context, maxAge time.Duration) error
}

type asyncStore struct {
	local      *localStore
	remote     *minioStore
	replicator *replicator.Replicator
}

func NewAsyncStore(
	ctx context.Context,
	local *localStore,
	remote *minioStore,
	queueSize,
	workerNum,
	maxRetries int,
) *asyncStore {
	repl := replicator.NewReplicator(local, remote, queueSize, workerNum, maxRetries)
	repl.Start(ctx)

	return &asyncStore{
		local:      local,
		remote:     remote,
		replicator: repl,
	}
}

func (s *asyncStore) Close(ctx context.Context) error {
	return s.replicator.Stop(ctx)
}

func (s *asyncStore) Save(
	ctx context.Context,
	reader io.Reader,
	filename string,
	size int64,
) (int64, string, error) {
	written, hash, err := s.local.Save(ctx, reader, filename, size)
	if err != nil {
		return 0, "", err
	}

	ok := s.replicator.Enqueue(replicator.ReplicateJob{
		Filename: filename,
		Size:     written,
		Hash:     hash,
		Retries:  0,
	})
	if !ok {
		slog.Error("asyncStore: replication queue full, file saved only locally",
			slog.String("filename", filename),
			slog.Int64("size", written),
		)
	}

	return written, hash, nil
}

func (s *asyncStore) Open(ctx context.Context, filename string) (io.ReadCloser, int64, error) {
	rc, size, err := s.local.Open(ctx, filename)
	if err == nil {
		return rc, size, nil
	}

	if !strings.Contains(err.Error(), "file not found") {
		return nil, 0, err
	}

	rc, size, err = s.remote.Open(ctx, filename)
	if err != nil {
		return nil, 0, err
	}

	return rc, size, nil
}

func (s *asyncStore) Delete(ctx context.Context, filename string) error {
	var firstErr error

	if err := s.local.Delete(ctx, filename); err != nil {
		firstErr = err
		slog.Warn("asyncStore: delete local failed",
			slog.String("filename", filename),
			slog.String("error", err.Error()),
		)
	}

	if err := s.remote.Delete(ctx, filename); err != nil {
		if firstErr == nil {
			firstErr = err
		}
		slog.Warn("asyncStore: delete remote failed",
			slog.String("filename", filename),
			slog.String("error", err.Error()),
		)
	}

	return firstErr
}

func (s *asyncStore) CleanupOlderThan(ctx context.Context, maxAge time.Duration) error {
	eg, eCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return s.local.CleanupOlderThan(eCtx, maxAge)
	})
	eg.Go(func() error {
		return s.remote.CleanupOlderThan(eCtx, maxAge)
	})

	return eg.Wait()
}
