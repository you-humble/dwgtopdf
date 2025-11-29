package filestore

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"github.com/you-humble/dwgtopdf/converter/internal/infra/file/replicator"
)

type FileStore interface {
	Save(ctx context.Context, reader io.Reader, filename string, size int64) (int64, string, error)
	Open(ctx context.Context, filename string) (io.ReadCloser, int64, error)
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
