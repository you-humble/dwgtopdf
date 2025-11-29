package replicator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

type Storage interface {
	Save(ctx context.Context, reader io.Reader, filename string, size int64) (int64, string, error)
	Open(ctx context.Context, filename string) (io.ReadCloser, int64, error)
	Delete(ctx context.Context, filename string) error
}

type ReplicateJob struct {
	Filename string
	Size     int64
	Hash     string
	Retries  int
}

type Replicator struct {
	local  Storage
	remote Storage

	queue      chan ReplicateJob
	workerNum  int
	maxRetries int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.RWMutex
	closed bool
}

func NewReplicator(local, remote Storage, queueSize, workerNum, maxRetries int) *Replicator {
	if queueSize <= 0 {
		queueSize = 100
	}
	if workerNum <= 0 {
		workerNum = 1
	}
	if maxRetries < 0 {
		maxRetries = 0
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Replicator{
		local:      local,
		remote:     remote,
		queue:      make(chan ReplicateJob, queueSize),
		workerNum:  workerNum,
		maxRetries: maxRetries,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (r *Replicator) Start(ctx context.Context) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	outerCtx := ctx
	innerCtx, innerCancel := context.WithCancel(outerCtx)
	r.ctx = innerCtx
	r.cancel = innerCancel
	r.mu.Unlock()

	r.wg.Add(r.workerNum)
	for i := 0; i < r.workerNum; i++ {
		go r.worker(i)
	}
}

func (r *Replicator) Stop(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.cancel()
	close(r.queue)
	r.mu.Unlock()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		r.wg.Wait()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
	}

	slog.Info("replicator: stopped")
	return nil
}

func (r *Replicator) Enqueue(job ReplicateJob) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return false
	}

	select {
	case r.queue <- job:
		return true
	default:
		return false
	}
}

func (r *Replicator) worker(id int) {
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case job, ok := <-r.queue:
			if !ok {
				return
			}

			r.handleJob(r.ctx, job)
		}
	}
}

func (r *Replicator) handleJob(ctx context.Context, job ReplicateJob) {
	l := slog.With(
		slog.String("filename", job.Filename),
		slog.Int("retries", job.Retries),
	)

	if err := r.replicateOnce(ctx, job); err != nil {
		if job.Retries >= r.maxRetries {
			l.Error("replication failed, max retries exceeded",
				slog.String("error", err.Error()),
			)
			return
		}

		job.Retries++
		select {
		case r.queue <- job:
			l.Warn("replication failed, job requeued",
				slog.String("error", err.Error()),
				slog.Int("next_retry", job.Retries),
			)
		default:
			l.Error("replication failed and queue is full, dropping job",
				slog.String("error", err.Error()),
			)
		}
	}
}

func (r *Replicator) replicateOnce(ctx context.Context, job ReplicateJob) error {

	rc, size, err := r.local.Open(ctx, job.Filename)
	if err != nil {
		return fmt.Errorf("open local file: %w", err)
	}
	defer rc.Close()

	if job.Size > 0 {
		size = job.Size
	}

	written, remoteHash, err := r.remote.Save(ctx, rc, job.Filename, size)
	if err != nil {
		return fmt.Errorf("save to remote: %w", err)
	}

	if written <= 0 {
		return fmt.Errorf("remote save wrote zero bytes")
	}

	if job.Hash != "" && remoteHash != "" && job.Hash != remoteHash {
		return fmt.Errorf("hash mismatch: local=%s remote=%s", job.Hash, remoteHash)
	}

	slog.Debug("replicator: file replicated",
		slog.String("filename", job.Filename),
		slog.Int64("size", written),
	)

	return nil
}
