package distributor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	converterpb "github.com/you-humble/dwgtopdf/core/grpc/gen"
	"github.com/you-humble/dwgtopdf/distributor/internal/domain"

	"github.com/nats-io/nats.go"
)

type TaskStore interface {
	Task(id string) (domain.Task, bool)
	UpdateStatus(id string, newStatus domain.TaskStatus, errReason string)
	SetResult(id string, pdfName string)
	ExpiredTasks(now time.Time) []string
	DeleteExpired(now time.Time, ttl time.Duration) int
}

type FileCleaner interface {
	Delete(ctx context.Context, filename string) error
	CleanupOlderThan(ctx context.Context, maxAge time.Duration) error
}

type natsDistributor struct {
	taskCleanupInterval time.Duration
	taskTTL             time.Duration
	js                  nats.JetStreamContext
	subject             string
	queueName           string
	size                int
	taskStore           TaskStore
	fileCleaner         FileCleaner
	converter           converterpb.ConverterServiceClient
	conversionTimeout   time.Duration

	done chan struct{}
	sub  *nats.Subscription
}

func New(
	taskCleanupInterval time.Duration,
	taskTTL time.Duration,
	js nats.JetStreamContext,
	subject, queueName string,
	size int,
	taskStore TaskStore,
	fileCleaner FileCleaner,
	converter converterpb.ConverterServiceClient,
	conversionTimeout time.Duration,
) *natsDistributor {

	return &natsDistributor{
		taskCleanupInterval: taskCleanupInterval,
		taskTTL:             taskTTL,
		js:                  js,
		subject:             subject,
		queueName:           queueName,
		size:                size,
		taskStore:           taskStore,
		fileCleaner:         fileCleaner,
		converter:           converter,
		conversionTimeout:   conversionTimeout,
		done:                make(chan struct{}, size),
	}
}

func (d *natsDistributor) Run(ctx context.Context) {
	consumerName := "dwg-conversion-consumer"
	_, err := d.js.AddConsumer("DWG_CONVERSION", &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: d.subject,
		MaxAckPending: d.size * 2,
	})
	if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		slog.Error("JetStream AddConsumer", slog.String("error", err.Error()))
		return
	}

	sub, err := d.js.PullSubscribe(d.subject, consumerName)
	if err != nil {
		slog.Error("JetStream PullSubscribe", slog.String("error", err.Error()))
		return
	}
	d.sub = sub

	for range d.size {
		go func() {
			defer func() { d.done <- struct{}{} }()
			d.runWorker(ctx)
		}()
	}

	slog.Info("NATS processor is running",
		slog.Int("workers", d.size),
		slog.String("subject", d.subject),
	)
}

func (d *natsDistributor) Stop(ctx context.Context) {
	<-ctx.Done()

	for range d.size {
		<-d.done
	}

	if d.sub != nil {
		if err := d.sub.Drain(); err != nil {
			slog.Warn("NATS subscription drain", slog.String("error", err.Error()))
		}
	}

	slog.Info("NATS processor stopped")
}

func (d *natsDistributor) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("Worker stopping")
			return
		default:
		}

		msgs, err := d.sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			slog.Warn("NATS Fetch", slog.String("error", err.Error()))
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range msgs {
			taskID := string(msg.Data)
			slog.Debug("Got message", slog.String("task_id", taskID))

			if err := d.process(ctx, taskID); err != nil {
				if errors.Is(err, domain.ErrTaskNotFound) || errors.Is(err, domain.ErrTaskExpired) {
					slog.Error("process",
						slog.String("task_id", taskID),
						slog.String("error", err.Error()),
					)
					_ = msg.Ack()
					continue
				}
				slog.Error("process",
					slog.String("task_id", taskID),
					slog.String("error", err.Error()),
				)
				_ = msg.Nak()
			}

			if err := msg.Ack(); err != nil {
				slog.Warn("NATS Ack", slog.String("error", err.Error()))
			}
		}
	}
}

func (d *natsDistributor) process(ctx context.Context, taskID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	task, found := d.taskStore.Task(taskID)
	if !found {
		return domain.ErrTaskNotFound
	}

	if task.Status == domain.StatusExpired {
		return domain.ErrTaskExpired
	}

	slog.Info("process start", slog.String("task_id", taskID))
	d.taskStore.UpdateStatus(taskID, domain.StatusProcessing, "")

	ctx, cancel := context.WithTimeout(ctx, d.conversionTimeout)
	defer cancel()

	resp, err := d.converter.Convert(ctx,
		&converterpb.ConvertRequest{
			InputPath:     task.InputFilename,
			SuggestedName: task.OriginalName,
		})
	if err != nil {
		d.taskStore.UpdateStatus(taskID, domain.StatusFailed, err.Error())
		return err
	}

	d.taskStore.SetResult(taskID, resp.PdfName)
	slog.Info("process done", slog.String("task_id", taskID))
	return nil
}

func (d *natsDistributor) StartCleanup(ctx context.Context) {
	ticker := time.NewTicker(d.taskCleanupInterval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				expired := d.taskStore.ExpiredTasks(now)
				if len(expired) > 0 {
					slog.Info("cleanup", slog.Int("count_of_expired_tasks", len(expired)))
				}

				for _, id := range expired {
					task, ok := d.taskStore.Task(id)
					if !ok {
						continue
					}
					if err := d.fileCleaner.Delete(ctx, task.InputFilename); err != nil {
						slog.Warn("cleanup input file", slog.String("error", err.Error()))
					}
					if task.ResultFilename != "" {
						if err := d.fileCleaner.Delete(ctx, task.ResultFilename); err != nil {
							slog.Warn("cleanup result file", slog.String("error", err.Error()))
						}
					}
				}
				if n := d.taskStore.DeleteExpired(now, 2*d.taskTTL); n > 0 {
					slog.Info("cleanup tasks map", slog.Int("deleted_tasks", n))
				}

				if err := d.fileCleaner.CleanupOlderThan(ctx, 2*d.taskTTL); err != nil && !errors.Is(err, context.Canceled) {
					slog.Warn("cleanup old files", slog.String("error", err.Error()))
				}
			}
		}
	}()
}
