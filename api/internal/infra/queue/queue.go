package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
)

type queue struct {
	js      nats.JetStreamContext
	subject string
}

func New(js nats.JetStreamContext, subject string) *queue {
	return &queue{
		js:      js,
		subject: subject,
	}
}

func (q *queue) Enqueue(ctx context.Context, taskID string) error {
	if taskID == "" {
		return fmt.Errorf("empty taskID")
	}

	msg := &nats.Msg{
		Subject: q.subject,
		Data:    []byte(taskID),
		Header:  nats.Header{},
	}

	ack, err := q.js.PublishMsg(msg)
	if err != nil {
		return fmt.Errorf("enqueue task %s: publish failed: %w", taskID, err)
	}

	slog.Debug(
		"task enqueued",
		slog.String("task_id", taskID),
		slog.String("subject", q.subject),
		slog.String("stream", ack.Stream),
		slog.Uint64("seq", ack.Sequence),
	)

	return nil
}
