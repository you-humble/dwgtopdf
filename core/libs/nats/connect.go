package natsq

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

type Config struct {
	Name          string
	MaxReconnects int
}

func NewConnect(url string, cfg Config) (*nats.Conn, error) {
	nc, err := nats.Connect(url,
		nats.Name(cfg.Name),
		nats.MaxReconnects(cfg.MaxReconnects),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	return nc, nil
}

func NewJetStream(nc *nats.Conn, cfg *nats.StreamConfig) (nats.JetStreamContext, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("JetStream: %w", err)
	}

	_, err = js.AddStream(cfg)
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return nil, fmt.Errorf("JetStream AddStream: %w", err)
	}

	return js, nil
}
