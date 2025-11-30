package mio

import (
	"context"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	Bucket          string
	BasePath        string
	Retry           RetryConfig
}

type RetryConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
}

func NewClient(ctx context.Context, cfg Config) (*minio.Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("empty MinIO endpoint")
	}

	if cfg.Bucket == "" {
		return nil, fmt.Errorf("empty MinIO bucket")
	}

	if cfg.Retry.MaxRetries <= 0 {
		cfg.Retry.MaxRetries = 5
	}

	if cfg.Retry.InitialInterval <= 0 {
		cfg.Retry.InitialInterval = time.Second
	}

	if cfg.Retry.MaxInterval <= 0 {
		cfg.Retry.MaxInterval = 30 * time.Second
	}

	var lastErr error
	interval := cfg.Retry.InitialInterval

	for attempt := range cfg.Retry.MaxRetries {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("context canceled before MinIO init: %w", ctx.Err())
		}
		client, err := minio.New(cfg.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
			Secure: cfg.UseSSL,
		})
		if err != nil {
			lastErr = fmt.Errorf("create MinIO client: %w", err)
		} else {
			if err := ensureBucket(ctx, client, cfg.Bucket); err != nil {
				lastErr = err
			} else {
				return client, nil
			}
		}

		if attempt < cfg.Retry.MaxRetries-1 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context canceled while waiting to retry MinIO: %w", ctx.Err())
			case <-time.After(interval):
				interval *= 2
				if interval > cfg.Retry.MaxInterval {
					interval = cfg.Retry.MaxInterval
				}
			}
		}
	}

	return nil, fmt.Errorf("init MinIO failed after %d attempts: %w", cfg.Retry.MaxRetries, lastErr)
}

func ensureBucket(ctx context.Context, client *minio.Client, bucket string) error {
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("check bucket exists: %w", err)
	}
	if exists {
		return nil
	}

	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}
