package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	mio "github.com/you-humble/dwgtopdf/core/libs/minio"

	"github.com/minio/minio-go/v7"
)

type minioStore struct {
	db       *minio.Client
	bucket   string
	basePath string
}

func NewMinIOStore(ctx context.Context, cfg mio.Config) (*minioStore, error) {
	mioClient, err := mio.NewClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	basePath := strings.Trim(cfg.BasePath, "/")
	if basePath != "" {
		basePath += "/"
	}

	return &minioStore{
		db:       mioClient,
		bucket:   cfg.Bucket,
		basePath: basePath,
	}, nil
}

func (s *minioStore) Save(
	ctx context.Context,
	reader io.Reader,
	filename string,
	size int64,
) (int64, string, error) {
	select {
	case <-ctx.Done():
		return 0, "", ctx.Err()
	default:
	}

	objectName, err := s.objectName(filename)
	if err != nil {
		return 0, "", err
	}

	hasher := sha256.New()
	hashingReader := io.TeeReader(reader, hasher)

	putSize := size
	if putSize <= 0 {
		putSize = -1
	}

	info, err := s.db.PutObject(ctx, s.bucket, objectName, hashingReader, putSize, minio.PutObjectOptions{})
	if err != nil {
		return 0, "", fmt.Errorf("put object: %w", err)
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	return info.Size, hash, nil
}

func (s *minioStore) Open(ctx context.Context, filename string) (io.ReadCloser, int64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	default:
	}

	objectName, err := s.objectName(filename)
	if err != nil {
		return nil, 0, err
	}

	obj, err := s.db.GetObject(ctx, s.bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("get object: %w", err)
	}

	st, err := obj.Stat()
	if err != nil {
		if resp := minio.ToErrorResponse(err); resp.Code == minio.NoSuchKey {
			obj.Close()
			return nil, 0, fmt.Errorf("file not found: %w", err)
		}
		obj.Close()
		return nil, 0, fmt.Errorf("stat object: %w", err)
	}

	return obj, st.Size, nil
}

func (s *minioStore) Delete(ctx context.Context, filename string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	objectName, err := s.objectName(filename)
	if err != nil {
		return err
	}

	err = s.db.RemoveObject(ctx, s.bucket, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		var merr minio.ErrorResponse
		if errors.As(err, &merr) && merr.Code == minio.NoSuchKey {
			return nil
		}
		return fmt.Errorf("remove object: %w", err)
	}

	return nil
}

func (s *minioStore) CleanupOlderThan(ctx context.Context, maxAge time.Duration) error {
	cutoff := time.Now().Add(-maxAge)

	opts := minio.ListObjectsOptions{
		Prefix:    s.basePath,
		Recursive: true,
	}

	for objectInfo := range s.db.ListObjects(ctx, s.bucket, opts) {
		if objectInfo.Err != nil {
			continue
		}

		if !objectInfo.LastModified.Before(cutoff) {
			continue
		}

		err := s.db.RemoveObject(ctx, s.bucket, objectInfo.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("remove old object %s: %w", objectInfo.Key, err)
		}
	}

	return nil
}

func (s *minioStore) objectName(filename string) (string, error) {
	if strings.TrimSpace(filename) == "" {
		return "", fmt.Errorf("empty filename")
	}

	clean := path.Clean(filename)
	if strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("invalid filename: %s", filename)
	}

	clean = strings.TrimLeft(clean, "/")

	return s.basePath + clean, nil
}
