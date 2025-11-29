package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type localStore struct {
	baseDir string
}

func NewLocalStore(baseDir string) (*localStore, error) {
	if baseDir == "" {
		return nil, fmt.Errorf("baseDir is empty")
	}

	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create base dir: %w", err)
	}

	return &localStore{baseDir: baseDir}, nil
}

func (s *localStore) Save(
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

	fullPath, err := s.fullFilePath(filename)
	if err != nil {
		return 0, "", err
	}

	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		return 0, "", fmt.Errorf("mkdir: %w", err)
	}

	tempPath := fullPath + ".tmp-" + fmt.Sprint(time.Now().UnixNano())
	f, err := os.Create(tempPath)
	if err != nil {
		return 0, "", fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tempPath)
	}()

	hasher := sha256.New()
	hashingReader := io.TeeReader(reader, hasher)

	written, err := io.Copy(f, hashingReader)
	if err != nil {
		return 0, "", fmt.Errorf("write file: %w", err)
	}

	if err := f.Close(); err != nil {
		return 0, "", fmt.Errorf("close file: %w", err)
	}

	if err := os.Rename(tempPath, fullPath); err != nil {
		return 0, "", fmt.Errorf("rename temp file: %w", err)
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	return written, hash, nil
}

func (s *localStore) Open(ctx context.Context, filename string) (io.ReadCloser, int64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	default:
	}

	fullPath, err := s.fullFilePath(filename)
	if err != nil {
		return nil, 0, err
	}

	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("file not found: %w", err)
		}
		return nil, 0, fmt.Errorf("stat file: %w", err)
	}

	f, err := os.Open(fullPath)
	if err != nil {
		return nil, 0, fmt.Errorf("open file: %w", err)
	}

	return f, info.Size(), nil
}

func (s *localStore) fullFilePath(filename string) (string, error) {
	if strings.TrimSpace(filename) == "" {
		return "", fmt.Errorf("empty filename")
	}

	clean := filepath.Clean(filename)
	if strings.HasPrefix(clean, "..") {
		return "", fmt.Errorf("invalid filename: %s", filename)
	}

	return filepath.Join(s.baseDir, clean), nil
}
