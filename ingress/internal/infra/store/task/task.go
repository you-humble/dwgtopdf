package taskstote

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/you-humble/dwgtopdf/ingress/internal/domain"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type redisTaskStore struct {
	rdb redis.Cmdable
}

func NewRedisTaskStore(rdb redis.Cmdable) *redisTaskStore {
	return &redisTaskStore{rdb: rdb}
}

func (s *redisTaskStore) CreateTask(p domain.CreateTaskParams) (string, error) {
	ctx := context.Background() // или прокидывать ctx извне, но интерфейс его сейчас не принимает

	if p.IdempotencyKey != "" {
		existingID, err := s.rdb.Get(ctx, idempKey(p.IdempotencyKey)).Result()
		if err == nil && existingID != "" {
			t, ok := s.Task(existingID)
			if !ok {
				_ = s.rdb.Del(ctx, idempKey(p.IdempotencyKey)).Err()
			} else {
				if t.FileHashSHA == p.FileHashSHA && t.FileSize == p.FileSize {
					return existingID, nil
				}
				return "", fmt.Errorf("idempotency key %q reused with different payload", p.IdempotencyKey)
			}
		} else if err != nil && err != redis.Nil {
			return "", fmt.Errorf("redis get idempotency: %w", err)
		}
	}

	if p.FileHashSHA != "" {
		existingID, err := s.rdb.Get(ctx, hashKey(p.FileHashSHA)).Result()
		if err == nil && existingID != "" {
			return existingID, nil
		} else if err != nil && err != redis.Nil {
			return "", fmt.Errorf("redis get hashIndex: %w", err)
		}
	}

	taskID := uuid.NewString()
	now := time.Now()
	t := domain.Task{
		ID:             taskID,
		Status:         domain.StatusPending,
		OriginalName:   p.OriginalName,
		InputFilename:  p.InputFilename,
		FileSize:       p.FileSize,
		FileHashSHA:    p.FileHashSHA,
		IdempotencyKey: p.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
		ExpiresAt:      now.Add(p.TTL),
	}

	pipe := s.rdb.TxPipeline()

	hk := taskKey(taskID)

	pipe.HSet(ctx, hk, map[string]interface{}{
		"id":              t.ID,
		"status":          string(t.Status),
		"original_name":   t.OriginalName,
		"input_filename":  t.InputFilename,
		"result_filename": t.ResultFilename,
		"file_size":       t.FileSize,
		"file_hash_sha":   t.FileHashSHA,
		"idempotency_key": t.IdempotencyKey,
		"error":           t.Error,
		"created_at":      t.CreatedAt.UnixNano(),
		"updated_at":      t.UpdatedAt.UnixNano(),
		"expires_at":      t.ExpiresAt.UnixNano(),
	})

	pipe.ZAdd(ctx, tasksByCreatedKey(), redis.Z{
		Score:  float64(t.CreatedAt.Unix()),
		Member: t.ID,
	})

	if p.IdempotencyKey != "" {
		pipe.Set(ctx, idempKey(p.IdempotencyKey), t.ID, 0)
	}
	if p.FileHashSHA != "" {
		pipe.Set(ctx, hashKey(p.FileHashSHA), t.ID, 0)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("redis pipeline CreateTask: %w", err)
	}

	return taskID, nil
}

func (s *redisTaskStore) Task(id string) (domain.Task, bool) {
	ctx := context.Background()
	hk := taskKey(id)

	res, err := s.rdb.HGetAll(ctx, hk).Result()
	if err != nil {
		return domain.Task{}, false
	}
	if len(res) == 0 {
		return domain.Task{}, false
	}

	t := domain.Task{
		ID: id,
	}

	t.Status = domain.TaskStatus(res["status"])
	t.OriginalName = res["original_name"]
	t.InputFilename = res["input_filename"]
	t.ResultFilename = res["result_filename"]
	t.FileHashSHA = res["file_hash_sha"]
	t.IdempotencyKey = res["idempotency_key"]
	t.Error = res["error"]

	if v, ok := res["file_size"]; ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			t.FileSize = n
		}
	}

	if v, ok := res["created_at"]; ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			t.CreatedAt = time.Unix(0, n)
		}
	}
	if v, ok := res["updated_at"]; ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			t.UpdatedAt = time.Unix(0, n)
		}
	}
	if v, ok := res["expires_at"]; ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			t.ExpiresAt = time.Unix(0, n)
		}
	}

	return t, true
}

func (s *redisTaskStore) UpdateStatus(id string, newStatus domain.TaskStatus, errReason string) {
	ctx := context.Background()
	hk := taskKey(id)

	now := time.Now().UnixNano()

	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, hk, "status", string(newStatus))
	pipe.HSet(ctx, hk, "error", errReason)
	pipe.HSet(ctx, hk, "updated_at", now)

	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("redis UpdateStatus", slog.String("error", err.Error()))
	}
}

func (s *redisTaskStore) SetResult(id string, pdfName string) {
	ctx := context.Background()
	hk := taskKey(id)

	now := time.Now().UnixNano()

	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, hk, "result_filename", pdfName)
	pipe.HSet(ctx, hk, "error", "")
	pipe.HSet(ctx, hk, "status", string(domain.StatusDone))
	pipe.HSet(ctx, hk, "updated_at", now)

	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("redis SetResult", slog.String("error", err.Error()))
	}
}

func (s *redisTaskStore) ByIdempotencyKey(key string) (domain.Task, bool) {
	if key == "" {
		return domain.Task{}, false
	}
	ctx := context.Background()

	id, err := s.rdb.Get(ctx, idempKey(key)).Result()
	if err == redis.Nil {
		return domain.Task{}, false
	}
	if err != nil {
		slog.Warn("redis ByIdempotencyKey", slog.String("error", err.Error()))
		return domain.Task{}, false
	}

	return s.Task(id)
}

func (s *redisTaskStore) ExpiredTasks(now time.Time) []string {
	ctx := context.Background()

	ids, err := s.rdb.ZRangeByScore(ctx, tasksByCreatedKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprint(now.Unix()),
	}).Result()
	if err != nil {
		return nil
	}

	var expiredIDs []string

	for _, id := range ids {
		t, ok := s.Task(id)
		if !ok {
			continue
		}
		if now.After(t.ExpiresAt) && t.Status != domain.StatusExpired {
			s.UpdateStatus(id, domain.StatusExpired, "task expired")
			expiredIDs = append(expiredIDs, id)
		}
	}

	return expiredIDs
}

func (s *redisTaskStore) DeleteExpired(now time.Time, ttl time.Duration) int {
	ctx := context.Background()

	border := now.Add(-ttl).Unix()

	ids, err := s.rdb.ZRangeByScore(ctx, tasksByCreatedKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprint(border),
	}).Result()
	if err != nil {
		return 0
	}

	deleted := 0
	for _, id := range ids {
		t, ok := s.Task(id)
		if !ok {
			continue
		}

		pipe := s.rdb.TxPipeline()

		pipe.Del(ctx, taskKey(id))
		pipe.ZRem(ctx, tasksByCreatedKey(), id)
		if t.IdempotencyKey != "" {
			pipe.Del(ctx, idempKey(t.IdempotencyKey))
		}
		if t.FileHashSHA != "" {
			pipe.Del(ctx, hashKey(t.FileHashSHA))
		}

		if _, err := pipe.Exec(ctx); err == nil {
			deleted++
		}
	}

	return deleted
}

func taskKey(id string) string {
	return "task:" + id
}

func idempKey(k string) string {
	return "task:idemp:" + k
}

func hashKey(h string) string {
	return "task:hash:" + h
}

func tasksByCreatedKey() string {
	return "tasks:by_created"
}
