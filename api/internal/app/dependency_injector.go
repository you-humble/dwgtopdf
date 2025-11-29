package app

import (
	"context"

	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/you-humble/dwgtopdf/api/internal/infra/config"
	"github.com/you-humble/dwgtopdf/api/internal/infra/queue"
	filestore "github.com/you-humble/dwgtopdf/api/internal/infra/store/file"
	taskstore "github.com/you-humble/dwgtopdf/api/internal/infra/store/task"
	"github.com/you-humble/dwgtopdf/api/internal/transport"
	"github.com/you-humble/dwgtopdf/api/internal/usecase"
	mio "github.com/you-humble/dwgtopdf/core/libs/minio"
	natsq "github.com/you-humble/dwgtopdf/core/libs/nats"
	rediscli "github.com/you-humble/dwgtopdf/core/libs/redis"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

const cfgPath = "./api/configs/local.yaml"

type Router interface {
	MountRoutes(*http.ServeMux) *http.ServeMux
}

type dependencyInjector struct {
	cfg    *config.Config
	logger *slog.Logger

	redis     *redis.Client
	taskStore usecase.TaskStore

	fileStore usecase.FileStore

	natsConn *nats.Conn
	js       nats.JetStreamContext

	taskQueue usecase.TaskQueue

	usecase transport.Usecase
	handler transport.Handler
	router  Router
}

func newDI() *dependencyInjector {
	return &dependencyInjector{}
}

func (di *dependencyInjector) Config() *config.Config {
	if di.cfg == nil {
		di.cfg = config.MustLoad(cfgPath)
	}

	return di.cfg
}

func (di *dependencyInjector) Logger() *slog.Logger {
	if di.logger == nil {
		di.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))

	}

	slog.SetDefault(di.logger)
	return di.logger
}

func (di *dependencyInjector) RedisClient(ctx context.Context) *redis.Client {
	if di.redis == nil {
		cfg := di.Config().Redis
		client, err := rediscli.NewClient(rediscli.Config{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		})
		if err != nil {
			log.Fatalf("FileStore minio: %+v", err)
		}

		di.redis = client
		di.Logger().Info("connected to redis", slog.String("addr", cfg.Addr))
	}
	return di.redis
}

func (di *dependencyInjector) TaskStore(ctx context.Context) usecase.TaskStore {
	if di.taskStore == nil {
		di.taskStore = taskstore.NewRedisTaskStore(di.RedisClient(ctx))
	}
	return di.taskStore
}

func (di *dependencyInjector) FileStore(ctx context.Context) usecase.FileStore {
	if di.fileStore == nil {
		cfg := di.Config()

		local, err := filestore.NewLocalStore(di.Config().BaseDir)
		if err != nil {
			log.Fatalf("FileStore local: %+v", err)
		}
		di.Logger().Info("initialized local file store", slog.String("base_dir", cfg.BaseDir))

		remote, err := filestore.NewMinIOStore(ctx, mio.Config{
			Endpoint:        cfg.MinIO.Endpoint,
			AccessKeyID:     cfg.MinIO.AccessKeyID,
			SecretAccessKey: cfg.MinIO.SecretAccessKey,
			UseSSL:          cfg.MinIO.UseSSL,
			Bucket:          cfg.MinIO.Bucket,
			BasePath:        cfg.BaseDir,
		})
		if err != nil {
			log.Fatalf("FileStore minio: %+v", err)
		}
		di.Logger().Info(
			"initialized MinIO file store",
			slog.String("endpoint", cfg.MinIO.Endpoint),
			slog.String("bucket", cfg.MinIO.Bucket),
		)

		di.fileStore = filestore.NewAsyncStore(ctx, local, remote, cfg.QueueCapacity, cfg.PoolSize, 3)
		di.Logger().Info(
			"using async file store (local + MinIO)",
			slog.Int("queue_size", cfg.QueueCapacity),
			slog.Int("worker_num", cfg.PoolSize),
			slog.Int("max_retries", 3),
		)
	}

	return di.fileStore
}

func (di *dependencyInjector) NATSConn(ctx context.Context) *nats.Conn {
	if di.natsConn == nil {
		cfg := di.Config()
		nc, err := natsq.NewConnect(cfg.NATS.URL, natsq.Config{
			Name:          cfg.NATS.QueueName,
			MaxReconnects: cfg.NATS.MaxReconnects,
		})
		if err != nil {
			log.Fatalf("NATS connect: %+v", err)
		}
		di.natsConn = nc
	}
	return di.natsConn
}

func (di *dependencyInjector) JetStream(ctx context.Context) nats.JetStreamContext {
	if di.js == nil {
		js, err := natsq.NewJetStream(di.NATSConn(ctx), &nats.StreamConfig{
			Name:     "DWG_CONVERSION",
			Subjects: []string{di.Config().NATS.Subject},
			Storage:  nats.FileStorage,
			Replicas: 1,
			MaxAge:   2 * di.Config().TaskTTL,
		})

		if err != nil {
			log.Fatalf("DI JetStream: %+v", err)
		}

		di.js = js
	}
	return di.js
}

func (di *dependencyInjector) TaskQueue(ctx context.Context) usecase.TaskQueue {
	if di.taskQueue == nil {
		di.taskQueue = queue.New(di.JetStream(ctx), di.Config().NATS.Subject)
	}
	return di.taskQueue
}

func (di *dependencyInjector) Usecase(ctx context.Context) transport.Usecase {
	if di.usecase == nil {
		cfg := di.Config()
		di.usecase = usecase.New(
			cfg.TaskTTL,
			di.TaskStore(ctx),
			di.FileStore(ctx),
			di.TaskQueue(ctx),
		)
	}

	return di.usecase
}

func (di *dependencyInjector) Handler(ctx context.Context) transport.Handler {
	if di.handler == nil {
		di.handler = transport.NewHandler(di.Config().MaxUploadBytesMb, di.Usecase(ctx))
	}

	return di.handler
}

func (di *dependencyInjector) Router(ctx context.Context) Router {
	if di.router == nil {
		di.router = transport.NewRouter(di.Handler(ctx))
	}

	return di.router
}
