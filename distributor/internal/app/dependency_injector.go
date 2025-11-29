package dapp

import (
	"context"
	"log"
	"log/slog"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	converterpb "github.com/you-humble/dwgtopdf/core/grpc/gen"
	mio "github.com/you-humble/dwgtopdf/core/libs/minio"
	natsq "github.com/you-humble/dwgtopdf/core/libs/nats"
	rediscli "github.com/you-humble/dwgtopdf/core/libs/redis"
	"github.com/you-humble/dwgtopdf/distributor/internal/distributor"
	"github.com/you-humble/dwgtopdf/distributor/internal/infra/config"
	"github.com/you-humble/dwgtopdf/distributor/internal/infra/converter"
	filestore "github.com/you-humble/dwgtopdf/distributor/internal/infra/store/file"
	taskstore "github.com/you-humble/dwgtopdf/distributor/internal/infra/store/task"

	"google.golang.org/grpc"
)

const cfgPath = "./distributor/configs/local.yaml"

type Distributor interface {
	Run(ctx context.Context)
	Stop(ctx context.Context)
	StartCleanup(ctx context.Context)
}

type dependencyInjector struct {
	cfg    *config.Config
	logger *slog.Logger

	grpcConn  *grpc.ClientConn
	converter converterpb.ConverterServiceClient

	redis     *redis.Client
	taskStore distributor.TaskStore

	fileStore distributor.FileCleaner

	natsConn *nats.Conn
	js       nats.JetStreamContext

	distributor Distributor
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
		di.logger = slog.New(
			slog.NewTextHandler(
				os.Stdout,
				&slog.HandlerOptions{
					Level: slog.LevelInfo,
				},
			),
		)
	}

	slog.SetDefault(di.logger)
	return di.logger
}

func (di *dependencyInjector) GRPCConnect(ctx context.Context) *grpc.ClientConn {
	if di.grpcConn == nil {
		cl, err := converter.NewConnection("localhost:50051")
		if err != nil {
			log.Fatalf("GRPCConnect: %+v", err)
		}
		di.grpcConn = cl
	}

	return di.grpcConn
}

func (di *dependencyInjector) DWGConverter(ctx context.Context) converterpb.ConverterServiceClient {
	if di.converter == nil {
		di.converter = converter.NewClient(di.GRPCConnect(ctx))
	}

	return di.converter
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

func (di *dependencyInjector) TaskStore(ctx context.Context) distributor.TaskStore {
	if di.taskStore == nil {
		di.taskStore = taskstore.NewRedisTaskStore(di.RedisClient(ctx))

	}
	return di.taskStore
}

func (di *dependencyInjector) FileStore(ctx context.Context) distributor.FileCleaner {
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

func (di *dependencyInjector) Distributor(ctx context.Context) Distributor {
	if di.distributor == nil {
		cfg := di.Config()
		d := distributor.New(
			cfg.TaskTTL,
			cfg.TaskCleanupInterval,
			di.JetStream(ctx),
			cfg.NATS.Subject,
			cfg.NATS.QueueName,
			cfg.PoolSize,
			di.TaskStore(ctx),
			di.FileStore(ctx),
			di.DWGConverter(ctx),
			cfg.ConversionTimeout,
		)
		di.distributor = d
	}
	return di.distributor
}
