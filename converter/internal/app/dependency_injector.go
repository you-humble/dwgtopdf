package capp

import (
	"context"
	"log"
	"log/slog"
	"os"

	"github.com/you-humble/dwgtopdf/converter/internal/converter"
	"github.com/you-humble/dwgtopdf/converter/internal/infra/config"
	filestore "github.com/you-humble/dwgtopdf/converter/internal/infra/file"
	"github.com/you-humble/dwgtopdf/converter/internal/service"
	converterpb "github.com/you-humble/dwgtopdf/core/grpc/gen"
	mio "github.com/you-humble/dwgtopdf/core/libs/minio"
)

type dependencyInjector struct {
	cfg    *config.Config
	logger *slog.Logger

	converter service.Converter
	fileStore converter.FileSaver
	service   converterpb.ConverterServiceServer
}

func newDI() *dependencyInjector {
	return &dependencyInjector{}
}

func (di *dependencyInjector) Config() *config.Config {
	if di.cfg == nil {
		di.cfg = config.MustLoad()
	}

	return di.cfg
}

func (di *dependencyInjector) Logger() *slog.Logger {
	if di.logger == nil {
		di.logger = slog.New(slog.NewTextHandler(
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

func (di *dependencyInjector) DWGConverter(ctx context.Context) service.Converter {
	if di.converter == nil {
		di.converter = converter.NewMockConverter(di.FileStore(ctx), di.Config().BaseDir, 16)
	}

	return di.converter
}

func (di *dependencyInjector) FileStore(ctx context.Context) converter.FileSaver {
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

		di.fileStore = filestore.NewAsyncStore(ctx,
			local,
			remote,
			cfg.QueueCapacity,
			cfg.PoolSize,
			3,
		)
		di.Logger().Info(
			"using async file store (local + MinIO)",
			slog.Int("queue_size", cfg.QueueCapacity),
			slog.Int("worker_num", cfg.PoolSize),
			slog.Int("max_retries", 3),
		)
	}

	return di.fileStore
}

func (di *dependencyInjector) Service(ctx context.Context) converterpb.ConverterServiceServer {
	if di.service == nil {
		di.service = service.NewConverterService(di.DWGConverter(ctx))
	}

	return di.service
}
