package capp

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	converterpb "github.com/you-humble/dwgtopdf/core/grpc/gen"

	"google.golang.org/grpc"
)

type app struct {
	di   *dependencyInjector
	addr string
	srv  *grpc.Server
}

func New(ctx context.Context) *app {
	di := newDI()
	di.Logger()

	grpcServer := grpc.NewServer()
	converterpb.RegisterConverterServiceServer(grpcServer, di.Service(ctx))

	return &app{
		di:   di,
		addr: ":50051",
		srv:  grpcServer,
	}
}

func (a *app) Run(ctx context.Context) error {
	l := a.di.Logger()
	errCh := make(chan error, 1)

	go func() {
		if err := a.startServer(); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		l.Info("shutdown signal received, starting graceful shutdown")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := a.shutdown(shutdownCtx); err != nil {
			l.Error("graceful shutdown failed", "err", err)
		} else {
			l.Info("graceful shutdown completed")
		}

	case err := <-errCh:
		l.Error("server exited with error", "err", err)
		return err
	}

	return nil
}

// addr = :50051
func (a *app) startServer() error {
	lis, err := net.Listen("tcp", a.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	log.Printf("Converter gRPC service listening on %s", a.addr)
	if err := a.srv.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

func (a *app) shutdown(ctx context.Context) error {
	l := a.di.Logger()
	errCh := make(chan error, 1)

	go func() {
		l.Info("stopping gRPC server gracefully...")
		l.Info("gRPC server stopped")

		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		l.Warn("graceful stop timed out, forcing stop")
		return fmt.Errorf("shutdown timeout exceeded: %w", ctx.Err())
	case err := <-errCh:
		return err
	}
}
