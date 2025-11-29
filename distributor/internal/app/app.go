package dapp

import (
	"context"
	"log/slog"
)

type app struct {
	di *dependencyInjector
}

func New(ctx context.Context) *app {
	return &app{di: newDI()}
}

func (a *app) Run(ctx context.Context) error {

	// distributor
	d := a.di.Distributor(ctx)
	slog.Info("distributor starting...")

	defer d.Stop(ctx)
	defer a.di.GRPCConnect(ctx).Close()
	slog.Info("GRPC connected")
	d.Run(ctx)
	slog.Info("distributor running...")
	// cleanup tasks
	a.di.Distributor(ctx).StartCleanup(ctx)
	slog.Info("cleanup running...")

	<-ctx.Done()

	slog.Info("distributor shutting down...")
	return nil
}
