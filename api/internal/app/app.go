package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/you-humble/dwgtopdf/api/internal/transport"
)

type app struct {
	di  *dependencyInjector
	srv *http.Server
}

func New(ctx context.Context) *app {
	di := newDI()
	di.Logger()
	mux := http.NewServeMux()
	return &app{
		di: di,
		srv: &http.Server{
			Addr: di.Config().Addr,
			Handler: transport.WithRecover(
				transport.LogMiddleware(
					di.Router(ctx).MountRoutes(mux),
				),
			),
		},
	}
}

func (a *app) Run(ctx context.Context) error {
	errCh := make(chan error)
	go func() {
		slog.Info("starting server", slog.String("addr", a.srv.Addr))
		if e := a.srv.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
			slog.Error("server error", slog.String("error", e.Error()))
			errCh <- e
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	slog.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		a.di.Config().ShutdownTimeout,
	)
	defer cancel()

	if err := a.srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", slog.String("error", err.Error()))
		return err
	}

	slog.Info("server gracefully stopped")
	return nil
}
