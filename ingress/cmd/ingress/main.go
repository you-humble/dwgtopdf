package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/you-humble/dwgtopdf/ingress/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer stop()

	a := app.New(ctx)
	if err := a.Run(ctx); err != nil {
		panic(err)
	}
}
