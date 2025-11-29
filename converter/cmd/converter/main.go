package main

import (
	"context"
	"os/signal"
	"syscall"

	capp "github.com/you-humble/dwgtopdf/converter/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
		syscall.SIGINT,
	)
	defer stop()

	a := capp.New(ctx)
	if err := a.Run(ctx); err != nil {
		panic(err)
	}
}
