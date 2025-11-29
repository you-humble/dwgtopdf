package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	dapp "github.com/you-humble/dwgtopdf/distributor/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	a := dapp.New(ctx)
	if err := a.Run(ctx); err != nil {
		log.Fatalln("distributor:", err)
	}
}
