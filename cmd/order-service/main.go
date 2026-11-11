package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"rt-ed-streaming/internal/config"
	"rt-ed-streaming/internal/db"
	"rt-ed-streaming/internal/httpserver"
	"rt-ed-streaming/internal/logger"
	"rt-ed-streaming/internal/order"
)

const serviceName = "order-service"

func main() {
	if err := run(); err != nil {
		// Logger may not exist yet, so write the fatal error plainly.
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(serviceName, "8080")
	if err != nil {
		return err
	}
	dbURL, err := config.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)

	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpserver.HealthHandler(log, map[string]httpserver.Pinger{"postgres": pool}))
	order.NewHandler(order.NewService(pool), log).Register(mux)

	if err := httpserver.Run(ctx, log, ":"+cfg.HTTPPort, mux, cfg.ShutdownTimeout); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
