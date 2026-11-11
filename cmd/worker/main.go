package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"soup-queue/internal/bootstrap"
	"soup-queue/internal/store"
	"soup-queue/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "event", "exit_error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug") {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := configFromEnv(logger)
	if err != nil {
		return err
	}

	pool, err := bootstrap.ConnectDB(ctx, bootstrap.Getenv("DATABASE_URL", bootstrap.DefaultDatabaseURL), "soup-worker:"+cfg.ID, 4)
	if err != nil {
		return err
	}
	defer pool.Close()

	logger.Info("worker started", "event", "started", "worker", cfg.ID,
		"lease", cfg.Lease.String(), "heartbeat_every", cfg.HeartbeatEvery.String(),
		"max_attempts", cfg.MaxAttempts, "fault", cfg.Fault)
	err = worker.Run(ctx, store.New(pool), cfg)
	logger.Info("worker stopped", "event", "stopped", "worker", cfg.ID)
	return err
}

func configFromEnv(logger *slog.Logger) (worker.Config, error) {
	cfg := worker.Config{
		ID:     os.Getenv("WORKER_ID"),
		Fault:  os.Getenv("FAULT"),
		Logger: logger,
	}
	if cfg.ID == "" {
		host, _ := os.Hostname()
		cfg.ID = fmt.Sprintf("%s-%d-%04x", host, os.Getpid(), rand.N(1<<16))
	}

	var err error
	if cfg.Lease, err = bootstrap.EnvDuration("LEASE", 5*time.Second); err != nil {
		return cfg, err
	}
	if cfg.HeartbeatEvery, err = bootstrap.EnvDuration("HEARTBEAT_EVERY", cfg.Lease/5); err != nil {
		return cfg, err
	}
	if cfg.PollEvery, err = bootstrap.EnvDuration("POLL_EVERY", 150*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.ReapEvery, err = bootstrap.EnvDuration("REAP_EVERY", time.Second); err != nil {
		return cfg, err
	}
	if cfg.ReapBatch, err = bootstrap.EnvInt("REAP_BATCH", 100); err != nil {
		return cfg, err
	}
	if cfg.MaxAttempts, err = bootstrap.EnvInt("MAX_ATTEMPTS", 5); err != nil {
		return cfg, err
	}
	return cfg, nil
}
