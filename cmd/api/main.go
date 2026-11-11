package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"soup-queue/internal/api"
	"soup-queue/internal/bootstrap"
	"soup-queue/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := bootstrap.Getenv("DATABASE_URL", bootstrap.DefaultDatabaseURL)
	addr := bootstrap.Getenv("ADDR", ":8080")
	maxConns, err := bootstrap.EnvInt("DB_MAX_CONNS", 20)
	if err != nil || maxConns < 1 {
		return errors.New("DB_MAX_CONNS must be a positive integer")
	}

	pool, err := bootstrap.ConnectDB(ctx, dbURL, int32(maxConns))
	if err != nil {
		return err
	}
	defer pool.Close()

	app := api.New(store.New(pool), api.Config{
		AccessLog: bootstrap.Getenv("ACCESS_LOG", "1") != "0",
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           app.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("api listening on %s", addr)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
