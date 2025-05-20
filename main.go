package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"taskcar/app"
	"taskcar/config"
	"taskcar/db"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func process() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Load config
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		return
	}

	dbCtx, dbCancel := context.WithCancel(context.Background())
	defer dbCancel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(dbCtx, cfg.PGConnStr)
	if err != nil {
		slog.Error("Failed to create connection pool", "error", err)
		os.Exit(1)
	}

	tc, err := app.NewTaskcar(*cfg, pool, ctx)
	if err != nil {
		slog.Error("Failed to create TaskCar", "error", err)
		os.Exit(1)
	}
	go func() {
		if err := tc.Serve(); err != nil {
			slog.Error("TaskCar failed", "error", err)
			cancel()
		}
	}()

	<-sigs
	slog.Info("Received shutdown signal, shutting down")
	cancel()

	select {
	case <-tc.Done:
		slog.Info("TaskCar shut down")
	case <-time.After(30 * time.Second):
		slog.Error("TaskCar did not shut down in time, forcing exit")
		os.Exit(1)
	}
	slog.Info("Shutdown complete")
}

func main() {
	argv := os.Args[1:]
	if len(argv) > 0 {
		switch argv[0] {
		case "process":
			process()
		case "migrate":
			if len(argv) < 2 {
				fmt.Println("Usage: taskcar migrate <connection_string>")
				os.Exit(1)
			}
			err := db.MigrateDb(argv[1])
			if err != nil {
				slog.Error("Failed to migrate database", "error", err)
				os.Exit(1)
			}
		default:
			slog.Error("Unknown command", "command", argv[0])
			os.Exit(1)
		}
	} else {
		slog.Error("No command provided")
		os.Exit(1)
	}
}
