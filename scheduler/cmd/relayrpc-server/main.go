package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anthropic/relayrpc/scheduler/internal/api"
	"github.com/anthropic/relayrpc/scheduler/internal/auth"
	"github.com/anthropic/relayrpc/scheduler/internal/config"
	"github.com/anthropic/relayrpc/scheduler/internal/scheduler"
	"github.com/anthropic/relayrpc/scheduler/internal/store"
	"github.com/anthropic/relayrpc/scheduler/internal/task"
)

func main() {
	cfgPath := "configs/config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	setupLogger(cfg.Log.Level)

	if len(cfg.Tokens) == 0 {
		slog.Error("no tokens configured, run scripts/gen-tokens.sh first")
		os.Exit(1)
	}

	tokenStore := auth.NewTokenStore(cfg.Tokens)
	mem := store.NewMemory()

	notifyCh := make(chan struct{}, 1)
	taskSvc := task.NewService(mem, cfg.Scheduler.DefaultTaskTimeout, cfg.Scheduler.DefaultTaskDeadline, notifyCh)
	workerMgr := scheduler.NewWorkerManager(mem, cfg.Scheduler.WorkerCooldown, notifyCh)
	sched := scheduler.NewScheduler(mem, workerMgr, notifyCh, cfg.Scheduler)
	go sched.Run()

	srv := api.NewServer(tokenStore, taskSvc, workerMgr, sched, mem, 60*time.Second)

	httpServer := &http.Server{
		Addr:         cfg.Server.ListenAddr,
		Handler:      srv.Router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("server starting", "addr", cfg.Server.ListenAddr)
		slog.Info("tokens loaded", "count", len(cfg.Tokens))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down...")

	sched.Stop()
	workerMgr.CloseAll("server shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownGracePeriod)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}

	fmt.Println("server stopped")
}

func setupLogger(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
}
