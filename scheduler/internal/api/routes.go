package api

import (
	"net/http"
	"time"

	"github.com/anthropic/relayrpc/scheduler/internal/auth"
	"github.com/anthropic/relayrpc/scheduler/internal/scheduler"
	"github.com/anthropic/relayrpc/scheduler/internal/store"
	"github.com/anthropic/relayrpc/scheduler/internal/task"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Server struct {
	Router     *chi.Mux
	TokenStore *auth.TokenStore
	TaskSvc    *task.Service
	WorkerMgr  *scheduler.WorkerManager
	Scheduler  *scheduler.Scheduler
	Mem        *store.Memory
}

func NewServer(tokenStore *auth.TokenStore, taskSvc *task.Service, wm *scheduler.WorkerManager, sched *scheduler.Scheduler, mem *store.Memory, defaultWait time.Duration) *Server {
	s := &Server{
		Router:     chi.NewRouter(),
		TokenStore: tokenStore,
		TaskSvc:    taskSvc,
		WorkerMgr:  wm,
		Scheduler:  sched,
		Mem:        mem,
	}
	s.routes(defaultWait)
	return s
}

func (s *Server) routes(defaultWait time.Duration) {
	s.Router.Use(middleware.Recoverer)
	s.Router.Use(middleware.RealIP)

	// Health check (no auth)
	s.Router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	th := NewTaskHandler(s.TaskSvc, s.Scheduler, defaultWait)
	wh := NewWorkerWSHandler(s.TokenStore, s.WorkerMgr, s.Mem)

	// Task API (auth required)
	s.Router.Group(func(r chi.Router) {
		r.Use(s.TokenStore.Middleware)
		r.Post("/api/v1/tasks", th.Handle)
	})

	// Worker WebSocket (auth inside handler)
	s.Router.Get("/api/v1/workers/ws", wh.HandleWS)
}
