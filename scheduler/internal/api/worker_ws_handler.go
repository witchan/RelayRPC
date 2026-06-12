package api

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/anthropic/relayrpc/scheduler/internal/auth"
	"github.com/anthropic/relayrpc/scheduler/internal/scheduler"
	"github.com/anthropic/relayrpc/scheduler/internal/store"
	"github.com/anthropic/relayrpc/scheduler/internal/worker"
	"nhooyr.io/websocket"
)

type WorkerWSHandler struct {
	tokenStore *auth.TokenStore
	wm         *scheduler.WorkerManager
	mem        *store.Memory
}

func NewWorkerWSHandler(tokenStore *auth.TokenStore, wm *scheduler.WorkerManager, mem *store.Memory) *WorkerWSHandler {
	return &WorkerWSHandler{tokenStore: tokenStore, wm: wm, mem: mem}
}

func (h *WorkerWSHandler) HandleWS(w http.ResponseWriter, r *http.Request) {
	rawToken := ""
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		rawToken = strings.TrimPrefix(authHeader, "Bearer ")
	}
	if rawToken == "" {
		Error(w, http.StatusUnauthorized, "MISSING_TOKEN", "authorization required")
		return
	}

	if !h.tokenStore.Valid(rawToken) {
		Error(w, http.StatusUnauthorized, "INVALID_TOKEN", "invalid token")
		return
	}

	// Use token as worker identity
	workerID := rawToken

	// Auto-register worker if not exists
	if h.mem.GetWorker(workerID) == nil {
		h.mem.SetWorker(workerID, []string{"default"})
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})
	if err != nil {
		slog.Error("websocket accept failed", "error", err)
		return
	}

	session := worker.NewSession(r.Context(), workerID, conn, h.wm.HandleDisconnect)
	h.wm.Register(session)

	slog.Info("worker connected", "worker_id", workerID[:8]+"...")

	session.ReadLoop(h.wm.HandleHeartbeat)
}
