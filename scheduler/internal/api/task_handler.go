package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/anthropic/relayrpc/scheduler/internal/auth"
	"github.com/anthropic/relayrpc/scheduler/internal/scheduler"
	"github.com/anthropic/relayrpc/scheduler/internal/store"
	"github.com/anthropic/relayrpc/scheduler/internal/task"
)

type TaskHandler struct {
	taskSvc     *task.Service
	sched       *scheduler.Scheduler
	defaultWait time.Duration
}

func NewTaskHandler(taskSvc *task.Service, sched *scheduler.Scheduler, defaultWait time.Duration) *TaskHandler {
	return &TaskHandler{taskSvc: taskSvc, sched: sched, defaultWait: defaultWait}
}

func (h *TaskHandler) Handle(w http.ResponseWriter, r *http.Request) {
	token := auth.ContextToken(r.Context())

	var req struct {
		Payload       json.RawMessage `json:"payload"`
		TimeoutMs     int             `json:"timeout_ms"`
		WaitTimeoutMs int             `json:"wait_timeout_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		Error(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		Error(w, http.StatusBadRequest, "INVALID_PAYLOAD", "payload is required")
		return
	}

	waitTimeout := time.Duration(req.WaitTimeoutMs) * time.Millisecond
	if waitTimeout == 0 {
		waitTimeout = h.defaultWait
	}

	idempKey := r.Header.Get("Idempotency-Key")

	result := h.taskSvc.Create(task.CreateRequest{
		ConsumerID:     token,
		IdempotencyKey: idempKey,
		Payload:        string(req.Payload),
		TimeoutMs:      req.TimeoutMs,
	})

	if result.IdempotentReplay && isFinished(result.Task) {
		respondResult(w, result.Task)
		return
	}

	// Wait for result
	ch := h.sched.Notifier().Wait(result.Task.ID)
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	select {
	case <-ch:
	case <-timer.C:
	case <-r.Context().Done():
	}

	t := h.taskSvc.GetByID(token, result.Task.ID)
	if t == nil {
		Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "task lost")
		return
	}

	if isFinished(t) {
		respondResult(w, t)
	} else {
		JSON(w, http.StatusAccepted, map[string]interface{}{
			"task_id": t.ID,
			"status":  t.Status,
			"message": "task is still processing",
		})
	}
}

func isFinished(t *store.Task) bool {
	return t.Status == "succeeded" || t.Status == "failed" || t.Status == "canceled"
}

func respondResult(w http.ResponseWriter, t *store.Task) {
	if t.Status == "succeeded" {
		resp := map[string]interface{}{"task_id": t.ID, "success": true, "status": t.Status}
		if t.Result != nil {
			resp["result"] = json.RawMessage(*t.Result)
		}
		JSON(w, http.StatusOK, resp)
	} else {
		resp := map[string]interface{}{"task_id": t.ID, "success": false, "status": t.Status}
		if t.ErrorCode != "" {
			resp["error_code"] = t.ErrorCode
		}
		if t.ErrorMessage != "" {
			resp["error_message"] = t.ErrorMessage
		}
		JSON(w, http.StatusOK, resp)
	}
}
