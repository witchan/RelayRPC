package scheduler

import (
	"log/slog"
	"sync"
	"time"

	"github.com/anthropic/relayrpc/scheduler/internal/store"
	"github.com/anthropic/relayrpc/scheduler/internal/worker"
)

type WorkerManager struct {
	mu       sync.RWMutex
	sessions map[string]*worker.Session
	mem      *store.Memory
	cooldown time.Duration
	notifyCh chan struct{}
}

func NewWorkerManager(mem *store.Memory, cooldown time.Duration, notifyCh chan struct{}) *WorkerManager {
	return &WorkerManager{
		sessions: make(map[string]*worker.Session),
		mem:      mem,
		cooldown: cooldown,
		notifyCh: notifyCh,
	}
}

func (wm *WorkerManager) Register(session *worker.Session) {
	wm.mu.Lock()
	old, exists := wm.sessions[session.WorkerID]
	wm.sessions[session.WorkerID] = session
	wm.mu.Unlock()

	if exists && old != nil {
		old.Close("replaced by new connection")
	}

	wm.mem.WorkerOnline(session.WorkerID)
	wm.notify()
}

func (wm *WorkerManager) Unregister(workerID string) {
	wm.mu.Lock()
	delete(wm.sessions, workerID)
	wm.mu.Unlock()
}

func (wm *WorkerManager) GetSession(workerID string) *worker.Session {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return wm.sessions[workerID]
}

func (wm *WorkerManager) HandleDisconnect(workerID string, hadTask bool, taskID, attemptID string) {
	wm.Unregister(workerID)

	if hadTask {
		wm.mem.WorkerDisconnectedWithTask(workerID, taskID, attemptID, func(failCount int) time.Duration {
			return CalcCooldown(failCount, wm.cooldown)
		})
		slog.Info("worker disconnected with task", "worker_id", workerID, "task_id", taskID)
	} else {
		wm.mem.WorkerOffline(workerID)
	}

	wm.notify()
}

func (wm *WorkerManager) HandleHeartbeat(workerID string) {
	wm.mem.WorkerHeartbeat(workerID)
}

func (wm *WorkerManager) CloseAll(reason string) {
	wm.mu.Lock()
	sessions := make(map[string]*worker.Session, len(wm.sessions))
	for k, v := range wm.sessions {
		sessions[k] = v
	}
	wm.mu.Unlock()

	for _, s := range sessions {
		s.Close(reason)
	}
}

func (wm *WorkerManager) notify() {
	select {
	case wm.notifyCh <- struct{}{}:
	default:
	}
}
