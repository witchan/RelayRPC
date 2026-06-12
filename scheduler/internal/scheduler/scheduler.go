package scheduler

import (
	"log/slog"
	"sync"
	"time"

	"github.com/anthropic/relayrpc/scheduler/internal/config"
	"github.com/anthropic/relayrpc/scheduler/internal/store"
	"github.com/anthropic/relayrpc/scheduler/internal/worker"
	"github.com/google/uuid"
)

type Scheduler struct {
	mem      *store.Memory
	wm       *WorkerManager
	notifyCh chan struct{}
	cfg      config.SchedulerConfig
	done     chan struct{}
	notifier *ResultNotifier
}

func NewScheduler(mem *store.Memory, wm *WorkerManager, notifyCh chan struct{}, cfg config.SchedulerConfig) *Scheduler {
	return &Scheduler{
		mem:      mem,
		wm:       wm,
		notifyCh: notifyCh,
		cfg:      cfg,
		done:     make(chan struct{}),
		notifier: NewResultNotifier(),
	}
}

func (s *Scheduler) Notifier() *ResultNotifier {
	return s.notifier
}

func (s *Scheduler) Run() {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-s.notifyCh:
			s.drainAndSchedule()
		case <-ticker.C:
			s.drainAndSchedule()
			s.checkDeadlines()
		}
	}
}

// drainAndSchedule keeps scheduling as long as progress is being made, so a
// burst of submissions or freed workers is handled in one wakeup instead of
// waiting for subsequent poll ticks. It also drains any pending notify signals
// that arrived while scheduling.
func (s *Scheduler) drainAndSchedule() {
	for {
		dispatched := s.scheduleAvailable()
		// drain a coalesced notify so we don't immediately loop again for it
		select {
		case <-s.notifyCh:
		default:
		}
		if dispatched == 0 {
			return
		}
	}
}

func (s *Scheduler) Stop() {
	close(s.done)
}

func (s *Scheduler) Notify() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func (s *Scheduler) scheduleAvailable() int {
	pending := s.mem.PendingTasks()
	dispatched := 0
	for _, pt := range pending {
		isConnected := func(workerID string) bool {
			return s.wm.GetSession(workerID) != nil
		}
		workerID := s.mem.PickAvailableWorker(pt.ID, pt.RequiredCapabilities, isConnected)
		if workerID == "" {
			continue
		}

		session := s.wm.GetSession(workerID)
		if session == nil {
			continue
		}

		attemptID := uuid.New().String()
		if !s.mem.ClaimTask(pt.ID, workerID, attemptID) {
			continue
		}

		dispatched++
		t := s.mem.GetTask(pt.ID)
		go s.runAttempt(session, t, attemptID, workerID)
	}
	return dispatched
}

func (s *Scheduler) runAttempt(session *worker.Session, t *store.Task, attemptID, workerID string) {
	session.MarkBusy(t.ID, attemptID)

	msg := worker.TaskMsg{
		Type:      worker.MsgTypeTask,
		TaskID:    t.ID,
		AttemptID: attemptID,
		TimeoutMs: t.TimeoutMs,
		Payload:   []byte(t.Payload),
	}
	if err := session.Send(session.Context(), msg); err != nil {
		s.handleAttemptFailure(t, attemptID, workerID, "WORKER_DISCONNECTED", "send failed")
		return
	}

	if err := session.WaitAck(t.ID, attemptID, s.cfg.TaskAckTimeout); err != nil {
		s.handleAttemptFailure(t, attemptID, workerID, "ACK_TIMEOUT", "ack timeout")
		return
	}

	runTimeout := time.Duration(t.TimeoutMs) * time.Millisecond
	if s.cfg.TaskRunTimeout < runTimeout {
		runTimeout = s.cfg.TaskRunTimeout
	}

	result, err := session.WaitResult(t.ID, attemptID, runTimeout)
	if err != nil {
		s.handleAttemptFailure(t, attemptID, workerID, "TASK_RUN_TIMEOUT", "run timeout")
		return
	}

	if result.AttemptID != attemptID {
		slog.Warn("stale attempt ignored", "task_id", t.ID, "expected", attemptID, "got", result.AttemptID)
		return
	}

	if result.Success {
		s.handleSuccess(t, attemptID, workerID, result)
	} else if result.Retryable {
		s.handleAttemptFailure(t, attemptID, workerID, result.ErrorCode, result.ErrorMessage)
	} else {
		s.handleNonRetryableFailure(t, attemptID, workerID, result)
	}
}

func (s *Scheduler) handleSuccess(t *store.Task, attemptID, workerID string, result *worker.TaskResultMsg) {
	s.mem.CompleteSuccess(t.ID, attemptID, workerID, string(result.Result))

	if session := s.wm.GetSession(workerID); session != nil {
		session.MarkOnline()
	}

	s.notifier.Notify(t.ID)
	s.Notify()
	slog.Info("task succeeded", "task_id", t.ID, "worker_id", workerID)
}

func (s *Scheduler) handleAttemptFailure(t *store.Task, attemptID, workerID, errorCode, errorMsg string) {
	allFailed := s.mem.RequeueAfterFailure(t.ID, attemptID, workerID, errorCode, errorMsg, s.calcCooldown)

	if session := s.wm.GetSession(workerID); session != nil {
		session.MarkOnline()
	}

	if allFailed {
		if s.mem.FailTask(t.ID, "ALL_WORKERS_FAILED", "all workers failed") {
			s.notifier.Notify(t.ID)
		}
	} else {
		s.Notify()
	}

	slog.Info("attempt failed", "task_id", t.ID, "worker_id", workerID, "error_code", errorCode)
}

func (s *Scheduler) handleNonRetryableFailure(t *store.Task, attemptID, workerID string, result *worker.TaskResultMsg) {
	s.mem.FailNonRetryable(t.ID, attemptID, workerID, result.ErrorCode, result.ErrorMessage)

	if session := s.wm.GetSession(workerID); session != nil {
		session.MarkOnline()
	}

	s.notifier.Notify(t.ID)
	s.Notify()
	slog.Info("task failed (non-retryable)", "task_id", t.ID, "error_code", result.ErrorCode)
}

func (s *Scheduler) checkDeadlines() {
	for _, taskID := range s.mem.DeadlineTasks() {
		if s.mem.FailTask(taskID, "TASK_DEADLINE_EXCEEDED", "task deadline exceeded") {
			s.notifier.Notify(taskID)
		}
	}
}

// CalcCooldown returns stepped cooldown duration based on consecutive failure count:
// 1st: 5s, 2nd: 30s, 3rd: 2m, 4th: 10m, 5th+: maxCooldown (default 30m)
func CalcCooldown(failCount int, maxCooldown time.Duration) time.Duration {
	switch {
	case failCount <= 1:
		return 5 * time.Second
	case failCount == 2:
		return 30 * time.Second
	case failCount == 3:
		return 2 * time.Minute
	case failCount == 4:
		return 10 * time.Minute
	default:
		return maxCooldown
	}
}

// calcCooldown returns stepped cooldown duration based on consecutive failure count:
// 1st: 5s, 2nd: 30s, 3rd: 2m, 4th: 10m, 5th+: WorkerCooldown (default 30m)
func (s *Scheduler) calcCooldown(failCount int) time.Duration {
	return CalcCooldown(failCount, s.cfg.WorkerCooldown)
}

// ResultNotifier

type ResultNotifier struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

func NewResultNotifier() *ResultNotifier {
	return &ResultNotifier{waiters: make(map[string][]chan struct{})}
}

func (rn *ResultNotifier) Wait(taskID string) <-chan struct{} {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	ch := make(chan struct{}, 1)
	rn.waiters[taskID] = append(rn.waiters[taskID], ch)
	return ch
}

func (rn *ResultNotifier) Notify(taskID string) {
	rn.mu.Lock()
	chs := rn.waiters[taskID]
	delete(rn.waiters, taskID)
	rn.mu.Unlock()
	for _, ch := range chs {
		select {
		case ch <- struct{}{}:
		default:
		}
		close(ch)
	}
}
