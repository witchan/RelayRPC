package store

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	ID                   string     `json:"task_id"`
	ConsumerID           string     `json:"consumer_id"`
	BizID                string     `json:"biz_id,omitempty"`
	IdempotencyKey       string     `json:"-"`
	Seq                  int64      `json:"seq"`
	Status               string     `json:"status"`
	Payload              string     `json:"payload"`
	Result               *string    `json:"result,omitempty"`
	RequiredCapabilities []string   `json:"-"`
	ErrorCode            string     `json:"error_code,omitempty"`
	ErrorMessage         string     `json:"error_message,omitempty"`
	CurrentWorkerID      string     `json:"current_worker_id,omitempty"`
	CurrentAttemptID     string     `json:"current_attempt_id,omitempty"`
	TimeoutMs            int        `json:"timeout_ms"`
	DeadlineAt           time.Time  `json:"deadline_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	FinishedAt           *time.Time `json:"finished_at,omitempty"`
	Attempts             []Attempt  `json:"-"`
}

type Attempt struct {
	ID           string
	TaskID       string
	WorkerID     string
	Status       string
	Retryable    bool
	ErrorCode    string
	ErrorMessage string
	StartedAt    time.Time
	FinishedAt   *time.Time
}

type WorkerState struct {
	ID               string
	RuntimeStatus    string // online, offline, busy, cooling
	CurrentTaskID    string
	CurrentAttemptID string
	FailUntil        time.Time
	FailCount        int // consecutive failure count, reset on success
	LastSeenAt       time.Time
	Capabilities     []string
}

type Memory struct {
	mu          sync.RWMutex
	tasks       map[string]*Task
	taskSeq     atomic.Int64
	idempMap    map[string]string // "consumer_id:key" -> task_id
	workers     map[string]*WorkerState
	workerOrder []string            // stable worker ID order for round-robin dispatch
	rrCursor    int                 // round-robin cursor into workerOrder
	pending     map[string]struct{} // task IDs currently in "pending" status
}

func NewMemory() *Memory {
	return &Memory{
		tasks:    make(map[string]*Task),
		idempMap: make(map[string]string),
		workers:  make(map[string]*WorkerState),
		pending:  make(map[string]struct{}),
	}
}

func (m *Memory) NextSeq() int64 {
	return m.taskSeq.Add(1)
}

// cloneTask returns a shallow copy of a task safe to hand out beyond the lock.
// The Attempts slice is copied so callers cannot mutate internal state.
func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	cp := *t
	if t.Attempts != nil {
		cp.Attempts = make([]Attempt, len(t.Attempts))
		copy(cp.Attempts, t.Attempts)
	}
	return &cp
}

func cloneWorker(w *WorkerState) *WorkerState {
	if w == nil {
		return nil
	}
	cp := *w
	if w.Capabilities != nil {
		cp.Capabilities = make([]string, len(w.Capabilities))
		copy(cp.Capabilities, w.Capabilities)
	}
	return &cp
}

// Task operations

func (m *Memory) CreateTask(t *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[t.ID] = t
	if t.Status == "pending" {
		m.pending[t.ID] = struct{}{}
	}
	if t.IdempotencyKey != "" {
		m.idempMap[t.ConsumerID+":"+t.IdempotencyKey] = t.ID
	}
}

// CreateTaskIdempotent atomically checks idempotency and creates. Returns existing task if duplicate.
func (m *Memory) CreateTaskIdempotent(t *Task) (*Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.IdempotencyKey != "" {
		if id, ok := m.idempMap[t.ConsumerID+":"+t.IdempotencyKey]; ok {
			return cloneTask(m.tasks[id]), true
		}
	}
	m.tasks[t.ID] = t
	if t.Status == "pending" {
		m.pending[t.ID] = struct{}{}
	}
	if t.IdempotencyKey != "" {
		m.idempMap[t.ConsumerID+":"+t.IdempotencyKey] = t.ID
	}
	return cloneTask(t), false
}

// GetTask returns a snapshot copy of the task, safe to read outside the lock.
func (m *Memory) GetTask(id string) *Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneTask(m.tasks[id])
}

func (m *Memory) GetTaskByIdempotency(consumerID, key string) *Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if id, ok := m.idempMap[consumerID+":"+key]; ok {
		return cloneTask(m.tasks[id])
	}
	return nil
}

func (m *Memory) ListTasksByConsumer(consumerID, status string, limit int) []*Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*Task
	for _, t := range m.tasks {
		if t.ConsumerID != consumerID {
			continue
		}
		if status != "" && t.Status != status {
			continue
		}
		result = append(result, cloneTask(t))
	}
	// sort by seq desc
	sort.Slice(result, func(i, j int) bool {
		return result[i].Seq > result[j].Seq
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result
}

// PendingTask is a lightweight snapshot of a pending task used by the scheduler
// to make dispatch decisions without holding a reference to internal state.
type PendingTask struct {
	ID                   string
	Seq                  int64
	RequiredCapabilities []string
}

func (m *Memory) PendingTasks() []PendingTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []PendingTask
	for id := range m.pending {
		t := m.tasks[id]
		if t == nil || t.Status != "pending" {
			// stale entry: lazily remove from index
			delete(m.pending, id)
			continue
		}
		result = append(result, PendingTask{
			ID:                   t.ID,
			Seq:                  t.Seq,
			RequiredCapabilities: t.RequiredCapabilities,
		})
	}
	// sort by seq asc for FIFO fairness
	sort.Slice(result, func(i, j int) bool {
		return result[i].Seq < result[j].Seq
	})
	return result
}

// DeadlineTasks returns the IDs of tasks past their deadline that are still
// pending or running.
func (m *Memory) DeadlineTasks() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	var result []string
	for _, t := range m.tasks {
		if (t.Status == "pending" || t.Status == "running") && !t.DeadlineAt.IsZero() && now.After(t.DeadlineAt) {
			result = append(result, t.ID)
		}
	}
	return result
}

// Worker operations

func (m *Memory) SetWorker(id string, caps []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.workers[id]; !ok {
		m.workers[id] = &WorkerState{
			ID:            id,
			RuntimeStatus: "offline",
			Capabilities:  caps,
		}
		m.workerOrder = append(m.workerOrder, id)
	}
}

// GetWorker returns a snapshot copy of the worker state, safe to read outside the lock.
func (m *Memory) GetWorker(id string) *WorkerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneWorker(m.workers[id])
}

func (m *Memory) AllWorkers() []*WorkerState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*WorkerState
	for _, w := range m.workers {
		result = append(result, cloneWorker(w))
	}
	return result
}

// ---- Transactional state operations ----
//
// All task/worker field mutations go through these methods so that every read
// and write of shared state happens under m.mu. Callers must not mutate the
// pointers returned by Get* (those are snapshots).

// ClaimTask atomically transitions a pending task to running on the given
// worker, marking the worker busy and recording a new attempt. Returns false
// if the task is no longer pending or the worker is not online/available.
func (m *Memory) ClaimTask(taskID, workerID, attemptID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.tasks[taskID]
	if t == nil || t.Status != "pending" {
		return false
	}
	ws := m.workers[workerID]
	if ws == nil || ws.RuntimeStatus != "online" || ws.CurrentTaskID != "" {
		return false
	}

	now := time.Now()
	t.Status = "running"
	t.CurrentWorkerID = workerID
	t.CurrentAttemptID = attemptID
	t.UpdatedAt = now
	delete(m.pending, taskID)

	ws.RuntimeStatus = "busy"
	ws.CurrentTaskID = taskID
	ws.CurrentAttemptID = attemptID

	t.Attempts = append(t.Attempts, Attempt{
		ID:        attemptID,
		TaskID:    taskID,
		WorkerID:  workerID,
		Status:    "running",
		StartedAt: now,
	})
	return true
}

// CompleteSuccess marks a task succeeded and frees its worker.
func (m *Memory) CompleteSuccess(taskID, attemptID, workerID, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.tasks[taskID]
	if t != nil {
		now := time.Now()
		r := result
		t.Status = "succeeded"
		t.Result = &r
		t.CurrentWorkerID = ""
		t.CurrentAttemptID = ""
		t.UpdatedAt = now
		t.FinishedAt = &now
		m.markAttemptLocked(t, attemptID, "succeeded", "", "")
	}

	if ws := m.workers[workerID]; ws != nil {
		ws.RuntimeStatus = "online"
		ws.CurrentTaskID = ""
		ws.CurrentAttemptID = ""
		ws.FailCount = 0
	}
}

// RequeueAfterFailure puts a task back to pending after a retryable attempt
// failure and applies a cooldown to the worker. Returns true if no eligible
// worker remains (caller should then fail the task).
func (m *Memory) RequeueAfterFailure(taskID, attemptID, workerID, errorCode, errorMsg string, cooldownFn func(failCount int) time.Duration) (allFailed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if ws := m.workers[workerID]; ws != nil {
		ws.FailCount++
		ws.RuntimeStatus = "cooling"
		ws.CurrentTaskID = ""
		ws.CurrentAttemptID = ""
		ws.FailUntil = now.Add(cooldownFn(ws.FailCount))
	}

	t := m.tasks[taskID]
	if t == nil {
		return false
	}
	t.Status = "pending"
	t.CurrentWorkerID = ""
	t.CurrentAttemptID = ""
	t.UpdatedAt = now
	m.pending[taskID] = struct{}{}
	m.markAttemptLocked(t, attemptID, "failed", errorCode, errorMsg)

	return m.allEligibleFailedLocked(t)
}

// FailNonRetryable marks a task failed (no retry) and frees its worker.
func (m *Memory) FailNonRetryable(taskID, attemptID, workerID, errorCode, errorMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ws := m.workers[workerID]; ws != nil {
		ws.RuntimeStatus = "online"
		ws.CurrentTaskID = ""
		ws.CurrentAttemptID = ""
	}

	t := m.tasks[taskID]
	if t != nil {
		now := time.Now()
		t.Status = "failed"
		t.ErrorCode = errorCode
		t.ErrorMessage = errorMsg
		t.CurrentWorkerID = ""
		t.CurrentAttemptID = ""
		t.UpdatedAt = now
		t.FinishedAt = &now
		delete(m.pending, taskID)
		m.markAttemptLocked(t, attemptID, "failed", errorCode, errorMsg)
	}
}

// FailTask marks a task failed without touching worker state (used for
// deadline/all-workers-failed cases).
func (m *Memory) FailTask(taskID, errorCode, errorMsg string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tasks[taskID]
	if t == nil || t.Status == "succeeded" || t.Status == "failed" || t.Status == "canceled" {
		return false
	}
	now := time.Now()
	t.Status = "failed"
	t.ErrorCode = errorCode
	t.ErrorMessage = errorMsg
	t.CurrentWorkerID = ""
	t.CurrentAttemptID = ""
	t.UpdatedAt = now
	t.FinishedAt = &now
	delete(m.pending, taskID)
	return true
}

// CancelTask marks a pending task canceled. Returns false if not cancelable.
func (m *Memory) CancelTask(consumerID, taskID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tasks[taskID]
	if t == nil || t.ConsumerID != consumerID || t.Status != "pending" {
		return false
	}
	now := time.Now()
	t.Status = "canceled"
	t.ErrorCode = "TASK_CANCELED"
	t.UpdatedAt = now
	t.FinishedAt = &now
	delete(m.pending, taskID)
	return true
}

// WorkerOnline marks a worker online (on connect/register).
func (m *Memory) WorkerOnline(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ws := m.workers[id]; ws != nil {
		ws.RuntimeStatus = "online"
		ws.LastSeenAt = time.Now()
	}
}

// WorkerOffline marks a worker offline (clean disconnect with no task).
func (m *Memory) WorkerOffline(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ws := m.workers[id]; ws != nil {
		ws.RuntimeStatus = "offline"
	}
}

// WorkerHeartbeat updates the last-seen timestamp.
func (m *Memory) WorkerHeartbeat(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ws := m.workers[id]; ws != nil {
		ws.LastSeenAt = time.Now()
	}
}

// WorkerDisconnectedWithTask handles a worker dropping while it held a task:
// cools the worker down and requeues the in-flight task.
func (m *Memory) WorkerDisconnectedWithTask(workerID, taskID, attemptID string, cooldownFn func(failCount int) time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if ws := m.workers[workerID]; ws != nil {
		ws.FailCount++
		ws.RuntimeStatus = "cooling"
		ws.CurrentTaskID = ""
		ws.CurrentAttemptID = ""
		ws.FailUntil = now.Add(cooldownFn(ws.FailCount))
	}

	t := m.tasks[taskID]
	if t != nil && t.Status == "running" {
		t.Status = "pending"
		t.CurrentWorkerID = ""
		t.CurrentAttemptID = ""
		t.UpdatedAt = now
		m.pending[taskID] = struct{}{}
		m.markAttemptLocked(t, attemptID, "failed", "WORKER_DISCONNECTED", "")
	}
}

func (m *Memory) markAttemptLocked(t *Task, attemptID, status, errCode, errMsg string) {
	now := time.Now()
	for i := range t.Attempts {
		if t.Attempts[i].ID == attemptID {
			t.Attempts[i].Status = status
			t.Attempts[i].ErrorCode = errCode
			t.Attempts[i].ErrorMessage = errMsg
			t.Attempts[i].FinishedAt = &now
			return
		}
	}
}

// allEligibleFailedLocked reports whether every capability-matching worker has
// already failed an attempt for this task. Caller must hold m.mu.
func (m *Memory) allEligibleFailedLocked(t *Task) bool {
	failed := make(map[string]bool)
	for _, a := range t.Attempts {
		if a.Status == "failed" {
			failed[a.WorkerID] = true
		}
	}
	var eligibleCount int
	for _, w := range m.workers {
		if matchCapsStore(w.Capabilities, t.RequiredCapabilities) {
			eligibleCount++
			if !failed[w.ID] {
				return false
			}
		}
	}
	return eligibleCount > 0
}

// PickAvailableWorker selects an online, idle, non-cooling worker that has not
// already failed this task and matches the required capabilities, using
// round-robin order so load is spread evenly across workers. Returns "" if none
// available. Advances the round-robin cursor past the chosen worker.
func (m *Memory) PickAvailableWorker(taskID string, requiredCaps []string, isConnected func(workerID string) bool) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	t := m.tasks[taskID]
	failed := make(map[string]bool)
	if t != nil {
		for _, a := range t.Attempts {
			if a.Status == "failed" {
				failed[a.WorkerID] = true
			}
		}
	}

	now := time.Now()
	n := len(m.workerOrder)
	for i := 0; i < n; i++ {
		idx := (m.rrCursor + i) % n
		id := m.workerOrder[idx]
		ws := m.workers[id]
		if ws == nil || ws.RuntimeStatus != "online" || ws.CurrentTaskID != "" {
			continue
		}
		if !ws.FailUntil.IsZero() && now.Before(ws.FailUntil) {
			continue
		}
		if failed[id] {
			continue
		}
		if !matchCapsStore(ws.Capabilities, requiredCaps) {
			continue
		}
		if isConnected != nil && !isConnected(id) {
			continue
		}
		// advance cursor to the next worker so the following dispatch starts elsewhere
		m.rrCursor = (idx + 1) % n
		return id
	}
	return ""
}

func matchCapsStore(workerCaps, required []string) bool {
	if len(required) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(workerCaps))
	for _, c := range workerCaps {
		set[c] = struct{}{}
	}
	for _, r := range required {
		if _, ok := set[r]; !ok {
			return false
		}
	}
	return true
}
