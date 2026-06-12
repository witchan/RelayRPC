package task

import (
	"time"

	"github.com/anthropic/relayrpc/scheduler/internal/store"
	"github.com/google/uuid"
)

type Service struct {
	mem             *store.Memory
	defaultTimeout  time.Duration
	defaultDeadline time.Duration
	notifyCh        chan struct{}
}

func NewService(mem *store.Memory, defaultTimeout, defaultDeadline time.Duration, notifyCh chan struct{}) *Service {
	return &Service{mem: mem, defaultTimeout: defaultTimeout, defaultDeadline: defaultDeadline, notifyCh: notifyCh}
}

type CreateRequest struct {
	ConsumerID           string
	BizID                string
	IdempotencyKey       string
	Payload              string
	TimeoutMs            int
	DeadlineMs           int
	RequiredCapabilities []string
}

type CreateResult struct {
	Task             *store.Task
	IdempotentReplay bool
}

func (s *Service) Create(req CreateRequest) *CreateResult {
	timeoutMs := req.TimeoutMs
	if timeoutMs == 0 {
		timeoutMs = int(s.defaultTimeout.Milliseconds())
	}

	var deadlineAt time.Time
	if req.DeadlineMs > 0 {
		deadlineAt = time.Now().Add(time.Duration(req.DeadlineMs) * time.Millisecond)
	} else {
		deadlineAt = time.Now().Add(s.defaultDeadline)
	}

	now := time.Now()
	t := &store.Task{
		ID:                   uuid.New().String(),
		ConsumerID:           req.ConsumerID,
		BizID:                req.BizID,
		IdempotencyKey:       req.IdempotencyKey,
		Seq:                  s.mem.NextSeq(),
		Status:               "pending",
		Payload:              req.Payload,
		RequiredCapabilities: req.RequiredCapabilities,
		TimeoutMs:            timeoutMs,
		DeadlineAt:           deadlineAt,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	result, duplicate := s.mem.CreateTaskIdempotent(t)
	if duplicate {
		return &CreateResult{Task: result, IdempotentReplay: true}
	}

	s.notify()
	return &CreateResult{Task: result, IdempotentReplay: false}
}

func (s *Service) GetByID(consumerID, taskID string) *store.Task {
	t := s.mem.GetTask(taskID)
	if t == nil || t.ConsumerID != consumerID {
		return nil
	}
	return t
}

func (s *Service) List(consumerID, status string, limit int) []*store.Task {
	return s.mem.ListTasksByConsumer(consumerID, status, limit)
}

func (s *Service) Cancel(consumerID, taskID string) bool {
	return s.mem.CancelTask(consumerID, taskID)
}

func (s *Service) notify() {
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}
