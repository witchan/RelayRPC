package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

type Session struct {
	WorkerID         string
	Conn             *websocket.Conn
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.Mutex
	currentTaskID    string
	currentAttemptID string
	ackCh            chan TaskAckMsg
	resultCh         chan TaskResultMsg
	onDisconnect     func(workerID string, hadTask bool, taskID, attemptID string)
}

func NewSession(_ context.Context, workerID string, conn *websocket.Conn, onDisconnect func(string, bool, string, string)) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	return &Session{
		WorkerID:     workerID,
		Conn:         conn,
		ctx:          ctx,
		cancel:       cancel,
		ackCh:        make(chan TaskAckMsg, 1),
		resultCh:     make(chan TaskResultMsg, 1),
		onDisconnect: onDisconnect,
	}
}

func (s *Session) Context() context.Context {
	return s.ctx
}

func (s *Session) Send(ctx context.Context, msg interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Conn.Write(ctx, websocket.MessageText, data)
}

func (s *Session) Close(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Conn.Close(websocket.StatusGoingAway, reason)
	s.cancel()
}

func (s *Session) MarkBusy(taskID, attemptID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTaskID = taskID
	s.currentAttemptID = attemptID
}

func (s *Session) MarkOnline() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTaskID = ""
	s.currentAttemptID = ""
}

func (s *Session) CurrentTask() (string, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTaskID, s.currentAttemptID
}

func (s *Session) WaitAck(taskID, attemptID string, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return fmt.Errorf("session closed")
		case <-timer.C:
			return fmt.Errorf("ack timeout")
		case ack := <-s.ackCh:
			if ack.TaskID == taskID && ack.AttemptID == attemptID {
				return nil
			}
		}
	}
}

func (s *Session) WaitResult(taskID, attemptID string, timeout time.Duration) (*TaskResultMsg, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return nil, fmt.Errorf("session closed")
		case <-timer.C:
			return nil, fmt.Errorf("run timeout")
		case result := <-s.resultCh:
			if result.TaskID == taskID && result.AttemptID == attemptID {
				return &result, nil
			}
			slog.Warn("stale result ignored", "task_id", result.TaskID, "attempt_id", result.AttemptID)
		}
	}
}

func (s *Session) ReadLoop(onHeartbeat func(workerID string)) {
	defer func() {
		taskID, attemptID := s.CurrentTask()
		hadTask := taskID != ""
		if s.onDisconnect != nil {
			s.onDisconnect(s.WorkerID, hadTask, taskID, attemptID)
		}
		s.cancel()
	}()

	for {
		_, data, err := s.Conn.Read(s.ctx)
		if err != nil {
			return
		}

		var env Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}

		switch env.Type {
		case MsgTypeHeartbeat:
			if onHeartbeat != nil {
				onHeartbeat(s.WorkerID)
			}
		case MsgTypeTaskAck:
			var msg TaskAckMsg
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			select {
			case s.ackCh <- msg:
			default:
			}
		case MsgTypeTaskResult:
			var msg TaskResultMsg
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			select {
			case s.resultCh <- msg:
			default:
			}
		}
	}
}
