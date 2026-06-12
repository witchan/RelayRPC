package worker

import "encoding/json"

type MsgType string

const (
	MsgTypeHeartbeat  MsgType = "heartbeat"
	MsgTypeTask       MsgType = "task"
	MsgTypeTaskAck    MsgType = "task_ack"
	MsgTypeTaskResult MsgType = "task_result"
)

type Envelope struct {
	Type MsgType `json:"type"`
}

type HeartbeatMsg struct {
	Type     MsgType `json:"type"`
	WorkerID string  `json:"worker_id"`
	Ts       int64   `json:"ts"`
}

type TaskMsg struct {
	Type      MsgType         `json:"type"`
	TaskID    string          `json:"task_id"`
	AttemptID string          `json:"attempt_id"`
	TimeoutMs int             `json:"timeout_ms"`
	Payload   json.RawMessage `json:"payload"`
}

type TaskAckMsg struct {
	Type      MsgType `json:"type"`
	TaskID    string  `json:"task_id"`
	AttemptID string  `json:"attempt_id"`
}

type TaskResultMsg struct {
	Type         MsgType         `json:"type"`
	TaskID       string          `json:"task_id"`
	AttemptID    string          `json:"attempt_id"`
	Success      bool            `json:"success"`
	Result       json.RawMessage `json:"result,omitempty"`
	Retryable    bool            `json:"retryable,omitempty"`
	ErrorCode    string          `json:"error_code,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	DurationMs   int             `json:"duration_ms,omitempty"`
}
