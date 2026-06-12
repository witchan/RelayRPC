package worker

type Worker struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	RuntimeType   string   `json:"runtime_type"`
	Status        string   `json:"status"`
	RuntimeStatus string   `json:"runtime_status"`
	Capabilities  []string `json:"capabilities"`
	CurrentTaskID string   `json:"current_task_id,omitempty"`
}
