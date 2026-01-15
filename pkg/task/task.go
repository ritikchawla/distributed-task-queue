// Package task provides the core task definitions and types for the distributed queue.
package task

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status represents the current state of a task in the queue.
type Status string

const (
	StatusPending   Status = "pending"
	StatusScheduled Status = "scheduled"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
	StatusDead      Status = "dead" // moved to dead-letter queue after max retries
)

// Priority defines task execution priority levels.
// Higher values indicate higher priority.
type Priority int

const (
	PriorityLow      Priority = 1
	PriorityNormal   Priority = 5
	PriorityHigh     Priority = 10
	PriorityCritical Priority = 100
)

// Task represents a unit of work to be processed by a worker.
type Task struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Payload     map[string]interface{} `json:"payload"`
	Priority    Priority               `json:"priority"`
	Status      Status                 `json:"status"`
	CreatedAt   time.Time              `json:"created_at"`
	ScheduledAt time.Time              `json:"scheduled_at,omitempty"`
	StartedAt   *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	RetryCount  int                    `json:"retry_count"`
	MaxRetries  int                    `json:"max_retries"`
	Error       string                 `json:"error,omitempty"`
	Result      interface{}            `json:"result,omitempty"`
	Timeout     time.Duration          `json:"timeout"`
	// IdempotencyKey ensures exactly-once execution
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// NewTask creates a new task with default values.
func NewTask(taskType string, payload map[string]interface{}) *Task {
	now := time.Now()
	return &Task{
		ID:          uuid.New().String(),
		Type:        taskType,
		Payload:     payload,
		Priority:    PriorityNormal,
		Status:      StatusPending,
		CreatedAt:   now,
		ScheduledAt: now,
		RetryCount:  0,
		MaxRetries:  3,
		Timeout:     30 * time.Second,
	}
}

// TaskOption is a function that modifies task configuration.
type TaskOption func(*Task)

// WithPriority sets the task priority.
func WithPriority(p Priority) TaskOption {
	return func(t *Task) {
		t.Priority = p
	}
}

// WithDelay schedules the task to run after the specified duration.
func WithDelay(d time.Duration) TaskOption {
	return func(t *Task) {
		t.ScheduledAt = time.Now().Add(d)
		t.Status = StatusScheduled
	}
}

// WithScheduleAt schedules the task to run at a specific time.
func WithScheduleAt(at time.Time) TaskOption {
	return func(t *Task) {
		t.ScheduledAt = at
		t.Status = StatusScheduled
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(n int) TaskOption {
	return func(t *Task) {
		t.MaxRetries = n
	}
}

// WithTimeout sets the task execution timeout.
func WithTimeout(d time.Duration) TaskOption {
	return func(t *Task) {
		t.Timeout = d
	}
}

// WithIdempotencyKey sets a unique key for exactly-once execution.
func WithIdempotencyKey(key string) TaskOption {
	return func(t *Task) {
		t.IdempotencyKey = key
	}
}

// Apply applies the given options to the task.
func (t *Task) Apply(opts ...TaskOption) *Task {
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Serialize converts the task to JSON bytes.
func (t *Task) Serialize() ([]byte, error) {
	return json.Marshal(t)
}

// Deserialize creates a task from JSON bytes.
func Deserialize(data []byte) (*Task, error) {
	var t Task
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// Clone creates a deep copy of the task.
func (t *Task) Clone() *Task {
	clone := *t
	if t.Payload != nil {
		clone.Payload = make(map[string]interface{})
		for k, v := range t.Payload {
			clone.Payload[k] = v
		}
	}
	return &clone
}

// IsDelayed returns true if the task is scheduled for future execution.
func (t *Task) IsDelayed() bool {
	return t.ScheduledAt.After(time.Now())
}

// ShouldRetry returns true if the task can be retried.
func (t *Task) ShouldRetry() bool {
	return t.RetryCount < t.MaxRetries
}
