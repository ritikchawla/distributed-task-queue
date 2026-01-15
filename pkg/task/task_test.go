package task

import (
	"testing"
	"time"
)

func TestNewTask(t *testing.T) {
	payload := map[string]interface{}{
		"key": "value",
	}

	task := NewTask("test.task", payload)

	if task.ID == "" {
		t.Error("Expected task ID to be generated")
	}

	if task.Type != "test.task" {
		t.Errorf("Expected task type 'test.task', got '%s'", task.Type)
	}

	if task.Priority != PriorityNormal {
		t.Errorf("Expected default priority %d, got %d", PriorityNormal, task.Priority)
	}

	if task.Status != StatusPending {
		t.Errorf("Expected status pending, got %s", task.Status)
	}

	if task.MaxRetries != 3 {
		t.Errorf("Expected default max retries 3, got %d", task.MaxRetries)
	}
}

func TestTaskWithOptions(t *testing.T) {
	payload := map[string]interface{}{"data": "test"}

	task := NewTask("email.send", payload).Apply(
		WithPriority(PriorityHigh),
		WithMaxRetries(5),
		WithTimeout(1*time.Minute),
		WithIdempotencyKey("test-key"),
	)

	if task.Priority != PriorityHigh {
		t.Errorf("Expected priority high, got %d", task.Priority)
	}

	if task.MaxRetries != 5 {
		t.Errorf("Expected max retries 5, got %d", task.MaxRetries)
	}

	if task.Timeout != 1*time.Minute {
		t.Errorf("Expected timeout 1m, got %v", task.Timeout)
	}

	if task.IdempotencyKey != "test-key" {
		t.Errorf("Expected idempotency key 'test-key', got '%s'", task.IdempotencyKey)
	}
}

func TestTaskWithDelay(t *testing.T) {
	payload := map[string]interface{}{}
	delay := 5 * time.Second

	task := NewTask("delayed.task", payload).Apply(
		WithDelay(delay),
	)

	if task.Status != StatusScheduled {
		t.Errorf("Expected status scheduled, got %s", task.Status)
	}

	// ScheduledAt should be approximately delay duration from now
	expectedTime := time.Now().Add(delay)
	diff := task.ScheduledAt.Sub(expectedTime)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("ScheduledAt not within expected range: %v", diff)
	}
}

func TestTaskWithScheduleAt(t *testing.T) {
	payload := map[string]interface{}{}
	scheduleTime := time.Now().Add(1 * time.Hour)

	task := NewTask("scheduled.task", payload).Apply(
		WithScheduleAt(scheduleTime),
	)

	if task.Status != StatusScheduled {
		t.Errorf("Expected status scheduled, got %s", task.Status)
	}

	if !task.ScheduledAt.Equal(scheduleTime) {
		t.Errorf("Expected ScheduledAt %v, got %v", scheduleTime, task.ScheduledAt)
	}
}

func TestTaskSerialization(t *testing.T) {
	payload := map[string]interface{}{
		"to":      "test@example.com",
		"subject": "Test Subject",
	}

	original := NewTask("email.send", payload).Apply(
		WithPriority(PriorityHigh),
		WithMaxRetries(5),
	)

	// Serialize
	data, err := original.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize task: %v", err)
	}

	// Deserialize
	recovered, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Failed to deserialize task: %v", err)
	}

	// Verify fields
	if recovered.ID != original.ID {
		t.Errorf("ID mismatch: expected %s, got %s", original.ID, recovered.ID)
	}

	if recovered.Type != original.Type {
		t.Errorf("Type mismatch: expected %s, got %s", original.Type, recovered.Type)
	}

	if recovered.Priority != original.Priority {
		t.Errorf("Priority mismatch: expected %d, got %d", original.Priority, recovered.Priority)
	}

	if recovered.MaxRetries != original.MaxRetries {
		t.Errorf("MaxRetries mismatch: expected %d, got %d", original.MaxRetries, recovered.MaxRetries)
	}
}

func TestTaskIsDelayed(t *testing.T) {
	tests := []struct {
		name        string
		scheduledAt time.Time
		expected    bool
	}{
		{
			name:        "future time",
			scheduledAt: time.Now().Add(1 * time.Hour),
			expected:    true,
		},
		{
			name:        "past time",
			scheduledAt: time.Now().Add(-1 * time.Hour),
			expected:    false,
		},
		{
			name:        "current time",
			scheduledAt: time.Now(),
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := NewTask("test", nil)
			task.ScheduledAt = tt.scheduledAt

			if task.IsDelayed() != tt.expected {
				t.Errorf("IsDelayed() = %v, expected %v", task.IsDelayed(), tt.expected)
			}
		})
	}
}

func TestTaskShouldRetry(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		maxRetries int
		expected   bool
	}{
		{
			name:       "no retries yet",
			retryCount: 0,
			maxRetries: 3,
			expected:   true,
		},
		{
			name:       "some retries left",
			retryCount: 2,
			maxRetries: 3,
			expected:   true,
		},
		{
			name:       "max retries reached",
			retryCount: 3,
			maxRetries: 3,
			expected:   false,
		},
		{
			name:       "exceeded max retries",
			retryCount: 5,
			maxRetries: 3,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := NewTask("test", nil)
			task.RetryCount = tt.retryCount
			task.MaxRetries = tt.maxRetries

			if task.ShouldRetry() != tt.expected {
				t.Errorf("ShouldRetry() = %v, expected %v", task.ShouldRetry(), tt.expected)
			}
		})
	}
}

func TestTaskClone(t *testing.T) {
	original := NewTask("test", map[string]interface{}{
		"key": "value",
	})

	clone := original.Clone()

	// Verify it's a different instance
	if clone == original {
		t.Error("Clone returned same instance")
	}

	// Modify clone and ensure original is not affected
	clone.Payload["key"] = "modified"
	if original.Payload["key"] == "modified" {
		t.Error("Modifying clone affected original")
	}
}

func TestPriorityValues(t *testing.T) {
	// Ensure priority values are ordered correctly
	if PriorityLow >= PriorityNormal {
		t.Error("PriorityLow should be less than PriorityNormal")
	}
	if PriorityNormal >= PriorityHigh {
		t.Error("PriorityNormal should be less than PriorityHigh")
	}
	if PriorityHigh >= PriorityCritical {
		t.Error("PriorityHigh should be less than PriorityCritical")
	}
}
