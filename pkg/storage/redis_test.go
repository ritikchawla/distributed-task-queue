package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ritikchawla/distributed-task-queue/pkg/task"
)

func getRedisAddr() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func setupTestStorage(t *testing.T) *RedisStorage {
	storage, err := NewRedisStorage(RedisConfig{
		Addr:   getRedisAddr(),
		Prefix: "dtq_test",
	})
	if err != nil {
		t.Skipf("Skipping test: Redis not available: %v", err)
	}
	return storage
}

func cleanupTestStorage(t *testing.T, storage *RedisStorage) {
	// Clean up test keys
	ctx := context.Background()
	keys, _ := storage.client.Keys(ctx, "dtq_test:*").Result()
	if len(keys) > 0 {
		storage.client.Del(ctx, keys...)
	}
	storage.Close()
}

func TestRedisStorageEnqueue(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Create and enqueue a task
	testTask := task.NewTask("test.task", map[string]interface{}{
		"key": "value",
	})

	err := storage.Enqueue(ctx, testTask)
	if err != nil {
		t.Fatalf("Failed to enqueue task: %v", err)
	}

	// Verify task can be retrieved
	retrieved, err := storage.GetTask(ctx, testTask.ID)
	if err != nil {
		t.Fatalf("Failed to retrieve task: %v", err)
	}

	if retrieved.ID != testTask.ID {
		t.Errorf("Retrieved task ID mismatch: expected %s, got %s", testTask.ID, retrieved.ID)
	}

	if retrieved.Type != testTask.Type {
		t.Errorf("Retrieved task type mismatch: expected %s, got %s", testTask.Type, retrieved.Type)
	}
}

func TestRedisStorageEnqueueDelayed(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Create a delayed task
	testTask := task.NewTask("delayed.task", nil).Apply(
		task.WithDelay(5 * time.Second),
	)

	err := storage.Enqueue(ctx, testTask)
	if err != nil {
		t.Fatalf("Failed to enqueue delayed task: %v", err)
	}

	// Delayed task should not be immediately dequeueable
	dequeued, err := storage.Dequeue(ctx, "test-worker")
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if dequeued != nil {
		t.Error("Delayed task should not be dequeueable before scheduled time")
	}
}

func TestRedisStorageDequeue(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Enqueue a task
	testTask := task.NewTask("test.dequeue", nil)
	err := storage.Enqueue(ctx, testTask)
	if err != nil {
		t.Fatalf("Failed to enqueue: %v", err)
	}

	// Dequeue the task
	dequeued, err := storage.Dequeue(ctx, "test-worker")
	if err != nil {
		t.Fatalf("Failed to dequeue: %v", err)
	}

	if dequeued == nil {
		t.Fatal("Expected to dequeue a task, got nil")
	}

	if dequeued.ID != testTask.ID {
		t.Errorf("Dequeued task ID mismatch: expected %s, got %s", testTask.ID, dequeued.ID)
	}

	if dequeued.Status != task.StatusRunning {
		t.Errorf("Dequeued task should have status Running, got %s", dequeued.Status)
	}
}

func TestRedisStoragePriority(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Enqueue tasks with different priorities
	lowTask := task.NewTask("low.priority", nil).Apply(task.WithPriority(task.PriorityLow))
	normalTask := task.NewTask("normal.priority", nil).Apply(task.WithPriority(task.PriorityNormal))
	highTask := task.NewTask("high.priority", nil).Apply(task.WithPriority(task.PriorityHigh))

	// Enqueue in reverse order
	storage.Enqueue(ctx, lowTask)
	storage.Enqueue(ctx, normalTask)
	storage.Enqueue(ctx, highTask)

	// Dequeue should return highest priority first
	first, _ := storage.Dequeue(ctx, "worker-1")
	if first == nil {
		t.Fatal("Expected first task")
	}
	if first.ID != highTask.ID {
		t.Errorf("Expected high priority task first, got %s", first.Type)
	}

	second, _ := storage.Dequeue(ctx, "worker-2")
	if second == nil {
		t.Fatal("Expected second task")
	}
	if second.ID != normalTask.ID {
		t.Errorf("Expected normal priority task second, got %s", second.Type)
	}

	third, _ := storage.Dequeue(ctx, "worker-3")
	if third == nil {
		t.Fatal("Expected third task")
	}
	if third.ID != lowTask.ID {
		t.Errorf("Expected low priority task third, got %s", third.Type)
	}
}

func TestRedisStorageComplete(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Enqueue and dequeue a task
	testTask := task.NewTask("test.complete", nil)
	storage.Enqueue(ctx, testTask)
	dequeued, _ := storage.Dequeue(ctx, "worker")

	// Complete the task
	result := map[string]string{"status": "done"}
	err := storage.Complete(ctx, dequeued.ID, result)
	if err != nil {
		t.Fatalf("Failed to complete task: %v", err)
	}

	// Verify task status
	completed, err := storage.GetTask(ctx, testTask.ID)
	if err != nil {
		t.Fatalf("Failed to get completed task: %v", err)
	}

	if completed.Status != task.StatusCompleted {
		t.Errorf("Expected status Completed, got %s", completed.Status)
	}
}

func TestRedisStorageFail(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Create task with retries
	testTask := task.NewTask("test.fail", nil).Apply(task.WithMaxRetries(2))
	storage.Enqueue(ctx, testTask)
	dequeued, _ := storage.Dequeue(ctx, "worker")

	// Fail the task
	err := storage.Fail(ctx, dequeued.ID, "test error")
	if err != nil {
		t.Fatalf("Failed to fail task: %v", err)
	}

	// Verify task status
	failed, _ := storage.GetTask(ctx, testTask.ID)
	if failed.Status != task.StatusRetrying {
		t.Errorf("Expected status Retrying, got %s", failed.Status)
	}

	if failed.RetryCount != 1 {
		t.Errorf("Expected retry count 1, got %d", failed.RetryCount)
	}
}

func TestRedisStorageDeadLetter(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Create task with no retries
	testTask := task.NewTask("test.dead", nil).Apply(task.WithMaxRetries(0))
	storage.Enqueue(ctx, testTask)
	dequeued, _ := storage.Dequeue(ctx, "worker")

	// Fail the task (should go to DLQ)
	storage.Fail(ctx, dequeued.ID, "permanent failure")

	// Check dead letter queue
	deadTasks, err := storage.GetDeadLetterTasks(ctx, 10)
	if err != nil {
		t.Fatalf("Failed to get dead letter tasks: %v", err)
	}

	found := false
	for _, dt := range deadTasks {
		if dt.ID == testTask.ID {
			found = true
			if dt.Status != task.StatusDead {
				t.Errorf("Expected status Dead, got %s", dt.Status)
			}
		}
	}

	if !found {
		t.Error("Task not found in dead letter queue")
	}
}

func TestRedisStorageIdempotency(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Enqueue with idempotency key
	testTask := task.NewTask("test.idempotent", nil).Apply(
		task.WithIdempotencyKey("unique-key-123"),
	)

	err := storage.Enqueue(ctx, testTask)
	if err != nil {
		t.Fatalf("First enqueue failed: %v", err)
	}

	// Try to enqueue again with same key
	duplicateTask := task.NewTask("test.idempotent", nil).Apply(
		task.WithIdempotencyKey("unique-key-123"),
	)

	err = storage.Enqueue(ctx, duplicateTask)
	if err != ErrDuplicateTask {
		t.Errorf("Expected ErrDuplicateTask, got %v", err)
	}
}

func TestRedisStorageStats(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	// Enqueue some tasks
	for i := 0; i < 3; i++ {
		task := task.NewTask("test.stats", nil)
		storage.Enqueue(ctx, task)
	}

	// Get stats
	stats, err := storage.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats["pending"] < 3 {
		t.Errorf("Expected at least 3 pending tasks, got %d", stats["pending"])
	}
}

func TestRedisStorageHealthCheck(t *testing.T) {
	storage := setupTestStorage(t)
	defer cleanupTestStorage(t, storage)

	ctx := context.Background()

	err := storage.HealthCheck(ctx)
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}
