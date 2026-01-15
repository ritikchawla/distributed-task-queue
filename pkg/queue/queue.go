// Package queue provides the main queue interface and client for the distributed task queue.
package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ritikchawla/distributed-task-queue/pkg/storage"
	"github.com/ritikchawla/distributed-task-queue/pkg/task"
)

// Queue represents the distributed task queue client.
type Queue struct {
	storage *storage.RedisStorage
	mu      sync.RWMutex
}

// Config holds queue configuration options.
type Config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	KeyPrefix     string
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() Config {
	return Config{
		RedisAddr:     "localhost:6379",
		RedisPassword: "",
		RedisDB:       0,
		KeyPrefix:     "dtq",
	}
}

// New creates a new Queue instance.
func New(cfg Config) (*Queue, error) {
	storage, err := storage.NewRedisStorage(storage.RedisConfig{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		Prefix:   cfg.KeyPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	return &Queue{
		storage: storage,
	}, nil
}

// Enqueue adds a new task to the queue.
func (q *Queue) Enqueue(ctx context.Context, taskType string, payload map[string]interface{}, opts ...task.TaskOption) (*task.Task, error) {
	t := task.NewTask(taskType, payload)
	t.Apply(opts...)

	if err := q.storage.Enqueue(ctx, t); err != nil {
		return nil, err
	}

	return t, nil
}

// EnqueueDelayed adds a task to be executed after a specified delay.
func (q *Queue) EnqueueDelayed(ctx context.Context, taskType string, payload map[string]interface{}, delay time.Duration, opts ...task.TaskOption) (*task.Task, error) {
	opts = append(opts, task.WithDelay(delay))
	return q.Enqueue(ctx, taskType, payload, opts...)
}

// EnqueueAt schedules a task to be executed at a specific time.
func (q *Queue) EnqueueAt(ctx context.Context, taskType string, payload map[string]interface{}, at time.Time, opts ...task.TaskOption) (*task.Task, error) {
	opts = append(opts, task.WithScheduleAt(at))
	return q.Enqueue(ctx, taskType, payload, opts...)
}

// GetTask retrieves a task by ID.
func (q *Queue) GetTask(ctx context.Context, taskID string) (*task.Task, error) {
	return q.storage.GetTask(ctx, taskID)
}

// GetDeadLetterTasks retrieves tasks from the dead-letter queue.
func (q *Queue) GetDeadLetterTasks(ctx context.Context, limit int64) ([]*task.Task, error) {
	return q.storage.GetDeadLetterTasks(ctx, limit)
}

// RequeueDeadLetter moves a task from the DLQ back to the pending queue.
func (q *Queue) RequeueDeadLetter(ctx context.Context, taskID string) error {
	return q.storage.RequeueDeadLetterTask(ctx, taskID)
}

// Stats returns queue statistics.
func (q *Queue) Stats(ctx context.Context) (map[string]int64, error) {
	return q.storage.GetQueueStats(ctx)
}

// HealthCheck verifies the queue system is healthy.
func (q *Queue) HealthCheck(ctx context.Context) error {
	return q.storage.HealthCheck(ctx)
}

// Close closes the queue and releases resources.
func (q *Queue) Close() error {
	return q.storage.Close()
}

// Storage returns the underlying storage for advanced operations.
func (q *Queue) Storage() *storage.RedisStorage {
	return q.storage
}
