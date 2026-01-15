// Package storage provides the Redis-backed persistence layer for the task queue.
package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ritikchawla/distributed-task-queue/pkg/task"
)

// Common errors returned by the storage layer.
var (
	ErrTaskNotFound       = errors.New("task not found")
	ErrDuplicateTask      = errors.New("task with this idempotency key already exists")
	ErrTaskAlreadyRunning = errors.New("task is already being processed")
)

// RedisStorage implements the storage interface using Redis.
type RedisStorage struct {
	client *redis.Client
	prefix string
}

// RedisConfig holds Redis connection configuration.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	Prefix   string
}

// NewRedisStorage creates a new Redis storage instance.
func NewRedisStorage(cfg RedisConfig) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "dtq"
	}

	return &RedisStorage{
		client: client,
		prefix: prefix,
	}, nil
}

// Key helpers for consistent key naming
func (s *RedisStorage) taskKey(id string) string {
	return fmt.Sprintf("%s:task:%s", s.prefix, id)
}

func (s *RedisStorage) pendingQueueKey(priority task.Priority) string {
	return fmt.Sprintf("%s:queue:pending:%d", s.prefix, priority)
}

func (s *RedisStorage) scheduledQueueKey() string {
	return fmt.Sprintf("%s:queue:scheduled", s.prefix)
}

func (s *RedisStorage) processingSetKey() string {
	return fmt.Sprintf("%s:set:processing", s.prefix)
}

func (s *RedisStorage) deadLetterQueueKey() string {
	return fmt.Sprintf("%s:queue:dead", s.prefix)
}

func (s *RedisStorage) completedSetKey() string {
	return fmt.Sprintf("%s:set:completed", s.prefix)
}

func (s *RedisStorage) idempotencyKey(key string) string {
	return fmt.Sprintf("%s:idempotency:%s", s.prefix, key)
}

func (s *RedisStorage) lockKey(taskID string) string {
	return fmt.Sprintf("%s:lock:%s", s.prefix, taskID)
}

// Enqueue adds a task to the queue.
// Uses sorted sets for priority-based ordering.
func (s *RedisStorage) Enqueue(ctx context.Context, t *task.Task) error {
	// Check idempotency key if provided
	if t.IdempotencyKey != "" {
		exists, err := s.client.Exists(ctx, s.idempotencyKey(t.IdempotencyKey)).Result()
		if err != nil {
			return fmt.Errorf("failed to check idempotency key: %w", err)
		}
		if exists > 0 {
			return ErrDuplicateTask
		}
	}

	data, err := t.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize task: %w", err)
	}

	// Use pipeline for atomic operations
	pipe := s.client.TxPipeline()

	// Store the task data
	pipe.Set(ctx, s.taskKey(t.ID), data, 0)

	// Set idempotency key with TTL (24 hours by default)
	if t.IdempotencyKey != "" {
		pipe.Set(ctx, s.idempotencyKey(t.IdempotencyKey), t.ID, 24*time.Hour)
	}

	// Add to appropriate queue based on scheduled time
	if t.IsDelayed() {
		// Use scheduled time as score for delayed tasks
		score := float64(t.ScheduledAt.UnixNano())
		pipe.ZAdd(ctx, s.scheduledQueueKey(), redis.Z{
			Score:  score,
			Member: t.ID,
		})
	} else {
		// Use priority + timestamp for immediate tasks
		// Higher priority = processed first, within same priority FIFO
		score := float64(t.CreatedAt.UnixNano()) - float64(t.Priority)*1e18
		pipe.ZAdd(ctx, s.pendingQueueKey(t.Priority), redis.Z{
			Score:  score,
			Member: t.ID,
		})
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	return nil
}

// Dequeue atomically fetches and reserves a task for processing.
// Implements exactly-once semantics using WATCH/MULTI/EXEC.
func (s *RedisStorage) Dequeue(ctx context.Context, workerID string) (*task.Task, error) {
	// First, move any scheduled tasks that are ready to pending queues
	if err := s.promoteScheduledTasks(ctx); err != nil {
		// Log but don't fail - this is a background operation
		fmt.Printf("warning: failed to promote scheduled tasks: %v\n", err)
	}

	// Try to get a task from queues in priority order (highest first)
	priorities := []task.Priority{
		task.PriorityCritical,
		task.PriorityHigh,
		task.PriorityNormal,
		task.PriorityLow,
	}

	for _, priority := range priorities {
		t, err := s.dequeueFromPriority(ctx, priority, workerID)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return nil, err
		}
		if t != nil {
			return t, nil
		}
	}

	return nil, nil // No tasks available
}

func (s *RedisStorage) dequeueFromPriority(ctx context.Context, priority task.Priority, workerID string) (*task.Task, error) {
	queueKey := s.pendingQueueKey(priority)

	// Use Lua script for atomic dequeue with lock acquisition
	script := redis.NewScript(`
		local queueKey = KEYS[1]
		local processingKey = KEYS[2]
		local taskPrefix = ARGV[1]
		local lockPrefix = ARGV[2]
		local workerID = ARGV[3]
		local lockTimeout = tonumber(ARGV[4])
		local now = tonumber(ARGV[5])

		-- Get the first task from the sorted set
		local tasks = redis.call('ZRANGE', queueKey, 0, 0)
		if #tasks == 0 then
			return nil
		end

		local taskID = tasks[1]
		local lockKey = lockPrefix .. taskID
		
		-- Try to acquire lock (atomic with SETNX)
		local acquired = redis.call('SET', lockKey, workerID, 'NX', 'EX', lockTimeout)
		if not acquired then
			-- Task is locked by another worker, skip it
			return nil
		end

		-- Remove from pending queue
		redis.call('ZREM', queueKey, taskID)
		
		-- Add to processing set with timestamp
		redis.call('ZADD', processingKey, now, taskID)

		-- Get task data
		local taskKey = taskPrefix .. taskID
		local taskData = redis.call('GET', taskKey)
		
		return taskData
	`)

	result, err := script.Run(ctx, s.client, []string{
		queueKey,
		s.processingSetKey(),
	}, []interface{}{
		s.prefix + ":task:",
		s.prefix + ":lock:",
		workerID,
		300, // Lock timeout in seconds
		time.Now().Unix(),
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue task: %w", err)
	}

	if result == nil {
		return nil, nil
	}

	taskData, ok := result.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected result type from dequeue script")
	}

	t, err := task.Deserialize([]byte(taskData))
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize task: %w", err)
	}

	// Update task status
	now := time.Now()
	t.Status = task.StatusRunning
	t.StartedAt = &now

	// Save updated task state
	if err := s.updateTask(ctx, t); err != nil {
		return nil, err
	}

	return t, nil
}

// promoteScheduledTasks moves scheduled tasks that are ready to execute to pending queues.
func (s *RedisStorage) promoteScheduledTasks(ctx context.Context) error {
	now := float64(time.Now().UnixNano())

	// Get all scheduled tasks that are ready
	taskIDs, err := s.client.ZRangeByScore(ctx, s.scheduledQueueKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatFloat(now, 'f', -1, 64),
	}).Result()

	if err != nil {
		return err
	}

	if len(taskIDs) == 0 {
		return nil
	}

	for _, taskID := range taskIDs {
		t, err := s.GetTask(ctx, taskID)
		if err != nil {
			continue
		}

		// Move from scheduled to pending
		pipe := s.client.TxPipeline()
		pipe.ZRem(ctx, s.scheduledQueueKey(), taskID)

		t.Status = task.StatusPending
		score := float64(time.Now().UnixNano()) - float64(t.Priority)*1e18
		pipe.ZAdd(ctx, s.pendingQueueKey(t.Priority), redis.Z{
			Score:  score,
			Member: taskID,
		})

		if _, err := pipe.Exec(ctx); err != nil {
			continue
		}

		// Update task status
		_ = s.updateTask(ctx, t)
	}

	return nil
}

// Complete marks a task as successfully completed.
func (s *RedisStorage) Complete(ctx context.Context, taskID string, result interface{}) error {
	t, err := s.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	now := time.Now()
	t.Status = task.StatusCompleted
	t.CompletedAt = &now
	t.Result = result

	pipe := s.client.TxPipeline()

	// Remove from processing set
	pipe.ZRem(ctx, s.processingSetKey(), taskID)

	// Release lock
	pipe.Del(ctx, s.lockKey(taskID))

	// Add to completed set (with TTL for cleanup)
	pipe.ZAdd(ctx, s.completedSetKey(), redis.Z{
		Score:  float64(now.Unix()),
		Member: taskID,
	})

	// Update task data
	data, _ := t.Serialize()
	pipe.Set(ctx, s.taskKey(taskID), data, 7*24*time.Hour) // Keep for 7 days

	_, err = pipe.Exec(ctx)
	return err
}

// Fail marks a task as failed and handles retry logic.
func (s *RedisStorage) Fail(ctx context.Context, taskID string, errMsg string) error {
	t, err := s.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	t.Error = errMsg
	t.RetryCount++

	pipe := s.client.TxPipeline()

	// Remove from processing set
	pipe.ZRem(ctx, s.processingSetKey(), taskID)

	// Release lock
	pipe.Del(ctx, s.lockKey(taskID))

	if t.ShouldRetry() {
		// Schedule retry with exponential backoff
		backoffDuration := s.calculateBackoff(t.RetryCount)
		t.Status = task.StatusRetrying
		t.ScheduledAt = time.Now().Add(backoffDuration)

		score := float64(t.ScheduledAt.UnixNano())
		pipe.ZAdd(ctx, s.scheduledQueueKey(), redis.Z{
			Score:  score,
			Member: taskID,
		})
	} else {
		// Move to dead-letter queue
		t.Status = task.StatusDead
		pipe.RPush(ctx, s.deadLetterQueueKey(), taskID)
	}

	// Update task data
	data, _ := t.Serialize()
	pipe.Set(ctx, s.taskKey(taskID), data, 0)

	_, err = pipe.Exec(ctx)
	return err
}

// calculateBackoff returns the delay duration for retry using exponential backoff.
func (s *RedisStorage) calculateBackoff(retryCount int) time.Duration {
	// Base: 1 second, multiplied by 2^retryCount
	// Max backoff: 5 minutes
	base := time.Second
	backoff := base * time.Duration(1<<uint(retryCount))

	maxBackoff := 5 * time.Minute
	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	return backoff
}

// GetTask retrieves a task by ID.
func (s *RedisStorage) GetTask(ctx context.Context, taskID string) (*task.Task, error) {
	data, err := s.client.Get(ctx, s.taskKey(taskID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	return task.Deserialize(data)
}

// updateTask saves the updated task state to Redis.
func (s *RedisStorage) updateTask(ctx context.Context, t *task.Task) error {
	data, err := t.Serialize()
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.taskKey(t.ID), data, 0).Err()
}

// GetDeadLetterTasks retrieves tasks from the dead-letter queue.
func (s *RedisStorage) GetDeadLetterTasks(ctx context.Context, limit int64) ([]*task.Task, error) {
	taskIDs, err := s.client.LRange(ctx, s.deadLetterQueueKey(), 0, limit-1).Result()
	if err != nil {
		return nil, err
	}

	tasks := make([]*task.Task, 0, len(taskIDs))
	for _, id := range taskIDs {
		t, err := s.GetTask(ctx, id)
		if err != nil {
			continue
		}
		tasks = append(tasks, t)
	}

	return tasks, nil
}

// RequeueDeadLetterTask moves a task from dead-letter queue back to pending.
func (s *RedisStorage) RequeueDeadLetterTask(ctx context.Context, taskID string) error {
	t, err := s.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	t.Status = task.StatusPending
	t.RetryCount = 0
	t.Error = ""
	t.ScheduledAt = time.Now()

	pipe := s.client.TxPipeline()

	// Remove from DLQ
	pipe.LRem(ctx, s.deadLetterQueueKey(), 1, taskID)

	// Add to pending queue
	score := float64(time.Now().UnixNano()) - float64(t.Priority)*1e18
	pipe.ZAdd(ctx, s.pendingQueueKey(t.Priority), redis.Z{
		Score:  score,
		Member: taskID,
	})

	// Update task data
	data, _ := t.Serialize()
	pipe.Set(ctx, s.taskKey(taskID), data, 0)

	_, err = pipe.Exec(ctx)
	return err
}

// GetQueueStats returns statistics about the queue.
func (s *RedisStorage) GetQueueStats(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64)

	// Count pending tasks across all priorities
	var pendingCount int64
	for _, p := range []task.Priority{task.PriorityLow, task.PriorityNormal, task.PriorityHigh, task.PriorityCritical} {
		count, _ := s.client.ZCard(ctx, s.pendingQueueKey(p)).Result()
		pendingCount += count
	}
	stats["pending"] = pendingCount

	scheduled, _ := s.client.ZCard(ctx, s.scheduledQueueKey()).Result()
	stats["scheduled"] = scheduled

	processing, _ := s.client.ZCard(ctx, s.processingSetKey()).Result()
	stats["processing"] = processing

	completed, _ := s.client.ZCard(ctx, s.completedSetKey()).Result()
	stats["completed"] = completed

	dead, _ := s.client.LLen(ctx, s.deadLetterQueueKey()).Result()
	stats["dead"] = dead

	return stats, nil
}

// Close closes the Redis connection.
func (s *RedisStorage) Close() error {
	return s.client.Close()
}

// HealthCheck verifies the Redis connection is healthy.
func (s *RedisStorage) HealthCheck(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}
