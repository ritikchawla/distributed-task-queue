// Package worker provides the worker pool implementation for processing tasks.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/ritikchawla/distributed-task-queue/pkg/storage"
	"github.com/ritikchawla/distributed-task-queue/pkg/task"
)

// Handler is a function that processes a task.
// It receives the task and should return a result and/or error.
type Handler func(ctx context.Context, t *task.Task) (interface{}, error)

// Worker represents a single worker that processes tasks.
type Worker struct {
	id           string
	storage      *storage.RedisStorage
	handlers     map[string]Handler
	shutdown     chan struct{}
	wg           sync.WaitGroup
	logger       *log.Logger
	pollInterval time.Duration
}

// Pool manages a pool of workers with configurable concurrency.
type Pool struct {
	storage      *storage.RedisStorage
	handlers     map[string]Handler
	concurrency  int
	workers      []*Worker
	shutdown     chan struct{}
	wg           sync.WaitGroup
	logger       *log.Logger
	mu           sync.RWMutex
	pollInterval time.Duration
}

// PoolConfig holds worker pool configuration.
type PoolConfig struct {
	// Number of concurrent workers
	Concurrency int
	// Redis connection settings
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	KeyPrefix     string
	// Polling interval for checking new tasks
	PollInterval time.Duration
	// Custom logger (optional)
	Logger *log.Logger
}

// DefaultPoolConfig returns configuration with sensible defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		Concurrency:   4,
		RedisAddr:     "localhost:6379",
		RedisPassword: "",
		RedisDB:       0,
		KeyPrefix:     "dtq",
		PollInterval:  100 * time.Millisecond,
		Logger:        log.New(os.Stdout, "[worker] ", log.LstdFlags),
	}
}

// NewPool creates a new worker pool.
func NewPool(cfg PoolConfig) (*Pool, error) {
	storage, err := storage.NewRedisStorage(storage.RedisConfig{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		Prefix:   cfg.KeyPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to storage: %w", err)
	}

	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "[worker] ", log.LstdFlags)
	}

	return &Pool{
		storage:      storage,
		handlers:     make(map[string]Handler),
		concurrency:  cfg.Concurrency,
		workers:      make([]*Worker, 0, cfg.Concurrency),
		shutdown:     make(chan struct{}),
		logger:       logger,
		pollInterval: cfg.PollInterval,
	}, nil
}

// RegisterHandler registers a handler for a specific task type.
func (p *Pool) RegisterHandler(taskType string, handler Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[taskType] = handler
	p.logger.Printf("Registered handler for task type: %s", taskType)
}

// Start begins processing tasks with the configured number of workers.
func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.handlers) == 0 {
		return errors.New("no handlers registered")
	}

	p.logger.Printf("Starting worker pool with %d workers", p.concurrency)

	// Create and start workers
	for i := 0; i < p.concurrency; i++ {
		w := &Worker{
			id:           uuid.New().String()[:8],
			storage:      p.storage,
			handlers:     p.handlers,
			shutdown:     p.shutdown,
			logger:       p.logger,
			pollInterval: p.pollInterval,
		}
		p.workers = append(p.workers, w)

		p.wg.Add(1)
		go func(worker *Worker) {
			defer p.wg.Done()
			worker.run(ctx)
		}(w)
	}

	return nil
}

// Stop gracefully shuts down all workers.
func (p *Pool) Stop() {
	p.logger.Println("Shutting down worker pool...")
	close(p.shutdown)
	p.wg.Wait()
	p.storage.Close()
	p.logger.Println("Worker pool stopped")
}

// WaitForSignal blocks until an interrupt signal is received.
func (p *Pool) WaitForSignal() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	p.Stop()
}

// run is the main loop for a worker.
func (w *Worker) run(ctx context.Context) {
	w.logger.Printf("Worker %s started", w.id)

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.shutdown:
			w.logger.Printf("Worker %s shutting down", w.id)
			return
		case <-ctx.Done():
			w.logger.Printf("Worker %s context cancelled", w.id)
			return
		case <-ticker.C:
			if err := w.processNext(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					w.logger.Printf("Worker %s error: %v", w.id, err)
				}
			}
		}
	}
}

// processNext fetches and processes a single task.
func (w *Worker) processNext(ctx context.Context) error {
	// Fetch next available task
	t, err := w.storage.Dequeue(ctx, w.id)
	if err != nil {
		return fmt.Errorf("failed to dequeue task: %w", err)
	}

	if t == nil {
		// No tasks available
		return nil
	}

	w.logger.Printf("Worker %s processing task %s (type: %s, attempt: %d/%d)",
		w.id, t.ID[:8], t.Type, t.RetryCount+1, t.MaxRetries+1)

	// Find handler for task type
	handler, ok := w.handlers[t.Type]
	if !ok {
		errMsg := fmt.Sprintf("no handler registered for task type: %s", t.Type)
		w.logger.Printf("Worker %s: %s", w.id, errMsg)
		return w.storage.Fail(ctx, t.ID, errMsg)
	}

	// Execute handler with timeout
	result, err := w.executeWithTimeout(ctx, t, handler)
	if err != nil {
		w.logger.Printf("Worker %s: task %s failed: %v", w.id, t.ID[:8], err)
		return w.storage.Fail(ctx, t.ID, err.Error())
	}

	w.logger.Printf("Worker %s: task %s completed successfully", w.id, t.ID[:8])
	return w.storage.Complete(ctx, t.ID, result)
}

// executeWithTimeout runs the handler with task timeout and panic recovery.
func (w *Worker) executeWithTimeout(ctx context.Context, t *task.Task, handler Handler) (result interface{}, err error) {
	// Create timeout context
	timeout := t.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Channel to receive result
	done := make(chan struct{})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic recovered: %v\n%s", r, debug.Stack())
			}
			close(done)
		}()

		result, err = handler(taskCtx, t)
	}()

	select {
	case <-done:
		return result, err
	case <-taskCtx.Done():
		if errors.Is(taskCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("task execution timed out after %v", timeout)
		}
		return nil, taskCtx.Err()
	}
}

// Stats returns statistics about the worker pool.
func (p *Pool) Stats(ctx context.Context) (map[string]interface{}, error) {
	queueStats, err := p.storage.GetQueueStats(ctx)
	if err != nil {
		return nil, err
	}

	stats := map[string]interface{}{
		"workers":    p.concurrency,
		"pending":    queueStats["pending"],
		"scheduled":  queueStats["scheduled"],
		"processing": queueStats["processing"],
		"completed":  queueStats["completed"],
		"dead":       queueStats["dead"],
	}

	return stats, nil
}
