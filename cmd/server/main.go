// HTTP server providing API endpoints for the distributed task queue.
// This server exposes REST APIs for enqueueing tasks, checking status,
// monitoring queue health, and managing dead-letter queues.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ritikchawla/distributed-task-queue/internal/config"
	"github.com/ritikchawla/distributed-task-queue/pkg/queue"
	"github.com/ritikchawla/distributed-task-queue/pkg/task"
)

var q *queue.Queue
var logger *log.Logger

func main() {
	cfg := config.Load()
	logger = log.New(os.Stdout, "[server] ", log.LstdFlags)

	// Connect to queue
	var err error
	q, err = queue.New(queue.Config{
		RedisAddr:     cfg.RedisAddr,
		RedisPassword: cfg.RedisPassword,
		RedisDB:       cfg.RedisDB,
		KeyPrefix:     cfg.KeyPrefix,
	})
	if err != nil {
		log.Fatalf("Failed to connect to queue: %v", err)
	}
	defer q.Close()

	// Setup routes using standard routing
	mux := http.NewServeMux()

	// Health and stats
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/stats", handleStats)

	// Task operations
	mux.HandleFunc("/tasks", handleTasks)
	mux.HandleFunc("/tasks/", handleTaskByID)

	// Dead letter queue management
	mux.HandleFunc("/dlq", handleGetDLQ)
	mux.HandleFunc("/dlq/", handleDLQTask)

	// Create server
	addr := fmt.Sprintf(":%s", cfg.HTTPPort)
	server := &http.Server{
		Addr:         addr,
		Handler:      loggingMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		logger.Printf("Starting HTTP server on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	logger.Println("Server stopped")
}

// loggingMiddleware logs all incoming requests.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})
}

// EnqueueRequest represents the request body for task creation.
type EnqueueRequest struct {
	Type           string                 `json:"type"`
	Payload        map[string]interface{} `json:"payload"`
	Priority       string                 `json:"priority,omitempty"`
	DelaySeconds   int                    `json:"delay_seconds,omitempty"`
	ScheduleAt     string                 `json:"schedule_at,omitempty"`
	MaxRetries     int                    `json:"max_retries,omitempty"`
	TimeoutSeconds int                    `json:"timeout_seconds,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty"`
}

// APIResponse is the standard API response format.
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeSuccess(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, APIResponse{Success: true, Data: data})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, APIResponse{Success: false, Error: message})
}

// handleHealth returns the health status of the queue system.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := q.HealthCheck(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "Queue unhealthy: "+err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// handleStats returns queue statistics.
func handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	stats, err := q.Stats(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to get stats: "+err.Error())
		return
	}

	writeSuccess(w, stats)
}

// handleTasks handles POST /tasks for enqueueing new tasks.
func handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "Task type is required")
		return
	}

	// Build task options
	var opts []task.TaskOption

	// Priority
	switch req.Priority {
	case "low":
		opts = append(opts, task.WithPriority(task.PriorityLow))
	case "high":
		opts = append(opts, task.WithPriority(task.PriorityHigh))
	case "critical":
		opts = append(opts, task.WithPriority(task.PriorityCritical))
	}

	// Delay
	if req.DelaySeconds > 0 {
		opts = append(opts, task.WithDelay(time.Duration(req.DelaySeconds)*time.Second))
	}

	// Schedule at specific time
	if req.ScheduleAt != "" {
		t, err := time.Parse(time.RFC3339, req.ScheduleAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid schedule_at format (use RFC3339)")
			return
		}
		opts = append(opts, task.WithScheduleAt(t))
	}

	// Max retries
	if req.MaxRetries > 0 {
		opts = append(opts, task.WithMaxRetries(req.MaxRetries))
	}

	// Timeout
	if req.TimeoutSeconds > 0 {
		opts = append(opts, task.WithTimeout(time.Duration(req.TimeoutSeconds)*time.Second))
	}

	// Idempotency key
	if req.IdempotencyKey != "" {
		opts = append(opts, task.WithIdempotencyKey(req.IdempotencyKey))
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	t, err := q.Enqueue(ctx, req.Type, req.Payload, opts...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to enqueue task: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, APIResponse{Success: true, Data: t})
}

// handleTaskByID handles GET /tasks/{id} for retrieving task details.
func handleTaskByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Extract task ID from path: /tasks/{id}
	taskID := strings.TrimPrefix(r.URL.Path, "/tasks/")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "Task ID is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	t, err := q.GetTask(ctx, taskID)
	if err != nil {
		writeError(w, http.StatusNotFound, "Task not found: "+err.Error())
		return
	}

	writeSuccess(w, t)
}

// handleGetDLQ retrieves tasks from the dead-letter queue.
func handleGetDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	tasks, err := q.GetDeadLetterTasks(ctx, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to get DLQ tasks: "+err.Error())
		return
	}

	writeSuccess(w, map[string]interface{}{
		"count": len(tasks),
		"tasks": tasks,
	})
}

// handleDLQTask handles POST /dlq/{id}/requeue for requeuing dead tasks.
func handleDLQTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Extract task ID from path: /dlq/{id}/requeue
	path := strings.TrimPrefix(r.URL.Path, "/dlq/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "requeue" {
		writeError(w, http.StatusBadRequest, "Invalid path. Expected /dlq/{id}/requeue")
		return
	}

	taskID := parts[0]
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "Task ID is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := q.RequeueDeadLetter(ctx, taskID); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to requeue task: "+err.Error())
		return
	}

	writeSuccess(w, map[string]string{
		"message": "Task requeued successfully",
		"task_id": taskID,
	})
}
