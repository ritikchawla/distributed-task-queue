// Example producer demonstrating how to enqueue tasks to the distributed queue.
// This file shows different ways to create and schedule tasks.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ritikchawla/distributed-task-queue/internal/config"
	"github.com/ritikchawla/distributed-task-queue/pkg/queue"
	"github.com/ritikchawla/distributed-task-queue/pkg/task"
)

func main() {
	cfg := config.Load()

	// Connect to the queue
	q, err := queue.New(queue.Config{
		RedisAddr:     cfg.RedisAddr,
		RedisPassword: cfg.RedisPassword,
		RedisDB:       cfg.RedisDB,
		KeyPrefix:     cfg.KeyPrefix,
	})
	if err != nil {
		log.Fatalf("Failed to connect to queue: %v", err)
	}
	defer q.Close()

	ctx := context.Background()

	fmt.Println("=== Distributed Task Queue - Example Producer ===")
	fmt.Println()

	// Example 1: Simple task with default priority
	fmt.Println("1. Enqueueing simple email task...")
	emailTask, err := q.Enqueue(ctx, "email.send", map[string]interface{}{
		"to":      "user@example.com",
		"subject": "Welcome to our platform!",
		"body":    "Thank you for signing up. We're excited to have you!",
	})
	if err != nil {
		log.Printf("Failed to enqueue email task: %v", err)
	} else {
		fmt.Printf("   Created task: %s\n", emailTask.ID)
	}

	// Example 2: High priority task
	fmt.Println("\n2. Enqueueing high priority notification...")
	notifTask, err := q.Enqueue(ctx, "notification.push", map[string]interface{}{
		"user_id": "user123",
		"title":   "Important Update",
		"message": "Your order has been shipped!",
	}, task.WithPriority(task.PriorityHigh))
	if err != nil {
		log.Printf("Failed to enqueue notification: %v", err)
	} else {
		fmt.Printf("   Created task: %s (priority: high)\n", notifTask.ID)
	}

	// Example 3: Critical priority task
	fmt.Println("\n3. Enqueueing critical security alert...")
	alertTask, err := q.Enqueue(ctx, "notification.push", map[string]interface{}{
		"user_id": "user456",
		"title":   "Security Alert",
		"message": "New login detected from unknown device",
	}, task.WithPriority(task.PriorityCritical))
	if err != nil {
		log.Printf("Failed to enqueue alert: %v", err)
	} else {
		fmt.Printf("   Created task: %s (priority: critical)\n", alertTask.ID)
	}

	// Example 4: Delayed task
	fmt.Println("\n4. Enqueueing delayed reminder (30 seconds)...")
	reminderTask, err := q.EnqueueDelayed(ctx, "email.send", map[string]interface{}{
		"to":      "user@example.com",
		"subject": "Don't forget!",
		"body":    "This is your scheduled reminder.",
	}, 30*time.Second)
	if err != nil {
		log.Printf("Failed to enqueue reminder: %v", err)
	} else {
		fmt.Printf("   Created task: %s (scheduled for: %s)\n", reminderTask.ID, reminderTask.ScheduledAt.Format(time.Kitchen))
	}

	// Example 5: Task scheduled for specific time
	fmt.Println("\n5. Enqueueing task for specific time (1 minute from now)...")
	scheduledTime := time.Now().Add(1 * time.Minute)
	reportTask, err := q.EnqueueAt(ctx, "report.generate", map[string]interface{}{
		"type":       "sales",
		"date_range": "last_30_days",
		"format":     "pdf",
	}, scheduledTime)
	if err != nil {
		log.Printf("Failed to enqueue report: %v", err)
	} else {
		fmt.Printf("   Created task: %s (scheduled for: %s)\n", reportTask.ID, scheduledTime.Format(time.Kitchen))
	}

	// Example 6: Task with custom retry settings
	fmt.Println("\n6. Enqueueing task with custom retry settings...")
	dataTask, err := q.Enqueue(ctx, "data.process", map[string]interface{}{
		"source":    "s3://bucket/data.csv",
		"operation": "transform",
	},
		task.WithMaxRetries(5),
		task.WithTimeout(5*time.Minute),
	)
	if err != nil {
		log.Printf("Failed to enqueue data task: %v", err)
	} else {
		fmt.Printf("   Created task: %s (max_retries: 5, timeout: 5m)\n", dataTask.ID)
	}

	// Example 7: Task with idempotency key (exactly-once guarantee)
	fmt.Println("\n7. Enqueueing task with idempotency key...")
	idempotentTask, err := q.Enqueue(ctx, "image.resize", map[string]interface{}{
		"url":    "https://example.com/image.jpg",
		"width":  800,
		"height": 600,
	}, task.WithIdempotencyKey("resize-image-123"))
	if err != nil {
		log.Printf("Failed to enqueue (or already exists): %v", err)
	} else {
		fmt.Printf("   Created task: %s (idempotency_key: resize-image-123)\n", idempotentTask.ID)
	}

	// Try the same idempotency key again - should fail
	fmt.Println("\n8. Trying duplicate idempotency key (should fail)...")
	_, err = q.Enqueue(ctx, "image.resize", map[string]interface{}{
		"url":    "https://example.com/image.jpg",
		"width":  800,
		"height": 600,
	}, task.WithIdempotencyKey("resize-image-123"))
	if err != nil {
		fmt.Printf("   Expected error: %v\n", err)
	}

	// Example 8: Batch enqueue multiple tasks
	fmt.Println("\n9. Batch enqueueing 5 low priority tasks...")
	for i := 1; i <= 5; i++ {
		_, err := q.Enqueue(ctx, "email.send", map[string]interface{}{
			"to":      fmt.Sprintf("batch-user-%d@example.com", i),
			"subject": fmt.Sprintf("Newsletter #%d", i),
			"body":    "This is your weekly newsletter.",
		}, task.WithPriority(task.PriorityLow))
		if err != nil {
			log.Printf("Failed to enqueue batch task %d: %v", i, err)
		} else {
			fmt.Printf("   Created batch task %d\n", i)
		}
	}

	// Display queue statistics
	fmt.Println("\n=== Queue Statistics ===")
	stats, err := q.Stats(ctx)
	if err != nil {
		log.Printf("Failed to get stats: %v", err)
	} else {
		fmt.Printf("   Pending:    %d\n", stats["pending"])
		fmt.Printf("   Scheduled:  %d\n", stats["scheduled"])
		fmt.Printf("   Processing: %d\n", stats["processing"])
		fmt.Printf("   Completed:  %d\n", stats["completed"])
		fmt.Printf("   Dead:       %d\n", stats["dead"])
	}

	fmt.Println("\n✓ All example tasks enqueued successfully!")
	fmt.Println("  Run the worker to process these tasks: go run cmd/worker/main.go")
}
