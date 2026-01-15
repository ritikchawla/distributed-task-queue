// Example worker demonstrating how to use the distributed task queue.
// This file shows how to register handlers and process various task types.
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/ritikchawla/distributed-task-queue/internal/config"
	"github.com/ritikchawla/distributed-task-queue/pkg/task"
	"github.com/ritikchawla/distributed-task-queue/pkg/worker"
)

func main() {
	cfg := config.Load()

	// Create worker pool with configuration
	pool, err := worker.NewPool(worker.PoolConfig{
		Concurrency:   cfg.WorkerConcurrency,
		RedisAddr:     cfg.RedisAddr,
		RedisPassword: cfg.RedisPassword,
		RedisDB:       cfg.RedisDB,
		KeyPrefix:     cfg.KeyPrefix,
		PollInterval:  cfg.PollInterval,
	})
	if err != nil {
		log.Fatalf("Failed to create worker pool: %v", err)
	}

	// Register handlers for different task types
	pool.RegisterHandler("email.send", handleSendEmail)
	pool.RegisterHandler("image.resize", handleImageResize)
	pool.RegisterHandler("data.process", handleDataProcess)
	pool.RegisterHandler("notification.push", handlePushNotification)
	pool.RegisterHandler("report.generate", handleGenerateReport)

	// Start the worker pool
	ctx := context.Background()
	if err := pool.Start(ctx); err != nil {
		log.Fatalf("Failed to start worker pool: %v", err)
	}

	log.Printf("Worker pool started with %d workers", cfg.WorkerConcurrency)
	log.Println("Press Ctrl+C to shutdown...")

	// Wait for interrupt signal
	pool.WaitForSignal()
}

// handleSendEmail processes email sending tasks.
func handleSendEmail(ctx context.Context, t *task.Task) (interface{}, error) {
	to, _ := t.Payload["to"].(string)
	subject, _ := t.Payload["subject"].(string)
	body, _ := t.Payload["body"].(string)

	log.Printf("Sending email to: %s, subject: %s", to, subject)

	// Simulate email sending with some processing time
	time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)

	// Simulate occasional failures for retry demonstration
	if rand.Float32() < 0.1 { // 10% failure rate
		return nil, fmt.Errorf("SMTP connection failed")
	}

	return map[string]interface{}{
		"status":    "sent",
		"to":        to,
		"subject":   subject,
		"body_size": len(body),
		"sent_at":   time.Now().Format(time.RFC3339),
	}, nil
}

// handleImageResize processes image resizing tasks.
func handleImageResize(ctx context.Context, t *task.Task) (interface{}, error) {
	imageURL, _ := t.Payload["url"].(string)
	width, _ := t.Payload["width"].(float64)
	height, _ := t.Payload["height"].(float64)

	log.Printf("Resizing image: %s to %dx%d", imageURL, int(width), int(height))

	// Simulate image processing
	time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)

	return map[string]interface{}{
		"original_url": imageURL,
		"resized_url":  fmt.Sprintf("%s?w=%d&h=%d", imageURL, int(width), int(height)),
		"dimensions": map[string]int{
			"width":  int(width),
			"height": int(height),
		},
	}, nil
}

// handleDataProcess processes data transformation tasks.
func handleDataProcess(ctx context.Context, t *task.Task) (interface{}, error) {
	dataSource, _ := t.Payload["source"].(string)
	operation, _ := t.Payload["operation"].(string)

	log.Printf("Processing data from %s with operation: %s", dataSource, operation)

	// Simulate data processing
	time.Sleep(time.Duration(800+rand.Intn(1200)) * time.Millisecond)

	recordsProcessed := rand.Intn(10000) + 1000

	return map[string]interface{}{
		"source":            dataSource,
		"operation":         operation,
		"records_processed": recordsProcessed,
		"processing_time":   "1.2s",
	}, nil
}

// handlePushNotification sends push notifications to devices.
func handlePushNotification(ctx context.Context, t *task.Task) (interface{}, error) {
	userID, _ := t.Payload["user_id"].(string)
	title, _ := t.Payload["title"].(string)
	message, _ := t.Payload["message"].(string)

	log.Printf("Sending push notification to user %s: %s", userID, title)

	// Simulate notification delivery
	time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)

	return map[string]interface{}{
		"user_id":      userID,
		"title":        title,
		"message":      message,
		"delivered_at": time.Now().Format(time.RFC3339),
	}, nil
}

// handleGenerateReport generates reports asynchronously.
func handleGenerateReport(ctx context.Context, t *task.Task) (interface{}, error) {
	reportType, _ := t.Payload["type"].(string)
	dateRange, _ := t.Payload["date_range"].(string)
	format, _ := t.Payload["format"].(string)

	log.Printf("Generating %s report for %s in %s format", reportType, dateRange, format)

	// Simulate report generation (this can be long-running)
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)

	reportID := fmt.Sprintf("report_%d", time.Now().UnixNano())

	return map[string]interface{}{
		"report_id":    reportID,
		"type":         reportType,
		"date_range":   dateRange,
		"format":       format,
		"download_url": fmt.Sprintf("/reports/%s.%s", reportID, format),
		"generated_at": time.Now().Format(time.RFC3339),
	}, nil
}
