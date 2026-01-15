// Package config provides configuration management for the distributed task queue.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds application configuration.
type Config struct {
	// Redis settings
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Worker settings
	WorkerConcurrency int
	PollInterval      time.Duration

	// Queue settings
	KeyPrefix         string
	DefaultMaxRetries int
	DefaultTimeout    time.Duration

	// Server settings
	HTTPPort string
}

// Load loads configuration from environment variables with defaults.
func Load() *Config {
	cfg := &Config{
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     getEnv("REDIS_PASSWORD", ""),
		RedisDB:           getEnvInt("REDIS_DB", 0),
		WorkerConcurrency: getEnvInt("WORKER_CONCURRENCY", 4),
		PollInterval:      getEnvDuration("POLL_INTERVAL", 100*time.Millisecond),
		KeyPrefix:         getEnv("KEY_PREFIX", "dtq"),
		DefaultMaxRetries: getEnvInt("DEFAULT_MAX_RETRIES", 3),
		DefaultTimeout:    getEnvDuration("DEFAULT_TIMEOUT", 30*time.Second),
		HTTPPort:          getEnv("HTTP_PORT", "8080"),
	}

	return cfg
}

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultValue
}
