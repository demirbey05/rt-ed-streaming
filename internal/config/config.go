// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	ServiceName     string
	HTTPPort        string
	DatabaseURL     string
	LogLevel        string
	ShutdownTimeout time.Duration
}

// Load reads configuration from the environment. DATABASE_URL is required.
func Load(serviceName string) (Config, error) {
	cfg := Config{
		ServiceName: serviceName,
		HTTPPort:    getEnv("HTTP_PORT", "8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
	}

	timeout, err := time.ParseDuration(getEnv("SHUTDOWN_TIMEOUT", "10s"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid SHUTDOWN_TIMEOUT: %w", err)
	}
	cfg.ShutdownTimeout = timeout

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}
