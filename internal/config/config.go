// Package config loads service configuration from environment variables.
//
// Load returns the settings every service shares; service-specific values
// are read in each main with String / Required / Duration / Int.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName     string
	HTTPPort        string
	LogLevel        string
	ShutdownTimeout time.Duration
}

// Load reads the settings shared by every service.
func Load(serviceName, defaultHTTPPort string) (Config, error) {
	timeout, err := Duration("SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	return Config{
		ServiceName:     serviceName,
		HTTPPort:        String("HTTP_PORT", defaultHTTPPort),
		LogLevel:        String("LOG_LEVEL", "info"),
		ShutdownTimeout: timeout,
	}, nil
}

// String returns the variable or fallback when it is unset or empty.
func String(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// Required returns the variable or an error when it is unset or empty.
func Required(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return v, nil
}

func Duration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return d, nil
}

func Int(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return n, nil
}
