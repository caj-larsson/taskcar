package config

import (
	"fmt"
	"log/slog"

	"os"

	"gopkg.in/yaml.v3"
)

// SecretConfig represents a secret configuration for a task.
type SecretConfig struct {
	// Name of the stored secret
	Name string `yaml:"name"`
	// Environment variable to bind the secret to
	EnvKey string `yaml:"env_key"`
}

// SecretConfigList is a list of SecretConfig.
type SecretConfigList []SecretConfig

// GetEnv returns a list of environment variables in correct format.
func (s SecretConfigList) GenerateEnvSlice(secrets map[string]string) []string {
	envs := make([]string, len(secrets))

	for i, secret := range s {
		envs[i] = fmt.Sprintf("%s=%s", secret.EnvKey, secrets[secret.Name])
	}
	return envs
}

// QueueConfig represents a configuration for a task queue.
type QueueConfig struct {
	QueueName string           `yaml:"name"`
	Command   string           `yaml:"command"`
	Secrets   SecretConfigList `yaml:"secrets"`
}

// Config represents the main configuration for the application.
type Config struct {
	PGConnStr      string        `yaml:"pg_conn_str"`
	LogLevelString string        `yaml:"log_level"`
	Queues         []QueueConfig `yaml:"queues"`
}

func (c Config) LogLevel() *slog.Level {
	var slogLevel slog.Level
	err := slogLevel.UnmarshalText([]byte(c.LogLevelString))
	if err != nil {
		slogLevel = slog.LevelWarn
	}
	return &slogLevel
}

// LoadConfig loads the configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	cfg := &Config{}

	slog.Debug("Loading config", "path", path)
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)

	if err != nil {
		slog.Error("Failed to open config file", "path", path, "error", err)
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}

	defer f.Close()

	err = yaml.NewDecoder(f).Decode(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode config file: %w", err)
	}

	if cfg.PGConnStr == "" {
		return nil, fmt.Errorf("pg_conn_str is required")
	}
	return cfg, nil
}
