package config

import (
	"fmt"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

// SecretConfig represents a secret configuration for a task.
type SecretConfig struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
	Type  string `yaml:"type"`
}

// SecretConfigList is a list of SecretConfig.
type SecretConfigList []SecretConfig

// GetEnv returns a list of environment variables in correct format.
func (s SecretConfigList) GetEnv() []string {
	envs := make([]string, 0)

	for _, secret := range s {
		if secret.Type == "env" {
			envs = append(envs, secret.Name+"="+secret.Value)
		}
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
	PGConnStr string        `yaml:"pg_conn_str"`
	Queues    []QueueConfig `yaml:"queues"`
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
	return cfg, nil
}
