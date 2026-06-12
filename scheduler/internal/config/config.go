package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Tokens    []string        `yaml:"tokens"`
	Log       LogConfig       `yaml:"log"`
}

type ServerConfig struct {
	ListenAddr          string        `yaml:"listen_addr"`
	ReadTimeout         time.Duration `yaml:"read_timeout"`
	WriteTimeout        time.Duration `yaml:"write_timeout"`
	ShutdownGracePeriod time.Duration `yaml:"shutdown_grace_period"`
}

type SchedulerConfig struct {
	WorkerCooldown      time.Duration `yaml:"worker_cooldown"`
	TaskRunTimeout      time.Duration `yaml:"task_run_timeout"`
	TaskAckTimeout      time.Duration `yaml:"task_ack_timeout"`
	HeartbeatInterval   time.Duration `yaml:"heartbeat_interval"`
	HeartbeatTimeout    time.Duration `yaml:"heartbeat_timeout"`
	PollInterval        time.Duration `yaml:"poll_interval"`
	DefaultTaskTimeout  time.Duration `yaml:"default_task_timeout"`
	DefaultTaskDeadline time.Duration `yaml:"default_task_deadline"`
}

type LogConfig struct {
	Level string `yaml:"level"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Server: ServerConfig{
			ListenAddr:          ":8080",
			ReadTimeout:         15 * time.Second,
			WriteTimeout:        180 * time.Second,
			ShutdownGracePeriod: 30 * time.Second,
		},
		Scheduler: SchedulerConfig{
			WorkerCooldown:      30 * time.Minute,
			TaskRunTimeout:      120 * time.Second,
			TaskAckTimeout:      3 * time.Second,
			HeartbeatInterval:   10 * time.Second,
			HeartbeatTimeout:    30 * time.Second,
			PollInterval:        200 * time.Millisecond,
			DefaultTaskTimeout:  120 * time.Second,
			DefaultTaskDeadline: 10 * time.Minute,
		},
		Log: LogConfig{Level: "info"},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
