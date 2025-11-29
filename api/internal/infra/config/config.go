package config

import (
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Addr            string        `yaml:"addr"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`

	BaseDir string `yaml:"base_dir"`

	QueueCapacity int `yaml:"queue_capacity"`
	PoolSize      int `yaml:"pool_size"`

	TaskTTL          time.Duration `yaml:"task_ttl"`
	MaxUploadBytesMb int64         `yaml:"max_upload_mb"`

	Redis Redis `yaml:"redis"`
	MinIO MinIO `yaml:"minio"`
	NATS  NATS  `yaml:"nats"`
}

type Redis struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type MinIO struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	UseSSL          bool   `yaml:"use_ssl"`
	Bucket          string `yaml:"bucket"`
}

type NATS struct {
	URL           string `yaml:"url"`
	QueueName     string `yaml:"queue_name"`
	MaxReconnects int    `yaml:"max_reconnects"`
	Subject       string `yaml:"subject"`
}

func MustLoad(path string) *Config {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("config: cannot read file %q: %v", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("config: cannot unmarshal yaml: %v", err)
	}

	if cfg.Addr == "" {
		log.Fatalf("config: addr is empty")
	}
	if cfg.BaseDir == "" {
		log.Fatalf("config: base_dir is empty")
	}
	if cfg.NATS.Subject == "" {
		log.Fatalf("config: nats.subject is empty")
	}
	if cfg.TaskTTL <= 0 {
		log.Fatalf("config: task_ttl must be positive, got %s", cfg.TaskTTL)
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.MaxUploadBytesMb <= 0 {
		cfg.MaxUploadBytesMb = 50
	}

	return &cfg
}
