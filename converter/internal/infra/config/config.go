package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	BaseDir string `yaml:"base_dir"`

	QueueCapacity int `yaml:"queue_capacity"`
	PoolSize      int `yaml:"pool_size"`

	MinIO MinIO `yaml:"minio"`
}

type MinIO struct {
	Endpoint        string `yaml:"endpoint"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	UseSSL          bool   `yaml:"use_ssl"`
	Bucket          string `yaml:"bucket"`
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

	if cfg.BaseDir == "" {
		log.Fatalf("config: base_dir is empty")
	}

	return &cfg
}
