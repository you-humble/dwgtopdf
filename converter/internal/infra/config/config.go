package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

const defaultCfgPath = "./configs/local.yaml"

type Config struct {
	GRPCAddr string `yaml:"grpc_addr"`
	BaseDir  string `yaml:"base_dir"`

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

func MustLoad() *Config {
	cfgPath := configPath()
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		log.Fatalf("config: cannot read file %q: %v", cfgPath, err)
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

func configPath() string {
	if v := os.Getenv("CONFIG_PATH"); v != "" {
		return v
	}
	return defaultCfgPath
}
