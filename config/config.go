// Package config loads service configuration from a YAML file and environment
// variables. Environment variables override the file and use the APP_ prefix
// with "__" as the nesting separator, e.g. APP_POSTGRES__URL or
// APP_SERVER__PPROF_PORT.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	envPrefix      = "APP_"
	envConfigPath  = "CONFIG_PATH"
	defaultCfgPath = "config/config.yaml"
)

type Config struct {
	App       App       `koanf:"app"`
	Server    Server    `koanf:"server"`
	Postgres  Postgres  `koanf:"postgres"`
	Mongo     Mongo     `koanf:"mongo"`
	Valkey    Valkey    `koanf:"valkey"`
	Kafka     Kafka     `koanf:"kafka"`
	Temporal  Temporal  `koanf:"temporal"`
	Account   Account   `koanf:"account"`
	Telemetry Telemetry `koanf:"telemetry"`
}

type App struct {
	Name        string `koanf:"name"`
	Environment string `koanf:"environment"`
	Version     string `koanf:"version"`
}

type Server struct {
	Host            string        `koanf:"host"`
	Port            int           `koanf:"port"`
	ReadTimeout     time.Duration `koanf:"read_timeout"`
	WriteTimeout    time.Duration `koanf:"write_timeout"`
	ShutdownTimeout time.Duration `koanf:"shutdown_timeout"`
	CORSOrigins     []string      `koanf:"cors_origins"`
	// PprofPort exposes net/http/pprof on a separate admin listener. 0 disables it.
	PprofPort int `koanf:"pprof_port"`
}

type Postgres struct {
	URL         string `koanf:"url"`
	MaxConns    int32  `koanf:"max_conns"`
	AutoMigrate bool   `koanf:"auto_migrate"`
}

type Mongo struct {
	URI      string `koanf:"uri"`
	Database string `koanf:"database"`
}

type Valkey struct {
	URL string        `koanf:"url"`
	TTL time.Duration `koanf:"ttl"`
}

type Kafka struct {
	Brokers       []string `koanf:"brokers"`
	ConsumerGroup string   `koanf:"consumer_group"`
	Topics        Topics   `koanf:"topics"`
}

type Topics struct {
	ExampleCreated string `koanf:"example_created"`
}

type Temporal struct {
	HostPort  string `koanf:"host_port"`
	Namespace string `koanf:"namespace"`
	TaskQueue string `koanf:"task_queue"`
}

type Account struct {
	Address string        `koanf:"address"`
	Timeout time.Duration `koanf:"timeout"`
}

type Telemetry struct {
	Enabled      bool    `koanf:"enabled"`
	OTLPEndpoint string  `koanf:"otlp_endpoint"`
	SampleRatio  float64 `koanf:"sample_ratio"`
}

func Load() (*Config, error) {
	k := koanf.New(".")

	path := os.Getenv(envConfigPath)
	if path == "" {
		path = defaultCfgPath
	}
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("load config file %q: %w", path, err)
	}

	err := k.Load(env.Provider(".", env.Opt{
		Prefix: envPrefix,
		TransformFunc: func(key, value string) (string, any) {
			key = strings.ToLower(strings.TrimPrefix(key, envPrefix))
			key = strings.ReplaceAll(key, "__", ".")
			if strings.Contains(value, ",") {
				return key, strings.Split(value, ",")
			}
			return key, value
		},
	}), nil)
	if err != nil {
		return nil, fmt.Errorf("load env config: %w", err)
	}

	var cfg Config
	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	return &cfg, nil
}
