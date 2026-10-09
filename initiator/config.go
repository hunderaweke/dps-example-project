package initiator

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/username/example-service/config"
)

func loadConfig() (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}

func isDevelopment(cfg *config.Config) bool {
	env := strings.ToLower(cfg.App.Environment)
	return env == "" || env == "development" || env == "dev" || env == "local"
}

func NewLogger(cfg *config.Config) (*zap.Logger, error) {
	var zc zap.Config
	if isDevelopment(cfg) {
		zc = zap.NewDevelopmentConfig()
		zc.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		zc = zap.NewProductionConfig()
	}
	logger, err := zc.Build()
	if err != nil {
		return nil, fmt.Errorf("build logger: %w", err)
	}
	return logger.With(zap.String("service", cfg.App.Name), zap.String("version", cfg.App.Version)), nil
}
