package drivers

import (
	"strings"

	"github.com/azhai/gobus/environ"
	"github.com/azhai/gobus/log"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
)

// DatabaseConfig is the configuration for the database.
type DatabaseConfig struct {
	Type    string
	DSN     string
	LogFile string
}

// LoadConfig loads the database configuration from the environment.
func LoadConfig(env *environ.Environ, defaultDSN string) DatabaseConfig {
	var logFile string
	if _, ok := env.Lookup("DB_LOG_FILE"); ok {
		logFile = env.Get("DB_LOG_FILE")
	} else {
		logFile = env.Get("LOG_FILE")
	}
	return DatabaseConfig{
		Type:    env.GetStr("DB_TYPE", "sqlite"),
		DSN:     env.GetStr("DB_DSN", defaultDSN),
		LogFile: logFile,
	}
}

// Connect connects to the database.
func Connect(cfg DatabaseConfig) (model.Driver, error) {
	drv := OpenDialect(cfg.Type, cfg.DSN)
	if drv == nil || cfg.LogFile == "" {
		return drv, nil
	}
	logger, err := log.NewDailyLogger(cfg.LogFile, 7)
	err = drv.AddLogger(logger, err)
	return drv, err
}

// OpenDialect opens a database driver.
//
// PostgreSQL 分支走 openPgDialect：默认构建是真实驱动，`-tags nopgsql` 构建换成
// 返回 nil 的空实现，且 pgsql 包不参与编译（pgx 不进二进制）。
func OpenDialect(dbType, dbDSN string) model.Driver {
	if dbType == "pgsql" || dbType == "postgres" {
		return openPgDialect(dbDSN)
	} else if dbType == "" && strings.HasPrefix(dbDSN, "postgres://") {
		return openPgDialect(dbDSN)
	} else {
		_ = log.MakeDirForFile(dbDSN)
		return sqlite.OpenDSN(dbDSN)
	}
}
