package drivers

import (
	"strings"

	"github.com/azhai/gobus/log"
	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// DatabaseConfig is the configuration for the database.
type DatabaseConfig struct {
	Type    string
	DSN     string
	LogFile string
}

// LoadConfig loads the database configuration from the environment.
func LoadConfig(env *utils.Environ, defaultDSN string) DatabaseConfig {
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
func OpenDialect(dbType, dbDSN string) model.Driver {
	if dbType == "pgsql" || dbType == "postgres" {
		return pgsql.OpenDSN(dbDSN)
	} else if dbType == "" && strings.HasPrefix(dbDSN, "postgres://") {
		return pgsql.OpenDSN(dbDSN)
	} else {
		_ = log.MakeDirForFile(dbDSN)
		return sqlite.OpenDSN(dbDSN)
	}
}
