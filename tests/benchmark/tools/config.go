package tools

import (
	"strings"

	"github.com/azhai/goent/utils"
)

// TODO: Add these ones to the .env file.
const (
	BulkInsertNumber     = 2000
	BatchSize            = 10000
	PageSize             = 10
	BulkInsertPageNumber = 100
	FindOneLoop          = 1
)

var PostgresDSN string

func init() {
	env := utils.NewEnvWithFile("../../../.env")
	if PostgresDSN = env.Get("POSTGRES_DSN"); PostgresDSN != "" {
		return
	}
	dbType := env.GetStr("GOE_DRIVER", "sqlite")
	dbType = strings.ToLower(dbType)
	if dbType == "pgsql" || dbType == "postgres" {
		PostgresDSN = env.Get("GOE_DATABASE_DSN")
	}
	if PostgresDSN == "" {
		panic("POSTGRES_DSN is required")
	}
}
