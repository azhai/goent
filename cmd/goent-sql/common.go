package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/azhai/goent/utils"
)

// DBConfig holds database connection configuration
type DBConfig struct {
	DSN    string
	IsPg   bool
	Driver string
	DBType string
}

// NewEnvSafe loads .env file safely
func NewEnvSafe() *utils.Environ {
	filename := ".env"
	if _, err := os.Stat(filename); err != nil {
		return &utils.Environ{}
	}
	defer func() {
		if r := recover(); r != nil {
		}
	}()
	return utils.NewEnvWithFile(filename)
}

// IsPostgresDSN checks if the DSN points to a PostgreSQL database
func IsPostgresDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}

// ToDBConfig creates a DBConfig from DSN and dbType strings
func ToDBConfig(dsn, dbType string) (DBConfig, error) {
	if dsn == "" {
		return DBConfig{}, fmt.Errorf("DSN is required")
	}
	isPg := IsPostgresDSN(dsn)
	driver := "sqlite"
	if isPg {
		driver = "pgsql"
	}
	if dbType == "pgsql" || dbType == "postgres" {
		return DBConfig{DSN: dsn, IsPg: true, Driver: "pgsql", DBType: dbType}, nil
	}
	if dbType != "" {
		return DBConfig{DSN: dsn, IsPg: false, Driver: "sqlite", DBType: dbType}, nil
	}
	return DBConfig{DSN: dsn, IsPg: isPg, Driver: driver, DBType: driver}, nil
}
