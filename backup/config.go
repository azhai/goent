// Package backup provides database backup and restore utilities for goent.
//
// It supports two database backends:
//   - SQLite: full backups by copying the database file.
//   - PostgreSQL: full backups using pg_dump schema + COPY data, and
//     incremental backups using JSON Lines for rows changed since a base time.
package backup

import (
	"time"
)

// Config customizes backup and restore behaviour.
type Config struct {
	// Dir is the directory where backup archives are stored.
	// If empty, the current working directory is used.
	Dir string

	// DSN is the database connection string used by external tools such as
	// pg_dump, psql, and for locating the SQLite database file.
	DSN string

	// DBPath is the explicit SQLite database file path. If empty, the engine
	// attempts to derive it from DSN.
	DBPath string

	// Schema is the PostgreSQL schema to back up. Defaults to "public".
	Schema string

	// LogTables are tables whose data should not be backed up.
	// Only structure is preserved for these tables.
	LogTables []string

	// ExcludedFromIncremental are tables that should be skipped in incremental
	// backups. Usually this includes log tables and the backup metadata table.
	ExcludedFromIncremental []string
}

// Result contains metadata about a created backup archive.
type Result struct {
	Path        string
	Size        int64
	StartedAt   *time.Time
	CompletedAt *time.Time
	Err         error
}

func (c *Config) schema() string {
	if c.Schema != "" {
		return c.Schema
	}
	return "public"
}

func (c *Config) logTables() []string {
	return c.LogTables
}

func (c *Config) excludedFromIncremental() []string {
	return c.ExcludedFromIncremental
}

func (c *Config) isLogTable(table string) bool {
	for _, t := range c.LogTables {
		if t == table {
			return true
		}
	}
	return false
}

func (c *Config) isExcludedFromIncremental(table string) bool {
	for _, t := range c.ExcludedFromIncremental {
		if t == table {
			return true
		}
	}
	return false
}
