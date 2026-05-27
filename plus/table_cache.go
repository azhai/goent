package plus

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
)

// TableCache caches existing table names from the database.
// It avoids repeated information_schema queries on every cross-table operation.
type TableCache struct {
	mu        sync.RWMutex
	tables    map[string]bool
	refreshed time.Time
	ttl       time.Duration
}

// NewTableCache creates a new TableCache with the given TTL.
func NewTableCache(ttl time.Duration) *TableCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &TableCache{
		tables: make(map[string]bool),
		ttl:    ttl,
	}
}

// Exists returns true if the table is in the cache.
func (c *TableCache) Exists(table string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tables[table]
}

// Add adds table names to the cache.
func (c *TableCache) Add(tables ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tables {
		c.tables[t] = true
	}
}

// Remove removes table names from the cache.
func (c *TableCache) Remove(tables ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tables {
		delete(c.tables, t)
	}
}

// Match returns sorted table names matching the glob pattern.
func (c *TableCache) Match(pattern string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	prefix := strings.TrimSuffix(pattern, "*")
	var result []string
	for t := range c.tables {
		matched, _ := filepath.Match(pattern, t)
		if matched && strings.HasPrefix(t, prefix) {
			result = append(result, t)
		}
	}
	sort.Strings(result)
	return result
}

// Refresh loads all table names from the database into the cache.
func (c *TableCache) Refresh(ctx context.Context, db *goent.DB) error {
	conn := db.Driver().NewConnection()
	cfg := db.Driver().GetDatabaseConfig()
	driverName := db.DriverName()

	var sqlStr string
	switch driverName {
	case "PostgreSQL":
		sqlStr = "SELECT tablename FROM pg_tables WHERE schemaname = $1"
	default:
		sqlStr = "SELECT name FROM sqlite_master WHERE type = 'table'"
	}

	qr := model.CreateQuery(sqlStr, nil)
	rows, err := qr.WrapQuery(ctx, conn, cfg)
	if err != nil {
		return fmt.Errorf("refresh table cache: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		tables[name] = true
	}

	c.mu.Lock()
	c.tables = tables
	c.refreshed = time.Now()
	c.mu.Unlock()
	return nil
}

// IsExpired returns true if the cache TTL has passed.
func (c *TableCache) IsExpired() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.refreshed.IsZero() || time.Since(c.refreshed) > c.ttl
}

// Size returns the number of cached table names.
func (c *TableCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.tables)
}
