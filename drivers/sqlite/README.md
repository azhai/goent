# SQLite
This is a no CGO sqlite driver for Goent ORM based on https://pkg.go.dev/modernc.org/sqlite.

## Features

- 🪝 Connection Hook
- 🧪 In Memory Database

## Usage

### Basic

```go
package main

import (
	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
)

type User struct {
	ID    int
	Name  string
	Email string `goe:"unique"`
}

type Database struct {
	User *User
	*goent.DB
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := sqlite.NewConfig(sqlite.Config{
		Logger: logger, IncludeArguments: true,
	})
	db, err := goent.Open[Database](sqlite.Open("goent.db", cfg))
}
```

### Connection Hook

```go
package main

import (
	"context"
	"os"
	"path/filepath"

	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
)

type User struct {
	ID    int
	Name  string
	Email string `goe:"unique"`
}

type Database struct {
	User *User
	*goent.DB
}

func main() {
	db, err := goent.Open[Database](sqlite.Open(filepath.Join(os.TempDir(), "goent.db"), sqlite.NewConfig(
		sqlite.Config{
			ConnectionHook: func(conn sqlite.ExecQuerierContext, dsn string) error {
                initSql := "PRAGMA foreign_keys = ON;"
                _, _ = conn.ExecContext(context.Background(), initSql, nil)
                return nil
			},
		},
	)))
}
```

### In Memory Database

```go
package main

import (
	"github.com/azhai/goent"
	"github.com/azhai/goent/drivers/sqlite"
)

type User struct {
	ID    int
	Name  string
	Email string `goe:"unique"`
}

type Database struct {
	User *User
	*goent.DB
}

func main() {
	cfg := sqlite.NewConfig(sqlite.Config{})
	db, err := goent.Open[Database](sqlite.OpenInMemory(cfg))
}
```