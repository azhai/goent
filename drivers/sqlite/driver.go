package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
	"modernc.org/sqlite"
)

// Driver implements the SQLite database driver.
type Driver struct {
	dsn string
	sql *sql.DB
	config
}

func (dr *Driver) GetDatabaseConfig() *model.DatabaseConfig {
	return &dr.config.DatabaseConfig
}

// ExecQuerierContext combines ExecerContext and QueryerContext interfaces.
type ExecQuerierContext interface {
	driver.ExecerContext
	driver.QueryerContext
}

// ConnectionHook is a callback function called after each connection is opened.
type ConnectionHook func(
	conn ExecQuerierContext,
	dsn string,
) error

type config struct {
	model.DatabaseConfig
	MigratePath    string
	ConnectionHook ConnectionHook
}

// Config contains SQLite driver configuration options.
type Config struct {
	Logger           model.Logger
	IncludeArguments bool          // include all arguments used on query
	QueryThreshold   time.Duration // query threshold to warning on slow queries

	MigratePath    string         // output sql file, if defined the driver will not auto apply the migration.
	ConnectionHook ConnectionHook // ConnectionHook is called after each connection is opened.
}

func NewConfig(c Config) config {
	return config{
		DatabaseConfig: model.DatabaseConfig{
			Logger:           c.Logger,
			IncludeArguments: c.IncludeArguments,
			QueryThreshold:   c.QueryThreshold,
		},
		MigratePath:    c.MigratePath,
		ConnectionHook: c.ConnectionHook,
	}
}

var lock = struct {
	sync.Mutex
}{sync.Mutex{}}

// OpenInMemory opens a in memory database.
func OpenInMemory(c config) (driver *Driver) {
	return Open("file:goe?mode=memory&cache=shared", c)
}

// Open opens a sqlite connection. By default uses "PRAGMA foreign_keys = ON;" and "PRAGMA busy_timeout = 5000;".
func Open(dsn string, c config) (driver *Driver) {
	return &Driver{
		dsn:    dsn,
		config: c,
	}
}

func OpenDSN(dsn string) (driver *Driver) {
	return Open(dsn, NewConfig(Config{}))
}

func (dr *Driver) AddLogger(logger model.Logger, err error) error {
	if logger != nil {
		dr.config.Logger = logger
		dr.config.IncludeArguments = true
	}
	return err
}

func (dr *Driver) Init() error {
	dr.DatabaseConfig.SetInitCallback(func() error {
		lock.Lock()
		defer lock.Unlock()
		var err error
		dr.setHooks()
		dr.sql, err = sql.Open("sqlite", dr.dsn)
		if err != nil {
			// logged by goe
			return err
		}

		return dr.sql.Ping()
	})
	return nil
}

func (dr *Driver) setHooks() {
	sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
		conn.ExecContext(context.Background(), "PRAGMA foreign_keys = ON;", nil)
		conn.ExecContext(context.Background(), "PRAGMA busy_timeout = 5000;", nil)
		return nil
	})
	if dr.ConnectionHook != nil {
		sqlite.RegisterConnectionHook(func(conn sqlite.ExecQuerierContext, dsn string) error {
			return dr.ConnectionHook(conn, dsn)
		})
	}
}

func (dr *Driver) KeywordHandler(s string) string {
	return keywordHandler(s)
}

func keywordHandler(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

func (dr *Driver) FormatTableName(schema, table string) string {
	return keywordHandler(table)
}

func (dr *Driver) SupportsReturning() bool {
	return false
}

func (dr *Driver) Name() string {
	return "SQLite"
}

func (dr *Driver) Stats() sql.DBStats {
	return dr.sql.Stats()
}

func (dr *Driver) Close() error {
	return dr.sql.Close()
}

var errMap = map[int][]error{
	1555: {goent.ErrBadRequest, goent.ErrUniqueValue},
	2067: {goent.ErrBadRequest, goent.ErrUniqueValue},
	787:  {goent.ErrBadRequest, goent.ErrForeignKey},
}

type wrapErrors struct {
	msg  string
	errs []error
}

func (e *wrapErrors) Error() string {
	return "goent: " + e.msg
}

func (e *wrapErrors) Unwrap() []error {
	return e.errs
}

func (dr *Driver) ErrorTranslator() func(err error) error {
	return func(err error) error {
		if sqliteError, ok := err.(*sqlite.Error); ok {
			return &wrapErrors{msg: err.Error(), errs: append(errMap[sqliteError.Code()], err)}
		}
		return err
	}
}

func (dr *Driver) NewConnection() model.Connection {
	return Connection{sql: dr.sql, config: dr.config, dsn: dr.dsn}
}

// Connection represents a SQLite database connection.
type Connection struct {
	dsn    string
	config config
	sql    *sql.DB
}

func (c Connection) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	return c.sql.QueryContext(ctx, query.RawSql, query.Arguments...)
}

func (c Connection) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	return c.sql.QueryRowContext(ctx, query.RawSql, query.Arguments...)
}

func (c Connection) ExecContext(ctx context.Context, query *model.Query) error {
	_, err := c.sql.ExecContext(ctx, query.RawSql, query.Arguments...)
	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (model.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, opts)
	return Transaction{tx: tx, config: dr.config, dsn: dr.dsn}, err
}

// Transaction represents a SQLite database transaction.
type Transaction struct {
	dsn    string
	config config
	tx     *sql.Tx
	saves  int64
}

func (t Transaction) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	return t.tx.QueryContext(ctx, query.RawSql, query.Arguments...)
}

func (t Transaction) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	return t.tx.QueryRowContext(ctx, query.RawSql, query.Arguments...)
}

func (t Transaction) ExecContext(ctx context.Context, query *model.Query) error {
	_, err := t.tx.ExecContext(ctx, query.RawSql, query.Arguments...)
	return err
}

func (t Transaction) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (t Transaction) Rollback() error {
	err := t.tx.Rollback()
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

// SavePoint represents a transaction savepoint for partial rollbacks.
type SavePoint struct {
	name string
	tx   Transaction
}

func (t Transaction) SavePoint() (model.SavePoint, error) {
	t.saves++
	point := "sp_" + strconv.FormatInt(t.saves, 10)
	_, err := t.tx.Exec("SAVEPOINT " + point)
	if err != nil {
		// goe can't log
		return nil, t.config.ErrorHandler(context.TODO(), err)
	}
	return SavePoint{point, t}, nil
}

func (s SavePoint) Rollback() error {
	_, err := s.tx.tx.Exec("ROLLBACK TO SAVEPOINT " + s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (s SavePoint) Commit() error {
	_, err := s.tx.tx.Exec("RELEASE SAVEPOINT " + s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}
