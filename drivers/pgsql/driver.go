package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/azhai/goent/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Rows struct {
	pgx.Rows
}

func (rs *Rows) Close() error {
	rs.Rows.Close()
	return rs.Err()
}

type Row struct {
	pgx.Row
}

func (r Row) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return sql.ErrNoRows
	}
	return err
}

// Driver implements the PostgreSQL database driver using pgx.
type Driver struct {
	dsn string
	sql *pgxpool.Pool
	config
}

func (dr *Driver) GetDatabaseConfig() *model.DatabaseConfig {
	return &dr.config.DatabaseConfig
}

type config struct {
	model.DatabaseConfig
	MigratePath string
}

// Config contains PostgreSQL driver configuration options.
type Config struct {
	Logger           model.Logger
	IncludeArguments bool          // include all arguments used on query
	QueryThreshold   time.Duration // query threshold to warning on slow queries

	MigratePath string // output sql file, if defined the driver will not auto apply the migration.
}

func NewConfig(c Config) config {
	return config{
		DatabaseConfig: model.DatabaseConfig{
			Logger:           c.Logger,
			IncludeArguments: c.IncludeArguments,
			QueryThreshold:   c.QueryThreshold,
		},
		MigratePath: c.MigratePath,
	}
}

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
	config, err := pgxpool.ParseConfig(dr.dsn)
	if err != nil {
		// logged by goe
		return err
	}

	dr.sql, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		// logged by goe
		return err
	}

	return dr.sql.Ping(context.Background())
}

func (dr *Driver) KeywordHandler(s string) string {
	return keywordHandler(s)
}

func keywordHandler(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

func (dr *Driver) FormatTableName(schema, table string) string {
	if schema != "" {
		return keywordHandler(schema) + "." + keywordHandler(table)
	}
	return keywordHandler(table)
}

func (dr *Driver) SupportsReturning() bool {
	return true
}

func (dr *Driver) Name() string {
	return "PostgreSQL"
}

func (dr *Driver) Stats() sql.DBStats {
	stat := dr.sql.Stat()
	return sql.DBStats{
		MaxOpenConnections: int(stat.MaxConns()),           // Max connections allowed
		OpenConnections:    int(stat.AcquiredConns()),      // Currently acquired (open) connections
		InUse:              int(stat.AcquiredConns()),      // Connections currently in use
		Idle:               int(stat.IdleConns()),          // Connections in idle state
		WaitCount:          stat.AcquireCount(),            // Total successful connection acquisitions
		WaitDuration:       stat.AcquireDuration(),         // Time spent waiting for a connection
		MaxIdleClosed:      stat.MaxIdleDestroyCount(),     // Connections closed due to idle timeout
		MaxLifetimeClosed:  stat.MaxLifetimeDestroyCount(), // Connections closed due to max lifetime
	}
}

func (dr *Driver) Close() error {
	dr.sql.Close()
	return nil
}

var errMap = map[string][]error{
	"23505": {model.ErrBadRequest, model.ErrUniqueValue},
	"23503": {model.ErrBadRequest, model.ErrForeignKey},
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
		if pgError, ok := err.(*pgconn.PgError); ok {
			return &wrapErrors{msg: err.Error(), errs: append(errMap[pgError.Code], err)}
		}
		return err
	}
}

func (dr *Driver) NewConnection() model.Connection {
	return Connection{sql: dr.sql, config: dr.config}
}

type Connection struct {
	config config
	sql    *pgxpool.Pool
}

func (c Connection) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	rows, err := c.sql.Query(ctx, query.RawSql, query.Arguments...)
	return &Rows{rows}, err
}

func (c Connection) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	return Row{c.sql.QueryRow(ctx, query.RawSql, query.Arguments...)}
}

func (c Connection) ExecContext(ctx context.Context, query *model.Query) error {
	_, err := c.sql.Exec(ctx, query.RawSql, query.Arguments...)
	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (model.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, convertTxOptions(opts))
	return Transaction{tx: tx, config: dr.config}, err
}

type Transaction struct {
	config config
	tx     pgx.Tx
	saves  int64
}

func (t Transaction) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	rows, err := t.tx.Query(ctx, query.RawSql, query.Arguments...)
	return &Rows{rows}, err
}

func (t Transaction) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	return Row{t.tx.QueryRow(ctx, query.RawSql, query.Arguments...)}
}

func (t Transaction) ExecContext(ctx context.Context, query *model.Query) error {
	_, err := t.tx.Exec(ctx, query.RawSql, query.Arguments...)
	return err
}

func (t Transaction) Commit() error {
	err := t.tx.Commit(context.TODO())
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (t Transaction) Rollback() error {
	err := t.tx.Rollback(context.Background())
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

type SavePoint struct {
	name string
	tx   Transaction
}

func (t Transaction) SavePoint() (model.SavePoint, error) {
	t.saves++
	point := "sp_" + strconv.FormatInt(t.saves, 10)
	_, err := t.tx.Exec(context.TODO(), "SAVEPOINT "+point)
	if err != nil {
		// goe can't log
		return nil, t.config.ErrorHandler(context.TODO(), err)
	}
	return SavePoint{point, t}, nil
}

func (s SavePoint) Rollback() error {
	_, err := s.tx.tx.Exec(context.TODO(), "ROLLBACK TO SAVEPOINT "+s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (s SavePoint) Commit() error {
	_, err := s.tx.tx.Exec(context.TODO(), "RELEASE SAVEPOINT "+s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func convertTxOptions(sqlOpts *sql.TxOptions) pgx.TxOptions {
	var isoLevel pgx.TxIsoLevel

	switch sqlOpts.Isolation {
	case sql.LevelDefault:
		isoLevel = pgx.ReadCommitted // Default for PostgreSQL
	case sql.LevelReadUncommitted:
		isoLevel = pgx.ReadUncommitted
	case sql.LevelReadCommitted:
		isoLevel = pgx.ReadCommitted
	case sql.LevelRepeatableRead:
		isoLevel = pgx.RepeatableRead
	case sql.LevelSerializable:
		isoLevel = pgx.Serializable
	default:
		isoLevel = pgx.Serializable
	}

	return pgx.TxOptions{
		IsoLevel: isoLevel,
		AccessMode: func() pgx.TxAccessMode {
			if sqlOpts.ReadOnly {
				return pgx.ReadOnly
			}
			return pgx.ReadWrite
		}(),
	}
}
