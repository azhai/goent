package goent

import (
	"context"

	"github.com/azhai/goent/drivers/pgsql"
	"github.com/azhai/goent/drivers/sqlite"
	"github.com/azhai/goent/model"
)

type SchemaOps struct {
	model.Structurer
	DB *DB
}

func NewSchemaOps(db *DB) *SchemaOps {
	return NewSchemaOpsWithSchema(db, "")
}

func NewSchemaOpsWithSchema(db *DB, schema string) *SchemaOps {
	s := &SchemaOps{DB: db}
	queryFn := func(ctx context.Context, sql string, args ...any) (model.Rows, error) {
		return db.RawQueryContext(ctx, sql, args...)
	}
	execFn := func(ctx context.Context, sql string, args ...any) error {
		return db.RawExecContext(ctx, sql, args...)
	}
	if db.DriverName() == "PostgreSQL" {
		s.Structurer = pgsql.NewPgSchemaDriver(schema, queryFn, execFn)
	} else {
		s.Structurer = sqlite.NewSQLiteSchemaDriver(queryFn, execFn)
	}
	return s
}

func (s *SchemaOps) IsPg() bool {
	return s.DB.DriverName() == "PostgreSQL"
}

func (s *SchemaOps) GetTableSchema(ctx context.Context, table string) (*model.TableSchema, error) {
	cols, err := s.Structurer.GetColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	indexes, err := s.Structurer.GetIndexes(ctx, table)
	if err != nil {
		return nil, err
	}
	pk, err := s.Structurer.GetPrimaryKey(ctx, table)
	if err != nil {
		return nil, err
	}
	fks, err := s.Structurer.GetForeignKeys(ctx, table)
	if err != nil {
		return nil, err
	}
	return &model.TableSchema{
		Table:   table,
		Columns: cols,
		Indexes: indexes,
		PK:      pk,
		FKs:     fks,
	}, nil
}

func (s *SchemaOps) AutoMigrate(ctx context.Context, ent any) error {
	db := getDatabase(ent)
	mig := migrateFrom(ent, db)
	if mig.Error != nil {
		return mig.Error
	}
	return s.DB.driver.MigrateContext(ctx, mig.Migrator)
}
