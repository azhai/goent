package model

import (
	"context"
	"database/sql"
)

type ColumnDef struct {
	Name     string
	DataType string
	Nullable bool
	Default  string
}

type IndexDef struct {
	Name    string
	Columns []string
	Unique  bool
}

type ForeignKeyDef struct {
	Name       string
	Columns    []string
	RefTable   string
	RefColumns []string
}

type TableSchema struct {
	Table   string
	Columns []ColumnDef
	Indexes []IndexDef
	FKs     []ForeignKeyDef
	PK      []string
}

type TableStat struct {
	TableName   string
	SeqScan     int64
	IdxScan     int64
	NDeadTup    int64
	NLiveTup    int64
	RowCount    int64
	LastVacuum  sql.NullTime
	LastAnalyze sql.NullTime
}

type IndexStat struct {
	TableName   string
	IndexName   string
	IdxScan     int64
	IdxFetch    int64
	IdxTupRead  int64
	IdxTupFetch int64
	Size        int64
}

type FKRef struct {
	FromTable  string
	FromColumn string
	Nullable   bool
}

type Structurer interface {
	ListTables(ctx context.Context) ([]string, error)
	ListTablesInSchema(ctx context.Context, schema string) ([]string, error)
	GetColumns(ctx context.Context, table string) ([]ColumnDef, error)
	GetIndexes(ctx context.Context, table string) ([]IndexDef, error)
	GetPrimaryKey(ctx context.Context, table string) ([]string, error)
	GetForeignKeys(ctx context.Context, table string) ([]ForeignKeyDef, error)
	DiscoverFKs(ctx context.Context, table string) ([]FKRef, error)
	IsColumnNullable(ctx context.Context, table, column string) bool
	SequenceExists(ctx context.Context, seqName string) (bool, error)
	ResetSequence(ctx context.Context, seqName string, nextVal int64) error
	GetVersion(ctx context.Context) (string, error)
	GetTableStats(ctx context.Context) ([]TableStat, error)
	GetIndexStats(ctx context.Context) ([]IndexStat, error)
	GetTableRowCount(ctx context.Context, table string) (int64, error)
	DropTables(ctx context.Context, tables []string) error
	TruncateTable(ctx context.Context, table string) error
}
