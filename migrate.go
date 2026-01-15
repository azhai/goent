package goent

import (
	"context"

	"github.com/azhai/goent/utils"
)

type Migration struct {
	db       *DB
	dbTarget any
}

func Migrate(dbTarget any) Migration {
	return Migration{db: getDatabase(dbTarget), dbTarget: dbTarget}
}

func (m Migration) AutoMigrate() error {
	return m.AutoMigrateContext(context.Background())
}

func (m Migration) AutoMigrateContext(ctx context.Context) error {
	data, err := MigrateFrom(m.dbTarget, m.db.driver)
	if err != nil {
		return err
	}
	return m.db.driver.MigrateContext(ctx, data)
}

func (m Migration) OnSchema(schema string) SchemaMigration {
	return SchemaMigration{m, schema}
}

func (m Migration) OnTable(table string) TableMigration {
	return TableMigration{SchemaMigration{Migration: m}, table}
}

type SchemaMigration struct {
	Migration
	schema string
}

func (m SchemaMigration) OnTable(table string) TableMigration {
	return TableMigration{m, table}
}

type TableMigration struct {
	SchemaMigration
	table string
}

func (m TableMigration) DropTable() error {
	return m.db.driver.DropTable(
		m.db.driver.KeywordHandler(utils.ColumnNamePattern(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)))
}

func (m TableMigration) RenameTable(newName string) error {
	return m.db.driver.RenameTable(
		m.db.driver.KeywordHandler(utils.ColumnNamePattern(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(newName)))
}

func (m TableMigration) DropColumn(column string) error {
	return m.db.driver.DropColumn(
		m.db.driver.KeywordHandler(utils.ColumnNamePattern(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)),
		m.db.driver.KeywordHandler(utils.ColumnNamePattern(column)))
}

func (m TableMigration) RenameColumn(column, newName string) error {
	return m.db.driver.RenameColumn(
		m.db.driver.KeywordHandler(utils.ColumnNamePattern(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)),
		m.db.driver.KeywordHandler(utils.ColumnNamePattern(column)),
		m.db.driver.KeywordHandler(utils.ColumnNamePattern(newName)))
}
