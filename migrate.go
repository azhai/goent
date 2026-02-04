package goent

import (
	"context"

	"github.com/azhai/goent/utils"
)

func AutoMigrate(ent any) error {
	return AutoMigrateContext(context.Background(), ent)
}

func AutoMigrateContext(ctx context.Context, ent any) error {
	db := getDatabase(ent)
	mig := migrateFrom(ent, db)
	if mig.Error != nil {
		return mig.Error
	}
	return db.driver.MigrateContext(ctx, mig.Migrator)
}

type Migration struct {
	db *DB
}

func Migrate(db *DB) Migration {
	return Migration{db: db}
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
		m.db.driver.KeywordHandler(utils.ToSnakeCase(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)))
}

func (m TableMigration) RenameTable(newName string) error {
	return m.db.driver.RenameTable(
		m.db.driver.KeywordHandler(utils.ToSnakeCase(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(newName)))
}

func (m TableMigration) DropColumn(column string) error {
	return m.db.driver.DropColumn(
		m.db.driver.KeywordHandler(utils.ToSnakeCase(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)),
		m.db.driver.KeywordHandler(utils.ToSnakeCase(column)))
}

func (m TableMigration) RenameColumn(column, newName string) error {
	return m.db.driver.RenameColumn(
		m.db.driver.KeywordHandler(utils.ToSnakeCase(m.schema)),
		m.db.driver.KeywordHandler(utils.TableNamePattern(m.table)),
		m.db.driver.KeywordHandler(utils.ToSnakeCase(column)),
		m.db.driver.KeywordHandler(utils.ToSnakeCase(newName)))
}
