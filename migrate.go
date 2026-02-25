package goent

import (
	"context"

	"github.com/azhai/goent/utils"
)

// AutoMigrate automatically migrates the database schema based on the entity struct definitions.
func AutoMigrate(ent any) error {
	return AutoMigrateContext(context.Background(), ent)
}

// AutoMigrateContext automatically migrates the database schema with the given context.
func AutoMigrateContext(ctx context.Context, ent any) error {
	db := getDatabase(ent)
	mig := migrateFrom(ent, db)
	if mig.Error != nil {
		return mig.Error
	}
	return db.driver.MigrateContext(ctx, mig.Migrator)
}

// Migration provides methods for database schema migrations.
type Migration struct {
	db *DB
}

// Migrate creates a new Migration instance for the given database.
func Migrate(db *DB) Migration {
	return Migration{db: db}
}

func (m Migration) OnSchema(schema string) SchemaMigration {
	return SchemaMigration{m, schema}
}

func (m Migration) OnTable(table string) TableMigration {
	return TableMigration{SchemaMigration{Migration: m}, table}
}

func (m Migration) KeywordHandler(name string) string {
	return m.db.driver.KeywordHandler(name)
}

// SchemaMigration provides migration methods scoped to a specific schema.
type SchemaMigration struct {
	Migration
	schema string
}

func (m SchemaMigration) OnTable(table string) TableMigration {
	return TableMigration{m, table}
}

// TableMigration provides migration methods scoped to a specific table.
type TableMigration struct {
	SchemaMigration
	table string
}

func (m TableMigration) SchemaTable() (schema, table string) {
	return m.KeywordHandler(utils.ToSnakeCase(m.schema)),
		m.KeywordHandler(utils.TableNamePattern(m.table))
}

func (m TableMigration) DropTable() error {
	schema, table := m.SchemaTable()
	return m.db.driver.DropTable(schema, table)
}

func (m TableMigration) RenameTable(newName string) error {
	schema, table := m.SchemaTable()
	newName = m.KeywordHandler(utils.TableNamePattern(newName))
	return m.db.driver.RenameTable(schema, table, newName)
}

func (m TableMigration) DropColumn(column string) error {
	schema, table := m.SchemaTable()
	column = m.KeywordHandler(utils.ToSnakeCase(column))
	return m.db.driver.DropColumn(schema, table, column)
}

func (m TableMigration) RenameColumn(column, newName string) error {
	schema, table := m.SchemaTable()
	column = m.KeywordHandler(utils.ToSnakeCase(column))
	newName = m.KeywordHandler(utils.ToSnakeCase(newName))
	return m.db.driver.RenameColumn(schema, table, column, newName)
}
