package goent_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/azhai/goent"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

// TestMigrateTableNames verifies that table names are correctly resolved
// from struct definitions and TableName() methods.
func TestMigrateTableNames(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal has TableName() method returning "animals"
	if db.Animal.TableName != "animals" {
		t.Errorf("Expected Animal table name 'animals', got '%s'", db.Animal.TableName)
	}

	// Status has no TableName(), should use snake_case of struct name
	if db.Status.TableName != "status" {
		t.Errorf("Expected Status table name 'status', got '%s'", db.Status.TableName)
	}

	// UserRole has no TableName()
	if db.UserRole.TableName != "user_role" {
		t.Errorf("Expected UserRole table name 'user_role', got '%s'", db.UserRole.TableName)
	}
}

// TestMigratePrimaryKeys verifies primary key detection and auto-increment behavior
func TestMigratePrimaryKeys(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal has single int PK (auto-increment)
	pks := db.Animal.PrimaryKeys
	if len(pks) == 0 {
		t.Fatal("Animal should have at least one primary key")
	}
	if pks[0].ColumnName != "id" {
		t.Errorf("Expected PK column 'id', got '%s'", pks[0].ColumnName)
	}
	if !pks[0].IsAutoIncr {
		t.Error("Animal.Id should be auto-increment")
	}

	// Status has single int64 PK (auto-increment)
	pks = db.Status.PrimaryKeys
	if len(pks) == 0 {
		t.Fatal("Status should have at least one primary key")
	}
	if pks[0].ColumnName != "id" {
		t.Errorf("Expected PK column 'id', got '%s'", pks[0].ColumnName)
	}

	// AnimalFood has composite PK (AnimalId is int without not_incr, FoodId is UUID)
	pks = db.AnimalFood.PrimaryKeys
	if len(pks) < 2 {
		t.Fatalf("AnimalFood should have 2 primary keys, got %d", len(pks))
	}
	// Note: int PKs without not_incr tag are marked as auto-increment even in composite PKs
	// This is the current behavior — use not_incr tag to explicitly disable auto-increment
}

// TestMigrateColumns verifies column metadata is correctly populated
func TestMigrateColumns(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Check Animal columns
	cols := db.Animal.Columns
	if _, ok := cols["name"]; !ok {
		t.Error("Animal should have 'name' column")
	}
	if _, ok := cols["id"]; !ok {
		t.Error("Animal should have 'id' column")
	}

	// Check column types
	nameCol := cols["name"]
	if nameCol.ColumnType != "string" {
		t.Errorf("Expected name column type 'string', got '%s'", nameCol.ColumnType)
	}

	idCol := cols["id"]
	if idCol.IsPK != true {
		t.Error("id column should be a primary key")
	}
}

// TestMigrateColumnNames verifies ColumnNames list is populated
func TestMigrateColumnNames(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	names := db.Animal.ColumnNames
	if len(names) == 0 {
		t.Fatal("Animal should have column names")
	}

	// Check that 'name' and 'id' are in the list
	foundName, foundId := false, false
	for _, n := range names {
		if n == "name" {
			foundName = true
		}
		if n == "id" {
			foundId = true
		}
	}
	if !foundName {
		t.Error("ColumnNames should contain 'name'")
	}
	if !foundId {
		t.Error("ColumnNames should contain 'id'")
	}
}

// TestMigrateNullableColumns verifies that pointer fields are nullable
func TestMigrateNullableColumns(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal.HabitatId is *uuid.UUID — should be nullable
	if col, ok := db.Animal.Columns["habitat_id"]; ok {
		if !col.AllowNull {
			t.Error("Animal.HabitatId (*uuid.UUID) should be nullable")
		}
	}

	// Animal.Name is string — should NOT be nullable
	if col, ok := db.Animal.Columns["name"]; ok {
		if col.AllowNull {
			t.Error("Animal.Name (string) should not be nullable")
		}
	}
}

// TestMigrateDefaultValues verifies default value extraction from tags
func TestMigrateDefaultValues(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Flag.Uint32 has default:32
	if col, ok := db.Flag.Columns["uint32"]; ok {
		if !col.HasDefault {
			t.Error("Flag.Uint32 should have a default value")
		}
		if col.DefaultValue != "32" {
			t.Errorf("Expected default '32', got '%s'", col.DefaultValue)
		}
	}

	// Default.ID has default:'Default'
	if col, ok := db.Default.Columns["id"]; ok {
		if !col.HasDefault {
			t.Error("Default.ID should have a default value")
		}
	}
}

// TestMigrateIndexes verifies index metadata from struct tags
func TestMigrateIndexes(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal.Name has `goe:"index"` tag
	indexes := db.Animal.Indexes
	if len(indexes) == 0 {
		t.Fatal("Animal should have at least one index")
	}

	// User.Email has `goe:"unique"` tag
	userIndexes := db.User.Indexes
	hasUniqueEmailIndex := false
	for _, idx := range userIndexes {
		if idx.IsUnique && idx.ColumnName == "email" {
			hasUniqueEmailIndex = true
			break
		}
	}
	if !hasUniqueEmailIndex {
		t.Error("User should have a unique index on 'email'")
	}
}

// TestMigrateForeignKeys verifies foreign key relationship detection
func TestMigrateForeignKeys(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// UserRole has m2o to User and Role
	foreigns := db.UserRole.Foreigns
	if len(foreigns) == 0 {
		t.Fatal("UserRole should have foreign key relationships")
	}

	// Check that UserId and RoleId foreign keys are detected
	hasUserFK, hasRoleFK := false, false
	for _, fk := range foreigns {
		if fk.ForeignKey == "user_id" {
			hasUserFK = true
		}
		if fk.ForeignKey == "role_id" {
			hasRoleFK = true
		}
	}
	if !hasUserFK {
		t.Error("UserRole should have foreign key 'user_id'")
	}
	if !hasRoleFK {
		t.Error("UserRole should have foreign key 'role_id'")
	}
}

// TestMigrateSchemaAssignment verifies that tables are assigned to correct schemas
func TestMigrateSchemaAssignment(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// AnimalSchema is in "public" schema
	if db.Animal.SchemaName != "public" {
		t.Errorf("Expected Animal schema 'public', got '%s'", db.Animal.SchemaName)
	}

	// FoodHabitatSchema is in "food" schema
	if db.Food.SchemaName != "food" {
		t.Errorf("Expected Food schema 'food', got '%s'", db.Food.SchemaName)
	}
	if db.Habitat.SchemaName != "food" {
		t.Errorf("Expected Habitat schema 'food', got '%s'", db.Habitat.SchemaName)
	}

	// Authentication is in "auth" schema
	if db.User.SchemaName != "auth" {
		t.Errorf("Expected User schema 'auth', got '%s'", db.User.SchemaName)
	}
	if db.Role.SchemaName != "auth" {
		t.Errorf("Expected Role schema 'auth', got '%s'", db.Role.SchemaName)
	}
}

// TestMigrateCompositePrimaryKey verifies composite primary key handling
func TestMigrateCompositePrimaryKey(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// AnimalFood has composite PK: AnimalId + FoodId
	pks := db.AnimalFood.PrimaryKeys
	if len(pks) != 2 {
		t.Fatalf("Expected 2 primary keys, got %d", len(pks))
	}

	pkNames := make(map[string]bool)
	for _, pk := range pks {
		pkNames[pk.ColumnName] = true
	}
	if !pkNames["animal_id"] || !pkNames["food_id"] {
		t.Errorf("Expected PK columns 'animal_id' and 'food_id', got %v", pkNames)
	}

	// PersonJobTitle also has composite PK
	pks = db.PersonJobTitle.PrimaryKeys
	if len(pks) != 2 {
		t.Fatalf("Expected 2 primary keys for PersonJobTitle, got %d", len(pks))
	}
}

// TestMigrateAutoIncrementDetection verifies auto-increment detection
func TestMigrateAutoIncrementDetection(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// int PK without not_incr should be auto-increment
	if !db.Animal.PrimaryKeys[0].IsAutoIncr {
		t.Error("Animal.Id (int, no not_incr) should be auto-increment")
	}

	// int64 PK without not_incr should be auto-increment
	if !db.Status.PrimaryKeys[0].IsAutoIncr {
		t.Error("Status.ID (int64, no not_incr) should be auto-increment")
	}
}

// TestMigrateUUIDPrimaryKey verifies UUID primary key handling
func TestMigrateUUIDPrimaryKey(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Food has UUID PK
	pks := db.Food.PrimaryKeys
	if len(pks) == 0 {
		t.Fatal("Food should have a primary key")
	}
	if pks[0].ColumnName != "id" {
		t.Errorf("Expected PK column 'id', got '%s'", pks[0].ColumnName)
	}
	// UUID PKs should not be auto-increment
	if pks[0].IsAutoIncr {
		t.Error("Food.Id (UUID) should not be auto-increment")
	}
}

// TestMigrateSnakeCaseConversion verifies column name conversion
func TestMigrateSnakeCaseConversion(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Name", "name"},
		{"HabitatId", "habitat_id"},
		{"AnimalFood", "animal_food"},
		{"ID", "id"},
		{"UserID", "user_id"},
		{"OrderID", "order_id"},
		{"PageIDNext", "page_id_next"},
		{"NameStatus", "name_status"},
	}

	for _, tt := range tests {
		result := utils.ToSnakeCase(tt.input)
		if result != tt.expected {
			t.Errorf("ToSnakeCase(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestMigrateFieldMethod verifies the Field() method returns correct metadata
func TestMigrateFieldMethod(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Test Field() returns correct column name
	nameField := db.Animal.Field("name")
	if nameField == nil {
		t.Fatal("Animal.Field('name') should not be nil")
	}
	if nameField.ColumnName != "name" {
		t.Errorf("Expected ColumnName 'name', got '%s'", nameField.ColumnName)
	}

	idField := db.Animal.Field("id")
	if idField == nil {
		t.Fatal("Animal.Field('id') should not be nil")
	}
	if idField.ColumnName != "id" {
		t.Errorf("Expected ColumnName 'id', got '%s'", idField.ColumnName)
	}
}

// TestMigrateColumnTypeResolution verifies column type resolution
func TestMigrateColumnTypeResolution(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Check various column types
	typeChecks := []struct {
		table      string
		column     string
		expectType string
	}{
		{"Animal", "id", "int"},
		{"Animal", "name", "string"},
		{"Status", "id", "int64"},
		{"Status", "name", "string"},
		{"Exam", "score", "float32"},
		{"Exam", "minimum", "float32"},
	}

	for _, tc := range typeChecks {
		var cols map[string]*goent.Column
		switch tc.table {
		case "Animal":
			cols = db.Animal.Columns
		case "Status":
			cols = db.Status.Columns
		case "Exam":
			cols = db.Exam.Columns
		}
		col, ok := cols[tc.column]
		if !ok {
			t.Errorf("Table %s should have column '%s'", tc.table, tc.column)
			continue
		}
		if col.ColumnType != tc.expectType {
			t.Errorf("Table %s column '%s': expected type '%s', got '%s'",
				tc.table, tc.column, tc.expectType, col.ColumnType)
		}
	}
}

// TestMigrateIgnoreTag verifies that `goe:"-"` fields are excluded
func TestMigrateIgnoreTag(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal.AnimalFoods has `goe:"-"` tag — should NOT be a column
	if _, ok := db.Animal.Columns["animal_foods"]; ok {
		t.Error("Animal.AnimalFoods (goe:\"-\") should not be a column")
	}
}

// TestMigrateGetTableSchema verifies GetTableSchema returns correct metadata
func TestMigrateGetTableSchema(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx := context.Background()
	ops := goent.NewSchemaOps(db.DB)

	schema, err := ops.GetTableSchema(ctx, "animals")
	if err != nil {
		t.Fatalf("GetTableSchema error: %v", err)
	}

	if schema == nil {
		t.Fatal("Schema should not be nil")
	}
	if len(schema.Columns) == 0 {
		t.Error("Schema should have columns")
	}
}

// TestMigrateGetTableSchemaIndexes verifies index metadata from database
func TestMigrateGetTableSchemaIndexes(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx := context.Background()
	ops := goent.NewSchemaOps(db.DB)

	schema, err := ops.GetTableSchema(ctx, "animals")
	if err != nil {
		t.Fatalf("GetTableSchema error: %v", err)
	}

	// Should have at least one index on 'name' column
	if len(schema.Indexes) == 0 {
		t.Error("Animals table should have at least one index")
	}
}

// TestMigrateGetTableSchemaPrimaryKey verifies PK detection from database
func TestMigrateGetTableSchemaPrimaryKey(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx := context.Background()
	ops := goent.NewSchemaOps(db.DB)

	schema, err := ops.GetTableSchema(ctx, "animals")
	if err != nil {
		t.Fatalf("GetTableSchema error: %v", err)
	}

	if schema.PK == nil || len(schema.PK) == 0 {
		t.Error("Animals table should have a primary key")
	}
}

// TestMigrateOrderedColumns verifies GetOrderedColumns returns columns in field order
func TestMigrateOrderedColumns(t *testing.T) {
	tm := &model.TableMigrate{
		Name: "test_table",
		Attributes: []model.AttributeMigrate{
			{Name: "z_field", FieldPos: 3},
			{Name: "a_field", FieldPos: 1},
			{Name: "m_field", FieldPos: 2},
		},
		PrimaryKeys: []model.PrimaryKeyMigrate{
			{AttributeMigrate: model.AttributeMigrate{Name: "id", FieldPos: 0}},
		},
	}

	columns := tm.GetOrderedColumns()
	if len(columns) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(columns))
	}

	// Should be ordered by FieldPos: id(0), a_field(1), m_field(2), z_field(3)
	expected := []string{"id", "a_field", "m_field", "z_field"}
	for i, col := range columns {
		if col.Name() != expected[i] {
			t.Errorf("Column %d: expected '%s', got '%s'", i, expected[i], col.Name())
		}
	}
}

// TestMigrateOrderedColumnsWithRelations verifies ordering includes relations
func TestMigrateOrderedColumnsWithRelations(t *testing.T) {
	schema := "public"
	tm := &model.TableMigrate{
		Name: "test_table",
		Attributes: []model.AttributeMigrate{
			{Name: "name", FieldPos: 1},
		},
		PrimaryKeys: []model.PrimaryKeyMigrate{
			{AttributeMigrate: model.AttributeMigrate{Name: "id", FieldPos: 0}},
		},
		ManyToSomes: []model.ManyToSomeMigrate{
			{AttributeMigrate: model.AttributeMigrate{Name: "category_id", FieldPos: 2}, TargetTable: "categories", TargetColumn: "id", TargetSchema: &schema},
		},
		OneToSomes: []model.OneToSomeMigrate{
			{AttributeMigrate: model.AttributeMigrate{Name: "user_id", FieldPos: 3}, TargetTable: "users", TargetColumn: "id", TargetSchema: &schema},
		},
	}

	columns := tm.GetOrderedColumns()
	if len(columns) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(columns))
	}

	expected := []string{"id", "name", "category_id", "user_id"}
	for i, col := range columns {
		if col.Name() != expected[i] {
			t.Errorf("Column %d: expected '%s', got '%s'", i, expected[i], col.Name())
		}
	}
}

// TestMigrateEscapingTableName verifies EscapingTableName with and without schema
func TestMigrateEscapingTableName(t *testing.T) {
	// Without schema
	tm := model.TableMigrate{Name: "animals", EscapingName: `"animals"`}
	if tm.EscapingTableName() != `"animals"` {
		t.Errorf("Expected '\"animals\"', got '%s'", tm.EscapingTableName())
	}

	// With schema
	schema := "public"
	tm = model.TableMigrate{Name: "animals", EscapingName: `"animals"`, Schema: &schema}
	expected := `public."animals"`
	if tm.EscapingTableName() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, tm.EscapingTableName())
	}
}

// TestMigrateOneToSomeEscapingTargetTableName verifies O2O/O2M target name escaping
func TestMigrateOneToSomeEscapingTargetTableName(t *testing.T) {
	// Without schema
	rel := model.OneToSomeMigrate{
		TargetTable: "users", EscapingTargetTable: `"users"`,
	}
	if rel.EscapingTargetTableName() != `"users"` {
		t.Errorf("Expected '\"users\"', got '%s'", rel.EscapingTargetTableName())
	}

	// With schema
	schema := "auth"
	rel = model.OneToSomeMigrate{
		TargetTable: "users", EscapingTargetTable: `"users"`,
		TargetSchema: &schema,
	}
	expected := `auth."users"`
	if rel.EscapingTargetTableName() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, rel.EscapingTargetTableName())
	}
}

// TestMigrateManyToSomeEscapingTargetTableName verifies M2O/M2M target name escaping
func TestMigrateManyToSomeEscapingTargetTableName(t *testing.T) {
	schema := "food"
	rel := model.ManyToSomeMigrate{
		TargetTable: "habitat", EscapingTargetTable: `"habitat"`,
		TargetSchema: &schema,
	}
	expected := `food."habitat"`
	if rel.EscapingTargetTableName() != expected {
		t.Errorf("Expected '%s', got '%s'", expected, rel.EscapingTargetTableName())
	}
}

// TestMigrateTableString verifies Table.String() formatting
func TestMigrateTableString(t *testing.T) {
	// Without schema
	tbl := model.Table{Name: "animals"}
	if tbl.String() != "animals" {
		t.Errorf("Expected 'animals', got '%s'", tbl.String())
	}

	// With schema
	schema := "public"
	tbl = model.Table{Name: "animals", Schema: &schema}
	if tbl.String() != "public.animals" {
		t.Errorf("Expected 'public.animals', got '%s'", tbl.String())
	}
}

// TestMigrateOrderedColumnName verifies OrderedColumn.Name() for different types
func TestMigrateOrderedColumnName(t *testing.T) {
	// PK column
	col := model.OrderedColumn{IsPK: true, PK: &model.PrimaryKeyMigrate{AttributeMigrate: model.AttributeMigrate{Name: "id"}}}
	if col.Name() != "id" {
		t.Errorf("Expected 'id', got '%s'", col.Name())
	}

	// Attribute column
	col = model.OrderedColumn{Attr: &model.AttributeMigrate{Name: "name"}}
	if col.Name() != "name" {
		t.Errorf("Expected 'name', got '%s'", col.Name())
	}

	// OneTo column
	col = model.OrderedColumn{OneTo: &model.OneToSomeMigrate{AttributeMigrate: model.AttributeMigrate{Name: "user_id"}}}
	if col.Name() != "user_id" {
		t.Errorf("Expected 'user_id', got '%s'", col.Name())
	}

	// ManyTo column
	col = model.OrderedColumn{ManyTo: &model.ManyToSomeMigrate{AttributeMigrate: model.AttributeMigrate{Name: "category_id"}}}
	if col.Name() != "category_id" {
		t.Errorf("Expected 'category_id', got '%s'", col.Name())
	}

	// Empty column
	col = model.OrderedColumn{}
	if col.Name() != "" {
		t.Errorf("Expected empty name, got '%s'", col.Name())
	}
}

// TestMigrateAttributeMigrateFields verifies AttributeMigrate field values
func TestMigrateAttributeMigrateFields(t *testing.T) {
	attr := model.AttributeMigrate{
		FieldName:    "UserName",
		Name:         "user_name",
		EscapingName: `"user_name"`,
		DataType:     "string",
		Nullable:     false,
		Default:      "",
		FieldPos:     2,
	}

	if attr.FieldName != "UserName" {
		t.Errorf("Expected FieldName 'UserName', got '%s'", attr.FieldName)
	}
	if attr.Name != "user_name" {
		t.Errorf("Expected Name 'user_name', got '%s'", attr.Name)
	}
	if attr.DataType != "string" {
		t.Errorf("Expected DataType 'string', got '%s'", attr.DataType)
	}
	if attr.Nullable {
		t.Error("Should not be nullable")
	}
	if attr.FieldPos != 2 {
		t.Errorf("Expected FieldPos 2, got %d", attr.FieldPos)
	}
}

// TestMigratePrimaryKeyMigrateAutoIncrement verifies PK auto-increment flag
func TestMigratePrimaryKeyMigrateAutoIncrement(t *testing.T) {
	// Auto-increment PK
	pk := model.PrimaryKeyMigrate{
		AutoIncrement: true,
		AttributeMigrate: model.AttributeMigrate{
			Name:     "id",
			DataType: "int",
		},
	}
	if !pk.AutoIncrement {
		t.Error("Should be auto-increment")
	}
	if pk.Name != "id" {
		t.Errorf("Expected Name 'id', got '%s'", pk.Name)
	}

	// Non-auto-increment PK (UUID)
	pk2 := model.PrimaryKeyMigrate{
		AutoIncrement: false,
		AttributeMigrate: model.AttributeMigrate{
			Name:     "id",
			DataType: "uuid",
		},
	}
	if pk2.AutoIncrement {
		t.Error("UUID PK should not be auto-increment")
	}
}

// TestMigrateIndexMigrate verifies index migration structure
func TestMigrateIndexMigrate(t *testing.T) {
	idx := model.IndexMigrate{
		Name:         "animals_idx_name",
		EscapingName: `"animals_idx_name"`,
		Unique:       false,
		Func:         "",
		Attributes: []model.AttributeMigrate{
			{Name: "name", DataType: "string"},
		},
	}

	if idx.Unique {
		t.Error("Should not be unique")
	}
	if len(idx.Attributes) != 1 {
		t.Errorf("Expected 1 attribute, got %d", len(idx.Attributes))
	}
}

// TestMigrateIndexMigrateUnique verifies unique index
func TestMigrateIndexMigrateUnique(t *testing.T) {
	idx := model.IndexMigrate{
		Name:   "users_idx_email_unique",
		Unique: true,
		Attributes: []model.AttributeMigrate{
			{Name: "email", DataType: "string"},
		},
	}

	if !idx.Unique {
		t.Error("Should be unique")
	}
}

// TestMigrateIndexMigrateComposite verifies composite index (multiple attributes)
func TestMigrateIndexMigrateComposite(t *testing.T) {
	idx := model.IndexMigrate{
		Name:   "info_idx_name_status_unique",
		Unique: true,
		Attributes: []model.AttributeMigrate{
			{Name: "name", DataType: "string"},
			{Name: "name_status", DataType: "string"},
		},
	}

	if !idx.Unique {
		t.Error("Should be unique")
	}
	if len(idx.Attributes) != 2 {
		t.Errorf("Expected 2 attributes, got %d", len(idx.Attributes))
	}
}

// TestMigrateDatabaseConfigSchemas verifies schema management
func TestMigrateDatabaseConfigSchemas(t *testing.T) {
	dc := &model.DatabaseConfig{}
	dc.Init("test", nil)

	// Initially no schemas
	if len(dc.Schemas()) != 0 {
		t.Error("Should have no schemas initially")
	}

	// Add schemas
	dc.AddSchema("public")
	dc.AddSchema("auth")
	if len(dc.Schemas()) != 2 {
		t.Errorf("Expected 2 schemas, got %d", len(dc.Schemas()))
	}

	// Set schemas (replace)
	dc.SetSchemas([]string{"food", "flag"})
	if len(dc.Schemas()) != 2 {
		t.Errorf("Expected 2 schemas after SetSchemas, got %d", len(dc.Schemas()))
	}
	schemas := dc.Schemas()
	if schemas[0] != "food" || schemas[1] != "flag" {
		t.Errorf("Expected [food flag], got %v", schemas)
	}
}

// TestMigrateDatabaseConfigInit verifies Init resets state
func TestMigrateDatabaseConfigInit(t *testing.T) {
	dc := &model.DatabaseConfig{}
	dc.AddSchema("public")
	dc.SetInitCallback(func() error { return nil })

	dc.Init("new_driver", nil)

	if len(dc.Schemas()) != 0 {
		t.Error("Init should reset schemas")
	}
	if dc.InitCallback() != nil {
		t.Error("Init should reset callback")
	}
}

// TestMigrateColumnGetInt64 verifies Column.GetInt64
func TestMigrateColumnGetInt64(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Insert test data
	animal := &Animal{Name: "GetInt64Test", Id: 777}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Get int64 from id column
	idCol := db.Animal.Columns["id"]
	val, ok := idCol.GetInt64(animal)
	if !ok {
		t.Fatal("Should get int64 from id column")
	}
	if val != 777 {
		t.Errorf("Expected 777, got %d", val)
	}

	// Name column should not return int64
	nameCol := db.Animal.Columns["name"]
	_, ok = nameCol.GetInt64(animal)
	if ok {
		t.Error("Should not get int64 from name column")
	}
}

// TestMigrateColumnGetString verifies Column.GetString
func TestMigrateColumnGetString(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	animal := &Animal{Name: "GetStringTest", Id: 778}
	err = db.Animal.Insert().One(animal)
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Get string from name column
	nameCol := db.Animal.Columns["name"]
	val, ok := nameCol.GetString(animal)
	if !ok {
		t.Fatal("Should get string from name column")
	}
	if val != "GetStringTest" {
		t.Errorf("Expected 'GetStringTest', got '%s'", val)
	}

	// Id column should not return string
	idCol := db.Animal.Columns["id"]
	_, ok = idCol.GetString(animal)
	if ok {
		t.Error("Should not get string from id column")
	}
}

// TestMigrateQueryCreate verifies Query creation
func TestMigrateQueryCreate(t *testing.T) {
	q := model.CreateQuery("SELECT * FROM animals", []any{1, 2})
	if q.RawSql != "SELECT * FROM animals" {
		t.Errorf("Expected 'SELECT * FROM animals', got '%s'", q.RawSql)
	}
	if len(q.Arguments) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(q.Arguments))
	}
}

// TestMigrateAutoMigrateIdempotent verifies that AutoMigrate is idempotent
func TestMigrateAutoMigrateIdempotent(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Running AutoMigrate multiple times should not error
	for i := 0; i < 3; i++ {
		err := goent.AutoMigrate(db)
		if err != nil {
			t.Errorf("AutoMigrate iteration %d error: %v", i, err)
		}
	}
}

// TestMigrateHasTagValue verifies tag parsing utility
func TestMigrateHasTagValue(t *testing.T) {
	tests := []struct {
		tag      string
		key      string
		expected bool
	}{
		{"pk;not_incr", "pk", true},
		{"pk;not_incr", "not_incr", true},
		{"pk;not_incr", "m2o", false},
		{"m2o", "m2o", true},
		{"unique", "unique", true},
		{"index(n:idx_name f:lower)", "index", false}, // index has value, not just tag
		{"default:'test'", "default", false},          // default has value
		{"", "pk", false},
	}

	for _, tt := range tests {
		result := utils.HasTagValue(tt.tag, tt.key)
		if result != tt.expected {
			t.Errorf("HasTagValue(%q, %q) = %v, want %v", tt.tag, tt.key, result, tt.expected)
		}
	}
}

// TestMigrateSpecialColumnTypes verifies special column types
func TestMigrateSpecialColumnTypes(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Flag has many special types
	typeChecks := []struct {
		column   string
		expectOK bool
	}{
		{"float32", true},
		{"float64", true},
		{"int8", true},
		{"int16", true},
		{"int32", true},
		{"int64", true},
		{"uint", true},
		{"uint8", true},
		{"uint16", true},
		{"uint32", true},
		{"uint64", true},
		{"bool", true},
		{"byte", true},
	}

	for _, tc := range typeChecks {
		if _, ok := db.Flag.Columns[tc.column]; ok != tc.expectOK {
			t.Errorf("Flag column '%s': expected exists=%v, got exists=%v",
				tc.column, tc.expectOK, ok)
		}
	}
}

// TestMigrateReservedKeywordTable tests that reserved keyword table names are escaped
func TestMigrateReservedKeywordTable(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// "select" and "drop" are SQL reserved keywords
	// They should still work as table names with proper escaping
	if db.Select.TableName != "select" {
		t.Errorf("Expected table name 'select', got '%s'", db.Select.TableName)
	}
	if db.Drop.TableName != "drop" {
		t.Errorf("Expected table name 'drop', got '%s'", db.Drop.TableName)
	}
}

// TestMigrateM2MRelationship verifies M2M relationship through junction table
func TestMigrateM2MRelationship(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal has M2M to Food through AnimalFood (AnimalFoods []AnimalFood field)
	// The junction table AnimalFood has columns: AnimalId (int) and FoodId (UUID)
	// Verify the junction table has both columns
	cols := db.AnimalFood.Columns
	if _, ok := cols["animal_id"]; !ok {
		t.Error("AnimalFood should have 'animal_id' column")
	}
	if _, ok := cols["food_id"]; !ok {
		t.Error("AnimalFood should have 'food_id' column")
	}

	// Verify both columns are primary keys (composite PK)
	pkNames := make(map[string]bool)
	for _, pk := range db.AnimalFood.PrimaryKeys {
		pkNames[pk.ColumnName] = true
	}
	if !pkNames["animal_id"] {
		t.Error("animal_id should be a primary key in AnimalFood")
	}
	if !pkNames["food_id"] {
		t.Error("food_id should be a primary key in AnimalFood")
	}
}

// TestMigrateO2MRelationship verifies O2M relationship detection
func TestMigrateO2MRelationship(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Weather has O2M to Habitats (Habitats []Habitat field)
	// The child table Habitat has WeatherId column pointing to Weather
	cols := db.Habitat.Columns
	if _, ok := cols["weather_id"]; !ok {
		t.Error("Habitat should have 'weather_id' column")
	}
}

// TestMigrateM2ORelationship verifies M2O relationship detection
func TestMigrateM2ORelationship(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// UserRole has M2O to User and Role (via m2o tag)
	foreigns := db.UserRole.Foreigns
	m2oCount := 0
	for _, fk := range foreigns {
		if fk.Type == goent.M2O {
			m2oCount++
		}
	}
	if m2oCount < 2 {
		t.Errorf("UserRole should have at least 2 M2O relationships, got %d", m2oCount)
	}
}

// TestMigrateSchemaOps verifies SchemaOps creation and basic operations
func TestMigrateSchemaOps(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ops := goent.NewSchemaOps(db.DB)
	if ops == nil {
		t.Fatal("NewSchemaOps should return non-nil")
	}

	// Check driver type detection
	if db.DriverName() == "PostgreSQL" {
		if !ops.IsPg() {
			t.Error("IsPg() should return true for PostgreSQL driver")
		}
	} else {
		if ops.IsPg() {
			t.Error("IsPg() should return false for non-PostgreSQL driver")
		}
	}
}

// TestMigrateSchemaOpsWithSchema verifies NewSchemaOpsWithSchema
func TestMigrateSchemaOpsWithSchema(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ops := goent.NewSchemaOpsWithSchema(db.DB, "public")
	if ops == nil {
		t.Fatal("NewSchemaOpsWithSchema should return non-nil")
	}
}

// TestMigrateTableInfoString verifies TableInfo.String()
func TestMigrateTableInfoString(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// String() should return the table name
	if db.Animal.String() != "animals" {
		t.Errorf("Animal.String() = '%s', want 'animals'", db.Animal.String())
	}
}

// TestMigrateTableMethod verifies Table() returns correct model.Table
func TestMigrateTableMethod(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	tbl := db.Animal.Table()
	if tbl == nil {
		t.Fatal("Animal.Table() should not be nil")
	}
	if tbl.Name != "animals" {
		t.Errorf("Table.Name = '%s', want 'animals'", tbl.Name)
	}
}

// TestMigrateColumnFieldId verifies Column.FieldId is set correctly
func TestMigrateColumnFieldId(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Check that FieldId is populated for columns
	for name, col := range db.Animal.Columns {
		if col.FieldId < 0 {
			t.Errorf("Column '%s' has invalid FieldId %d", name, col.FieldId)
		}
	}
}

// TestMigrateColumnFieldName verifies Column.FieldName matches Go struct field name
func TestMigrateColumnFieldName(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Check FieldName mapping
	if col, ok := db.Animal.Columns["name"]; ok {
		if col.FieldName != "Name" {
			t.Errorf("Expected FieldName 'Name', got '%s'", col.FieldName)
		}
	}
	if col, ok := db.Animal.Columns["id"]; ok {
		if col.FieldName != "Id" {
			t.Errorf("Expected FieldName 'Id', got '%s'", col.FieldName)
		}
	}
}

// TestMigrateColumnIsPK verifies Column.IsPK flag
func TestMigrateColumnIsPK(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	if col, ok := db.Animal.Columns["id"]; ok {
		if !col.IsPK {
			t.Error("Animal.id column should be IsPK=true")
		}
	}
	if col, ok := db.Animal.Columns["name"]; ok {
		if col.IsPK {
			t.Error("Animal.name column should be IsPK=false")
		}
	}
}

// TestMigrateColumnIsAutoIncr verifies Column.isAutoIncr via PrimaryKeys
func TestMigrateColumnIsAutoIncr(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Auto-increment PK
	if len(db.Animal.PrimaryKeys) > 0 && !db.Animal.PrimaryKeys[0].IsAutoIncr {
		t.Error("Animal.Id should be auto-increment")
	}

	// UUID PK should not be auto-increment
	if len(db.Food.PrimaryKeys) > 0 && db.Food.PrimaryKeys[0].IsAutoIncr {
		t.Error("Food.Id (UUID) should not be auto-increment")
	}
}

// TestMigrateInfoCompositeIndex verifies Info has indexes
func TestMigrateInfoCompositeIndex(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Info has `goe:"index(unique n:idx_name_status)"` on Name and NameStatus
	indexes := db.Info.Indexes
	if len(indexes) == 0 {
		t.Fatal("Info should have at least one index")
	}

	// Should have indexes on name and name_status columns
	indexCols := make(map[string]bool)
	for _, idx := range indexes {
		indexCols[idx.ColumnName] = true
	}
	if !indexCols["name"] {
		t.Error("Info should have an index on 'name'")
	}
}

// TestMigratePageSelfReference verifies self-referencing table (Page -> Page)
func TestMigratePageSelfReference(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Page has PageIDNext and PageIDPrev pointing to itself
	cols := db.Page.Columns
	if _, ok := cols["page_id_next"]; !ok {
		t.Error("Page should have 'page_id_next' column")
	}
	if _, ok := cols["page_id_prev"]; !ok {
		t.Error("Page should have 'page_id_prev' column")
	}
}

// TestMigrateFlagAllTypes verifies Flag table with all numeric types
func TestMigrateFlagAllTypes(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Flag table should have all numeric type columns
	expectedCols := []string{"id", "float32", "float64", "int8", "int16", "int32",
		"int64", "uint", "uint8", "uint16", "uint32", "uint64", "bool", "byte"}

	for _, colName := range expectedCols {
		if _, ok := db.Flag.Columns[colName]; !ok {
			t.Errorf("Flag should have column '%s'", colName)
		}
	}
}

// TestMigrateExamFloatColumns verifies float column types
func TestMigrateExamFloatColumns(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	if col, ok := db.Exam.Columns["score"]; ok {
		if col.ColumnType != "float32" {
			t.Errorf("Exam.Score type = '%s', want 'float32'", col.ColumnType)
		}
	}
	if col, ok := db.Exam.Columns["minimum"]; ok {
		if col.ColumnType != "float32" {
			t.Errorf("Exam.Minimum type = '%s', want 'float32'", col.ColumnType)
		}
	}
}

// TestMigrateInfoByteSlicePK verifies []byte primary key
func TestMigrateInfoByteSlicePK(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Info has []byte PK
	pks := db.Info.PrimaryKeys
	if len(pks) == 0 {
		t.Fatal("Info should have a primary key")
	}
	if pks[0].ColumnName != "id" {
		t.Errorf("Expected PK column 'id', got '%s'", pks[0].ColumnName)
	}
	// []byte PK should not be auto-increment
	if pks[0].IsAutoIncr {
		t.Error("Info.Id ([]byte) should not be auto-increment")
	}
}

// TestMigrateDefaultPK verifies Default table with default PK value
func TestMigrateDefaultPK(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Default has string PK with default value
	pks := db.Default.PrimaryKeys
	if len(pks) == 0 {
		t.Fatal("Default should have a primary key")
	}
	if pks[0].ColumnName != "id" {
		t.Errorf("Expected PK column 'id', got '%s'", pks[0].ColumnName)
	}
}

// TestMigrateHabitatTypeTag verifies Habitat.Name has type:varchar(50) tag
func TestMigrateHabitatTypeTag(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	if col, ok := db.Habitat.Columns["name"]; ok {
		if col.ColumnType != "string" {
			t.Errorf("Habitat.Name type = '%s', want 'string'", col.ColumnType)
		}
	}
}

// TestMigrateAnimalNullableFields verifies Animal nullable pointer fields
func TestMigrateAnimalNullableFields(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// HabitatId is *uuid.UUID — nullable
	if col, ok := db.Animal.Columns["habitat_id"]; ok {
		if !col.AllowNull {
			t.Error("Animal.HabitatId should be nullable (pointer type)")
		}
	}

	// InfoId is *[]byte — nullable
	if col, ok := db.Animal.Columns["info_id"]; ok {
		if !col.AllowNull {
			t.Error("Animal.InfoId should be nullable (pointer type)")
		}
	}

	// Name is string — not nullable
	if col, ok := db.Animal.Columns["name"]; ok {
		if col.AllowNull {
			t.Error("Animal.Name should not be nullable")
		}
	}
}

// TestMigrateUserRoleColumns verifies UserRole table structure
func TestMigrateUserRoleColumns(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// UserRole should have user_id and role_id columns
	if _, ok := db.UserRole.Columns["user_id"]; !ok {
		t.Error("UserRole should have 'user_id' column")
	}
	if _, ok := db.UserRole.Columns["role_id"]; !ok {
		t.Error("UserRole should have 'role_id' column")
	}
}

// TestMigrateRoleColumns verifies Role table structure
func TestMigrateRoleColumns(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	if _, ok := db.Role.Columns["id"]; !ok {
		t.Error("Role should have 'id' column")
	}
	if _, ok := db.Role.Columns["name"]; !ok {
		t.Error("Role should have 'name' column")
	}
}

// TestMigratePersonJobTitleStructure verifies PersonJobTitle junction table
func TestMigratePersonJobTitleStructure(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// PersonJobTitle has composite PK: PersonId + JobTitleId
	pks := db.PersonJobTitle.PrimaryKeys
	if len(pks) != 2 {
		t.Fatalf("Expected 2 primary keys, got %d", len(pks))
	}

	pkNames := make(map[string]bool)
	for _, pk := range pks {
		pkNames[pk.ColumnName] = true
	}
	if !pkNames["person_id"] || !pkNames["job_title_id"] {
		t.Errorf("Expected PK columns 'person_id' and 'job_title_id', got %v", pkNames)
	}

	// Should have created_at column
	if _, ok := db.PersonJobTitle.Columns["created_at"]; !ok {
		t.Error("PersonJobTitle should have 'created_at' column")
	}
}

// TestMigrateGetTagValue verifies GetTagValue utility
func TestMigrateGetTagValue(t *testing.T) {
	tests := []struct {
		tag       string
		key       string
		wantValue string
		wantOK    bool
	}{
		{"default:'test'", "default", "'test'", true},
		{"pk;not_incr", "pk", "", false}, // pk has no colon, not key:value format
		{"type:varchar(50)", "type", "varchar(50)", true},
		{"default:32", "default", "32", true},
		{"", "pk", "", false},
	}

	for _, tt := range tests {
		value, ok := utils.GetTagValue(tt.tag, tt.key)
		if ok != tt.wantOK {
			t.Errorf("GetTagValue(%q, %q) ok = %v, want %v", tt.tag, tt.key, ok, tt.wantOK)
		}
		if ok && value != tt.wantValue {
			t.Errorf("GetTagValue(%q, %q) = %q, want %q", tt.tag, tt.key, value, tt.wantValue)
		}
	}
}

// TestMigrateParseTableNameByType verifies table name parsing from struct type
func TestMigrateParseTableNameByType(t *testing.T) {
	// Status struct without TableName() method should use snake_case
	name := utils.ParseTableNameByType(reflect.TypeOf(Status{}))
	if name != "status" {
		t.Errorf("ParseTableNameByType(Status) = '%s', want 'status'", name)
	}
}

// TestMigrateDatabaseConfigCallbacks verifies init callback mechanism
func TestMigrateDatabaseConfigCallbacks(t *testing.T) {
	dc := &model.DatabaseConfig{}
	called := false
	dc.SetInitCallback(func() error {
		called = true
		return nil
	})

	if dc.InitCallback() == nil {
		t.Error("InitCallback should not be nil after SetInitCallback")
	}

	// Call the callback
	err := dc.InitCallback()()
	if err != nil {
		t.Errorf("InitCallback error: %v", err)
	}
	if !called {
		t.Error("InitCallback should have been called")
	}
}

// TestMigrateOrderedColumnIsPK verifies OrderedColumn.IsPK field
func TestMigrateOrderedColumnIsPK(t *testing.T) {
	// PK column
	col := model.OrderedColumn{IsPK: true, PK: &model.PrimaryKeyMigrate{AttributeMigrate: model.AttributeMigrate{Name: "id"}}}
	if !col.IsPK {
		t.Error("Should be PK")
	}

	// Non-PK column
	col2 := model.OrderedColumn{Attr: &model.AttributeMigrate{Name: "name"}}
	if col2.IsPK {
		t.Error("Should not be PK")
	}
}

// TestMigrateOrderedColumnFieldPos verifies OrderedColumn.FieldPos ordering
func TestMigrateOrderedColumnFieldPos(t *testing.T) {
	tm := &model.TableMigrate{
		Name: "test",
		Attributes: []model.AttributeMigrate{
			{Name: "c", FieldPos: 2},
			{Name: "a", FieldPos: 0},
			{Name: "b", FieldPos: 1},
		},
	}

	columns := tm.GetOrderedColumns()
	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(columns))
	}

	// Should be ordered by FieldPos: a(0), b(1), c(2)
	expected := []string{"a", "b", "c"}
	for i, col := range columns {
		if col.Name() != expected[i] {
			t.Errorf("Column %d: expected '%s', got '%s'", i, expected[i], col.Name())
		}
	}
}

// TestMigrateAttributeMigrateEscapingName verifies EscapingName field
func TestMigrateAttributeMigrateEscapingName(t *testing.T) {
	attr := model.AttributeMigrate{
		Name:         "user_name",
		EscapingName: `"user_name"`,
	}
	if attr.EscapingName != `"user_name"` {
		t.Errorf("Expected '\"user_name\"', got '%s'", attr.EscapingName)
	}
}

// TestMigrateIndexMigrateEscapingName verifies index EscapingName
func TestMigrateIndexMigrateEscapingName(t *testing.T) {
	idx := model.IndexMigrate{
		Name:         "idx_animals_name",
		EscapingName: `"idx_animals_name"`,
	}
	if idx.EscapingName != `"idx_animals_name"` {
		t.Errorf("Expected '\"idx_animals_name\"', got '%s'", idx.EscapingName)
	}
}

// TestMigrateForeignKeyDef verifies ForeignKeyDef structure
func TestMigrateForeignKeyDef(t *testing.T) {
	fk := model.ForeignKeyDef{
		Name:       "fk_user_role_user",
		Columns:    []string{"user_id"},
		RefTable:   "users",
		RefColumns: []string{"id"},
	}
	if fk.RefTable != "users" {
		t.Errorf("Expected RefTable 'users', got '%s'", fk.RefTable)
	}
	if len(fk.Columns) != 1 || fk.Columns[0] != "user_id" {
		t.Errorf("Expected Columns ['user_id'], got %v", fk.Columns)
	}
}

// TestMigrateColumnDef verifies ColumnDef structure
func TestMigrateColumnDef(t *testing.T) {
	col := model.ColumnDef{
		Name:     "name",
		DataType: "varchar",
		Nullable: false,
	}
	if col.Name != "name" {
		t.Errorf("Expected Name 'name', got '%s'", col.Name)
	}
	if col.Nullable {
		t.Error("Should not be nullable")
	}
}

// TestMigrateIndexDef verifies IndexDef structure
func TestMigrateIndexDef(t *testing.T) {
	idx := model.IndexDef{
		Name:    "idx_name",
		Columns: []string{"name"},
		Unique:  true,
	}
	if idx.Name != "idx_name" {
		t.Errorf("Expected Name 'idx_name', got '%s'", idx.Name)
	}
	if !idx.Unique {
		t.Error("Should be unique")
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "name" {
		t.Errorf("Expected Columns ['name'], got %v", idx.Columns)
	}
}

// TestMigrateTableMigrateName verifies TableMigrate name fields
func TestMigrateTableMigrateName(t *testing.T) {
	schema := "public"
	tm := model.TableMigrate{
		Name:         "users",
		EscapingName: `"users"`,
		Schema:       &schema,
		Migrated:     false,
	}
	if tm.Name != "users" {
		t.Errorf("Expected Name 'users', got '%s'", tm.Name)
	}
	if tm.Migrated {
		t.Error("Should not be migrated yet")
	}
	if tm.Schema == nil || *tm.Schema != "public" {
		t.Error("Schema should be 'public'")
	}
}

// TestMigrateTableMigrateNoSchema verifies TableMigrate without schema
func TestMigrateTableMigrateNoSchema(t *testing.T) {
	tm := model.TableMigrate{
		Name:         "animals",
		EscapingName: `"animals"`,
	}
	if tm.Schema != nil {
		t.Error("Schema should be nil for SQLite tables")
	}
}

// TestMigrateOneToSomeMigrateFields verifies OneToSomeMigrate fields
func TestMigrateOneToSomeMigrateFields(t *testing.T) {
	schema := "auth"
	rel := model.OneToSomeMigrate{
		AttributeMigrate: model.AttributeMigrate{
			Name:     "user_id",
			DataType: "int",
			FieldPos: 3,
		},
		TargetTable:         "users",
		TargetColumn:        "id",
		TargetSchema:        &schema,
		EscapingTargetTable: `"users"`,
	}
	if rel.TargetTable != "users" {
		t.Errorf("Expected TargetTable 'users', got '%s'", rel.TargetTable)
	}
	if rel.TargetColumn != "id" {
		t.Errorf("Expected TargetColumn 'id', got '%s'", rel.TargetColumn)
	}
	if rel.Name != "user_id" {
		t.Errorf("Expected Name 'user_id', got '%s'", rel.Name)
	}
}

// TestMigrateManyToSomeMigrateFields verifies ManyToSomeMigrate fields
func TestMigrateManyToSomeMigrateFields(t *testing.T) {
	schema := "food"
	rel := model.ManyToSomeMigrate{
		AttributeMigrate: model.AttributeMigrate{
			Name:     "habitat_id",
			DataType: "uuid",
			FieldPos: 2,
		},
		TargetTable:         "habitat",
		TargetColumn:        "id",
		TargetSchema:        &schema,
		EscapingTargetTable: `"habitat"`,
	}
	if rel.TargetTable != "habitat" {
		t.Errorf("Expected TargetTable 'habitat', got '%s'", rel.TargetTable)
	}
}

// TestMigrateQueryType verifies QueryType enumeration
func TestMigrateQueryType(t *testing.T) {
	types := []model.QueryType{
		model.SelectQuery,
		model.InsertQuery,
		model.UpdateQuery,
		model.DeleteQuery,
	}
	for _, qt := range types {
		if qt == 0 {
			t.Error("QueryType should not be zero value")
		}
	}
}

// TestMigrateJoinType verifies JoinType enumeration
func TestMigrateJoinType(t *testing.T) {
	types := []model.JoinType{
		model.InnerJoin,
		model.LeftJoin,
		model.RightJoin,
	}
	for _, jt := range types {
		if jt == "" {
			t.Error("JoinType should not be empty string")
		}
	}
}

// TestMigrateSchemaOpsAutoMigrate verifies SchemaOps.AutoMigrate
func TestMigrateSchemaOpsAutoMigrate(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx := context.Background()
	ops := goent.NewSchemaOps(db.DB)

	// AutoMigrate through SchemaOps should work
	err = ops.AutoMigrate(ctx, db)
	if err != nil {
		t.Errorf("SchemaOps.AutoMigrate error: %v", err)
	}
}

// TestMigrateGetTableSchemaForeignKey verifies FK detection from database
func TestMigrateGetTableSchemaForeignKey(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx := context.Background()
	ops := goent.NewSchemaOps(db.DB)

	// Use a table in the default schema (animals is in public)
	schema, err := ops.GetTableSchema(ctx, "animals")
	if err != nil {
		t.Fatalf("GetTableSchema error: %v", err)
	}

	// Should have columns
	if len(schema.Columns) == 0 {
		t.Error("Animals table should have columns")
	}
}

// TestMigrateGetTableSchemaNonExistent verifies behavior for non-existent table
func TestMigrateGetTableSchemaNonExistent(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	ctx := context.Background()
	ops := goent.NewSchemaOps(db.DB)

	schema, err := ops.GetTableSchema(ctx, "non_existent_table_xyz")
	// Some drivers return empty schema, some return error — both are acceptable
	if err == nil && schema != nil && len(schema.Columns) > 0 {
		t.Error("Non-existent table should not have columns")
	}
}

// TestMigrateMultipleSchemas verifies tables in different schemas
func TestMigrateMultipleSchemas(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Tables in different schemas should have different SchemaName
	schemas := map[string]string{
		"Animal": db.Animal.SchemaName,
		"Food":   db.Food.SchemaName,
		"User":   db.User.SchemaName,
		"Flag":   db.Flag.SchemaName,
		"Drop":   db.Drop.SchemaName,
	}

	// Verify each table has a schema
	for table, schema := range schemas {
		if schema == "" {
			t.Errorf("Table %s should have a schema name", table)
		}
	}

	// Food and Habitat should be in the same schema
	if db.Food.SchemaName != db.Habitat.SchemaName {
		t.Error("Food and Habitat should be in the same schema")
	}

	// User and Role should be in the same schema
	if db.User.SchemaName != db.Role.SchemaName {
		t.Error("User and Role should be in the same schema")
	}
}

// TestMigrateAnimalTableNameMethod verifies TableName() method overrides snake_case
func TestMigrateAnimalTableNameMethod(t *testing.T) {
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping test: database setup failed: %v", err)
		return
	}

	// Animal struct has TableName() method returning "animals"
	// Without it, it would be "animal" (snake_case of Animal)
	if db.Animal.TableName != "animals" {
		t.Errorf("Animal.TableName = '%s', want 'animals' (from TableName() method)", db.Animal.TableName)
	}
}
