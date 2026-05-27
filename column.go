package goent

import (
	"reflect"
)

// Column represents a database column with its metadata and field information
// It contains information about the column's properties and mapping to Go struct fields
type Column struct {
	FieldName    string  // Go struct field name
	FieldId      int     // Index of the field in the struct
	ColumnName   string  // Database column name
	ColumnType   string  // Database column type (e.g., "int", "varchar")
	AllowNull    bool    // Whether the column allows NULL values
	HasDefault   bool    // Whether the column has a default value
	DefaultValue string  // The default value from struct tag
	tableName    string  // Table name (internal)
	schemaName   *string // Schema name (internal)
	isAutoIncr   bool    // Whether the column is auto-increment
}

// GetInt64 returns the int64 value of the column from the given object
// It checks if the column is an int type before retrieving the value
// Returns (0, false) if the column is not an int type
func (c *Column) GetInt64(obj any) (int64, bool) {
	if c.ColumnType != "int" && c.ColumnType != "int64" {
		return 0, false
	}
	valueOf := reflect.ValueOf(obj).Elem()
	return valueOf.Field(c.FieldId).Int(), true
}

// GetString returns the string value of the column from the given object
// It checks if the column is a string type before retrieving the value
// Returns ("", false) if the column is not a string type
func (c *Column) GetString(obj any) (string, bool) {
	if c.ColumnType != "string" && c.ColumnType != "[]byte" {
		return "", false
	}
	valueOf := reflect.ValueOf(obj).Elem()
	return valueOf.Field(c.FieldId).String(), true
}

// Index represents a database index with uniqueness and auto-increment flags
// It extends Column with index-specific properties
type Index struct {
	IsUnique   bool // Whether the index is unique
	IsAutoIncr bool // Whether the column is auto-increment
	*Column         // Embedded column information
}
