package goent

import "errors"

var (
	ErrUniqueValue = errors.New("goent: unique constraint violation")
	ErrForeignKey  = errors.New("goent: foreign key constraint violation")
	ErrBadRequest  = errors.New("goent: bad request")
	ErrNotFound    = errors.New("goent: not found any element on result set")

	ErrInvalidDatabase    = errors.New("goent: invalid database, the target needs to be a struct")
	ErrInvalidDBField     = errors.New("goent: invalid database, last struct field needs to be goent.DB")
	ErrDBNotFound         = errors.New("goent: db not found")
	ErrFieldNotFound      = errors.New("goent: field or table not found")
	ErrColumnNotFound     = errors.New("goent: column not found")
	ErrNoPrimaryKey       = errors.New("goent: struct does not have a primary key set")
	ErrSliceTypeMigration = errors.New("goent: cannot migrate slice type")
	ErrDuplicateIndex     = errors.New("goent: struct has two or more indexes with same name but different uniqueness/function")
	ErrForeignKeyNotFound = errors.New("goent: foreign key not found")
	ErrMiddleTableNotSet  = errors.New("goent: middle table not configured for M2M relation")
)

// NewColumnNotFoundError creates an error indicating that the specified column was not found.
func NewColumnNotFoundError(column string) error {
	return &ColumnNotFoundError{Column: column}
}

// ColumnNotFoundError is returned when a column cannot be found in a table.
type ColumnNotFoundError struct {
	Column string
}

func (e *ColumnNotFoundError) Error() string {
	return "goent: column " + e.Column + " not found"
}

// NewFieldNotFoundError creates an error indicating that the specified field or table was not found.
func NewFieldNotFoundError(field string) error {
	return &FieldNotFoundError{Field: field}
}

// FieldNotFoundError is returned when a field or table cannot be found.
type FieldNotFoundError struct {
	Field string
}

func (e *FieldNotFoundError) Error() string {
	return "goent: field " + e.Field + " or table not found"
}

// NewNoPrimaryKeyError creates an error indicating that the struct does not have a primary key set.
func NewNoPrimaryKeyError(typeName string) error {
	return &NoPrimaryKeyError{TypeName: typeName}
}

// NoPrimaryKeyError is returned when a struct does not have a primary key defined.
type NoPrimaryKeyError struct {
	TypeName string
}

func (e *NoPrimaryKeyError) Error() string {
	return "goent: struct \"" + e.TypeName + "\" does not have a primary key set"
}

// NewDuplicateIndexError creates an error indicating duplicate index definitions with conflicting settings.
func NewDuplicateIndexError(tableName, indexName string) error {
	return &DuplicateIndexError{TableName: tableName, IndexName: indexName}
}

// DuplicateIndexError is returned when two or more indexes have the same name but different settings.
type DuplicateIndexError struct {
	TableName string
	IndexName string
}

func (e *DuplicateIndexError) Error() string {
	return "goent: struct \"" + e.TableName + "\" has two or more indexes with same name but different uniqueness/function \"" + e.IndexName + "\""
}

// NewForeignKeyNotFoundError creates an error indicating that the specified foreign key was not found.
func NewForeignKeyNotFoundError(fk string) error {
	return &ForeignKeyNotFoundError{ForeignKey: fk}
}

// ForeignKeyNotFoundError is returned when a foreign key column cannot be found.
type ForeignKeyNotFoundError struct {
	ForeignKey string
}

func (e *ForeignKeyNotFoundError) Error() string {
	return "goent: foreign key " + e.ForeignKey + " not found"
}
