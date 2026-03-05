package reverse

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/azhai/goent/utils"
)

// TableType represents the type of table
type TableType int

const (
	TableTypeNormal TableType = iota
	TableTypeJunction
	TableTypeComplexJunction
)

type TableRegistry = utils.CoMap[string, TableInfo]

// TableInfo represents database table metadata
type TableInfo struct {
	Schema      string
	Name        string
	Columns     []*ColumnInfo
	PrimaryKey  *PrimaryKeyInfo
	ForeignKeys []*ForeignKeyInfo
	Indexes     []*IndexInfo
}

// ColumnInfo represents database column metadata
type ColumnInfo struct {
	Name         string
	DataType     string
	IsNullable   bool
	IsPrimaryKey bool
	IsUnique     bool
	IsIndex      bool
	DefaultValue *string
}

// PrimaryKeyInfo represents primary key metadata
type PrimaryKeyInfo struct {
	Name    string
	Columns []string
}

// ForeignKeyInfo represents foreign key metadata
type ForeignKeyInfo struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	IsUnique          bool
}

// IndexInfo represents index metadata
type IndexInfo struct {
	Name     string
	Columns  []string
	IsUnique bool
}

// JunctionInfo represents a junction table for m2m relationships
type JunctionInfo struct {
	Name             string
	ReferencedTables []string
}

// O2MField represents an o2m relationship field
type O2MField struct {
	FieldName string
	TypeName  string
}

// TableRelationshipInfo holds relationship information for a table
type TableRelationshipInfo struct {
	Type                 TableType
	JunctionOtherTable   string
	ComplexJunctionTable string
	O2OFields            []string
	O2MFields            []O2MField
}

// detectTableType determines the type of table and its relationships
func detectTableType(info *TableInfo, junctionTables map[string]*JunctionInfo, complexJunctionTables map[string]string) TableRelationshipInfo {
	result := TableRelationshipInfo{
		O2OFields: make([]string, 0),
		O2MFields: make([]O2MField, 0),
	}

	for junctionName, junctionInfo := range junctionTables {
		if junctionName == info.Name {
			result.Type = TableTypeJunction
			if len(junctionInfo.ReferencedTables) == 2 {
				if strings.HasPrefix(info.Name, junctionInfo.ReferencedTables[0]) {
					result.JunctionOtherTable = junctionInfo.ReferencedTables[1]
				} else {
					result.JunctionOtherTable = junctionInfo.ReferencedTables[0]
				}
			}
			return result
		}
	}

	if len(info.ForeignKeys) == 2 {
		result.Type = TableTypeComplexJunction
		var primaryTable string
		for _, fk := range info.ForeignKeys {
			if len(fk.Columns) == 1 {
				if primaryTable == "" || strings.HasPrefix(info.Name, fk.ReferencedTable) {
					primaryTable = fk.ReferencedTable
				}
			}
		}
		for _, fk := range info.ForeignKeys {
			if len(fk.Columns) == 1 && fk.ReferencedTable != primaryTable {
				result.JunctionOtherTable = fk.ReferencedTable
				break
			}
		}
		return result
	}

	result.Type = TableTypeNormal
	return result
}

// collectRelationshipFields collects relationship fields for a table
func collectRelationshipFields(info *TableInfo, junctionTables map[string]*JunctionInfo, complexJunctionTables map[string]string, structName, prefix string) TableRelationshipInfo {
	relInfo := detectTableType(info, junctionTables, complexJunctionTables)

	if relInfo.Type == TableTypeNormal {
		for _, junctionInfo := range junctionTables {
			for i, refTable := range junctionInfo.ReferencedTables {
				if refTable == info.Name {
					otherTable := junctionInfo.ReferencedTables[1-i]
					otherStructName := utils.ToCamelCase(otherTable)
					otherStructName = TrimShortPrefix(otherStructName, prefix)
					otherStructName = utils.ToSingular(otherStructName)
					relInfo.O2MFields = append(relInfo.O2MFields, O2MField{
						FieldName: otherStructName,
						TypeName:  otherStructName,
					})
				}
			}
		}

		if junctionTable, hasComplex := complexJunctionTables[info.Name]; hasComplex {
			junctionStructName := utils.ToCamelCase(junctionTable)
			junctionStructName = TrimShortPrefix(junctionStructName, prefix)
			junctionStructName = utils.ToSingular(junctionStructName)
			fieldName := junctionStructName
			if strings.HasPrefix(junctionStructName, structName) {
				fieldName = junctionStructName[len(structName):]
			}
			relInfo.O2MFields = append(relInfo.O2MFields, O2MField{
				FieldName: fieldName,
				TypeName:  junctionStructName,
			})
		}
	} else if relInfo.Type == TableTypeJunction && relInfo.JunctionOtherTable != "" {
		otherStructName := utils.ToCamelCase(relInfo.JunctionOtherTable)
		otherStructName = TrimShortPrefix(otherStructName, prefix)
		otherStructName = utils.ToSingular(otherStructName)
		relInfo.O2OFields = append(relInfo.O2OFields, otherStructName)
	} else if relInfo.Type == TableTypeComplexJunction && relInfo.JunctionOtherTable != "" {
		otherStructName := utils.ToCamelCase(relInfo.JunctionOtherTable)
		otherStructName = TrimShortPrefix(otherStructName, prefix)
		otherStructName = utils.ToSingular(otherStructName)
		relInfo.O2OFields = append(relInfo.O2OFields, otherStructName)
	}

	return relInfo
}

// generateStructFields generates struct fields from table columns
func generateStructFields(buf *bytes.Buffer, info *TableInfo, relInfo TableRelationshipInfo, driver string) {
	for _, col := range info.Columns {
		goType := MapSQLTypeToGo(col.DataType, driver)
		fieldName := utils.ToCamelCase(col.Name)

		var tagParts []string
		if col.IsPrimaryKey {
			if len(info.PrimaryKey.Columns) > 1 {
				tagParts = append(tagParts, "pk", "not_incr")
			} else {
				tagParts = append(tagParts, "pk")
			}
		} else if col.IsUnique {
			tagParts = append(tagParts, "unique")
		} else if col.IsIndex {
			tagParts = append(tagParts, "index")
		}

		for _, fk := range info.ForeignKeys {
			if len(fk.Columns) == 1 && fk.Columns[0] == col.Name {
				if relInfo.Type == TableTypeComplexJunction && fk.ReferencedTable == relInfo.JunctionOtherTable {
					tagParts = append(tagParts, "o2o")
				} else {
					tagParts = append(tagParts, "m2o")
				}
				break
			}
		}

		tagStr := ""
		if len(tagParts) > 0 {
			tagStr = fmt.Sprintf(" `goe:\"%s\"`", strings.Join(tagParts, ";"))
		}
		fmt.Fprintf(buf, "\t%s %s%s\n", fieldName, goType, tagStr)
	}
}

// generateRelationshipFields generates relationship fields
func generateRelationshipFields(buf *bytes.Buffer, info *TableInfo, relInfo TableRelationshipInfo, prefix string) {
	if relInfo.Type == TableTypeNormal {
		for _, fk := range info.ForeignKeys {
			if len(fk.Columns) == 1 {
				refStructName := utils.ToCamelCase(fk.ReferencedTable)
				refStructName = TrimShortPrefix(refStructName, prefix)
				refStructName = utils.ToSingular(refStructName)
				fmt.Fprintf(buf, "\t%s *%s `goe:\"-\"`\n", refStructName, refStructName)
			}
		}

		for _, f := range relInfo.O2OFields {
			fmt.Fprintf(buf, "\t%s *%s `goe:\"o2o\"`\n", f, f)
		}

		for _, f := range relInfo.O2MFields {
			fmt.Fprintf(buf, "\t%s []*%s `goe:\"-\"`\n", utils.ToPlural(f.FieldName), f.TypeName)
		}
	} else if relInfo.Type == TableTypeJunction && relInfo.JunctionOtherTable != "" {
		otherStructName := utils.ToCamelCase(relInfo.JunctionOtherTable)
		otherStructName = TrimShortPrefix(otherStructName, prefix)
		otherStructName = utils.ToSingular(otherStructName)
		fmt.Fprintf(buf, "\t%s *%s `goe:\"-\"`\n", otherStructName, otherStructName)
	} else if relInfo.Type == TableTypeComplexJunction && relInfo.JunctionOtherTable != "" {
		otherStructName := utils.ToCamelCase(relInfo.JunctionOtherTable)
		otherStructName = TrimShortPrefix(otherStructName, prefix)
		otherStructName = utils.ToSingular(otherStructName)
		fmt.Fprintf(buf, "\t%s *%s `goe:\"-\"`\n", otherStructName, otherStructName)
	}
}

// generateTableNameMethod generates the TableName method
func generateTableNameMethod(buf *bytes.Buffer, structName, tableName string) {
	fmt.Fprintf(buf, "}\n\n")
	fmt.Fprintf(buf, "// TableName returns the database table name\n")
	fmt.Fprintf(buf, "func (*%s) TableName() string {\n", structName)
	fmt.Fprintf(buf, "\treturn \"%s\"\n", tableName)
	fmt.Fprintf(buf, "}\n")
}

// GenerateModel generates Go struct code for a table
func GenerateModel(buf *bytes.Buffer, info *TableInfo,
	junctionTables map[string]*JunctionInfo, complexJunctionTables map[string]string,
	driver, prefix string) error {
	structName := utils.ToCamelCase(info.Name)
	structName = TrimShortPrefix(structName, prefix)
	structName = utils.ToSingular(structName)
	tableName := info.Name

	fmt.Fprintf(buf, "// %s represents the %s table\n", structName, tableName)
	fmt.Fprintf(buf, "type %s struct {\n", structName)

	relInfo := collectRelationshipFields(info, junctionTables, complexJunctionTables, structName, prefix)
	generateStructFields(buf, info, relInfo, driver)
	generateRelationshipFields(buf, info, relInfo, prefix)
	generateTableNameMethod(buf, structName, tableName)

	return nil
}

// FindJunctionTables identifies junction tables (m2m relationship tables)
// A junction table must have exactly 2 foreign keys and only primary keys + bool columns
// Bool columns are identified by names starting with "Is" (e.g., IsActive)
func FindJunctionTables(allTables *TableRegistry) map[string]*JunctionInfo {
	result := make(map[string]*JunctionInfo)

	for tableName, info := range allTables.Each() {
		if len(info.ForeignKeys) != 2 {
			continue
		}

		hasNonKeyNonBool := false
		for _, col := range info.Columns {
			if !col.IsPrimaryKey {
				if col.DataType != "boolean" && col.DataType != "bool" {
					hasNonKeyNonBool = true
					break
				}
				if !strings.HasPrefix(col.Name, "Is") {
					hasNonKeyNonBool = true
					break
				}
			}
		}
		if hasNonKeyNonBool {
			continue
		}

		var referencedTables []string
		for _, fk := range info.ForeignKeys {
			if len(fk.Columns) == 1 {
				referencedTables = append(referencedTables, fk.ReferencedTable)
			}
		}
		if len(referencedTables) == 2 {
			result[tableName] = &JunctionInfo{
				Name:             tableName,
				ReferencedTables: referencedTables,
			}
		}
	}
	return result
}

// FindComplexJunctionTables identifies complex junction tables (junction tables with additional fields)
func FindComplexJunctionTables(allTables *TableRegistry, junctionTables map[string]*JunctionInfo) map[string]string {
	result := make(map[string]string)

	for tableName, info := range allTables.Each() {
		if len(info.ForeignKeys) != 2 {
			continue
		}

		if _, isJunction := junctionTables[tableName]; isJunction {
			continue
		}

		primaryTable := determinePrimaryTable(tableName, info.ForeignKeys)
		if primaryTable != "" {
			result[primaryTable] = tableName
		}
	}

	return result
}

// TrimShortPrefix trims the short prefix from the name if it exists.
func TrimShortPrefix(name, prefix string) string {
	if prefix == "" {
		return name
	}
	prefixLen := len(prefix)
	if prefixLen < 2 || prefixLen > 3 {
		return name
	}
	prefixPart := strings.TrimSuffix(prefix, "_")
	if len(prefixPart) != 1 {
		return name
	}
	upperPrefix := strings.ToUpper(prefixPart)
	if strings.HasPrefix(name, upperPrefix) {
		return name[1:]
	}
	return name
}

// determinePrimaryTable determines the primary table for a complex junction table
func determinePrimaryTable(tableName string, foreignKeys []*ForeignKeyInfo) string {
	var primaryTable string

	for _, fk := range foreignKeys {
		if len(fk.Columns) != 1 {
			continue
		}

		if primaryTable == "" || strings.HasPrefix(tableName, fk.ReferencedTable) {
			primaryTable = fk.ReferencedTable
		}
	}

	return primaryTable
}
