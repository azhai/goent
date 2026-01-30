package utils

import (
	"reflect"
	"strings"
	"unicode"
)

// ParseTableNameByValue parse table name by value
func ParseTableNameByValue(valueOf reflect.Value) string {
	if tableName := TableNameMethod(valueOf); tableName != "" {
		return tableName
	}
	return TableNamePattern(valueOf.Type().Name())
}

// ParseTableNameByType parse table name by type
func ParseTableNameByType(typeOf reflect.Type) string {
	valueOf := reflect.New(typeOf)
	if tableName := TableNameMethod(valueOf); tableName != "" {
		return tableName
	}
	return TableNamePattern(typeOf.Name())
}

// TableNameMethod try to get table name from method TableNames
// If method TableNames is not found, return empty string
func TableNameMethod(valueOf reflect.Value) string {
	var method reflect.Value
	if method = valueOf.MethodByName("TableName"); method.IsValid() {
		if method.Type().NumIn() == 0 && method.Type().NumOut() == 1 {
			return method.Call(nil)[0].String()
		}
	}
	if valueOf.Type().Kind() == reflect.Struct && valueOf.Addr().IsValid() {
		if method = valueOf.Addr().MethodByName("TableName"); method.IsValid() {
			if method.Type().NumIn() == 0 && method.Type().NumOut() == 1 {
				return method.Call(nil)[0].String()
			}
		}
	}
	return ""
}

// TableNamePattern is the default name patterning for mapping struct to table
func TableNamePattern(name string) string {
	if len(name) == 0 {
		return name
	}
	name = ToSnakeCase(name)
	if name[len(name)-1] != 's' {
		name += "s"
	}
	return name
}

// ColumnNamePattern is the default name patterning for mapping struct fields to table columns
func ColumnNamePattern(name string) string {
	if len(name) == 0 {
		return name
	}
	return ToSnakeCase(name)
}

// ToSnakeCase convert camelCase or PascalCase to snake_case
func ToSnakeCase(name string) string {
	if len(name) == 0 {
		return name
	}

	result := strings.Builder{}
	for i := 0; i < len(name); i++ {
		letter := rune(name[i])
		if unicode.IsUpper(letter) {
			letter = unicode.ToLower(letter)
			if i > 0 {
				prevLetter := rune(name[i-1])
				if unicode.IsLower(prevLetter) {
					result.WriteRune('_')
				} else if i+1 < len(name) && unicode.IsLower(rune(name[i+1])) {
					result.WriteRune('_')
				}
			}
		}
		result.WriteRune(letter)
	}

	return result.String()
}

func TagValueExist(tag string, subTag string) bool {
	return strings.Contains(";"+tag+";", ";"+subTag+";")
	// values := strings.Split(tag, ";")
	// for _, v := range values {
	// 	if v == subTag {
	// 		return true
	// 	}
	// }
	// return false
}

// IsFieldHasSchema check if field has schema tag or schema suffix
func IsFieldHasSchema(valueOf reflect.Value, i int) bool {
	goeTag := valueOf.Type().Field(i).Tag.Get("goe")
	return TagValueExist(goeTag, "schema") ||
		strings.HasSuffix(valueOf.Field(i).Elem().Type().Name(), "Schema")
}

func GetFieldNames(typeOf reflect.Type) (names []string) {
	for i := 0; i < typeOf.NumField(); i++ {
		names = append(names, typeOf.Field(i).Name)
	}
	return
}

func IsTableModel(fieldOf reflect.Value) bool {
	return strings.HasPrefix(fieldOf.Type().Elem().Name(), "Table")
}

func GetTableModel(fieldOf reflect.Value) reflect.Value {
	if IsTableModel(fieldOf) {
		return fieldOf.Elem().FieldByName("Model")
	}
	return fieldOf
}

func GetTableID(typeOf reflect.Type) (reflect.StructField, bool) {
	return typeOf.FieldByNameFunc(func(s string) bool {
		return strings.ToUpper(s) == "ID"
	})
}
