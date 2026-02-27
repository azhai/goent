package utils

import (
	"reflect"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// TitleCase converts a string to title case
// It capitalizes the first letter of each word
func TitleCase(word string) string {
	caser := cases.Title(language.Und)
	return caser.String(word)
	// if word == "" {
	// 	return ""
	// }
	// runes := []rune(word)
	// runes[0] = unicode.ToTitle(runes[0])
	// return string(runes)
}

// ParseTableNameByValue parses table name by value
// If method TableName is found, return its value
// Otherwise, parse table name by type name
func ParseTableNameByValue(valueOf reflect.Value) string {
	if tableName := TableNameMethod(valueOf); tableName != "" {
		return tableName
	}

	actualValue := valueOf
	if valueOf.Type().Kind() == reflect.Pointer {
		actualValue = valueOf.Elem()
	}

	typeName := actualValue.Type().Name()
	typeString := actualValue.Type().String()

	if strings.Contains(typeString, "Table[") && strings.Contains(typeString, "]") {
		if actualValue.Type().Kind() == reflect.Struct {
			modelField, ok := actualValue.Type().FieldByName("Model")
			if ok {
				modelType := modelField.Type
				if modelType.Kind() == reflect.Pointer {
					modelType = modelType.Elem()
				}
				modelTypeString := modelType.String()
				if idx := strings.LastIndex(modelTypeString, "."); idx >= 0 {
					typeName = modelTypeString[idx+1:]
				} else {
					typeName = modelType.Name()
				}
			}
		} else {
			if idx := strings.LastIndex(typeString, "."); idx >= 0 {
				typeName = typeString[idx+1:]
			}
			typeName = strings.TrimPrefix(typeName, "Table[")
			typeName = strings.TrimSuffix(typeName, "]")
		}
	}

	if strings.Contains(typeName, "[") {
		if idx := strings.LastIndex(typeName, "["); idx >= 0 {
			typeName = typeName[:idx]
		}
	}

	if strings.Contains(typeName, ".") {
		if idx := strings.LastIndex(typeName, "."); idx >= 0 {
			typeName = typeName[idx+1:]
		}
	}

	return TableNamePattern(typeName)
}

// ParseTableNameByType parses table name by type
// It uses the TableName method if available, otherwise uses the type name
func ParseTableNameByType(typeOf reflect.Type) string {
	valueOf := reflect.New(typeOf)
	if tableName := TableNameMethod(valueOf); tableName != "" {
		return tableName
	}
	return TableNamePattern(typeOf.Name())
}

// TableNameMethod tries to get table name from method TableName
// If method TableName is not found, return empty string
func TableNameMethod(valueOf reflect.Value) string {
	method := valueOf.MethodByName("TableName")
	if method.IsValid() && method.Type().NumIn() == 0 && method.Type().NumOut() == 1 {
		return method.Call(nil)[0].String()
	}
	return ""
}

// TableNamePattern is the default name patterning for mapping struct to table
// It converts the name to snake_case format
func TableNamePattern(name string) string {
	if len(name) == 0 {
		return name
	}
	name = ToSnakeCase(name)
	return name
}

// ToSnakeCase converts camelCase or PascalCase to snake_case
// It inserts underscores before uppercase letters based on context
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

// HasTagValue checks if tag has key value
// It returns true if the tag contains the specified key
func HasTagValue(tag string, key string) bool {
	return strings.Contains(";"+tag+";", ";"+key+";")
	// values := strings.Split(tag, ";")
	// for _, v := range values {
	// 	if v == subTag {
	// 		return true
	// 	}
	// }
	// return false
}

// GetTagValue gets tag value by key
// It returns the value and a boolean indicating if the key was found
func GetTagValue(tag string, key string) (string, bool) {
	pieces := strings.SplitN(";"+tag+";", ";"+key+":", 2)
	if len(pieces) < 2 {
		return "", false
	}
	pieces = strings.SplitN(pieces[1], ";", 2)
	return strings.TrimSpace(pieces[0]), true
}

// ParseSchemaTag parses the schema tag and returns the schema name and table prefix.
// Example: "public;prefix:t_" returns ("public", "t_")
// Example: "auth" returns ("auth", "")
func ParseSchemaTag(tag string) (schema, prefix string) {
	parts := strings.SplitSeq(tag, ";")
	for part := range parts {
		if after, ok := strings.CutPrefix(part, "prefix:"); ok {
			prefix = after
		} else if part != "" {
			schema = part
		}
	}
	return schema, prefix
}

// GetElemValue returns the element value of a pointer
// If the value is a non-nil pointer, it returns the element
// Otherwise, it returns the value itself
func GetElemValue(valueOf reflect.Value) reflect.Value {
	if valueOf.Kind() == reflect.Pointer && !valueOf.IsNil() {
		return valueOf.Elem()
	}
	return valueOf
}

// GetElemType returns the element type of a pointer
// If the value is a pointer, it returns the element type
// Otherwise, it returns the type itself
func GetElemType(valueOf reflect.Value) reflect.Type {
	typeOf := valueOf.Type()
	if valueOf.Kind() == reflect.Pointer && valueOf.IsNil() {
		return typeOf.Elem()
	}
	if typeOf.Kind() == reflect.Pointer {
		return typeOf.Elem()
	}
	return typeOf
}

// GetFieldNames returns a slice of field names for the given struct type.
// It only includes exported fields.
func GetFieldNames(typeOf reflect.Type) (names []string) {
	if typeOf.Kind() != reflect.Struct {
		return names
	}
	for field := range typeOf.Fields() {
		names = append(names, field.Name)
	}
	return
}

// IsTableModel checks if the given field is a table model.
// A table model is a struct type that has a field named "Model" of type *T.
func IsTableModel(fieldOf reflect.Value) bool {
	if !fieldOf.IsValid() {
		return false
	}
	typ := fieldOf.Type()
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return strings.HasPrefix(typ.Name(), "Table")
}

// GetTableModel returns the "Model" field of the given table model field.
// If the field is not a table model, or if the "Model" field does not exist,
// it returns an invalid reflect.Value.
func GetTableModel(fieldOf reflect.Value) reflect.Value {
	if !fieldOf.IsValid() {
		return reflect.Value{}
	}
	if IsTableModel(fieldOf) {
		if fieldOf.Kind() == reflect.Pointer {
			return fieldOf.Elem().FieldByName("Model")
		}
		return fieldOf.FieldByName("Model")
	}
	return reflect.Value{}
}

// GetTableID returns the struct field with the name "ID" in the given table model type.
// If no such field exists, it returns an empty StructField and false.
func GetTableID(typeOf reflect.Type) (reflect.StructField, bool) {
	if typeOf.Kind() != reflect.Struct {
		return reflect.StructField{}, false
	}
	return typeOf.FieldByNameFunc(func(s string) bool {
		return strings.ToUpper(s) == "ID"
	})
}
