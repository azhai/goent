package utils

import (
	"reflect"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

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

// ParseTableNameByValue parse table name by value
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
	method := valueOf.MethodByName("TableName")
	if method.IsValid() && method.Type().NumIn() == 0 && method.Type().NumOut() == 1 {
		return method.Call(nil)[0].String()
	}
	return ""
}

// TableNamePattern is the default name patterning for mapping struct to table
func TableNamePattern(name string) string {
	if len(name) == 0 {
		return name
	}
	name = ToSnakeCase(name)
	return name
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

// IsFieldHasSchema check if field has schema tag or schema suffix
func IsFieldHasSchema(valueOf reflect.Value, i int) bool {
	goeTag := valueOf.Type().Field(i).Tag.Get("goe")
	if HasTagValue(goeTag, "schema") {
		return true
	}
	field := valueOf.Field(i)
	if field.Kind() == reflect.Pointer && !field.IsNil() {
		field = field.Elem()
	}
	if field.Kind() == reflect.Struct {
		return strings.HasSuffix(field.Type().Name(), "Schema")
	}
	return false
}

func GetElemValue(valueOf reflect.Value) reflect.Value {
	if valueOf.Kind() == reflect.Pointer && !valueOf.IsNil() {
		return valueOf.Elem()
	}
	return valueOf
}

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

func GetFieldNames(typeOf reflect.Type) (names []string) {
	if typeOf.Kind() != reflect.Struct {
		return names
	}
	for field := range typeOf.Fields() {
		names = append(names, field.Name)
	}
	return
}

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

func GetTableID(typeOf reflect.Type) (reflect.StructField, bool) {
	if typeOf.Kind() != reflect.Struct {
		return reflect.StructField{}, false
	}
	return typeOf.FieldByNameFunc(func(s string) bool {
		return strings.ToUpper(s) == "ID"
	})
}
