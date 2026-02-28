package main

import (
	"bytes"
	"fmt"
	"go/types"
	"reflect"
	"strings"

	"github.com/azhai/goent/utils"
)

// ModelStruct represents a struct with its name and type.
type ModelStruct struct {
	Name, Class string
	named       *types.Named
	*types.Struct
}

// ModelField represents a field in a struct with its name and type.
type ModelField struct {
	Name string
	Type string
}

func filterModels(structs []*ModelStruct, pkgName string) (models []*ModelStruct) {
	allNames, names := make(map[string]bool), make([]string, 0)
	for _, st := range structs {
		if st.Class, names = getStructClass(st, pkgName); len(names) > 0 {
			for _, n := range names {
				allNames[n] = true
			}
		}
		// fmt.Printf("class: %s, name: %s, models: %+v\n", class, st.Name, names)
		if st.Class == "" && hasMethod(st, "TableName") {
			st.Class = "Model"
			models = append(models, st)
		}
	}
	for _, st := range structs {
		if st.Class == "" {
			if _, ok := allNames[st.Name]; ok {
				st.Class = "Model"
			}
		}
		if st.Class == "Model" {
			models = append(models, st)
		}
	}
	return
}

// generateStructScanner generates the scanner methods for a struct.
func generateStructScanner(buf *bytes.Buffer, st *ModelStruct) bool {
	fields, pkIndex := getModelFields(st)
	if len(fields) == 0 {
		return false
	}

	fmt.Fprintf(buf, "// ------------------------------\n")
	fmt.Fprintf(buf, "// %s\n", st.Name)
	fmt.Fprintf(buf, "// ------------------------------\n\n")

	// implement goent.Entity interface
	if pkIndex >= 0 {
		pkField := fields[pkIndex]
		fmt.Fprintf(buf, "// implement goent.Entity interface GetID for %s\n", st.Name)
		fmt.Fprintf(buf, "func (t *%s) GetID() int64 {\n", st.Name)
		if pkField.Type == "int64" {
			fmt.Fprintf(buf, "\treturn t.%s\n", pkField.Name)
		} else {
			fmt.Fprintf(buf, "\treturn int64(t.%s)\n", pkField.Name)
		}
		fmt.Fprintf(buf, "}\n\n")

		fmt.Fprintf(buf, "// implement goent.Entity interface SetID for %s\n", st.Name)
		fmt.Fprintf(buf, "func (t *%s) SetID(id int64) {\n", st.Name)
		if pkField.Type == "int64" {
			fmt.Fprintf(buf, "\tt.%s = id\n", pkField.Name)
		} else {
			fmt.Fprintf(buf, "\tt.%s = int(id)\n", pkField.Name)
		}
		fmt.Fprintf(buf, "}\n\n")
	}

	// implement goent.GenScanFields interface
	fmt.Fprintf(buf, "// ScanFields returns a slice of pointers to %s fields for database scanning.\n", st.Name)
	fmt.Fprintf(buf, "func (t *%s) ScanFields() []any {\n", st.Name)
	fmt.Fprintf(buf, "\treturn []any{\n")
	for _, f := range fields {
		fmt.Fprintf(buf, "\t\t&t.%s,\n", f.Name)
	}
	fmt.Fprintf(buf, "\t}\n}\n\n")

	fmt.Fprintf(buf, "// Fetch%s creates a FetchFunc for %s.\n", st.Name, st.Name)
	fmt.Fprintf(buf, "func Fetch%s() goent.FetchFunc {\n", st.Name)
	fmt.Fprintf(buf, "\treturn func(target any) []any {\n")
	fmt.Fprintf(buf, "\t\treturn target.(*%s).ScanFields()\n", st.Name)
	fmt.Fprintf(buf, "\t}\n}\n\n")
	return true
}

// getModelFields extracts the model fields from a struct type.
func getModelFields(st *ModelStruct) ([]ModelField, int) {
	var fields []ModelField
	idIndex, pkIndex, isComposite := -1, -1, false

	for i := range st.NumFields() {
		field := st.Field(i)
		if !field.Exported() || field.Embedded() {
			continue
		}
		goeTag := reflect.StructTag(st.Tag(i)).Get("goe")
		if goeTag == "-" {
			continue
		}

		fieldType := field.Type().String()
		if strings.HasPrefix(fieldType, "[]") {
			continue
		}
		if strings.HasPrefix(fieldType, "*") && isStructType(field.Type()) {
			continue
		}

		fieldName, fieldType := field.Name(), fieldType
		if utils.HasTagValue(goeTag, "pk") {
			if pkIndex >= 0 {
				isComposite = true
			}
			pkIndex = i
		} else if strings.ToUpper(fieldName) == "ID" {
			if fieldType == "int" || fieldType == "int64" {
				idIndex = i
			}
		}
		fields = append(fields, ModelField{Name: fieldName, Type: fieldType})
	}

	if isComposite {
		return fields, -1
	}
	if pkIndex < 0 && idIndex >= 0 {
		pkIndex = idIndex
	}
	return fields, pkIndex
}

// getStructClass returns the class of a struct.
func getStructClass(st *ModelStruct, pkgName string) (string, []string) {
	var size int
	if size = st.NumFields(); size == 0 {
		return "Nothing", nil
	}
	// if strings.HasSuffix(st.Name, "Database") {
	// 	return "Database", nil
	// }
	// if strings.HasSuffix(st.Name, "Schema") {
	// 	return "Schema", nil
	// }
	lastFieldType := st.Field(size - 1).Type().String()
	if strings.HasSuffix(lastFieldType, "goent.DB") {
		return "Database", nil
	}

	var names []string
	modelName, isSchema := "", false
	for i := range size {
		if st.Field(i).Embedded() {
			continue
		}
		fieldType := st.Field(i).Type().String()
		if modelName, isSchema = cutModelName(fieldType, pkgName); isSchema {
			// fmt.Printf("fieldType: %s, name: %s\n", fieldType, name)
			names = append(names, modelName)
		}
		if !isSchema {
			goeTag := reflect.StructTag(st.Tag(i)).Get("goe")
			if goeTag != "" && goeTag != "-" {
				return "Model", nil
			}
		}
	}
	if isSchema {
		return "Schema", names
	}
	return "", nil
}

func cutModelName(typeName string, pkgName string) (name string, ok bool) {
	if _, name, ok = strings.Cut(typeName, "goent.Table["); !ok {
		return "", false
	}
	var finalName string
	name = strings.TrimSuffix(name, "]")
	if _, finalName, ok = strings.Cut(name, pkgName+"."); ok {
		name = finalName
	}
	return name, true
}

// hasGoeTag checks if a struct has any goe tags.
func hasGoeTag(st *ModelStruct) bool {
	size := st.NumFields()
	for i := range size {
		if st.Field(i).Embedded() {
			continue
		}
		goeTag := reflect.StructTag(st.Tag(i)).Get("goe")
		if goeTag != "" && goeTag != "-" {
			return true
		}
	}
	return false
}

// hasMethod checks if a struct implements the specified method.
func hasMethod(st *ModelStruct, methodName string) bool {
	if st.named == nil {
		return false
	}
	for method := range st.named.Methods() {
		if method.Name() == methodName {
			return true
		}
	}
	return false
}

// isStructType checks if a type is a struct.
func isStructType(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	if !ok {
		return false
	}
	_, ok = named.Underlying().(*types.Struct)
	return ok
}
