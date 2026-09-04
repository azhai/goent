package utils_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/azhai/goent/utils"
)

type taggedUser struct {
	ID   int
	Name string
}

func (*taggedUser) TableName() string { return "t_user" }

type plainUser struct {
	ID   int
	Name string
}

func TestTableNameMethod(t *testing.T) {
	withMethod := reflect.New(reflect.TypeFor[taggedUser]())
	if got, want := utils.TableNameMethod(withMethod), "t_user"; got != want {
		t.Errorf("TableNameMethod() = %q, want %q", got, want)
	}
	withoutMethod := reflect.New(reflect.TypeFor[plainUser]())
	if got := utils.TableNameMethod(withoutMethod); got != "" {
		t.Errorf("TableNameMethod() = %q, want empty", got)
	}
}

func TestParseTableName(t *testing.T) {
	if got, want := utils.ParseTableNameByType(reflect.TypeFor[taggedUser]()), "t_user"; got != want {
		t.Errorf("ParseTableNameByType() = %q, want %q", got, want)
	}
	// No TableName() method: fall back to the snake_case type name.
	if got, want := utils.ParseTableNameByType(reflect.TypeFor[plainUser]()), "plain_user"; got != want {
		t.Errorf("ParseTableNameByType() = %q, want %q", got, want)
	}
	value := reflect.New(reflect.TypeFor[plainUser]())
	if got, want := utils.ParseTableNameByValue(value), "plain_user"; got != want {
		t.Errorf("ParseTableNameByValue() = %q, want %q", got, want)
	}
	if got, want := utils.TableNamePattern("UserRole"), "user_role"; got != want {
		t.Errorf("TableNamePattern() = %q, want %q", got, want)
	}
}

func TestHasTagValue(t *testing.T) {
	cases := []struct {
		tag  string
		key  string
		want bool
	}{
		{"pk", "pk", true},
		{"pk;not_incr", "pk", true},
		{"pk,not_incr", "not_incr", true},
		{"index(n:idx f:lower)", "index", false},
		{"pk", "index", false},
		{"type:varchar(10)", "type", false},
		{"", "pk", false},
	}
	for _, c := range cases {
		if got := utils.HasTagValue(c.tag, c.key); got != c.want {
			t.Errorf("HasTagValue(%q, %q) = %v, want %v", c.tag, c.key, got, c.want)
		}
	}
}

func TestGetTagValue(t *testing.T) {
	cases := []struct {
		tag   string
		key   string
		want  string
		found bool
	}{
		{"type:varchar(10)", "type", "varchar(10)", true},
		{"pk;type:uuid", "type", "uuid", true},
		{"default:uuid_generate_v4()", "default", "uuid_generate_v4()", true},
		{"size=10", "size", "10", true},
		{"o2m;fk=animal_id", "fk", "animal_id", true},
		{"pk", "type", "", false},
		// Parameters of index(...) are space separated and handled by the
		// goent package, not by GetTagValue.
		{"index(n:idx_name f:lower)", "n", "", false},
	}
	for _, c := range cases {
		got, ok := utils.GetTagValue(c.tag, c.key)
		if ok != c.found || got != c.want {
			t.Errorf("GetTagValue(%q, %q) = (%q, %v), want (%q, %v)",
				c.tag, c.key, got, ok, c.want, c.found)
		}
	}
}

func TestParseSchemaTag(t *testing.T) {
	cases := []struct {
		tag        string
		wantSchema string
		wantPrefix string
	}{
		{"public", "public", ""},
		{"public;prefix:t_", "public", "t_"},
		{"prefix:t_", "", "t_"},
		{"", "", ""},
	}
	for _, c := range cases {
		schema, prefix := utils.ParseSchemaTag(c.tag)
		if schema != c.wantSchema || prefix != c.wantPrefix {
			t.Errorf("ParseSchemaTag(%q) = (%q, %q), want (%q, %q)",
				c.tag, schema, prefix, c.wantSchema, c.wantPrefix)
		}
	}
}

func TestToAnySlice(t *testing.T) {
	values, n := utils.ToAnySlice([]int{1, 2, 3})
	if n != 3 || len(values) != 3 || values[2] != 3 {
		t.Errorf("ToAnySlice([]int{1,2,3}) = %v, %d", values, n)
	}
	// A non-slice is wrapped into a single-element slice.
	values, n = utils.ToAnySlice("abc")
	if n != 1 || len(values) != 1 || values[0] != "abc" {
		t.Errorf("ToAnySlice(\"abc\") = %v, %d", values, n)
	}
	values, n = utils.ToAnySlice(nil)
	if n != 0 || values != nil {
		t.Errorf("ToAnySlice(nil) = %v, %d, want nil, 0", values, n)
	}
}

func TestGetElemTypeAndValue(t *testing.T) {
	ptr := &plainUser{ID: 7}
	typeOf := utils.GetElemType(reflect.ValueOf(ptr))
	if typeOf != reflect.TypeFor[plainUser]() {
		t.Errorf("GetElemType() = %v, want plainUser", typeOf)
	}

	var nilPtr *plainUser
	if got := utils.GetElemType(reflect.ValueOf(nilPtr)); got != reflect.TypeFor[plainUser]() {
		t.Errorf("GetElemType(nil ptr) = %v, want plainUser", got)
	}
	// A nil pointer must be returned untouched (an Elem() call would panic).
	if got := utils.GetElemValue(reflect.ValueOf(nilPtr)); got.Kind() != reflect.Pointer {
		t.Errorf("GetElemValue(nil ptr) = %v, want a pointer", got.Kind())
	}
	if got := utils.GetElemValue(reflect.ValueOf(ptr)); got.Kind() != reflect.Struct {
		t.Errorf("GetElemValue(ptr) = %v, want a struct", got.Kind())
	}
}

func TestGetFieldNames(t *testing.T) {
	names := utils.GetFieldNames(reflect.TypeFor[plainUser]())
	if len(names) != 2 || names[0] != "ID" || names[1] != "Name" {
		t.Errorf("GetFieldNames() = %v, want [ID Name]", names)
	}
	if got := utils.GetFieldNames(reflect.TypeFor[int]()); got != nil {
		t.Errorf("GetFieldNames(int) = %v, want nil", got)
	}
}

func TestImports(t *testing.T) {
	imports := utils.NewImports()
	if got := imports.String(); got != "" {
		t.Errorf("empty Imports.String() = %q, want empty", got)
	}
	// Duplicates must collapse and the output must be sorted and grouped.
	imports.AddStdPackage(`"fmt"`)
	imports.AddStdPackage(`"fmt"`)
	imports.AddStdPackage(`"bytes"`)
	imports.AddThirdPackage(`"github.com/azhai/goent"`)

	want := "import (\n\t\"bytes\"\n\t\"fmt\"\n\n\t\"github.com/azhai/goent\"\n)\n\n"
	if got := imports.String(); got != want {
		t.Errorf("Imports.String() =\n%q\nwant\n%q", got, want)
	}
	if got := imports.GetStdPackage(); len(got) != 2 || got[0] != `"bytes"` {
		t.Errorf("GetStdPackage() = %v, want [\"bytes\" \"fmt\"]", got)
	}
	if got := imports.GetThirdPackage(); len(got) != 1 {
		t.Errorf("GetThirdPackage() = %v, want 1 entry", got)
	}

	var buf bytes.Buffer
	imports.WriteTo(&buf)
	if buf.String() != want {
		t.Errorf("WriteTo() = %q, want %q", buf.String(), want)
	}
}
