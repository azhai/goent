package goent_test

import (
	"testing"

	"github.com/azhai/goent/utils"
)

// TestGetTagValueKeyColonFormat verifies the original "key:value" format
// for struct tags still works correctly.
func TestGetTagValueKeyColonFormat(t *testing.T) {
	tag := "pk;type:varchar(50);default:32"

	val, ok := utils.GetTagValue(tag, "type")
	if !ok {
		t.Error("Expected to find 'type' in tag")
	}
	if val != "varchar(50)" {
		t.Errorf("Expected 'varchar(50)', got '%s'", val)
	}

	val, ok = utils.GetTagValue(tag, "default")
	if !ok {
		t.Error("Expected to find 'default' in tag")
	}
	if val != "32" {
		t.Errorf("Expected '32', got '%s'", val)
	}
}

// TestGetTagValueKeyEqualsFormat verifies that GetTagValue supports "key=value"
// format in struct tags. Before the fix, only "key:value" was supported,
// causing tags like `goe:"fk=order_id"` to be unparseable.
func TestGetTagValueKeyEqualsFormat(t *testing.T) {
	tag := "fk=order_id;type=varchar(100);index(unique n:idx_name)"

	val, ok := utils.GetTagValue(tag, "fk")
	if !ok {
		t.Error("Expected to find 'fk' via '=' format in tag (bug: GetTagValue doesn't support key=value)")
	}
	if val != "order_id" {
		t.Errorf("Expected 'order_id', got '%s'", val)
	}

	val, ok = utils.GetTagValue(tag, "type")
	if !ok {
		t.Error("Expected to find 'type' via '=' format in tag")
	}
	if val != "varchar(100)" {
		t.Errorf("Expected 'varchar(100)', got '%s'", val)
	}
}

// TestGetTagValueMixedFormats verifies that both "key:value" and "key=value"
// formats can coexist in the same tag.
func TestGetTagValueMixedFormats(t *testing.T) {
	tag := "pk;type:varchar(50);fk=order_id;index(unique n:idx_name)"

	val, ok := utils.GetTagValue(tag, "type")
	if !ok {
		t.Error("Expected to find 'type' via ':' format")
	}
	if val != "varchar(50)" {
		t.Errorf("Expected 'varchar(50)', got '%s'", val)
	}

	val, ok = utils.GetTagValue(tag, "fk")
	if !ok {
		t.Error("Expected to find 'fk' via '=' format")
	}
	if val != "order_id" {
		t.Errorf("Expected 'order_id', got '%s'", val)
	}
}

// TestGetTagValueNotFound verifies that GetTagValue returns false for
// keys that don't exist in the tag.
func TestGetTagValueNotFound(t *testing.T) {
	tag := "pk;type:varchar(50)"

	_, ok := utils.GetTagValue(tag, "index")
	if ok {
		t.Error("Expected 'index' to NOT be found in tag")
	}

	_, ok = utils.GetTagValue(tag, "nonexistent")
	if ok {
		t.Error("Expected 'nonexistent' to NOT be found in tag")
	}
}

// TestHasTagValueSemicolonSeparator verifies the original semicolon-separated
// format for HasTagValue. HasTagValue matches keys WITHOUT values (standalone keys).
func TestHasTagValueSemicolonSeparator(t *testing.T) {
	tag := "pk;type:varchar(50);m2o"

	if !utils.HasTagValue(tag, "pk") {
		t.Error("Expected 'pk' to be found in tag")
	}
	if !utils.HasTagValue(tag, "m2o") {
		t.Error("Expected 'm2o' to be found in tag")
	}
	if utils.HasTagValue(tag, "pk2") {
		t.Error("Expected 'pk2' to NOT be found in tag")
	}
}

// TestHasTagValueCommaSeparator verifies that HasTagValue supports comma
// as a separator in addition to semicolon. Before the fix, tags using
// comma separators like `goe:"pk,type:varchar(50)"` would not be parsed.
func TestHasTagValueCommaSeparator(t *testing.T) {
	tag := "pk,type:varchar(50),m2o"

	if !utils.HasTagValue(tag, "pk") {
		t.Error("Expected 'pk' to be found in comma-separated tag (bug: HasTagValue doesn't support comma)")
	}
	if !utils.HasTagValue(tag, "m2o") {
		t.Error("Expected 'm2o' to be found in comma-separated tag")
	}
	if utils.HasTagValue(tag, "pk2") {
		t.Error("Expected 'pk2' to NOT be found in comma-separated tag")
	}
}

// TestGetTagValueCommaSeparator verifies that GetTagValue also supports
// comma-separated tags.
func TestGetTagValueCommaSeparator(t *testing.T) {
	tag := "pk,type:varchar(50),fk=order_id"

	val, ok := utils.GetTagValue(tag, "type")
	if !ok {
		t.Error("Expected to find 'type' in comma-separated tag")
	}
	if val != "varchar(50)" {
		t.Errorf("Expected 'varchar(50)', got '%s'", val)
	}

	val, ok = utils.GetTagValue(tag, "fk")
	if !ok {
		t.Error("Expected to find 'fk' in comma-separated tag")
	}
	if val != "order_id" {
		t.Errorf("Expected 'order_id', got '%s'", val)
	}
}

// TestHasTagValueNoPartialMatch verifies that HasTagValue does not match
// partial keys. For example, "pk" should not match "pk2".
func TestHasTagValueNoPartialMatch(t *testing.T) {
	tag := "pk;pk2;type:varchar(50)"

	if !utils.HasTagValue(tag, "pk") {
		t.Error("Expected 'pk' to be found")
	}
	if !utils.HasTagValue(tag, "pk2") {
		t.Error("Expected 'pk2' to be found")
	}
	if utils.HasTagValue(tag, "p") {
		t.Error("Expected 'p' to NOT be found (partial match should fail)")
	}
}
