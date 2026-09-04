package plus_test

import (
	"testing"

	"github.com/azhai/goent/plus"
)

// Category64 declares Lft/Rgt as int64 to guard the regression where the
// nested-set writer only handled the int kind and silently skipped wider
// integer columns.
type Category64 struct {
	ID   int64 `goe:"pk"`
	Name string
	Lft  int64
	Rgt  int64
}

func TestNestedSet_Int64FieldsRoundTrip(t *testing.T) {
	records := []*Category64{
		{ID: 1, Name: "Root", Lft: 1, Rgt: 10},
		{ID: 2, Name: "Child", Lft: 2, Rgt: 3},
	}
	ns := plus.NewNestedSet[Category64](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)
	if root == nil {
		t.Fatal("BuildTree returned nil for int64 fields")
	}
	if root.Lft != 1 || root.Rgt != 10 {
		t.Errorf("root bounds = [%d,%d], want [1,10]", root.Lft, root.Rgt)
	}

	// Re-number the tree from scratch; the int64 columns must be written back.
	flat, last := ns.FlattenTree(root, 1)
	if last != 4 { // 2 nodes => 4 boundary numbers
		t.Errorf("FlattenTree last = %d, want 4", last)
	}
	for _, r := range flat {
		if r.Lft == 0 || r.Rgt == 0 {
			t.Errorf("int64 Lft/Rgt not written back: %+v", r)
		}
	}
}

// CategoryU exercises unsigned columns, which the writer must also accept.
type CategoryU struct {
	ID   int64  `goe:"pk"`
	Name string
	Lft  uint32
	Rgt  uint32
}

func TestNestedSet_UintFieldsRoundTrip(t *testing.T) {
	records := []*CategoryU{
		{ID: 1, Name: "Root", Lft: 1, Rgt: 6},
		{ID: 2, Name: "Child", Lft: 2, Rgt: 3},
		{ID: 3, Name: "Sub", Lft: 4, Rgt: 5},
	}
	ns := plus.NewNestedSet[CategoryU](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)
	flat, last := ns.FlattenTree(root, 1)
	if last != 6 {
		t.Errorf("FlattenTree last = %d, want 6", last)
	}
	for _, r := range flat {
		if r.Rgt <= r.Lft {
			t.Errorf("malformed uint bounds after flatten: %+v", r)
		}
	}
}

func TestNestedSet_FlattenTree_NilRoot(t *testing.T) {
	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	flat, last := ns.FlattenTree(nil, 1)
	if flat != nil {
		t.Errorf("FlattenTree(nil) = %v, want nil", flat)
	}
	if last != 0 {
		t.Errorf("FlattenTree(nil) last = %d, want 0 (start-1)", last)
	}
}

// Malformed rows whose Lft falls outside the root's Rgt must not be dropped;
// the builder must keep them under the root rather than lose the data.
func TestNestedSet_BuildTree_MalformedRows(t *testing.T) {
	records := []*Category{
		{ID: 1, Name: "Root", Lft: 1, Rgt: 4},
		{ID: 2, Name: "Orphan", Lft: 9, Rgt: 10}, // outside the root's [1,4]
	}
	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)
	if root == nil {
		t.Fatal("BuildTree returned nil")
	}
	// The orphan is attached under the root instead of being discarded.
	total := 0
	root.WalkDepthFirst(func(n *plus.TreeNode[Category]) { total++ })
	if total != 2 {
		t.Errorf("tree has %d nodes, want 2 (orphan must be kept)", total)
	}
}

func TestNestedSet_BuildTree_Empty(t *testing.T) {
	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	if root := ns.BuildTree(nil); root != nil {
		t.Errorf("BuildTree(nil) = %v, want nil", root)
	}
	if root := ns.BuildTree([]*Category{}); root != nil {
		t.Errorf("BuildTree(empty) = %v, want nil", root)
	}
}

// RoundTrip verifies that flattening a tree built from a flat list yields a
// valid nested-set numbering (each node's Lft < Rgt, and children are nested).
func TestNestedSet_RoundTrip(t *testing.T) {
	records := []*Category{
		{ID: 1, Name: "A", Lft: 1, Rgt: 12},
		{ID: 2, Name: "B", Lft: 2, Rgt: 5},
		{ID: 3, Name: "C", Lft: 3, Rgt: 4},
		{ID: 4, Name: "D", Lft: 6, Rgt: 11},
		{ID: 5, Name: "E", Lft: 7, Rgt: 10},
		{ID: 6, Name: "F", Lft: 8, Rgt: 9},
	}
	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)
	flat, _ := ns.FlattenTree(root, 1)

	byID := make(map[int64]*Category, len(flat))
	for _, r := range flat {
		byID[r.ID] = r
	}
	check := func(c *Category) {
		if c.Lft <= 0 || c.Rgt <= c.Lft {
			t.Errorf("invalid bounds for %q: [%d,%d]", c.Name, c.Lft, c.Rgt)
		}
	}
	for _, c := range flat {
		check(c)
	}
	// Descendants must be nested: C (3,4) lives inside B (2,5).
	if b, c := byID[2], byID[3]; !(b.Lft < c.Lft && c.Rgt < b.Rgt) {
		t.Errorf("C not nested under B: B=%v C=%v", *b, *c)
	}
}
