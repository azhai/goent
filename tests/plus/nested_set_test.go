package plus_test

import (
	"testing"

	"github.com/azhai/goent/plus"
)

type Category struct {
	ID       int64 `goe:"pk"`
	Name     string
	ParentID int64
	Lft      int
	Rgt      int
}

func TestTreeNode_Basic(t *testing.T) {
	root := &plus.TreeNode[Category]{
		Data:  Category{ID: 1, Name: "Root", Lft: 1, Rgt: 10},
		Lft:   1,
		Rgt:   10,
		Depth: 1,
	}

	if root.Data.Name != "Root" {
		t.Errorf("root name = %q, want %q", root.Data.Name, "Root")
	}
	if !root.IsRoot() {
		t.Error("root should be root")
	}
	if !root.IsLeaf() {
		t.Error("root with no children should be leaf")
	}

	child := &plus.TreeNode[Category]{
		Data:  Category{ID: 2, Name: "Child", Lft: 2, Rgt: 5},
		Lft:   2,
		Rgt:   5,
		Depth: 2,
	}
	root.AddChild(child)

	if root.IsLeaf() {
		t.Error("root with child should not be leaf")
	}
	if child.IsRoot() {
		t.Error("child should not be root")
	}
	if !child.IsLeaf() {
		t.Error("child with no children should be leaf")
	}
	if len(root.Children) != 1 {
		t.Errorf("root should have 1 child, got %d", len(root.Children))
	}
}

func TestNestedSet_BuildTree(t *testing.T) {
	records := []*Category{
		{ID: 1, Name: "Electronics", Lft: 1, Rgt: 10},
		{ID: 2, Name: "Phones", Lft: 2, Rgt: 5},
		{ID: 3, Name: "iPhone", Lft: 3, Rgt: 4},
		{ID: 4, Name: "Laptops", Lft: 6, Rgt: 9},
		{ID: 5, Name: "MacBook", Lft: 7, Rgt: 8},
	}

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)

	if root == nil {
		t.Fatal("BuildTree returned nil")
	}
	if root.Data.Name != "Electronics" {
		t.Errorf("root name = %q, want %q", root.Data.Name, "Electronics")
	}
	if len(root.Children) != 2 {
		t.Errorf("root has %d children, want 2", len(root.Children))
	}
}

func TestNestedSet_FlattenTree(t *testing.T) {
	root := &plus.TreeNode[Category]{
		Data:  Category{Name: "Root"},
		Lft:   1,
		Rgt:   4,
		Depth: 1,
	}
	child := &plus.TreeNode[Category]{
		Data:  Category{Name: "Child"},
		Lft:   2,
		Rgt:   3,
		Depth: 2,
	}
	root.AddChild(child)

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	flattened, _ := ns.FlattenTree(root, 1)

	if len(flattened) != 2 {
		t.Errorf("flattened has %d records, want 2", len(flattened))
	}
	if flattened[0].Name != "Child" {
		t.Errorf("first record = %q, want %q", flattened[0].Name, "Child")
	}
	if flattened[1].Name != "Root" {
		t.Errorf("second record = %q, want %q", flattened[1].Name, "Root")
	}
}

func TestTreeNode_WalkDepthFirst(t *testing.T) {
	root := &plus.TreeNode[int]{Data: 1, Lft: 1, Rgt: 4, Depth: 1}
	child1 := &plus.TreeNode[int]{Data: 2, Lft: 2, Rgt: 3, Depth: 2}
	root.AddChild(child1)

	var order []int
	root.WalkDepthFirst(func(n *plus.TreeNode[int]) { order = append(order, n.Data) })

	expected := []int{1, 2}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %d, want %d", i, order[i], v)
		}
	}
}

func TestTreeNode_WalkBreadthFirst(t *testing.T) {
	root := &plus.TreeNode[int]{Data: 1, Lft: 1, Rgt: 6, Depth: 1}
	child1 := &plus.TreeNode[int]{Data: 2, Lft: 2, Rgt: 3, Depth: 2}
	child2 := &plus.TreeNode[int]{Data: 3, Lft: 4, Rgt: 5, Depth: 2}
	root.AddChild(child1)
	root.AddChild(child2)

	var order []int
	root.WalkBreadthFirst(func(n *plus.TreeNode[int]) { order = append(order, n.Data) })

	expected := []int{1, 2, 3}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("order[%d] = %d, want %d", i, order[i], v)
		}
	}
}
