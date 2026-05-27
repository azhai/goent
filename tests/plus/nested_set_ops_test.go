package plus_test

import (
	"reflect"
	"testing"

	"github.com/azhai/goent/plus"
)

func findTreeNode[T any](root *plus.TreeNode[T], name string) *plus.TreeNode[T] {
	var result *plus.TreeNode[T]
	root.WalkDepthFirst(func(n *plus.TreeNode[T]) {
		if result != nil {
			return
		}
		val := reflect.ValueOf(n.Data)
		if val.Kind() == reflect.Pointer {
			val = val.Elem()
		}
		if val.Kind() == reflect.Struct {
			nameField := val.FieldByName("Name")
			if nameField.IsValid() && nameField.CanInterface() {
				if s, ok := nameField.Interface().(string); ok && s == name {
					result = n
				}
			}
		}
	})
	return result
}

func TestNestedSet_IsDescendant(t *testing.T) {
	ancestor := &Category{Lft: 1, Rgt: 10}
	child := &Category{Lft: 2, Rgt: 5}
	grandchild := &Category{Lft: 3, Rgt: 4}
	unrelated := &Category{Lft: 11, Rgt: 12}

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	if !ns.IsDescendant(ancestor, child) {
		t.Error("child should be descendant of ancestor")
	}
	if !ns.IsDescendant(ancestor, grandchild) {
		t.Error("grandchild should be descendant of ancestor")
	}
	if ns.IsDescendant(ancestor, unrelated) {
		t.Error("unrelated should not be descendant")
	}
	if ns.IsDescendant(ancestor, ancestor) {
		t.Error("node should not be descendant of itself")
	}
}

func TestNestedSet_IsLeafNode(t *testing.T) {
	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")

	leaf := &Category{Lft: 3, Rgt: 4}
	if !ns.IsLeafNode(leaf) {
		t.Error("rgt == lft+1 should be leaf")
	}

	parent := &Category{Lft: 1, Rgt: 6}
	if ns.IsLeafNode(parent) {
		t.Error("rgt > lft+1 should not be leaf")
	}
}

func TestNestedSet_PrintTree(t *testing.T) {
	root := &plus.TreeNode[Category]{
		Data:  Category{Name: "Root", Lft: 1, Rgt: 10},
		Lft:   1,
		Rgt:   10,
		Depth: 1,
	}
	child := &plus.TreeNode[Category]{
		Data:  Category{Name: "Child", Lft: 2, Rgt: 5},
		Lft:   2,
		Rgt:   5,
		Depth: 2,
	}
	grandchild := &plus.TreeNode[Category]{
		Data:  Category{Name: "GrandChild", Lft: 3, Rgt: 4},
		Lft:   3,
		Rgt:   4,
		Depth: 3,
	}
	root.AddChild(child)
	child.AddChild(grandchild)

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	output := ns.PrintTree(root)
	if output == "" {
		t.Error("PrintTree should return non-empty string")
	}
}

func TestNestedSet_BuildComplexTree(t *testing.T) {
	records := []*Category{
		{ID: 1, Name: "Electronics", Lft: 1, Rgt: 14},
		{ID: 2, Name: "Phones", Lft: 2, Rgt: 7},
		{ID: 3, Name: "iPhone", Lft: 3, Rgt: 4},
		{ID: 4, Name: "Android", Lft: 5, Rgt: 6},
		{ID: 5, Name: "Laptops", Lft: 8, Rgt: 13},
		{ID: 6, Name: "MacBook", Lft: 9, Rgt: 10},
		{ID: 7, Name: "ThinkPad", Lft: 11, Rgt: 12},
	}

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)

	if root == nil {
		t.Fatal("BuildTree returned nil")
	}

	var nodeCount int
	root.WalkDepthFirst(func(n *plus.TreeNode[Category]) { nodeCount++ })
	if nodeCount != 7 {
		t.Errorf("tree has %d nodes, want 7", nodeCount)
	}

	if len(root.Children) != 2 {
		t.Errorf("root should have 2 children, got %d", len(root.Children))
	}

	phones := root.Children[0]
	if phones.Data.Name != "Phones" {
		t.Errorf("first child = %q, want %q", phones.Data.Name, "Phones")
	}
	if len(phones.Children) != 2 {
		t.Errorf("Phones should have 2 children, got %d", len(phones.Children))
	}
}

func TestTreeNode_DeepNesting(t *testing.T) {
	root := &plus.TreeNode[int]{Data: 0, Lft: 1, Rgt: 16, Depth: 1}
	current := root
	for i := 1; i <= 5; i++ {
		child := &plus.TreeNode[int]{Data: i, Lft: i * 2, Rgt: i*2 + 1, Depth: i + 1}
		current.AddChild(child)
		current = child
	}

	var depths []int
	root.WalkDepthFirst(func(n *plus.TreeNode[int]) { depths = append(depths, n.Depth) })
	expected := []int{1, 2, 3, 4, 5, 6}
	for i, v := range expected {
		if depths[i] != v {
			t.Errorf("depth[%d] = %d, want %d", i, depths[i], v)
		}
	}
}

func TestNestedSet_GetParent(t *testing.T) {
	records := []*Category{
		{ID: 1, Name: "Electronics", Lft: 1, Rgt: 14},
		{ID: 2, Name: "Phones", Lft: 2, Rgt: 7},
		{ID: 3, Name: "iPhone", Lft: 3, Rgt: 4},
		{ID: 4, Name: "Android", Lft: 5, Rgt: 6},
		{ID: 5, Name: "Laptops", Lft: 8, Rgt: 13},
		{ID: 6, Name: "MacBook", Lft: 9, Rgt: 10},
		{ID: 7, Name: "ThinkPad", Lft: 11, Rgt: 12},
	}

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)

	iPhoneNode := findTreeNode(root, "iPhone")
	if iPhoneNode == nil {
		t.Fatal("iPhone node not found")
	}
	if iPhoneNode.Parent == nil || iPhoneNode.Parent.Data.Name != "Phones" {
		t.Errorf("parent of iPhone = %v, want Phones", iPhoneNode.Parent)
	}

	phonesNode := findTreeNode(root, "Phones")
	if phonesNode == nil || phonesNode.Parent == nil || phonesNode.Parent.Data.Name != "Electronics" {
		t.Error("parent of Phones should be Electronics")
	}

	if root.Parent != nil {
		t.Error("root should have no parent")
	}
}

func TestNestedSet_GetParents(t *testing.T) {
	records := []*Category{
		{ID: 1, Name: "Electronics", Lft: 1, Rgt: 14},
		{ID: 2, Name: "Phones", Lft: 2, Rgt: 7},
		{ID: 3, Name: "iPhone", Lft: 3, Rgt: 4},
	}

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)

	iPhoneNode := findTreeNode(root, "iPhone")
	if iPhoneNode == nil {
		t.Fatal("iPhone node not found")
	}

	var parents []string
	current := iPhoneNode.Parent
	for current != nil {
		parents = append(parents, current.Data.Name)
		current = current.Parent
	}

	if len(parents) != 2 {
		t.Errorf("iPhone should have 2 ancestors, got %d", len(parents))
	}
	if parents[0] != "Phones" || parents[1] != "Electronics" {
		t.Errorf("ancestors = %v, want [Phones, Electronics]", parents)
	}
}

func TestNestedSet_GetChildren(t *testing.T) {
	records := []*Category{
		{ID: 1, Name: "Electronics", Lft: 1, Rgt: 14},
		{ID: 2, Name: "Phones", Lft: 2, Rgt: 7},
		{ID: 3, Name: "iPhone", Lft: 3, Rgt: 4},
		{ID: 4, Name: "Android", Lft: 5, Rgt: 6},
		{ID: 5, Name: "Laptops", Lft: 8, Rgt: 13},
		{ID: 6, Name: "MacBook", Lft: 9, Rgt: 10},
		{ID: 7, Name: "ThinkPad", Lft: 11, Rgt: 12},
	}

	ns := plus.NewNestedSet[Category](nil, "Lft", "Rgt")
	root := ns.BuildTree(records)

	electronics := root
	if len(electronics.Children) != 2 {
		t.Errorf("Electronics should have 2 direct children, got %d", len(electronics.Children))
	}

	var allDescendants []string
	electronics.WalkDepthFirst(func(n *plus.TreeNode[Category]) {
		if n != electronics {
			allDescendants = append(allDescendants, n.Data.Name)
		}
	})
	if len(allDescendants) != 6 {
		t.Errorf("Electronics should have 6 descendants, got %d", len(allDescendants))
	}

	phones := findTreeNode(root, "Phones")
	if phones == nil {
		t.Fatal("Phones node not found")
	}
	var phoneDescendants []string
	phones.WalkDepthFirst(func(n *plus.TreeNode[Category]) {
		if n != phones {
			phoneDescendants = append(phoneDescendants, n.Data.Name)
		}
	})
	if len(phoneDescendants) != 2 {
		t.Errorf("Phones should have 2 descendants, got %d", len(phoneDescendants))
	}

	laptops := findTreeNode(root, "Laptops")
	if laptops == nil {
		t.Fatal("Laptops node not found")
	}
	var laptopDescendants []string
	laptops.WalkDepthFirst(func(n *plus.TreeNode[Category]) {
		if n != laptops {
			laptopDescendants = append(laptopDescendants, n.Data.Name)
		}
	})
	if len(laptopDescendants) != 2 {
		t.Errorf("Laptops should have 2 descendants, got %d", len(laptopDescendants))
	}

	self, err := ns.GetChildren(&electronics.Data, 0)
	if err != nil {
		t.Fatalf("GetChildren(depth=0): %v", err)
	}
	if len(self) != 1 || self[0].Name != "Electronics" {
		t.Error("depth=0 should return self")
	}
}
