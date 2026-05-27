package plus

import (
	"reflect"
	"sort"

	"github.com/azhai/goent"
)

// TreeNode represents a node in a nested set tree structure.
type TreeNode[T any] struct {
	Data     T
	Children []*TreeNode[T]
	Parent   *TreeNode[T]
	Lft      int
	Rgt      int
	Depth    int
}

// IsLeaf returns true if this node has no children.
func (n *TreeNode[T]) IsLeaf() bool { return len(n.Children) == 0 }

// IsRoot returns true if this node has no parent.
func (n *TreeNode[T]) IsRoot() bool { return n.Parent == nil }

// AddChild appends a child node and sets the parent reference.
func (n *TreeNode[T]) AddChild(child *TreeNode[T]) {
	child.Parent = n
	n.Children = append(n.Children, child)
}

// WalkDepthFirst traverses the tree depth-first (pre-order).
func (n *TreeNode[T]) WalkDepthFirst(fn func(*TreeNode[T])) {
	fn(n)
	for _, child := range n.Children {
		child.WalkDepthFirst(fn)
	}
}

// WalkBreadthFirst traverses the tree breadth-first.
func (n *TreeNode[T]) WalkBreadthFirst(fn func(*TreeNode[T])) {
	queue := []*TreeNode[T]{n}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		fn(node)
		queue = append(queue, node.Children...)
	}
}

// NestedSet provides Nested Set model operations for tree-structured data.
// The model T must have Lft and Rgt fields (int type) for left/right values.
type NestedSet[T any] struct {
	table *goent.Table[T]
	lft   string
	rgt   string
}

// NewNestedSet creates a new NestedSet instance for the given table.
// lftField and rgtField are the column names for left/right values.
func NewNestedSet[T any](tbl *goent.Table[T], lftField, rgtField string) *NestedSet[T] {
	return &NestedSet[T]{
		table: tbl,
		lft:   lftField,
		rgt:   rgtField,
	}
}

// BuildTree builds a tree structure from flat records sorted by Lft value.
func (ns *NestedSet[T]) BuildTree(records []*T) *TreeNode[T] {
	if len(records) == 0 {
		return nil
	}

	sort.SliceStable(records, func(i, j int) bool {
		return getLftValue(ns.lft, records[i]) < getLftValue(ns.lft, records[j])
	})

	root := &TreeNode[T]{
		Data:  *records[0],
		Lft:   getLftValue(ns.lft, records[0]),
		Rgt:   getRgtValue(ns.rgt, records[0]),
		Depth: 1,
	}
	stack := []*TreeNode[T]{root}

	for i := 1; i < len(records); i++ {
		node := &TreeNode[T]{
			Data:  *records[i],
			Lft:   getLftValue(ns.lft, records[i]),
			Rgt:   getRgtValue(ns.rgt, records[i]),
			Depth: len(stack) + 1,
		}

		for len(stack) > 0 && stack[len(stack)-1].Rgt < node.Lft {
			stack = stack[:len(stack)-1]
		}

		if len(stack) > 0 {
			parent := stack[len(stack)-1]
			parent.AddChild(node)
		}
		stack = append(stack, node)
	}

	return root
}

// FlattenTree converts a tree back to flat records with updated Lft/Rgt values.
func (ns *NestedSet[T]) FlattenTree(root *TreeNode[T], start int) ([]*T, int) {
	var result []*T
	current := start
	ns.flattenNode(root, &current, &result)
	return result, current - 1
}

func (ns *NestedSet[T]) flattenNode(node *TreeNode[T], counter *int, result *[]*T) {
	node.Lft = *counter
	*counter++

	for _, child := range node.Children {
		ns.flattenNode(child, counter, result)
	}

	node.Rgt = *counter
	*counter++
	setLftValue(ns.lft, &node.Data, node.Lft)
	setRgtValue(ns.rgt, &node.Data, node.Rgt)
	*result = append(*result, &node.Data)
}

func getLftValue(field string, item any) int {
	return getIntFieldValue(field, item)
}

func getRgtValue(field string, item any) int {
	return getIntFieldValue(field, item)
}

func setLftValue(field string, item any, val int) {
	setIntFieldValue(field, item, val)
}

func setRgtValue(field string, item any, val int) {
	setIntFieldValue(field, item, val)
}

func getIntFieldValue(fieldName string, item any) int {
	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() == reflect.Struct {
		field := val.FieldByName(fieldName)
		if field.IsValid() && field.CanInt() {
			return int(field.Int())
		}
	}
	return 0
}

func setIntFieldValue(fieldName string, item any, value int) {
	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() == reflect.Struct {
		field := val.FieldByName(fieldName)
		if field.IsValid() && field.CanSet() && field.Kind() == reflect.Int {
			field.SetInt(int64(value))
		}
	}
}
