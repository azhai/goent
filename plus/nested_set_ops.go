package plus

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/azhai/goent"
)

// InsertAsLastChild inserts a new node as the last child of parent.
func (ns *NestedSet[T]) InsertAsLastChild(parent *T, child *T) error {
	parentRgt := getRgtValue(ns.rgt, parent)

	err := ns.shiftRightValues(parentRgt, 2)
	if err != nil {
		return fmt.Errorf("shift right: %w", err)
	}

	setLftValue(ns.lft, child, parentRgt)
	setRgtValue(ns.rgt, child, parentRgt+1)
	return nil
}

// InsertAsFirstChild inserts a new node as the first child of parent.
func (ns *NestedSet[T]) InsertAsFirstChild(parent *T, child *T) error {
	parentLft := getLftValue(ns.lft, parent)

	err := ns.shiftRightValues(parentLft, 2)
	if err != nil {
		return fmt.Errorf("shift right: %w", err)
	}

	setLftValue(ns.lft, child, parentLft)
	setRgtValue(ns.rgt, child, parentLft+1)
	return nil
}

// InsertAsPrevSibling inserts a new node as the previous sibling of sibling.
func (ns *NestedSet[T]) InsertAsPrevSibling(sibling *T, node *T) error {
	siblingLft := getLftValue(ns.lft, sibling)

	err := ns.shiftRightValues(siblingLft, 2)
	if err != nil {
		return fmt.Errorf("shift right: %w", err)
	}

	setLftValue(ns.lft, node, siblingLft)
	setRgtValue(ns.rgt, node, siblingLft+1)
	return nil
}

// InsertAsNextSibling inserts a new node as the next sibling of sibling.
func (ns *NestedSet[T]) InsertAsNextSibling(sibling *T, node *T) error {
	siblingRgt := getRgtValue(ns.rgt, sibling)

	err := ns.shiftRightValues(siblingRgt, 2)
	if err != nil {
		return fmt.Errorf("shift right: %w", err)
	}

	setLftValue(ns.lft, node, siblingRgt)
	setRgtValue(ns.rgt, node, siblingRgt+1)
	return nil
}

// DeleteSubtree removes a node and all its descendants.
func (ns *NestedSet[T]) DeleteSubtree(node *T) error {
	nodeLft := getLftValue(ns.lft, node)
	nodeRgt := getRgtValue(ns.rgt, node)
	width := nodeRgt - nodeLft + 1

	err := ns.deleteRange(nodeLft, nodeRgt)
	if err != nil {
		return fmt.Errorf("delete range: %w", err)
	}
	ns.shiftLeftValues(nodeRgt+1, width)
	return nil
}

// shiftRightValues shifts all Lft/Rgt values >= threshold by delta.
func (ns *NestedSet[T]) shiftRightValues(threshold, delta int) error {
	lftField := ns.table.Field(ns.lft)
	rgtField := ns.table.Field(ns.rgt)

	err := ns.table.Update().
		Set(goent.Pair{Key: ns.lft, Value: lftField.String() + " + " + fmt.Sprintf("%d", delta)}).
		Filter(goent.GreaterEquals(lftField, threshold)).
		Exec()
	if err != nil {
		return err
	}

	err = ns.table.Update().
		Set(goent.Pair{Key: ns.rgt, Value: rgtField.String() + " + " + fmt.Sprintf("%d", delta)}).
		Filter(goent.GreaterEquals(rgtField, threshold)).
		Exec()
	return err
}

// shiftLeftValues shifts all Lft/Rgt values >= threshold by -delta.
func (ns *NestedSet[T]) shiftLeftValues(threshold, delta int) error {
	lftField := ns.table.Field(ns.lft)
	rgtField := ns.table.Field(ns.rgt)

	err := ns.table.Update().
		Set(goent.Pair{Key: ns.lft, Value: lftField.String() + " - " + fmt.Sprintf("%d", delta)}).
		Filter(goent.GreaterEquals(lftField, threshold)).
		Exec()
	if err != nil {
		return err
	}

	err = ns.table.Update().
		Set(goent.Pair{Key: ns.rgt, Value: rgtField.String() + " - " + fmt.Sprintf("%d", delta)}).
		Filter(goent.GreaterEquals(rgtField, threshold)).
		Exec()
	return err
}

// deleteRange removes records where Lft is within [lft, rgt].
func (ns *NestedSet[T]) deleteRange(lft, rgt int) error {
	lftField := ns.table.Field(ns.lft)
	_ = ns.table.Delete().
		Filter(goent.GreaterEquals(lftField, lft)).
		Filter(goent.LessEquals(lftField, rgt)).
		Exec()
	return nil
}

// GetAncestors returns all ancestor nodes from root to parent.
func (ns *NestedSet[T]) GetAncestors(node *T) ([]*T, error) {
	if ns.table == nil {
		return []*T{}, nil
	}
	nodeLft := getLftValue(ns.lft, node)
	lftField := ns.table.Field(ns.lft)
	rgtField := ns.table.Field(ns.rgt)

	return ns.table.Select().
		Filter(goent.Less(lftField, nodeLft)).
		Filter(goent.Greater(rgtField, getRgtValue(ns.rgt, node))).
		OrderBy(ns.lft).
		All()
}

// GetParent returns the direct parent node of the given node.
func (ns *NestedSet[T]) GetParent(node *T) (*T, error) {
	if ns.table == nil {
		return nil, nil
	}
	nodeLft := getLftValue(ns.lft, node)
	lftField := ns.table.Field(ns.lft)
	rgtField := ns.table.Field(ns.rgt)

	results, err := ns.table.Select().
		Filter(goent.Less(lftField, nodeLft)).
		Filter(goent.Greater(rgtField, getRgtValue(ns.rgt, node))).
		OrderBy(ns.rgt + " ASC").
		Take(1).
		All()
	if err != nil || len(results) == 0 {
		return nil, err
	}
	return results[0], nil
}

// GetParents returns all ancestor nodes from root to direct parent.
func (ns *NestedSet[T]) GetParents(node *T) ([]*T, error) {
	return ns.GetAncestors(node)
}

// GetChildren returns descendant nodes within specified depth.
// depth=0 returns self, depth>0 limits levels, depth<0 returns all descendants.
func (ns *NestedSet[T]) GetChildren(node *T, depth int) ([]*T, error) {
	if depth == 0 {
		result := *node
		return []*T{&result}, nil
	}

	if ns.table == nil {
		nodeLft := getLftValue(ns.lft, node)
		nodeRgt := getRgtValue(ns.rgt, node)
		if depth < 0 {
			return []*T{}, nil
		}
		maxRgt := nodeLft + (nodeRgt-nodeLft+1)/(depth+1)*2 - 1
		if maxRgt >= nodeRgt {
			maxRgt = nodeRgt - 1
		}
		return []*T{}, nil
	}

	nodeLft := getLftValue(ns.lft, node)
	nodeRgt := getRgtValue(ns.rgt, node)
	lftField := ns.table.Field(ns.lft)
	rgtField := ns.table.Field(ns.rgt)

	if depth < 0 {
		return ns.table.Select().
			Filter(goent.Greater(lftField, nodeLft)).
			Filter(goent.Less(rgtField, nodeRgt)).
			OrderBy(ns.lft).
			All()
	}

	maxRgt := nodeLft + (nodeRgt-nodeLft+1)/(depth+1)*2 - 1
	if maxRgt >= nodeRgt {
		maxRgt = nodeRgt - 1
	}

	return ns.table.Select().
		Filter(goent.Greater(lftField, nodeLft)).
		Filter(goent.Less(lftField, maxRgt+1)).
		Filter(goent.Less(rgtField, nodeRgt)).
		OrderBy(ns.lft).
		All()
}

// GetDescendants returns all descendant nodes (not including self).
func (ns *NestedSet[T]) GetDescendants(node *T) ([]*T, error) {
	nodeLft := getLftValue(ns.lft, node)
	nodeRgt := getRgtValue(ns.rgt, node)
	lftField := ns.table.Field(ns.lft)
	rgtField := ns.table.Field(ns.rgt)

	return ns.table.Select().
		Filter(goent.Greater(lftField, nodeLft)).
		Filter(goent.Less(rgtField, nodeRgt)).
		OrderBy(ns.lft).
		All()
}

// GetLevel returns the depth/level of a node in the tree (root = 1).
func (ns *NestedSet[T]) GetLevel(node *T) (int, error) {
	ancestors, err := ns.GetAncestors(node)
	if err != nil {
		return 0, err
	}
	return len(ancestors) + 1, nil
}

// IsDescendant checks if potentialChild is a descendant of ancestor.
func (ns *NestedSet[T]) IsDescendant(ancestor, potentialChild *T) bool {
	aLft := getLftValue(ns.lft, ancestor)
	aRgt := getRgtValue(ns.rgt, ancestor)
	cLft := getLftValue(ns.lft, potentialChild)
	cRgt := getRgtValue(ns.rgt, potentialChild)
	return cLft > aLft && cRgt < aRgt
}

// IsLeafNode checks if a node has no children.
func (ns *NestedSet[T]) IsLeafNode(node *T) bool {
	lft := getLftValue(ns.lft, node)
	rgt := getRgtValue(ns.rgt, node)
	return rgt == lft+1
}

// PrintTree prints tree structure with indentation for debugging.
func (ns *NestedSet[T]) PrintTree(root *TreeNode[T]) string {
	var sb strings.Builder
	root.WalkDepthFirst(func(n *TreeNode[T]) {
		sb.WriteString(strings.Repeat("  ", n.Depth-1))
		fmt.Fprintf(&sb, "%s (lft=%d, rgt=%d)\n",
			getNameValue(n.Data), n.Lft, n.Rgt)
	})
	return sb.String()
}

func getNameValue(item any) string {
	val := reflect.ValueOf(item)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	if val.Kind() == reflect.Struct {
		nameField := val.FieldByName("Name")
		if nameField.IsValid() && nameField.CanInterface() {
			if s, ok := nameField.Interface().(string); ok {
				return s
			}
		}
		idField := val.FieldByName("ID")
		if idField.IsValid() && idField.CanInterface() {
			return fmt.Sprintf("#%v", idField.Interface())
		}
	}
	return "?"
}
