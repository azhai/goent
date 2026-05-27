package goent

import (
	"github.com/azhai/goent/model"
)

// JoinTable represents a JOIN clause with the join type, target table, and ON condition.
type JoinTable struct {
	JoinType model.JoinType
	Table    *model.Table
	fullName string
	On       Condition
}

// Pair represents a key-value pair.
type Pair struct {
	Key   string
	Value any
}

// Order represents an ORDER BY clause with a field and descending flag.
type Order struct {
	Desc bool
	*Field
}

// Group represents a GROUP BY clause with a field and optional HAVING condition.
type Group struct {
	Having Condition
	*Field
}
