package enum

// WhereType represents the type of WHERE clause condition.
type WhereType uint

const (
	_ WhereType = iota
	LogicalWhere
	OperationWhere
	OperationAttributeWhere
	OperationIsWhere
	OperationInWhere
)

// QueryType represents the type of SQL query (SELECT, INSERT, UPDATE, DELETE, etc.).
type QueryType uint

const (
	_ QueryType = iota
	SelectQuery
	InsertQuery
	InsertAllQuery
	UpdateQuery
	DeleteQuery
	RawQuery
)

// AggregateType represents the type of SQL aggregate function (COUNT, SUM, AVG, etc.).
type AggregateType uint

const (
	_ AggregateType = iota
	CountAggregate
	MaxAggregate
	MinAggregate
	SumAggregate
	AvgAggregate
)

// FunctionType represents the type of SQL string function (UPPER, LOWER, etc.).
type FunctionType uint

const (
	_ FunctionType = iota
	UpperFunction
	LowerFunction
)

// JoinType represents the type of SQL JOIN (INNER JOIN, LEFT JOIN, RIGHT JOIN).
type JoinType string

const (
	Join      JoinType = "JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
)

// OperatorType represents the type of comparison operator in SQL conditions.
type OperatorType uint

const (
	_             OperatorType = iota
	Equals                     // =
	NotEquals                  // <>
	Is                         // IS
	IsNot                      // IS NOT
	Greater                    // >
	GreaterEquals              // >=
	Less                       // <
	LessEquals                 // <=
	In                         // IN
	NotIn                      // NOT IN
	Like                       // LIKE
	NotLike                    // NOT LIKE
	And                        // AND
	Or                         // OR
)
