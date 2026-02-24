package model

// QueryType represents the type of a SQL query.
type QueryType uint

const (
	_               QueryType = iota
	SelectQuery               // SELECT query
	SelectJoinQuery           // SELECT query with JOIN clause
	InsertQuery               // INSERT query
	InsertAllQuery            // INSERT ALL query
	UpdateQuery               // UPDATE query
	UpdateJoinQuery           // UPDATE query with JOIN clause
	DeleteQuery               // DELETE query
	RawQuery                  // Raw SQL query
)

// JoinType represents the type of a JOIN clause in a SQL query.
type JoinType string

const (
	Join      JoinType = "JOIN"       // JOIN or OUTER JOIN clause
	LeftJoin  JoinType = "LEFT JOIN"  // LEFT JOIN clause
	RightJoin JoinType = "RIGHT JOIN" // RIGHT JOIN clause
	InnerJoin JoinType = "INNER JOIN" // INNER JOIN clause
)
