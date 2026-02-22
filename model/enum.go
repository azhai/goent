package model

type QueryType uint

const (
	_ QueryType = iota
	SelectQuery
	SelectJoinQuery
	InsertQuery
	InsertAllQuery
	UpdateQuery
	UpdateJoinQuery
	DeleteQuery
	RawQuery
)

type JoinType string

const (
	Join      JoinType = "JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
	InnerJoin JoinType = "INNER JOIN"
)
