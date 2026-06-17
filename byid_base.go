package goent

import (
	"context"

	"github.com/azhai/goent/model"
)

// byIDBase holds the shared state and behavior for two-phase (query-then-modify)
// operations. It is embedded by StateUpdateByID and StateDeleteByID.
//
// Shared fields:
//   - table:     target table
//   - where:     conditions copied from TableQuery
//   - ctx:       context for query execution
//   - conn:      transaction connection (nil = auto)
//   - batchSize: IN clause batch size for Phase 2 (0 = default 500)
//   - limit:     limit on ID query in Phase 1 (0 = no limit)
type byIDBase[T any] struct {
	table     *Table[T]
	where     Condition
	ctx       context.Context
	conn      model.Connection
	batchSize int
	limit     int
}

// BatchSize sets the IN clause batch size for Phase 2.
// When the ID count exceeds batchSize, the IN clause is split into
// multiple OR-connected batches to stay within parameter limits.
// A value of 0 or less uses the default (500).
func (s *byIDBase[T]) BatchSize(size int) *byIDBase[T] {
	s.batchSize = size
	return s
}

// Take limits the number of IDs queried in Phase 1.
// Unlike StateUpdate.Take / StateDelete.Take, this works on PostgreSQL
// because the LIMIT is applied to the SELECT query, not the UPDATE/DELETE.
func (s *byIDBase[T]) Take(i int) *byIDBase[T] {
	if i >= TakeNoLimit {
		s.limit = i
	}
	return s
}

// OnTransaction sets the transaction for both Phase 1 (SELECT) and Phase 2 (UPDATE/DELETE).
func (s *byIDBase[T]) OnTransaction(tx model.Transaction) *byIDBase[T] {
	s.conn = tx
	return s
}

// queryIDs runs Phase 1: SELECT pk FROM table WHERE <conditions> [LIMIT n].
// Returns ErrNoPrimaryKey for tables without a single integer primary key.
func (s *byIDBase[T]) queryIDs() ([]int64, error) {
	return queryIDsByPK(s.table, s.where, s.ctx, s.conn, s.limit)
}

// buildInBatchCond runs Phase 1 and builds the IN-batched condition for Phase 2.
// Returns (ids, cond, error). If ids is empty, cond is zero-value and should be skipped.
func (s *byIDBase[T]) buildInBatchCond() ([]int64, Condition, error) {
	ids, err := s.queryIDs()
	if err != nil {
		return nil, Condition{}, err
	}
	if len(ids) == 0 {
		return ids, Condition{}, nil
	}
	pkField := s.table.GetPKField()
	cond := InBatch(pkField, ids, s.batchSize)
	return ids, cond, nil
}
