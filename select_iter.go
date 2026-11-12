//go:build go1.23

package goent

import (
	"iter"

	"github.com/azhai/goent/model"
)

// IterRows returns an iterator over the query results
// It yields each row along with any error encountered
func (s *StateSelect[T, R]) IterRows(to FetchFunc) iter.Seq2[*R, error] {
	if to == nil {
		to = s.getFetchFunc()
	}
	qr := model.CreateQuery(s.builder.Build(false))
	builder := s.builder
	conn, cfg := s.Prepare(s.table.TableInfo)
	hd := NewHandler(s.ctx, conn, cfg)
	seq := FetchResult[R](hd, qr, to)
	return func(yield func(*R, error) bool) {
		// Ranging the same iterator twice would otherwise put the builder into
		// the pool twice and hand the same *Builder to two goroutines.
		b := builder
		builder = nil
		if b != nil {
			defer PutBuilder(b)
		}
		for obj, err := range seq {
			if !yield(obj, err) {
				return
			}
		}
	}
}
