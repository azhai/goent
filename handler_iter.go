//go:build go1.23

package goent

import (
	"iter"

	"github.com/azhai/goent/model"
)

// FetchResult returns an iterator that yields typed results from the query result set.
// This is memory-efficient for processing large result sets.
//
// Example:
//
//	for user, err := range FetchResult[User](hd, query, FetchValue) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(user.Name)
//	}
func FetchResult[R any](hd *Handler, query model.Query, to FetchFunc) iter.Seq2[*R, error] {
	rows, err := query.WrapQuery(hd.ctx, hd.conn, hd.cfg)
	if err != nil || rows == nil {
		return func(yield func(*R, error) bool) {
			yield(nil, err)
		}
	}

	return func(yield func(*R, error) bool) {
		defer rows.Close()
		for rows.Next() {
			target := new(R)
			query.Err = rows.Scan(to(target)...)
			if query.Err != nil {
				yield(target, hd.cfg.ErrorQueryHandler(hd.ctx, query))
				return
			}
			if !yield(target, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(nil, err)
		}
	}
}
