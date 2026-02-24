package goent

import (
	"github.com/azhai/goent/model"
)

// Transaction wraps a database transaction and provides additional functionality.
type Transaction struct {
	model.Transaction
}

// BeginTransaction starts a nested transaction using a savepoint.
// Any panic or error will trigger a rollback of the savepoint.
//
// Example:
//
//	err := tx.BeginTransaction(func(tx goent.Transaction) error {
//		user := &User{Name: "John"}
//		return goent.Insert(db.User).OnTransaction(tx).One(user)
//	})
func (t Transaction) BeginTransaction(txFunc func(Transaction) error) (err error) {
	var sv model.SavePoint
	if sv, err = t.SavePoint(); err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			sv.Rollback()
		}
	}()
	if err = txFunc(t); err != nil {
		sv.Rollback()
		return
	}
	return sv.Commit()
}
