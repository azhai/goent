package goent

import (
	"github.com/azhai/goent/model"
)

type Transaction struct {
	model.Transaction
}

// This will make a pseudo nested transaction using StateSave point.
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
