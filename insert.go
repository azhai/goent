package goent

import (
	"reflect"

	"github.com/azhai/goent/model"
)

// StateInsert represents an INSERT query state for inserting new records into a table.
type StateInsert[T any] struct {
	table *Table[T]
	*StateWhere
}

func (s *StateInsert[T]) One(obj *T) error {
	s.builder.Type = model.InsertQuery
	s.builder.SetTable(s.table.TableInfo)
	s.builder.ResetForSave()

	valueOf := reflect.ValueOf(obj).Elem()
	primary, retFid := CollectFields(s.builder, s.table, valueOf)
	for name, val := range primary {
		s.builder.Changes[s.table.Field(name)] = val
	}

	qr := model.CreateQuery(s.builder.Build(true))
	hd := s.Prepare(s.table.db.driver)
	if retFid >= 0 {
		return hd.ExecuteReturning(qr, valueOf, retFid)
	}
	return hd.ExecuteNoReturn(qr)
}

func (s *StateInsert[T]) All(autoIncr bool, data []*T) error {
	if len(data) == 0 {
		return nil
	} else if len(data) == 1 {
		return s.One(data[0])
	}
	s.builder.Type = model.InsertAllQuery
	s.builder.SetTable(s.table.TableInfo)
	s.builder.ResetForSave()

	pkFid, pkName := -1, ""
	if autoIncr {
		pkFid, pkName, _ = s.table.TableInfo.GetPrimaryInfo()
		s.builder.Returning = pkName
	}
	for _, col := range s.table.Columns {
		if autoIncr && col.ColumnName == pkName {
			continue
		}
		fld := s.table.Field(col.ColumnName)
		seq := col.FieldId
		if autoIncr && pkFid >= 0 && seq > pkFid { // jump over pk
			seq -= 1
		}
		s.builder.Changes[fld] = seq
	}

	size := len(s.builder.Changes)
	for _, row := range data {
		newbie := make([]any, size)
		valueOf := reflect.ValueOf(row).Elem()
		for f, idx := range s.builder.Changes {
			i := idx.(int)
			if val := valueOf.Field(f.GetFid()); val.IsValid() {
				newbie[i] = val.Interface()
			}
		}
		s.builder.MoreRows = append(s.builder.MoreRows, newbie)
	}

	qr := model.CreateQuery(s.builder.Build(true))
	hd := s.Prepare(s.table.db.driver)
	if pkFid >= 0 {
		valueOf := reflect.ValueOf(data[0]).Elem()
		return hd.BatchReturning(qr, valueOf, pkFid)
	}
	return hd.ExecuteNoReturn(qr)
}

func (s *StateInsert[T]) OnTransaction(tx model.Transaction) *StateInsert[T] {
	s.StateWhere.conn = tx
	return s
}

// StateSave represents a save state that intelligently inserts or updates records based on primary key presence.
type StateSave[T any] struct {
	table *Table[T]
	*StateWhere
}

func (s *StateSave[T]) getQuery(primary Dict) model.Query {
	if len(primary) > 0 {
		s.builder.Type = model.UpdateQuery
		fld := &Field{TableAddr: s.table.TableAddr}
		s.builder.Where = EqualsMap(fld, primary)
	} else {
		s.builder.Type = model.InsertQuery
	}
	return model.CreateQuery(s.builder.Build(true))
}

func (s *StateSave[T]) One(obj *T) error {
	s.builder.SetTable(s.table.TableInfo)
	s.builder.ResetForSave()

	valueOf := reflect.ValueOf(obj).Elem()
	primary, retFid := CollectFields(s.builder, s.table, valueOf)
	qr := s.Take(1).getQuery(primary)
	hd := s.Prepare(s.table.db.driver)
	if s.builder.Returning != "" {
		return hd.ExecuteReturning(qr, valueOf, retFid)
	}
	return hd.ExecuteNoReturn(qr)
}

func (s *StateSave[T]) Map(value Dict) error {
	s.builder.SetTable(s.table.TableInfo)
	s.builder.ResetForSave()

	primary := make(Dict)
	for _, pkey := range s.table.PrimaryKeys {
		name := pkey.ColumnName
		if val, ok := value[name]; ok {
			primary[name] = val
			delete(value, name)
		}
	}
	for name, val := range value {
		fld := s.table.Field(name)
		s.builder.Changes[fld] = val
	}

	qr := s.getQuery(primary)
	hd := s.Prepare(s.table.db.driver)
	return hd.ExecuteNoReturn(qr)
}

func (s *StateSave[T]) OnTransaction(tx model.Transaction) *StateSave[T] {
	s.StateWhere.conn = tx
	return s
}

func (s *StateSave[T]) Match(obj T) *StateSave[T] {
	s.StateWhere = MatchWhere(s.StateWhere, s.table, obj)
	return s
}

// Take takes i elements
func (s *StateSave[T]) Take(i int) *StateSave[T] {
	if i >= 0 {
		s.builder.Limit = i
	}
	return s
}
