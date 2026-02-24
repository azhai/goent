package goent

import (
	"reflect"
	"sort"
	"time"

	"github.com/azhai/goent/model"
)

// StateInsert represents an INSERT query state for inserting new records into a table.
type StateInsert[T any] struct {
	table *Table[T]
	*StateWhere
}

func (s *StateInsert[T]) One(obj *T) error {
	s.builder.Type = model.InsertQuery
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	s.builder.ResetForSave()

	valueOf := reflect.ValueOf(obj).Elem()
	primary, retFid := CollectFields(s.builder, s.table, valueOf)
	for name, val := range primary {
		s.builder.Changes[s.table.Field(name)] = val
	}

	qr := model.CreateQuery(s.builder.Build(true))
	hd := s.Prepare(s.table.db.driver)
	if retFid >= 0 && s.table.db.driver.SupportsReturning() {
		return hd.ExecuteReturning(qr, valueOf, retFid)
	}
	err := hd.ExecuteNoReturn(qr)
	if err != nil {
		return err
	}
	if retFid >= 0 && s.table.PrimaryKeys[0].IsAutoIncr {
		return s.getLastInsertId(valueOf, retFid)
	}
	return nil
}

func (s *StateInsert[T]) getLastInsertId(valueOf reflect.Value, retFid int) error {
	qr := model.CreateQuery("SELECT last_insert_rowid()", nil)
	hd := s.Prepare(s.table.db.driver)
	startTime := time.Now()
	row := hd.conn.QueryRowContext(hd.ctx, &qr)
	qr.QueryDuration = time.Since(startTime)
	fieldOf := valueOf.Field(retFid)
	qr.Err = row.Scan(fieldOf.Addr().Interface())
	if qr.Err != nil {
		return hd.ErrHandler(qr)
	}
	hd.InfoHandler(qr)
	return nil
}

func (s *StateInsert[T]) All(retPK bool, data []*T) error {
	if len(data) == 0 {
		return nil
	} else if len(data) == 1 {
		return s.One(data[0])
	}
	s.builder.Type = model.InsertAllQuery
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	s.builder.ResetForSave()

	pkFid, pkName := -1, ""
	isAutoIncr := false
	if retPK {
		pkFid, pkName, _ = s.table.TableInfo.GetPrimaryInfo()
		if pkFid >= 0 && len(s.table.PrimaryKeys) > 0 && s.table.PrimaryKeys[0].IsAutoIncr {
			isAutoIncr = true
			s.builder.Returning = pkName
		}
	}

	var columns []*Column
	for _, col := range s.table.Columns {
		if col.ColumnName == pkName && isAutoIncr {
			continue
		}
		columns = append(columns, col)
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].FieldId < columns[j].FieldId
	})

	for _, col := range columns {
		fld := s.table.Field(col.ColumnName)
		s.builder.Changes[fld] = col.FieldId
	}

	size := len(columns)
	for _, row := range data {
		newbie := make([]any, size)
		valueOf := reflect.ValueOf(row).Elem()
		for i, col := range columns {
			if val := valueOf.Field(col.FieldId); val.IsValid() {
				newbie[i] = val.Interface()
			}
		}
		s.builder.MoreRows = append(s.builder.MoreRows, newbie)
	}

	qr := model.CreateQuery(s.builder.Build(true))
	hd := s.Prepare(s.table.db.driver)
	if pkFid >= 0 && pkName != "" && isAutoIncr && s.table.db.driver.SupportsReturning() {
		valueOf := reflect.ValueOf(data[0]).Elem()
		return hd.BatchReturning(qr, valueOf, pkFid)
	}
	err := hd.ExecuteNoReturn(qr)
	if err != nil {
		return err
	}
	if pkFid >= 0 && pkName != "" && isAutoIncr {
		return s.getLastInsertIds(data, pkFid)
	}
	return nil
}

func (s *StateInsert[T]) getLastInsertIds(data []*T, pkFid int) error {
	lastId, err := s.queryLastInsertId()
	if err != nil {
		return err
	}
	startId := lastId - int64(len(data)) + 1
	for i, row := range data {
		valueOf := reflect.ValueOf(row).Elem()
		fieldOf := valueOf.Field(pkFid)
		switch fieldOf.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldOf.SetInt(startId + int64(i))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fieldOf.SetUint(uint64(startId) + uint64(i))
		}
	}
	return nil
}

func (s *StateInsert[T]) queryLastInsertId() (int64, error) {
	qr := model.CreateQuery("SELECT last_insert_rowid()", nil)
	hd := s.Prepare(s.table.db.driver)
	startTime := time.Now()
	row := hd.conn.QueryRowContext(hd.ctx, &qr)
	qr.QueryDuration = time.Since(startTime)
	var id int64
	qr.Err = row.Scan(&id)
	if qr.Err != nil {
		return 0, hd.ErrHandler(qr)
	}
	hd.InfoHandler(qr)
	return id, nil
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
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
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
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
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
