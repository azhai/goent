package goent

import (
	"reflect"

	"github.com/azhai/goent/model"
)

// StateInsert represents an INSERT query state for inserting new records into a table
// It provides methods for inserting single and multiple records
type StateInsert[T any] struct {
	table       *Table[T] // The table to insert records into
	*StateWhere           // Embedded StateWhere for query context
}

// One inserts a single record into the table
// It handles auto-increment primary keys and returning values
func (s *StateInsert[T]) One(obj *T) error {
	s.builder.Type = model.InsertQuery
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	s.builder.ResetForSave()

	valueOf := reflect.ValueOf(obj).Elem()
	primary, retFid := CollectFields(s.builder, s.table, valueOf, nil)
	for name, val := range primary {
		s.builder.Changes[s.table.Field(name)] = val
	}

	returning := s.builder.Returning
	qr := model.CreateQuery(s.builder.Build(true))
	conn, cfg := s.Prepare(s.table.db.driver)
	if retFid >= 0 && returning != "" && s.table.db.driver.SupportsReturning() {
		hd := NewHandler(s.ctx, conn, cfg)
		return hd.ExecuteReturning(qr, valueOf, retFid)
	}
	err := qr.WrapExec(s.ctx, conn, cfg)
	if err != nil {
		return err
	}
	if retFid >= 0 && returning != "" && s.table.PrimaryKeys[0].IsAutoIncr && !s.table.db.driver.SupportsReturning() {
		return s.getLastInsertId(valueOf, retFid)
	}
	return nil
}

func (s *StateInsert[T]) getLastInsertId(valueOf reflect.Value, retFid int) error {
	qr := model.CreateQuery("SELECT last_insert_rowid()", nil)
	conn, cfg := s.Prepare(s.table.db.driver)
	row, err := qr.WrapQueryRow(s.ctx, conn, cfg)
	if err != nil {
		return err
	}
	fieldOf := valueOf.Field(retFid)
	qr.Err = row.Scan(fieldOf.Addr().Interface())
	if qr.Err != nil {
		return cfg.ErrorQueryHandler(s.ctx, qr)
	}
	return nil
}

func (s *StateInsert[T]) queryLastInsertId() (int64, error) {
	qr := model.CreateQuery("SELECT last_insert_rowid()", nil)
	conn, cfg := s.Prepare(s.table.db.driver)
	row, err := qr.WrapQueryRow(s.ctx, conn, cfg)
	if err != nil {
		return 0, err
	}

	var id int64
	qr.Err = row.Scan(&id)
	if qr.Err != nil {
		return 0, cfg.ErrorQueryHandler(s.ctx, qr)
	}
	return id, nil
}

// All inserts multiple records into the table
// It handles batch insertion and optionally returns auto-increment primary keys
func (s *StateInsert[T]) All(retPK bool, data []*T) error {
	if len(data) == 0 {
		return nil
	} else if len(data) == 1 {
		return s.One(data[0])
	}
	s.builder.Type = model.InsertAllQuery
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	s.builder.ResetForSave()

	isAutoIncr := false
	pkFid, pkName, _ := s.table.TableInfo.GetPrimaryInfo()
	if pkFid >= 0 && len(s.table.PrimaryKeys) > 0 && s.table.PrimaryKeys[0].IsAutoIncr {
		isAutoIncr = true
	}
	if retPK && isAutoIncr {
		s.builder.Returning = pkName
	} else {
		pkFid = -1
	}

	s.builder.VisitFields = make([]*Field, 0, len(s.table.ColumnNames))
	for _, name := range s.table.ColumnNames {
		col := s.table.Columns[name]
		if col.ColumnName == pkName && isAutoIncr {
			continue
		}
		fld := s.table.Field(col.ColumnName)
		s.builder.VisitFields = append(s.builder.VisitFields, fld)
	}

	size := len(s.builder.VisitFields)
	for _, row := range data {
		newbie := make([]any, size)
		valueOf := reflect.ValueOf(row).Elem()
		for i, fld := range s.builder.VisitFields {
			if val := valueOf.Field(fld.FieldId); val.IsValid() {
				newbie[i] = val.Interface()
			}
		}
		s.builder.InsertValues = append(s.builder.InsertValues, newbie)
	}

	returning := s.builder.Returning
	qr := model.CreateQuery(s.builder.Build(true))
	conn, cfg := s.Prepare(s.table.db.driver)
	if pkFid >= 0 && returning != "" && isAutoIncr && s.table.db.driver.SupportsReturning() {
		valueOf := reflect.ValueOf(data)
		hd := NewHandler(s.ctx, conn, cfg)
		return hd.BatchReturning(qr, valueOf, pkFid)
	}
	err := qr.WrapExec(s.ctx, conn, cfg)
	if err != nil {
		return err
	}
	if pkFid >= 0 && returning != "" && isAutoIncr && !s.table.db.driver.SupportsReturning() {
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

// OnTransaction sets the transaction for the insert operation
// It ensures the insert runs within the specified transaction
func (s *StateInsert[T]) OnTransaction(tx model.Transaction) *StateInsert[T] {
	s.StateWhere.conn = tx
	return s
}

// StateSave represents a save state that intelligently inserts or updates records based on primary key presence
// It automatically decides whether to insert a new record or update an existing one

type StateSave[T any] struct {
	table       *Table[T] // The table to save records to
	*StateWhere           // Embedded StateWhere for query context
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

// One saves a record to the table, inserting if no primary key exists or updating if it does
// It automatically handles insert/update logic based on primary key presence
func (s *StateSave[T]) One(obj *T) error {
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	s.builder.ResetForSave()

	// Fast path: use UpdatePairs for single PK update (no reflection needed)
	if len(s.table.PrimaryKeys) == 1 && len(s.table.Ignores) == 0 {
		if updater, ok := any(obj).(GenUpdatePairs); ok {
			if pkID := updater.GetID(); pkID > 0 {
				return s.table.Update().Set(updater.UpdatePairs()...).ByPK(pkID)
			}
		}
	}

	valueOf := reflect.ValueOf(obj).Elem()
	primary, retFid := CollectFields(s.builder, s.table, valueOf, s.table.Ignores)
	qr := s.Take(1).getQuery(primary)
	conn, cfg := s.Prepare(s.table.db.driver)
	if s.builder.Returning != "" {
		hd := NewHandler(s.ctx, conn, cfg)
		return hd.ExecuteReturning(qr, valueOf, retFid)
	}
	return qr.WrapExec(s.ctx, conn, cfg)
}

// Map saves records from a map, inserting or updating based on primary key presence
// It extracts primary keys from the map to determine insert/update logic
func (s *StateSave[T]) Map(value Dict) error {
	s.builder.SetTable(s.table.TableInfo, s.table.db.driver)
	s.builder.ResetForSave()

	primary := make(Dict, len(s.table.PrimaryKeys))
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
	conn, cfg := s.Prepare(s.table.db.driver)
	return qr.WrapExec(s.ctx, conn, cfg)
}

// OnTransaction sets the transaction for the save operation
// It ensures the save runs within the specified transaction
func (s *StateSave[T]) OnTransaction(tx model.Transaction) *StateSave[T] {
	s.StateWhere.conn = tx
	return s
}

// Match sets the WHERE conditions based on the non-zero fields of the given object
// It automatically generates conditions for fields with non-zero values
func (s *StateSave[T]) Match(obj T) *StateSave[T] {
	s.StateWhere = MatchWhere(s.StateWhere, s.table, obj)
	return s
}

// Take limits the number of rows affected by the save operation
// Note: PostgreSQL does not support LIMIT in UPDATE statements
func (s *StateSave[T]) Take(i int) *StateSave[T] {
	if s.table.db.DriverName() == "PostgreSQL" {
		return s // PostgreSQL does not support LIMIT in UPDATE
	}
	if i >= TakeNoLimit {
		s.builder.Limit = i
	}
	return s
}
