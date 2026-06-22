package goent

import (
	"fmt"
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
	defer PutBuilder(s.builder)
	s.builder.Type = model.InsertQuery
	s.builder.SetTable(s.table.TableInfo)
	s.builder.ResetForSave()

	valueOf := reflect.ValueOf(obj).Elem()
	primary, retFid := CollectFields(s.builder, s.table, valueOf, nil)
	for name, val := range primary {
		s.builder.Changes[s.table.Field(name)] = val
	}

	returning := s.builder.Returning
	sql, args := s.builder.Build(true)
	if sql == "" {
		return fmt.Errorf("goent: StateInsert.One built empty SQL (Type=%d, Changes=%d, args=%v)",
			s.builder.Type, len(s.builder.Changes), args)
	}
	qr := model.CreateQuery(sql, args)
	conn, cfg := s.Prepare(s.table.TableInfo)
	info := s.table.TableInfo
	changes := changesToMap(s.builder.Changes)
	if retFid >= 0 && returning != "" && s.table.db.driver.SupportsReturning() {
		hd := NewHandler(s.ctx, conn, cfg)
		if err := hd.ExecuteReturning(qr, valueOf, retFid); err != nil {
			return err
		}
		publishEvent(info.db.bus, info, conn, EventTopicInsertOne, "", extractID(valueOf, retFid), changes, 1)
		return nil
	}
	err := qr.WrapExec(s.ctx, conn, cfg)
	if err != nil {
		return err
	}
	if retFid >= 0 && returning != "" && s.table.PrimaryKeys[0].IsAutoIncr && !s.table.db.driver.SupportsReturning() {
		if err := s.getLastInsertId(valueOf, retFid); err != nil {
			return err
		}
	}
	publishEvent(info.db.bus, info, conn, EventTopicInsertOne, "", extractID(valueOf, retFid), changes, 1)
	return nil
}

// extractID reads the int64 primary key value at field index fid from a reflect.Value.
// Returns nil if fid is invalid or the field is not an integer kind.
func extractID(valueOf reflect.Value, fid int) []int64 {
	if fid < 0 {
		return nil
	}
	f := valueOf.Field(fid)
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []int64{f.Int()}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []int64{int64(f.Uint())}
	}
	return nil
}

func (s *StateInsert[T]) getLastInsertId(valueOf reflect.Value, retFid int) error {
	qr := model.CreateQuery("SELECT last_insert_rowid()", nil)
	conn, cfg := s.Prepare(s.table.TableInfo)
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
	conn, cfg := s.Prepare(s.table.TableInfo)
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
	defer PutBuilder(s.builder)
	if len(data) == 0 {
		return nil
	} else if len(data) == 1 {
		return s.One(data[0])
	}
	s.builder.Type = model.InsertAllQuery
	s.builder.SetTable(s.table.TableInfo)
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
	useGenInsertValues := len(data) > 0 && data[0] != nil
	if useGenInsertValues {
		_, useGenInsertValues = any(data[0]).(GenInsertValues)
	}
	for _, row := range data {
		var newbie []any
		if useGenInsertValues {
			newbie = any(row).(GenInsertValues).InsertValues()
		} else {
			newbie = make([]any, size)
			valueOf := reflect.ValueOf(row).Elem()
			for i, fld := range s.builder.VisitFields {
				if val := valueOf.Field(fld.FieldId); val.IsValid() {
					newbie[i] = val.Interface()
				}
			}
		}
		s.builder.InsertValues = append(s.builder.InsertValues, newbie)
	}

	returning := s.builder.Returning
	qr := model.CreateQuery(s.builder.Build(true))
	conn, cfg := s.Prepare(s.table.TableInfo)
	info := s.table.TableInfo
	n := int64(len(data))
	if pkFid >= 0 && returning != "" && isAutoIncr && s.table.db.driver.SupportsReturning() {
		valueOf := reflect.ValueOf(data)
		hd := NewHandler(s.ctx, conn, cfg)
		if err := hd.BatchReturning(qr, valueOf, pkFid); err != nil {
			return err
		}
		publishEvent(info.db.bus, info, conn, EventTopicInsertBulk, "", extractIDs(data, pkFid), nil, n)
		return nil
	}
	err := qr.WrapExec(s.ctx, conn, cfg)
	if err != nil {
		return err
	}
	if pkFid >= 0 && returning != "" && isAutoIncr && !s.table.db.driver.SupportsReturning() {
		if err := s.getLastInsertIds(data, pkFid); err != nil {
			return err
		}
	}
	publishEvent(info.db.bus, info, conn, EventTopicInsertBulk, "", extractIDs(data, pkFid), nil, n)
	return nil
}

// extractIDs reads the int64 primary key values at field index fid from a slice of records.
// Returns nil if fid is invalid or no records are provided.
func extractIDs[T any](data []*T, fid int) []int64 {
	if fid < 0 || len(data) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(data))
	for _, row := range data {
		if row == nil {
			continue
		}
		f := reflect.ValueOf(row).Elem().Field(fid)
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			ids = append(ids, f.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			ids = append(ids, int64(f.Uint()))
		}
	}
	return ids
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
		s.builder.core.Where = EqualsMap(fld, primary)
	} else {
		s.builder.Type = model.InsertQuery
	}
	sql, args := s.builder.Build(true)
	if sql == "" {
		return model.CreateQuery("", nil)
	}
	return model.CreateQuery(sql, args)
}

// One saves a record to the table, inserting if no primary key exists or updating if it does
// It automatically handles insert/update logic based on primary key presence
func (s *StateSave[T]) One(obj *T) error {
	defer PutBuilder(s.builder)
	s.builder.SetTable(s.table.TableInfo)
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
	conn, cfg := s.Prepare(s.table.TableInfo)
	info := s.table.TableInfo
	isUpdate := len(primary) > 0
	if s.builder.Returning != "" {
		hd := NewHandler(s.ctx, conn, cfg)
		if err := hd.ExecuteReturning(qr, valueOf, retFid); err != nil {
			return err
		}
		if isUpdate {
			publishEvent(info.db.bus, info, conn, EventTopicUpdate, "", nil, changesToMap(s.builder.Changes), 1)
		} else {
			publishEvent(info.db.bus, info, conn, EventTopicInsertOne, "", extractID(valueOf, retFid), changesToMap(s.builder.Changes), 1)
		}
		return nil
	}
	if err := qr.WrapExec(s.ctx, conn, cfg); err != nil {
		return err
	}
	if isUpdate {
		publishEvent(info.db.bus, info, conn, EventTopicUpdate, "", nil, changesToMap(s.builder.Changes), qr.RowsAffected)
	} else {
		publishEvent(info.db.bus, info, conn, EventTopicInsertOne, "", nil, changesToMap(s.builder.Changes), 1)
	}
	return nil
}

// Map saves records from a map, inserting or updating based on primary key presence
// It extracts primary keys from the map to determine insert/update logic
func (s *StateSave[T]) Map(value Dict) error {
	defer PutBuilder(s.builder)
	s.builder.SetTable(s.table.TableInfo)
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
	conn, cfg := s.Prepare(s.table.TableInfo)
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
		s.builder.core.Limit = i
	}
	return s
}
