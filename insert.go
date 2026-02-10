package goent

import (
	"reflect"
	"strings"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

type dict = map[string]any

type StateInsert[T any] struct {
	table *Table[T]
	*StateWhere
}

func (s *StateInsert[T]) One(obj *T) error {
	schemaName := s.table.SchemaName
	if schemaName == "" {
		schemaName = "public"
	}

	// Collect and sort all columns by FieldId
	columns := make([]*Column, 0, len(s.table.Columns))
	for _, col := range s.table.Columns {
		col.tableName = s.table.TableName
		col.schemaName = &schemaName
		columns = append(columns, col)
	}

	// Sort by FieldId
	for i := 0; i < len(columns)-1; i++ {
		for j := i + 1; j < len(columns); j++ {
			if columns[i].FieldId > columns[j].FieldId {
				columns[i], columns[j] = columns[j], columns[i]
			}
		}
	}

	s.builder.fields = make([]field, 0, len(columns))
	for _, col := range columns {
		s.builder.fields = append(s.builder.fields, col)
	}

	s.builder.buildInsert()
	s.builder.buildValues(reflect.ValueOf(obj).Elem())

	// 如果有 ReturningID，使用 handlerValuesReturning 处理返回值
	if s.builder.query.ReturningID != nil {
		dc := s.prepare(s.table.db.driver)
		return handlerValuesReturning(s.ctx, s.conn, s.builder.query, reflect.ValueOf(obj).Elem(), s.builder.pkFieldId, dc)
	}

	return s.exec(s.table.db.driver)
}

func (s *StateInsert[T]) All(data []*T) error {
	schemaName := s.table.SchemaName
	if schemaName == "" {
		schemaName = "public"
	}

	// 收集所有列
	columns := make([]*Column, 0, len(s.table.Columns))
	for _, col := range s.table.Columns {
		col.tableName = s.table.TableName
		col.schemaName = &schemaName
		columns = append(columns, col)
	}

	// Sort by FieldId
	for i := 0; i < len(columns)-1; i++ {
		for j := i + 1; j < len(columns); j++ {
			if columns[i].FieldId > columns[j].FieldId {
				columns[i], columns[j] = columns[j], columns[i]
			}
		}
	}

	s.builder.fields = make([]field, 0, len(columns))
	for _, col := range columns {
		s.builder.fields = append(s.builder.fields, col)
	}

	s.builder.buildInsert()
	s.builder.buildBatchValues(reflect.ValueOf(data))
	return s.exec(s.table.db.driver)
}

func (s *StateInsert[T]) OnTransaction(tx model.Transaction) *StateInsert[T] {
	s.StateWhere.conn = tx
	return s
}

type StateSave[T any] struct {
	table *Table[T]
	*StateWhere
}

type valueOp struct {
	value any
}

func (vo valueOp) GetValue() any {
	return vo.value
}

func (s *StateSave[T]) One(obj *T) error {
	argSave := getArgsSave(addrMap.mapField, s.table.Model, *obj, nil, nil)
	if argSave.skip {
		return nil
	}

	// Check if primary key value is zero (0 or nil), then use INSERT instead of UPDATE
	var pkValue any
	if len(argSave.valuesWhere) > 0 {
		pkValue = argSave.valuesWhere[0]
	}

	isZeroValue := pkValue == nil || pkValue == 0
	if isZeroValue {
		// Use INSERT for new records
		s.builder.query.Type = enum.InsertQuery

		// Collect and sort all columns by fieldId
		schemaName := s.table.SchemaName
		if schemaName == "" {
			schemaName = "public"
		}
		columns := make([]*Column, 0, len(s.table.Columns))
		for _, col := range s.table.Columns {
			col.tableName = s.table.TableName
			col.schemaName = &schemaName
			columns = append(columns, col)
		}

		// Sort by FieldId
		for i := 0; i < len(columns)-1; i++ {
			for j := i + 1; j < len(columns); j++ {
				if columns[i].FieldId > columns[j].FieldId {
					columns[i], columns[j] = columns[j], columns[i]
				}
			}
		}

		s.builder.fields = make([]field, 0, len(columns))
		for _, col := range columns {
			s.builder.fields = append(s.builder.fields, col)
		}

		s.builder.buildInsert()
		s.builder.buildValues(reflect.ValueOf(obj).Elem())

		// If has ReturningID, use handlerValuesReturning to handle return value
		if s.builder.query.ReturningID != nil {
			dc := s.prepare(s.table.db.driver)
			return handlerValuesReturning(s.ctx, s.conn, s.builder.query, reflect.ValueOf(obj).Elem(), s.builder.pkFieldId, dc)
		}

		dc := s.prepare(s.table.db.driver)
		return handlerValues(s.ctx, s.conn, s.builder.query, dc)
	}

	// Use UPDATE for existing records
	s.builder.query.Type = enum.UpdateQuery
	s.builder.sets = argSave.sets

	// Build WHERE conditions
	if len(argSave.valuesWhere) > 0 {
		tableOf := reflect.ValueOf(s.table.Model).Elem()
		for i, pkValue := range argSave.valuesWhere {
			fld := addrMap.mapField[uintptr(tableOf.Field(i).Addr().UnsafePointer())]
			if fld != nil {
				filter := Condition{
					Type:      enum.OperationWhere,
					Operator:  enum.Equals,
					Attribute: fld.getAttributeName(),
					Table:     model.Table{Schema: fld.schema(), Name: fld.table()},
					Value:     valueOp{value: pkValue},
				}
				s.builder.filters = append(s.builder.filters, filter)
			}
		}
		// Add AND connector
		if len(s.builder.filters) > 1 {
			for i := len(s.builder.filters) - 1; i > 0; i-- {
				andOp := Condition{
					Operator: enum.And,
					Type:     enum.LogicalWhere,
				}
				s.builder.filters = append(s.builder.filters[:i], append([]Condition{andOp}, s.builder.filters[i:]...)...)
			}
		}
	}

	s.builder.buildUpdate()
	dc := s.prepare(s.table.db.driver)
	return handlerValues(s.ctx, s.conn, s.builder.query, dc)
}

func (s *StateSave[T]) All(data []*T) error {
	// TODO: add table to builder
	return s.exec(s.table.db.driver)
}

func (s *StateSave[T]) OneMap(val dict) error {
	s.builder.query.Type = enum.UpdateQuery
	if len(val) == 0 {
		return nil
	}
	sets := make([]set, 0, len(val))
	var pkValue any
	for k, v := range val {
		col := s.table.Columns[k]
		if col == nil {
			col = s.table.Columns[strings.Title(k)]
		}
		if col != nil {
			sets = append(sets, set{attribute: col, value: v})
			if k == "id" || k == "ID" {
				pkValue = v
			}
		}
	}
	if len(sets) == 0 {
		return nil
	}
	s.builder.sets = sets

	// 构建 WHERE 条件（使用 id 字段）
	if pkValue != nil && len(s.table.PrimaryKeys) > 0 {
		for _, pk := range s.table.PrimaryKeys {
			filter := Condition{
				Type:      enum.OperationWhere,
				Operator:  enum.Equals,
				Attribute: pk.Column.getAttributeName(),
				Table:     model.Table{Schema: pk.Column.schema(), Name: pk.Column.table()},
				Value:     valueOp{value: pkValue},
			}
			s.builder.filters = append(s.builder.filters, filter)
			break
		}
	}

	s.builder.buildUpdate()
	dc := s.prepare(s.table.db.driver)
	return handlerValues(s.ctx, s.conn, s.builder.query, dc)
}

func (s *StateSave[T]) AllMap(data []dict) error {
	// TODO: add table to builder
	return s.exec(s.table.db.driver)
}

func (s *StateSave[T]) OnTransaction(tx model.Transaction) *StateSave[T] {
	s.StateWhere.conn = tx
	return s
}

func (s *StateSave[T]) Match(obj T) *StateSave[T] {
	s.StateWhere = MatchWhere[T](s.StateWhere, s.table, obj)
	return s
}
