package goent

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
)

type StateSave[T any] struct {
	table  *Table[T]
	update StateUpdate[T]
}

// Save is a wrapper over [Update] for more simple updates,
// uses the value for create a where matching the primary keys
// and includes for update all non-zero values excluding the primary keys.
//
// Save can return [ErrUniqueValue, ErrForeignKey and ErrBadRequest];
// use ErrBadRequest as a generic error for any user interaction.
//
// Save uses [context.Background] internally;
// to specify the context, use [SaveContext].
//
// # Examples
//
//	// updates animal name on record id 1
//	err = goent.Save(db.Animal).One(Animal{ID: 1, Name: "Cat"})
func Save[T any](table *T) StateSave[T] {
	return SaveContext(context.Background(), table)
}

// SaveContext is a wrapper over [Update] for more simple updates,
// uses the value for create a where matching the primary keys
// and includes for update all non-zero values excluding the primary keys.
//
// See [Save] for examples.
func SaveContext[T any](ctx context.Context, table *T) StateSave[T] {
	return StateSave[T]{update: UpdateContext(ctx, table), table: NewTableModel(table)}
}

// SaveTable is a wrapper over [Update] for more simple updates
func SaveTable[T any](table *Table[T]) StateSave[T] {
	return SaveTableContext(context.Background(), table)
}

// SaveTableContext is a wrapper over [Update] for more simple updates
func SaveTableContext[T any](ctx context.Context, table *Table[T]) StateSave[T] {
	return StateSave[T]{update: UpdateContext(ctx, table.Model), table: table}
}

// OnTransaction sets a transaction on the query.
//
// # Example
//
//	tx, err = db.NewTransaction()
//	if err != nil {
//		// handler error
//	}
//	defer tx.Rollback()
//
//	err = goent.Save(db.Animal).OnTransaction(tx).One(Animal{ID: 2})
//	if err != nil {
//		// handler error
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		// handler error
//	}
func (s StateSave[T]) OnTransaction(tx model.Transaction) StateSave[T] {
	s.update.conn = tx
	return s
}

func (s StateSave[T]) One(v T) error {
	argsSave := getArgsSave(addrMap.mapField, s.table.Model, v, nil, s.table.Ignore)
	// skip queries on empty models
	if argsSave.skip {
		return nil
	}

	s.update.builder.sets = argsSave.sets
	return s.update.Where(operations(argsSave.argsWhere, argsSave.valuesWhere))
}

// Sets receives a map of attribute names and values for update
func (s StateSave[T]) Sets(changes map[string]any) StateSave[T] {
	for key, val := range changes {
		if fld := s.table.FieldInfo(key); fld != nil {
			newbie := set{attribute: fld, value: val}
			s.update.builder.sets = append(s.update.builder.sets, newbie)
		}
	}
	return s
}

// ByPK receives primary key values as arguments for update
func (s StateSave[T]) ByPK(args ...any) error {
	if len(s.update.builder.sets) == 0 {
		return fmt.Errorf("no sets for update")
	}
	var pkeys []any
	for _, k := range s.table.pkeys {
		if v, ok := s.table.FieldAddr(k); ok {
			pkeys = append(pkeys, v.Interface())
		}
	}
	return s.update.Where(operations(pkeys, args))
}

type StateUpdate[T any] struct {
	conn    model.Connection
	builder builder
	ctx     context.Context
}

// Update updates records in the given table.
//
// Update can return [ErrUniqueValue, ErrForeignKey and ErrBadRequest];
// use ErrBadRequest as a generic error for any user interaction.
//
// Update uses [context.Background] internally;
// to specify the context, use [UpdateContext].
//
// # Examples
//
//	// update only the attribute JobTitleID from PersonJobTitle with the value 3
//	err = goent.Update(db.PersonJobTitle).
//	Sets(update.Sets(&db.PersonJobTitle.JobTitleID, 3)).
//	Where(
//		where.And(
//			where.Equals(&db.PersonJobTitle.PersonID, 2),
//			where.Equals(&db.PersonJobTitle.JobTitleID, 1),
//	    ),
//	)
//
//	// update all animals name to Cat
//	goent.Update(db.Animal).Sets(update.Sets(&db.Animal.Name, "Cat")).All()
func Update[T any](table *T) StateUpdate[T] {
	return UpdateContext(context.Background(), table)
}

// UpdateContext updates records in the given table.
//
// See [Update] for examples
func UpdateContext[T any](ctx context.Context, _ *T) StateUpdate[T] {
	return createUpdateState[T](ctx)
}

// Set one or more arguments for update
func (s StateUpdate[T]) Set(sets ...model.Set) StateUpdate[T] {
	for i := range sets {
		attr := getArg(sets[i].Attribute, addrMap.mapField, nil)
		newbie := set{attribute: attr, value: sets[i].Value}
		s.builder.sets = append(s.builder.sets, newbie)
	}

	return s
}

// OnTransaction sets a transaction on the query.
//
// # Example
//
//	tx, err = db.NewTransaction()
//	if err != nil {
//		// handler error
//	}
//	defer tx.Rollback()
//
//	err = goent.Update(db.Animal).OnTransaction(tx).
//	  		Sets(update.Sets(&db.Animal.HabitatID, 44)).
//	  		Where(where.Equals(&db.Animal.ID, 2))
//	if err != nil {
//		// handler error
//	}
//
//	err = tx.Commit()
//	if err != nil {
//		// handler error
//	}
func (s StateUpdate[T]) OnTransaction(tx model.Transaction) StateUpdate[T] {
	s.conn = tx
	return s
}

// All update all records
func (s StateUpdate[T]) All() error {
	return s.Where(model.Operation{})
}

// Where receives [model.Operation] as where operations from where sub package
func (s StateUpdate[T]) Where(o model.Operation) error {
	helperWhere(&s.builder, addrMap.mapField, o)

	s.builder.buildUpdate()

	driver := s.builder.sets[0].attribute.getDb().driver
	if s.conn == nil {
		s.conn = driver.NewConnection()
	}

	dc := driver.GetDatabaseConfig()
	return handlerValues(s.ctx, s.conn, s.builder.query, dc)
}

type argSave struct {
	sets        []set
	argsWhere   []any
	valuesWhere []any
	skip        bool
}

func getArgsSave[T any](addrMap map[uintptr]field, table *T, value T, only, ignore []string) argSave {
	if table == nil {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	tableOf := reflect.ValueOf(table).Elem()

	if tableOf.Kind() != reflect.Struct {
		panic("goent: invalid argument. try sending a pointer to a database mapped struct as argument")
	}

	valueOf := reflect.ValueOf(value)

	sets := make([]set, 0)
	pksWhere, valuesWhere := make([]any, 0, valueOf.NumField()), make([]any, 0, valueOf.NumField())

	fld, ok := field(nil), false
	for i := 0; i < valueOf.NumField(); i++ {
		if valueOf.Field(i).IsZero() {
			continue
		}
		fieldName := tableOf.Type().Field(i).Name
		if len(only) > 0 { // only update fields, opposite ignore
			if !slices.Contains(only, fieldName) {
				continue
			}
		} else if slices.Contains(ignore, fieldName) {
			continue
		}

		addr := uintptr(tableOf.Field(i).Addr().UnsafePointer())
		if fld, ok = addrMap[addr]; !ok || fld == nil {
			continue
		}
		if fld.isPrimaryKey() {
			pksWhere = append(pksWhere, tableOf.Field(i).Addr().Interface())
			valuesWhere = append(valuesWhere, valueOf.Field(i).Interface())
		} else {
			val := valueOf.Field(i).Interface()
			sets = append(sets, set{attribute: fld, value: val})
		}
	}
	if len(pksWhere) == 0 || len(valuesWhere) == 0 {
		return argSave{skip: true}
	}
	return argSave{sets: sets, argsWhere: pksWhere, valuesWhere: valuesWhere}
}

func createUpdateState[T any](ctx context.Context) StateUpdate[T] {
	return StateUpdate[T]{builder: createBuilder(enum.UpdateQuery), ctx: ctx}
}
