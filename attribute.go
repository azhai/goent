package goent

import (
	"reflect"

	"github.com/azhai/goent/enum"
	"github.com/azhai/goent/model"
	"github.com/azhai/goent/utils"
)

type OneToSomeRelation struct {
	IsOneToOne bool
	attributeStrings
}

func (r OneToSomeRelation) schema() *string {
	return r.schemaName
}

func (r OneToSomeRelation) getDb() *DB {
	return r.db
}

func (r OneToSomeRelation) isPrimaryKey() bool {
	return false
}

func (r OneToSomeRelation) getTableId() int {
	return r.tableId
}

func (r OneToSomeRelation) table() string {
	return r.tableName
}

func (r OneToSomeRelation) getAttributeName() string {
	return r.attributeName
}

func createOneToSome(b body, typeOf reflect.Type) any {
	rel := OneToSomeRelation{}
	targetPks := primaryKeys(typeOf)
	count := 0
	for i := range targetPks {
		if targetPks[i].Name == b.prefixName {
			count++
		}
	}

	if count == 0 {
		return nil
	}

	rel.attributeStrings = createAttributeStrings(
		b.mapp.db,
		b.schema,
		b.mapp.pks[0].tableName,
		b.fieldName,
		b.mapp.tableId,
		b.fieldId,
		b.driver,
	)
	return rel
}

type ManyToSomeRelation struct {
	IsDefault bool
	attributeStrings
}

func (r ManyToSomeRelation) schema() *string {
	return r.schemaName
}

func (r ManyToSomeRelation) getDb() *DB {
	return r.db
}

func (r ManyToSomeRelation) isPrimaryKey() bool {
	return false
}

func (r ManyToSomeRelation) getTableId() int {
	return r.tableId
}

func (r ManyToSomeRelation) table() string {
	return r.tableName
}

func (r ManyToSomeRelation) getAttributeName() string {
	return r.attributeName
}

func createManyToSome(b body, typeOf reflect.Type) any {
	rel := ManyToSomeRelation{}
	targetPks := primaryKeys(typeOf)
	count := 0
	for i := range targetPks {
		if targetPks[i].Name == b.prefixName {
			count++
		}
	}

	if count == 0 {
		return nil
	}
	rel.IsDefault = getTagValue(b.valueOf.Type().Field(b.fieldId).Tag.Get("goe"), "default:") != ""
	rel.attributeStrings = createAttributeStrings(
		b.mapp.db,
		b.schema,
		b.mapp.pks[0].tableName,
		b.fieldName,
		b.mapp.tableId,
		b.fieldId,
		b.driver,
	)
	return rel
}

type attributeStrings struct {
	db            *DB
	schemaName    *string
	tableId       int
	tableName     string
	attributeName string
	fieldId       int
}

func createAttributeStrings(db *DB, schema *string, table string, attributeName string, tableId, fieldId int, Driver model.Driver) attributeStrings {
	return attributeStrings{
		db:            db,
		tableName:     table,
		tableId:       tableId,
		fieldId:       fieldId,
		schemaName:    schema,
		attributeName: Driver.KeywordHandler(utils.ColumnNamePattern(attributeName)),
	}
}

type pk struct {
	autoIncrement bool
	attributeStrings
}

func (p pk) schema() *string {
	return p.schemaName
}

func (p pk) getDb() *DB {
	return p.db
}

func (p pk) isPrimaryKey() bool {
	return true
}

func (p pk) getTableId() int {
	return p.tableId
}

func (p pk) table() string {
	return p.tableName
}

func (p pk) getAttributeName() string {
	return p.attributeName
}

func createPk(db *DB, schema *string, table string, attributeName string, autoIncrement bool, tableId, fieldId int, Driver model.Driver) pk {
	table = Driver.KeywordHandler(table)
	return pk{
		attributeStrings: createAttributeStrings(db, schema, table, attributeName, tableId, fieldId, Driver),
		autoIncrement:    autoIncrement}
}

type att struct {
	isDefault bool
	attributeStrings
}

func (a att) schema() *string {
	return a.schemaName
}

func (a att) getDb() *DB {
	return a.db
}

func (a att) isPrimaryKey() bool {
	return false
}

func (a att) getTableId() int {
	return a.tableId
}

func (a att) table() string {
	return a.tableName
}

func (a att) getAttributeName() string {
	return a.attributeName
}

func createAtt(db *DB, attributeName string, schema *string, table string, tableId, fieldId int, isDefault bool, d model.Driver) att {
	return att{
		isDefault:        isDefault,
		attributeStrings: createAttributeStrings(db, schema, table, attributeName, tableId, fieldId, d)}
}

func (p pk) buildAttributeSelect(atts []model.Attribute, i int) {
	atts[i] = model.Attribute{
		Table: p.tableName,
		Name:  p.attributeName,
	}
}

func (a att) buildAttributeSelect(atts []model.Attribute, i int) {
	atts[i] = model.Attribute{
		Table: a.tableName,
		Name:  a.attributeName,
	}
}

func (r ManyToSomeRelation) buildAttributeSelect(atts []model.Attribute, i int) {
	atts[i] = model.Attribute{
		Table: r.tableName,
		Name:  r.attributeName,
	}
}

func (r OneToSomeRelation) buildAttributeSelect(atts []model.Attribute, i int) {
	atts[i] = model.Attribute{
		Table: r.tableName,
		Name:  r.attributeName,
	}
}

func (p pk) buildAttributeInsert(b *builder) {
	if !p.autoIncrement {
		b.fieldIds = append(b.fieldIds, p.fieldId)
		b.query.Attributes = append(b.query.Attributes, model.Attribute{Name: p.getAttributeName()})
		return
	}
	b.query.ReturningID = &model.Attribute{Name: p.getAttributeName()}
	b.pkFieldId = p.fieldId
}

func (a att) buildAttributeInsert(b *builder) {
	b.fieldIds = append(b.fieldIds, a.fieldId)
	b.query.Attributes = append(b.query.Attributes, model.Attribute{Name: a.getAttributeName()})
}

func (r ManyToSomeRelation) buildAttributeInsert(b *builder) {
	b.fieldIds = append(b.fieldIds, r.fieldId)
	b.query.Attributes = append(b.query.Attributes, model.Attribute{Name: r.getAttributeName()})
}

func (r OneToSomeRelation) buildAttributeInsert(b *builder) {
	b.fieldIds = append(b.fieldIds, r.fieldId)
	b.query.Attributes = append(b.query.Attributes, model.Attribute{Name: r.getAttributeName()})
}

func (p pk) getFieldId() int {
	return p.fieldId
}

func (a att) getFieldId() int {
	return a.fieldId
}

func (r ManyToSomeRelation) getFieldId() int {
	return r.fieldId
}

func (r OneToSomeRelation) getFieldId() int {
	return r.fieldId
}

func (p pk) getDefault() bool {
	return false
}

func (a att) getDefault() bool {
	return a.isDefault
}

func (r ManyToSomeRelation) getDefault() bool {
	return r.IsDefault
}

func (r OneToSomeRelation) getDefault() bool {
	return false
}

type aggregateResult struct {
	attributeName string
	tableName     string
	schemaName    *string
	aggregateType enum.AggregateType
	tableId       int
	db            *DB
}

func (a aggregateResult) buildAttributeSelect(atts []model.Attribute, i int) {
	atts[i] = model.Attribute{
		Table:         a.tableName,
		Name:          a.attributeName,
		AggregateType: a.aggregateType}
}

func (a aggregateResult) schema() *string {
	return a.schemaName
}

func (a aggregateResult) table() string {
	return a.tableName
}

func (a aggregateResult) getTableId() int {
	return a.tableId
}

func (a aggregateResult) getDb() *DB {
	return a.db
}

type functionResult struct {
	attributeName string
	tableName     string
	schemaName    *string
	functionType  enum.FunctionType
	tableId       int
	db            *DB
}

func (f functionResult) buildAttributeSelect(atts []model.Attribute, i int) {
	atts[i] = model.Attribute{
		Table:        f.tableName,
		Name:         f.attributeName,
		FunctionType: f.functionType}
}

func (f functionResult) schema() *string {
	return f.schemaName
}

func (f functionResult) table() string {
	return f.tableName
}

func (f functionResult) getTableId() int {
	return f.tableId
}

func (f functionResult) getDb() *DB {
	return f.db
}
