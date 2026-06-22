package goent

import (
	"fmt"

	"github.com/azhai/gobus"
	"github.com/azhai/goent/model"
)

// Event topic prefixes for table modification operations.
// The full topic format is "ent:<operation>", e.g. "ent:insert-one".
const (
	EventTopicInsertOne  = "ent:insert-one"  // Single row insert (with returning)
	EventTopicInsertBulk = "ent:insert-bulk" // Batch insert (with returning)
	EventTopicUpdate     = "ent:update"      // Conditional update (no JOIN, has WHERE)
	EventTopicUpdateByPK = "ent:update-bypk" // Update by primary key
	EventTopicUpdateByID = "ent:update-byid" // Two-phase update by queried IDs
	EventTopicDelete     = "ent:delete"      // Conditional delete (has WHERE)
	EventTopicDeleteByPK = "ent:delete-bypk" // Delete by primary key
	EventTopicDeleteByID = "ent:delete-byid" // Two-phase delete by queried IDs
)

// EventPriority is the default priority for table modification events.
const EventPriority = 3

// changesToMap converts a builder.Changes map (keyed by *Field) to a plain
// map[string]any keyed by column name. Returns nil if changes is empty.
func changesToMap(changes map[*Field]any) map[string]any {
	if len(changes) == 0 {
		return nil
	}
	m := make(map[string]any, len(changes))
	for fld, val := range changes {
		m[fld.ColumnName] = val
	}
	return m
}

// transNoFromConn returns a transaction identifier string for the given connection.
// Returns "" if the connection is not a Transaction (i.e. not in a transaction).
// The ID is derived from the pointer address, which is stable for the lifetime
// of the transaction and unique across concurrent transactions.
func transNoFromConn(conn model.Connection) string {
	if conn == nil {
		return ""
	}
	if tx, ok := conn.(model.Transaction); ok {
		return fmt.Sprintf("tx:%p", tx)
	}
	return ""
}

// EventData holds structured data for table modification events.
type EventData struct {
	Model    string         // Go struct type name (e.g. "Animal")
	Table    string         // database table name (e.g. "animals")
	Where    string         // WHERE clause template string (empty for insert/bypk)
	IDs      []int64        // affected primary key IDs
	Changes  map[string]any // column changes map (nil for bulk insert and deletes)
	Affected int64          // number of affected rows
	TransNo  string         // transaction identifier string (empty if not in a transaction)
}

// buildEventData constructs an EventData from the given parameters.
func buildEventData(info *TableInfo, conn model.Connection, topic, where string,
	ids []int64, changes map[string]any, affecteds int64) EventData {
	return EventData{
		Model:    info.modelType.Name(),
		Table:    info.TableName,
		Where:    where,
		IDs:      ids,
		Changes:  changes,
		Affected: affecteds,
		TransNo:  transNoFromConn(conn),
	}
}

// ToMap converts EventData to a plain map for publishing.
// Nil Changes is stored as untyped nil so that data["changes"] == nil holds.
func (e *EventData) ToMap() map[string]any {
	data := map[string]any{
		"model":     e.Model,
		"table":     e.Table,
		"where":     e.Where,
		"ids":       e.IDs,
		"affecteds": e.Affected,
		"trans_no":  e.TransNo,
	}
	if e.Changes != nil {
		data["changes"] = e.Changes
	}
	return data
}

// publishEvent publishes a table modification event to the event bus.
// It is a no-op if the bus is nil or the table is not watched.
func publishEvent(bus *gobus.EventBus, info *TableInfo, conn model.Connection, topic, where string,
	ids []int64, changes map[string]any, affecteds int64) {
	if bus == nil || info == nil || !info.isWatched {
		return
	}
	data := buildEventData(info, conn, topic, where, ids, changes, affecteds)
	_, _ = bus.Publish(topic, data.ToMap(), EventPriority, false)
}
