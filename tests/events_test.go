package goent_test

import (
	"sync"
	"testing"

	"github.com/azhai/gobus"
	"github.com/azhai/goent"
)

// eventCapture collects events from the event bus for testing.
type eventCapture struct {
	mu     sync.Mutex
	events []*gobus.Event
}

func newEventCapture() *eventCapture {
	return &eventCapture{}
}

func (c *eventCapture) handler(event *gobus.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

func (c *eventCapture) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

func (c *eventCapture) get(i int) *gobus.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i < 0 || i >= len(c.events) {
		return nil
	}
	return c.events[i]
}

func (c *eventCapture) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = nil
}

// setupWatchedDB creates an EventBus, subscribes to all ent: topics,
// and marks the Animal table as watched.
func setupWatchedDB(t *testing.T) (*Database, *gobus.EventBus, *eventCapture) {
	t.Helper()
	db, err := Setup()
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return nil, nil, nil
	}
	bus := gobus.NewEventBus(1024)
	cap := newEventCapture()
	topics := []string{
		goent.EventTopicInsertOne, goent.EventTopicInsertBulk,
		goent.EventTopicUpdate, goent.EventTopicUpdateByPK, goent.EventTopicUpdateByID,
		goent.EventTopicDelete, goent.EventTopicDeleteByPK, goent.EventTopicDeleteByID,
	}
	for _, topic := range topics {
		if err := bus.Subscribe(topic, gobus.Fanout, "test-capture", cap.handler); err != nil {
			t.Fatalf("Failed to subscribe to %s: %v", topic, err)
		}
	}
	db.Watching(bus, db.Animal.TableInfo)
	return db, bus, cap
}

func TestEvent_InsertOne(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	a := &Animal{Name: "Lion"}
	if err := db.Animal.Insert().One(a); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicInsertOne {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicInsertOne, evt.Topic)
	}
	if evt.Data["model"] != "Animal" {
		t.Errorf("Expected model 'Animal', got %v", evt.Data["model"])
	}
	if evt.Data["table"] != "animals" {
		t.Errorf("Expected table 'animals', got %v", evt.Data["table"])
	}
	affecteds, ok := evt.Data["affecteds"].(int64)
	if !ok || affecteds != 1 {
		t.Errorf("Expected affecteds=1, got %v", evt.Data["affecteds"])
	}
	ids, ok := evt.Data["ids"].([]int64)
	if !ok || len(ids) != 1 || ids[0] != int64(a.Id) {
		t.Errorf("Expected ids=[%d], got %v", a.Id, evt.Data["ids"])
	}
	// Single insert should have changes
	if evt.Data["changes"] == nil {
		t.Error("Expected non-nil changes for single insert")
	}
	// Non-transactional: trans_no should be empty
	if transNo, _ := evt.Data["trans_no"].(string); transNo != "" {
		t.Errorf("Expected empty trans_no for non-tx operation, got %q", transNo)
	}
}

func TestEvent_InsertBulk(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	animals := []*Animal{{Name: "Cat"}, {Name: "Dog"}, {Name: "Bird"}}
	if err := db.Animal.Insert().All(true, animals); err != nil {
		t.Fatalf("InsertAll failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicInsertBulk {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicInsertBulk, evt.Topic)
	}
	affecteds, ok := evt.Data["affecteds"].(int64)
	if !ok || affecteds != 3 {
		t.Errorf("Expected affecteds=3, got %v", evt.Data["affecteds"])
	}
	ids, ok := evt.Data["ids"].([]int64)
	if !ok || len(ids) != 3 {
		t.Errorf("Expected 3 ids, got %v", evt.Data["ids"])
	}
	// Bulk insert should NOT have changes
	if evt.Data["changes"] != nil {
		t.Errorf("Expected nil changes for bulk insert, got %#v", evt.Data["changes"])
	}
}

func TestEvent_UpdateByPK(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 1)
	cap.reset() // Reset to ignore insert events

	if err := db.Animal.Update().
		Set(goent.Pair{Key: "name", Value: "Updated"}).
		ByPK(int64(animals[0].Id)); err != nil {
		t.Fatalf("UpdateByPK failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicUpdateByPK {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicUpdateByPK, evt.Topic)
	}
	ids, ok := evt.Data["ids"].([]int64)
	if !ok || len(ids) != 1 || ids[0] != int64(animals[0].Id) {
		t.Errorf("Expected ids=[%d], got %v", animals[0].Id, evt.Data["ids"])
	}
	// ByPK update should have changes
	if evt.Data["changes"] == nil {
		t.Error("Expected non-nil changes for ByPK update")
	}
}

func TestEvent_UpdateConditional(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 5)
	cap.reset()

	err := db.Animal.Update().
		Set(goent.Pair{Key: "name", Value: "Modified"}).
		Filter(goent.Greater(db.Animal.Field("id"), animals[0].Id)).
		Exec()
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicUpdate {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicUpdate, evt.Topic)
	}
	affecteds, ok := evt.Data["affecteds"].(int64)
	if !ok || affecteds != 4 {
		t.Errorf("Expected affecteds=4, got %v", evt.Data["affecteds"])
	}
	// Conditional update has changes
	if evt.Data["changes"] == nil {
		t.Error("Expected non-nil changes for conditional update")
	}
	// Conditional update should have a where string
	where, ok := evt.Data["where"].(string)
	if !ok || where == "" {
		t.Error("Expected non-empty where string")
	}
}

func TestEvent_UpdateByID(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 5)
	cap.reset()

	ids, err := db.Animal.Filter(goent.Greater(db.Animal.Field("id"), animals[0].Id)).
		UpdateByID().Set(goent.Pair{Key: "name", Value: "ByID"}).Exec()
	if err != nil {
		t.Fatalf("UpdateByID failed: %v", err)
	}
	if len(ids) != 4 {
		t.Fatalf("Expected 4 affected IDs, got %d", len(ids))
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicUpdateByID {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicUpdateByID, evt.Topic)
	}
	evtIDs, ok := evt.Data["ids"].([]int64)
	if !ok || len(evtIDs) != 4 {
		t.Errorf("Expected 4 ids in event, got %v", evt.Data["ids"])
	}
	// UpdateByID should have changes
	if evt.Data["changes"] == nil {
		t.Error("Expected non-nil changes for UpdateByID")
	}
}

func TestEvent_DeleteByPK(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 1)
	cap.reset()

	if err := db.Animal.Delete().ByPK(int64(animals[0].Id)); err != nil {
		t.Fatalf("DeleteByPK failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicDeleteByPK {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicDeleteByPK, evt.Topic)
	}
	ids, ok := evt.Data["ids"].([]int64)
	if !ok || len(ids) != 1 || ids[0] != int64(animals[0].Id) {
		t.Errorf("Expected ids=[%d], got %v", animals[0].Id, evt.Data["ids"])
	}
}

func TestEvent_DeleteConditional(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 5)
	cap.reset()

	err := db.Animal.Delete().
		Filter(goent.Greater(db.Animal.Field("id"), animals[0].Id)).
		Exec()
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicDelete {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicDelete, evt.Topic)
	}
	affecteds, ok := evt.Data["affecteds"].(int64)
	if !ok || affecteds != 4 {
		t.Errorf("Expected affecteds=4, got %v", evt.Data["affecteds"])
	}
}

func TestEvent_DeleteByID(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 5)
	cap.reset()

	ids, err := db.Animal.Filter(goent.Greater(db.Animal.Field("id"), animals[0].Id)).
		DeleteByID().Exec()
	if err != nil {
		t.Fatalf("DeleteByID failed: %v", err)
	}
	if len(ids) != 4 {
		t.Fatalf("Expected 4 deleted IDs, got %d", len(ids))
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicDeleteByID {
		t.Errorf("Expected topic %s, got %s", goent.EventTopicDeleteByID, evt.Topic)
	}
	evtIDs, ok := evt.Data["ids"].([]int64)
	if !ok || len(evtIDs) != 4 {
		t.Errorf("Expected 4 ids in event, got %v", evt.Data["ids"])
	}
}

func TestEvent_DeleteAllSkipped(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	insertTestAnimals(t, 3)
	cap.reset()

	// Delete without WHERE (clear-all) should NOT send event
	err := db.Animal.Delete().Exec()
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if cap.len() != 0 {
		t.Errorf("Expected 0 events for clear-all delete, got %d", cap.len())
	}
}

func TestEvent_UnwatchedTableNoEvent(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	// Status table is NOT watched, only Animal is watched
	db.Status.Delete().Exec()
	t.Cleanup(func() { db.Status.Delete().Exec() })

	s := &Status{Name: "Active"}
	if err := db.Status.Insert().One(s); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if cap.len() != 0 {
		t.Errorf("Expected 0 events for unwatched table, got %d", cap.len())
	}
}

func TestEvent_TransactionHasTransNo(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}
	defer tx.Rollback()

	a := &Animal{Name: "TxCat"}
	if err := db.Animal.Insert().OnTransaction(tx).One(a); err != nil {
		t.Fatalf("Insert in tx failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	transNo, ok := evt.Data["trans_no"].(string)
	if !ok || transNo == "" {
		t.Errorf("Expected non-empty trans_no for tx operation, got %v", evt.Data["trans_no"])
	}
}

func TestEvent_TransactionSameTransNo(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	tx, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("NewTransaction failed: %v", err)
	}
	defer tx.Rollback()

	// Multiple operations in the same transaction should share the same trans_no
	a1 := &Animal{Name: "Cat1"}
	if err := db.Animal.Insert().OnTransaction(tx).One(a1); err != nil {
		t.Fatalf("Insert 1 failed: %v", err)
	}
	a2 := &Animal{Name: "Cat2"}
	if err := db.Animal.Insert().OnTransaction(tx).One(a2); err != nil {
		t.Fatalf("Insert 2 failed: %v", err)
	}
	if err := db.Animal.Update().OnTransaction(tx).
		Set(goent.Pair{Key: "name", Value: "Updated"}).
		ByPK(int64(a1.Id)); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if cap.len() != 3 {
		t.Fatalf("Expected 3 events, got %d", cap.len())
	}
	firstTransNo, _ := cap.get(0).Data["trans_no"].(string)
	if firstTransNo == "" {
		t.Fatal("Expected non-empty trans_no for first event")
	}
	for i := 1; i < 3; i++ {
		transNo, _ := cap.get(i).Data["trans_no"].(string)
		if transNo != firstTransNo {
			t.Errorf("Event %d: expected trans_no %q, got %q", i, firstTransNo, transNo)
		}
	}
}

func TestEvent_DifferentTransactionsDifferentTransNo(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	tx1, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("NewTransaction 1 failed: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := db.NewTransaction()
	if err != nil {
		t.Fatalf("NewTransaction 2 failed: %v", err)
	}
	defer tx2.Rollback()

	a1 := &Animal{Name: "Tx1Cat"}
	if err := db.Animal.Insert().OnTransaction(tx1).One(a1); err != nil {
		t.Fatalf("Insert in tx1 failed: %v", err)
	}
	a2 := &Animal{Name: "Tx2Cat"}
	if err := db.Animal.Insert().OnTransaction(tx2).One(a2); err != nil {
		t.Fatalf("Insert in tx2 failed: %v", err)
	}

	if cap.len() != 2 {
		t.Fatalf("Expected 2 events, got %d", cap.len())
	}
	no1, _ := cap.get(0).Data["trans_no"].(string)
	no2, _ := cap.get(1).Data["trans_no"].(string)
	if no1 == "" || no2 == "" {
		t.Errorf("Expected non-empty trans_no, got %q and %q", no1, no2)
	}
	if no1 == no2 {
		t.Errorf("Expected different trans_no for different transactions, got %q for both", no1)
	}
}

func TestEvent_SaveInsert(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	a := &Animal{Name: "SaveCat"}
	if err := db.Animal.Save().One(a); err != nil {
		t.Fatalf("Save (insert) failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicInsertOne {
		t.Errorf("Expected topic %s for save-insert, got %s", goent.EventTopicInsertOne, evt.Topic)
	}
	if evt.Data["model"] != "Animal" {
		t.Errorf("Expected model 'Animal', got %v", evt.Data["model"])
	}
}

func TestEvent_SaveUpdate(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 1)
	cap.reset()

	a := animals[0]
	a.Name = "SavedDog"
	if err := db.Animal.Save().One(a); err != nil {
		t.Fatalf("Save (update) failed: %v", err)
	}
	if cap.len() != 1 {
		t.Fatalf("Expected 1 event, got %d", cap.len())
	}
	evt := cap.get(0)
	if evt.Topic != goent.EventTopicUpdate {
		t.Errorf("Expected topic %s for save-update, got %s", goent.EventTopicUpdate, evt.Topic)
	}
}

func TestEvent_ChangesContent(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	db.Animal.Delete().Exec()
	t.Cleanup(func() { db.Animal.Delete().Exec() })

	a := &Animal{Name: "Lion"}
	if err := db.Animal.Insert().One(a); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	evt := cap.get(0)
	changes, ok := evt.Data["changes"].(map[string]any)
	if !ok {
		t.Fatalf("Expected changes to be map[string]any, got %T", evt.Data["changes"])
	}
	if name, exists := changes["name"]; !exists || name != "Lion" {
		t.Errorf("Expected changes[name]='Lion', got %v", changes["name"])
	}
}

func TestEvent_UpdateChangesContent(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 1)
	cap.reset()

	err := db.Animal.Update().
		Set(goent.Pair{Key: "name", Value: "Tiger"}).
		ByPK(int64(animals[0].Id))
	if err != nil {
		t.Fatalf("UpdateByPK failed: %v", err)
	}
	evt := cap.get(0)
	changes, ok := evt.Data["changes"].(map[string]any)
	if !ok {
		t.Fatalf("Expected changes to be map[string]any, got %T", evt.Data["changes"])
	}
	if name, exists := changes["name"]; !exists || name != "Tiger" {
		t.Errorf("Expected changes[name]='Tiger', got %v", changes["name"])
	}
}

func TestEvent_WhereField(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 3)
	cap.reset()

	err := db.Animal.Delete().
		Filter(goent.Equals(db.Animal.Field("id"), animals[0].Id)).
		Exec()
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	evt := cap.get(0)
	where, ok := evt.Data["where"].(string)
	if !ok || where == "" {
		t.Errorf("Expected non-empty where string, got %v", evt.Data["where"])
	}
}

func TestEvent_UpdateByIDChangesContent(t *testing.T) {
	db, _, cap := setupWatchedDB(t)
	animals := insertTestAnimals(t, 3)
	cap.reset()

	_, err := db.Animal.Filter(goent.Greater(db.Animal.Field("id"), animals[0].Id)).
		UpdateByID().Set(goent.Pair{Key: "name", Value: "ByIDCat"}).Exec()
	if err != nil {
		t.Fatalf("UpdateByID failed: %v", err)
	}
	evt := cap.get(0)
	changes, ok := evt.Data["changes"].(map[string]any)
	if !ok {
		t.Fatalf("Expected changes to be map[string]any, got %T", evt.Data["changes"])
	}
	if name, exists := changes["name"]; !exists || name != "ByIDCat" {
		t.Errorf("Expected changes[name]='ByIDCat', got %v", changes["name"])
	}
}
