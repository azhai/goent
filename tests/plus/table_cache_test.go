package plus_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/azhai/goent/plus"
)

func TestTableCache_AddRemoveExists(t *testing.T) {
	c := plus.NewTableCache(5 * time.Minute)
	if c.Exists("t_user") {
		t.Error("empty cache should not contain anything")
	}
	c.Add("t_user", "t_order")
	if !c.Exists("t_user") || !c.Exists("t_order") {
		t.Error("Add failed")
	}
	c.Remove("t_user")
	if c.Exists("t_user") {
		t.Error("Remove failed")
	}
	if !c.Exists("t_order") {
		t.Error("Remove should not affect other entries")
	}
}

func TestTableCache_Match(t *testing.T) {
	c := plus.NewTableCache(5 * time.Minute)
	c.Add("t_order_00", "t_order_01", "t_order_0f", "t_product_00")
	got := c.Match("t_order_*")
	if len(got) != 3 {
		t.Fatalf("Match returned %d items, want 3", len(got))
	}
	if got[0] != "t_order_00" || got[2] != "t_order_0f" {
		t.Errorf("Match result not sorted: %v", got)
	}
}

func TestTableCache_MatchEmpty(t *testing.T) {
	c := plus.NewTableCache(5 * time.Minute)
	got := c.Match("nonexistent_*")
	if len(got) != 0 {
		t.Errorf("expected empty, got %d", len(got))
	}
}

func TestTableCache_ConcurrentAccess(t *testing.T) {
	c := plus.NewTableCache(time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("table_%02x", n)
			c.Add(name)
			c.Exists(name)
		}(i)
	}
	wg.Wait()
	if c.Size() != 100 {
		t.Errorf("expected 100 entries, got %d", c.Size())
	}
}

func TestTableCache_IsExpired(t *testing.T) {
	c := plus.NewTableCache(time.Nanosecond)
	if !c.IsExpired() {
		t.Error("cache with tiny TTL should be expired after creation")
	}
	c = plus.NewTableCache(time.Hour)
	if !c.IsExpired() {
		t.Error("unrefreshed cache should be considered expired")
	}
}

func TestTableCache_NewTableCacheDefaultTTL(t *testing.T) {
	c := plus.NewTableCache(0)
	c.Add("test_table")
	if !c.Exists("test_table") {
		t.Error("cache should be usable after creation")
	}
}
