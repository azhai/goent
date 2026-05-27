package plus_test

import (
	"testing"
	"time"

	"github.com/azhai/goent/plus"
)

func TestModuloHex_ResolveTableName(t *testing.T) {
	m := plus.ModuloHex{Bits: 4}
	tests := []struct {
		val    int64
		expect string
	}{
		{0, "t_order_00"},
		{1, "t_order_01"},
		{15, "t_order_0f"},
		{16, "t_order_00"},
		{255, "t_order_0f"},
		{256, "t_order_00"},
	}
	for _, tt := range tests {
		got := m.ResolveTableName("t_order", tt.val)
		if got != tt.expect {
			t.Errorf("ResolveTableName(%d) = %q, want %q", tt.val, got, tt.expect)
		}
	}
}

func TestModuloHex_Bits6(t *testing.T) {
	m := plus.ModuloHex{Bits: 6}
	tests := []struct {
		val    int64
		expect string
	}{
		{0, "t_order_00"},
		{63, "t_order_3f"},
		{64, "t_order_00"},
		{255, "t_order_3f"},
	}
	for _, tt := range tests {
		got := m.ResolveTableName("t_order", tt.val)
		if got != tt.expect {
			t.Errorf("Bits=6 ResolveTableName(%d) = %q, want %q", tt.val, got, tt.expect)
		}
	}
}

func TestModuloHex_MatchPattern(t *testing.T) {
	m := plus.ModuloHex{Bits: 4}
	if got := m.MatchPattern("t_order"); got != "t_order_*" {
		t.Errorf("MatchPattern() = %q, want %q", got, "t_order_*")
	}
}

func TestTimeGranularity_ResolveTableName(t *testing.T) {
	tm, _ := time.Parse("2006-01-02", "2025-05-21")
	tests := []struct {
		g      plus.TimeGranularity
		expect string
	}{
		{plus.ByDay, "order_20250521"},
		{plus.ByMonth, "order_202505"},
		{plus.ByYear, "order_2025"},
	}
	for _, tt := range tests {
		got := tt.g.ResolveTableName("order", tm)
		if got != tt.expect {
			t.Errorf("%v ResolveTableName() = %q, want %q", tt.g, got, tt.expect)
		}
	}
}

func TestTimeGranularity_MatchPattern(t *testing.T) {
	if got := plus.ByDay.MatchPattern("log"); got != "log_*" {
		t.Errorf("ByDay MatchPattern() = %q, want %q", got, "log_*")
	}
	if got := plus.ByYear.MatchPattern("log"); got != "log_*" {
		t.Errorf("ByYear MatchPattern() = %q, want %q", got, "log_*")
	}
}

func TestShardingStrategy_Interface(t *testing.T) {
	var _ plus.ShardingStrategy = &plus.ModuloHex{}
	var _ plus.ShardingStrategy = plus.ByDay
}
