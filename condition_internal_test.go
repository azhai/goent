package goent

import (
	"strings"
	"testing"
)

// TestNot_EmptyCondition guards against generating the illegal "NOT ()" SQL.
// Not of an empty condition must itself be empty.
func TestNot_EmptyCondition(t *testing.T) {
	got := Not(Condition{})
	if !got.IsEmpty() {
		t.Fatalf("Not(Condition{}) = %#v, want an empty condition", got)
	}
}

// TestNot_WrapsNonEmpty verifies Not wraps a real condition in "NOT (...)".
func TestNot_WrapsNonEmpty(t *testing.T) {
	f := &Field{ColumnName: "id"}
	c := Equals(f, 1)
	got := Not(c)
	want := "NOT (%s = ?)"
	if got.Template != want {
		t.Errorf("Not(Equals) template = %q, want %q", got.Template, want)
	}
	if len(got.Fields) != 1 || len(got.Values) != 1 {
		t.Errorf("Not(Equals) fields/values = %d/%d, want 1/1", len(got.Fields), len(got.Values))
	}
}

// TestApplyFilter_DoesNotMutateCallerSlice is a regression test for the bug where
// applyFilter appended into the caller's variadic backing array (Filter(s...)
// shares storage with s), corrupting the caller's slice. The fix copies into a
// fresh slice first.
func TestApplyFilter_DoesNotMutateCallerSlice(t *testing.T) {
	// full has len 4; s is a len-2 reslice sharing the same backing array, so
	// a buggy append(s, w) would spill into full[2] and be observable here.
	full := make([]Condition, 4)
	s := full[:2]
	s[0] = Condition{Template: "a = ?"}
	s[1] = Condition{Template: "b = ?"}
	w := Condition{Template: "extra = ?"}

	got := applyFilter(&w, s...)

	if !full[2].IsEmpty() || !full[3].IsEmpty() {
		t.Fatalf("applyFilter corrupted the caller's backing array: full[2]=%#v full[3]=%#v", full[2], full[3])
	}
	if got.IsEmpty() {
		t.Fatal("expected a merged (non-empty) condition")
	}
	if !strings.Contains(got.Template, "extra = ?") {
		t.Errorf("merged template = %q, want it to contain %q", got.Template, "extra = ?")
	}
}

// TestApplyFilter_EmptyCondsReturnsW covers the len(conds)==0 short-circuit:
// it must return the existing where-clause unchanged.
func TestApplyFilter_EmptyCondsReturnsW(t *testing.T) {
	w := Condition{Template: "w = ?", Fields: []*Field{{ColumnName: "id"}}}
	got := applyFilter(&w)
	if got.Template != "w = ?" || len(got.Fields) != 1 {
		t.Errorf("applyFilter(nil conds) = %#v, want w unchanged", got)
	}
}
