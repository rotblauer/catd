package cattrack

import (
	"testing"
	"time"
)

func TestOffsetIndexT_IsEmpty(t *testing.T) {
	t.Run("Nil", testOffsetIndexT_IsEmpty_Nil)
	t.Run("Empty", testOffsetIndexT_IsEmpty_Empty)
}

func testOffsetIndexT_IsEmpty_Nil(t *testing.T) {
	var old, next Indexer
	old, next = nil, &OffsetIndexT{}
	ix := &OffsetIndexT{}
	out := ix.Index(old, next)
	if out == nil {
		t.Fatal("unexpected nil")
	}
	if out.IsEmpty() {
		t.Fatal("unexpected empty")
	}
}

func testOffsetIndexT_IsEmpty_Empty(t *testing.T) {
	old, next := &OffsetIndexT{}, &OffsetIndexT{}
	ix := &OffsetIndexT{}
	out := ix.Index(old, next)
	if out == nil {
		t.Fatal("unexpected nil")
	}
	if out.IsEmpty() {
		t.Fatal("unexpected empty")
	}
	if out.(*OffsetIndexT).Count != 1 {
		t.Errorf("unexpected count: %d", out.(*OffsetIndexT).Count)
	}
	if out.(*OffsetIndexT).VisitCount != 1 {
		t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
	}
}

func TestOffsetIndexT_Index(t *testing.T) {
	t.Run("++", testOffsetIndexT_Index_Count1)
	t.Run("123", testOffsetIndexT_Index_Count123)
	t.Run("VisitCount/Threshold", testOffsetIndexT_Index_VisitCount)
}

func testOffsetIndexT_Index_VisitCount(t *testing.T) {
	ix := &OffsetIndexT{VisitThreshold: time.Second}
	var old, next Indexer

	ti := time.Now()
	old = nil
	next = &OffsetIndexT{FirstTime: ti, LastTime: ti}

	out := ix.Index(old, next)
	if out.(*OffsetIndexT).VisitCount != 1 {
		t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
	}

	// 0.999-second increments are within (<) the VisitThreshold.
	// Expect 1 visit.
	for i := 0; i < 123; i++ {
		ti = ti.Add(999 * time.Millisecond)
		old = out
		next = &OffsetIndexT{FirstTime: ti, LastTime: ti}
		out = ix.Index(old, next)
		if out.(*OffsetIndexT).VisitCount != 1 {
			t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
			break
		}
	}

	// 1-second increments are within (==) the VisitThreshold.
	// Expect 1 visit.
	for i := 0; i < 123; i++ {
		ti = ti.Add(time.Second)
		old = out
		next = &OffsetIndexT{FirstTime: ti, LastTime: ti}
		out = ix.Index(old, next)
		if out.(*OffsetIndexT).VisitCount != 1 {
			t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
			break
		}
	}

	// 2-second increments are outside (>) the VisitThreshold.
	// Expect many visits.
	for i := 0; i < 123; i++ {
		ti = ti.Add(2 * time.Second)
		old = out
		next = &OffsetIndexT{FirstTime: ti, LastTime: ti}
		out = ix.Index(old, next)
		if out.(*OffsetIndexT).VisitCount != i+2 {
			t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
			break
		}
	}
}

func testOffsetIndexT_Index_Count1(t *testing.T) {
	ix := &OffsetIndexT{}
	var old, next Indexer

	// Zero-value old.
	old, next = nil, &OffsetIndexT{Count: 1}

	out := ix.Index(old, next)
	if out.(*OffsetIndexT).Count != 1 {
		t.Errorf("unexpected count: %d", out.(*OffsetIndexT).Count)
	}
	if out.(*OffsetIndexT).VisitCount != 1 {
		t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
	}

	// With non-zero-value old.
	old, next = &OffsetIndexT{Count: 0}, &OffsetIndexT{Count: 1}
	out = ix.Index(old, next)
	if out.(*OffsetIndexT).Count != 1 {
		t.Errorf("unexpected count: %d", out.(*OffsetIndexT).Count)
	}
	if out.(*OffsetIndexT).VisitCount != 1 {
		t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
	}

	// With zero-value next.
	// Incoming tracks MAY have a Count attribute assigned, but may not;
	// (currently depends on FromCatTrack/Decode).
	// Need to ensure that the Count attribute is always incremented.
	old, next = &OffsetIndexT{Count: 0}, &OffsetIndexT{Count: 0}
	out = ix.Index(old, next)
	if out.(*OffsetIndexT).Count != 1 {
		t.Errorf("unexpected count: %d", out.(*OffsetIndexT).Count)
	}
	if out.(*OffsetIndexT).VisitCount != 1 {
		t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
	}
}

func testOffsetIndexT_Index_Count123(t *testing.T) {
	var old, step Indexer
	old, step = nil, &OffsetIndexT{Count: 1}
	ix := &OffsetIndexT{}
	out := ix.Index(old, step)
	if out.(*OffsetIndexT).Count != 1 {
		t.Errorf("unexpected count: %d", out.(*OffsetIndexT).Count)
	}
	if out.(*OffsetIndexT).VisitCount != 1 {
		t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
	}
	for i := 2; i < 123; i++ {
		old = out
		step = &OffsetIndexT{Count: 1}
		out = ix.Index(old, step)
		if out.(*OffsetIndexT).Count != i {
			t.Errorf("unexpected count: %d", out.(*OffsetIndexT).Count)
			break
		}
		if out.(*OffsetIndexT).VisitCount != 1 {
			t.Errorf("unexpected visit count: %d", out.(*OffsetIndexT).VisitCount)
			break
		}
	}
}
