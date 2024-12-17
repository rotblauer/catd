package cattrack

import (
	"testing"
)

func TestMyReducerT_Index(t *testing.T) {
	t.Run("Empty", testMyReducerT_Index_Empty)
	t.Run("Nil", testMyReducerT_Index_Nil)

}

func testMyReducerT_Index_Nil(t *testing.T) {
	var old, next Indexer
	old, next = nil, &MyReducerT{}
	ix := &MyReducerT{}
	out := ix.Index(old, next)
	if out == nil {
		t.Fatal("unexpected nil")
	}
	if out.IsEmpty() {
		t.Fatal("unexpected empty")
	}
}

func testMyReducerT_Index_Empty(t *testing.T) {
	old, next := &MyReducerT{}, &MyReducerT{}
	ix := &MyReducerT{}
	out := ix.Index(old, next)
	if out == nil {
		t.Fatal("unexpected nil")
	}
	if out.IsEmpty() {
		t.Fatal("unexpected empty")
	}
	if out.(*MyReducerT).Count != 1 {
		t.Fatal("unexpected count")
	}
	if out.(*MyReducerT).VisitCount != 1 {
		t.Fatal("unexpected visit count")
	}
}
