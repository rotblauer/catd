package common

import (
	"fmt"
	"testing"
)

func HelloWorld() {
	fmt.Println("Hello, World!")
}

type TestStruct struct{}

func (t *TestStruct) HelloWorld() error {
	fmt.Println("Hello, World!")
	return nil
}

func TestReflectFunctionName(t *testing.T) {
	got := ReflectFunctionName(HelloWorld)
	want := "github.com/rotblauer/catd/common.HelloWorld"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

//func TestReflectMethodName(t *testing.T) {
//	ts := &TestStruct{}
//	got := ReflectMethodName(ts, ts.HelloWorld)
//	want := "HelloWorld"
//	if got != want {
//		t.Errorf("got %q, want %q", got, want)
//	}
//	tss := TestStruct{}
//	got = ReflectMethodName(tss, tss.HelloWorld)
//	want = ""
//	if got != want {
//		t.Errorf("got %q, want %q", got, want)
//	}
//}
