package common

import (
	"reflect"
	"runtime"
)

// ReflectFunctionName returns the fully-qualifed name of a function.
// eg. "github.com/rotblauer/geom.(*Polygon).Area"
// eg. "github.com/rotblauer/geom.(*Polygon).Length"
// eg. "github.com/sams96/rgeo.Countries110
func ReflectFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
