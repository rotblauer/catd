package common

import "math"
import "github.com/shopspring/decimal"

// https://stackoverflow.com/questions/18390266/how-can-we-truncate-float64-type-to-a-particular-precision
func Round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func decimalToFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(Round(num*output)) / output
}

func DecimalToFixed(num float64, precision int) float64 {
	d := decimal.NewFromFloat(num)
	d = d.Truncate(int32(precision))
	f, _ := d.Float64()
	return f
}
