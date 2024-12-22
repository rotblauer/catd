package common

import "testing"

func TestDecimalToFixed(t *testing.T) {
	tests := []struct {
		num       float64
		precision int
		expected  float64
	}{
		{0.123456789, 0, 0},
		{0.123456789, 1, 0.1},
		{0.123456789, 2, 0.12},
		{0.123456789, 3, 0.123},
		{0.123456789, 4, 0.1234},
		{0.123456789, 5, 0.12345},
		{0.123456789, 6, 0.123456},
		// finish these through 123.456789
		{0.123456789, 7, 0.1234567},
		{0.123456789, 8, 0.12345678},
		{0.123456789, 9, 0.123456789},
		{0.123456789, 10, 0.123456789},
		{1.23456789, 0, 1},
		{1.23456789, 1, 1.2},
		{1.23456789, 2, 1.23},
		{1.23456789, 3, 1.234},
		{1.23456789, 4, 1.2345},
		{1.23456789, 5, 1.23456},
		{1.23456789, 6, 1.234567},
		{1.23456789, 7, 1.2345678},
		{1.23456789, 8, 1.23456789},
		{1.23456789, 9, 1.23456789},
		{1.23456789, 10, 1.23456789},
		{12.3456789, 0, 12},
		{12.3456789, 1, 12.3},
		{12.3456789, 2, 12.34},
		{12.3456789, 3, 12.345},
		{12.3456789, 4, 12.3456},
	}
	for _, test := range tests {
		actual := DecimalToFixed(test.num, test.precision)
		if actual != test.expected {
			t.Errorf("DecimalToFixed(%v, %v): expected %v, actual %v", test.num, test.precision, test.expected, actual)
		}
	}
}