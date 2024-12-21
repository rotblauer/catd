package common

import "log/slog"

// SlogTempLevel returns a function that resets the slog level to the previous level,
// pairs well with defer.
// Use like:
// func Test123(t *testing.T) {
//     defer common.SlogTempLevel(slog.Level(slog.LevelWarn + 1))()
func SlogResetLevel(level slog.Level) (reset func()) {
	oldLevel := slog.SetLogLoggerLevel(level)
	return func() {
		slog.SetLogLoggerLevel(oldLevel)
	}
}
