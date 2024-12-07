package params

import "time"

// S2DefaultVisitThreshold is the default time threshold for
// considering a cat as having left a cell; it is the threshold
// difference between the last visit and the current time.
var S2DefaultVisitThreshold = time.Hour
