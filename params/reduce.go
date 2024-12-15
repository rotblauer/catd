package params

import "time"

// S2DefaultVisitThreshold is the default time threshold for
// considering a cat as having left a cell; it is the threshold
// difference between the last visit and the current time.
// "How long does a cat stay outside a cell before we consider it to have left?"
var S2DefaultVisitThreshold = time.Hour

// RgeoDefaultVisitThreshold is the default time threshold for
// considering a cat as having left a cell; it is the threshold
// difference between the last visit and the current time.
var RgeoDefaultVisitThreshold = 24 * time.Hour
