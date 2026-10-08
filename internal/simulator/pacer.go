package simulator

import (
	"math"
	"time"
)

func PlannedRequestCount(rate float64, duration time.Duration, cap int64) int64 {
	planned := int64(math.Floor(rate * duration.Seconds()))
	if cap > 0 && cap < planned {
		return cap
	}
	return planned
}
