package simulator

import (
	"fmt"
	"time"
)

func GenerateRunID(t time.Time, seed int64) string {
	return fmt.Sprintf("run-%s-%d", t.UTC().Format("20060102T150405Z"), seed)
}
