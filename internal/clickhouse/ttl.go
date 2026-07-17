package clickhouse

import (
	"fmt"
	"strings"
)

func spansTTLExpr(hotHours, retentionHours int, hasCold bool) string {
	if hotHours <= 0 {
		hotHours = 48
	}
	var parts []string
	if hasCold {
		parts = append(parts, fmt.Sprintf("toDateTime(timestamp) + toIntervalHour(%d) TO VOLUME 'cold'", hotHours))
	}
	if retentionHours > 0 {
		if retentionHours < hotHours {
			retentionHours = hotHours
		}
		parts = append(parts, fmt.Sprintf("toDateTime(timestamp) + toIntervalHour(%d) DELETE", retentionHours))
	}
	return strings.Join(parts, ", ")
}
