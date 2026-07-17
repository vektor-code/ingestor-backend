package processor

import (
	"encoding/json"

	"github.com/kubetrace/ingestor-backend/internal/model"
)

func ProcessEnriched(spans []model.EnrichedSpan) []model.ClickHouseSpan {
	out := make([]model.ClickHouseSpan, 0, len(spans))
	for _, sp := range spans {
		durationNS := int64(sp.DurationMs * 1e6)
		if durationNS == 0 && sp.EndTime.After(sp.StartTime) {
			durationNS = sp.EndTime.Sub(sp.StartTime).Nanoseconds()
		}

		events := ""
		if len(sp.Events) > 0 {
			if data, err := json.Marshal(sp.Events); err == nil {
				events = string(data)
			}
		}

		tags := sp.Attributes
		if tags == nil {
			tags = map[string]string{}
		}

		out = append(out, model.ClickHouseSpan{
			Timestamp:     sp.StartTime.UTC().Format("2006-01-02 15:04:05.999999"),
			TraceID:       sp.TraceID,
			SpanID:        sp.SpanID,
			ParentSpanID:  sp.ParentSpanID,
			ServiceName:   sp.ServiceName,
			OperationName: sp.Name,
			DurationNS:    durationNS,
			StatusCode:    sp.Status,
			StatusMessage: sp.Error,
			Tags:          tags,
			Namespace:     sp.Namespace,
			Cluster:       sp.Cluster,
			Kind:          sp.Kind,
			PodName:       sp.PodName,
			NodeName:      sp.NodeName,
			Events:        events,
		})
	}
	return out
}
