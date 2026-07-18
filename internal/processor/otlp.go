package processor

import (
	"fmt"
	"time"

	"github.com/kubetrace/ingestor-backend/internal/model"
	colpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

var otlpKindNames = map[int32]string{
	1: "INTERNAL",
	2: "SERVER",
	3: "CLIENT",
	4: "PRODUCER",
	5: "CONSUMER",
}

func ProcessOTLP(req *colpb.ExportTraceServiceRequest) []model.ClickHouseSpan {
	out := make([]model.ClickHouseSpan, 0)
	for _, rs := range req.ResourceSpans {
		serviceName := "unknown"
		attrs := make(map[string]string)

		for _, attr := range rs.Resource.GetAttributes() {
			attrs[attr.Key] = stringVal(attr.Value)
			if attr.Key == "service.name" {
				serviceName = stringVal(attr.Value)
			}
		}

		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				tags := mergeSpanTags(attrs, sp.Attributes)
				enrichTelemetryTags(tags, sp.Name)
				startTime := time.Unix(0, int64(sp.StartTimeUnixNano)).UTC()
				statusCode := otlpStatusCode(sp.Status)
				if statusCode != "ERROR" && spanLooksErrored(tags, sp.Events) {
					statusCode = "ERROR"
				}

				out = append(out, model.ClickHouseSpan{
					Timestamp:     startTime.Format("2006-01-02 15:04:05.999999"),
					TraceID:       fmt.Sprintf("%x", sp.TraceId),
					SpanID:        fmt.Sprintf("%x", sp.SpanId),
					ParentSpanID:  fmt.Sprintf("%x", sp.ParentSpanId),
					ServiceName:   serviceName,
					OperationName: sp.Name,
					DurationNS:    int64(sp.EndTimeUnixNano - sp.StartTimeUnixNano),
					StatusCode:    statusCode,
					StatusMessage: sp.Status.GetMessage(),
					Tags:          tags,
					Namespace:     tags["k8s.namespace.name"],
					Cluster:       tags["k8s.cluster.name"],
					Kind:          otlpKindNames[int32(sp.Kind)],
					PodName:       tags["k8s.pod.name"],
					NodeName:      tags["k8s.node.name"],
				})
			}
		}
	}
	return out
}
