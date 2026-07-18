package processor

import (
	"testing"
	"time"

	colpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestProcessOTLPEnrichesModernSemconvAndErrors(t *testing.T) {
	start := time.Now().UTC()
	req := &colpb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonpb.KeyValue{
					stringKV("service.name", "checkout-api"),
					stringKV("k8s.namespace.name", "prod"),
				},
			},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId:           []byte("1234567890123456"),
					SpanId:            []byte("12345678"),
					Name:              "POST /orders",
					Kind:              tracepb.Span_SPAN_KIND_CLIENT,
					StartTimeUnixNano: uint64(start.UnixNano()),
					EndTimeUnixNano:   uint64(start.Add(50 * time.Millisecond).UnixNano()),
					Attributes: []*commonpb.KeyValue{
						stringKV("http.request.method", "post"),
						stringKV("server.address", "postgres.default.svc"),
						stringKV("server.port", "5432"),
						stringKV("http.response.status_code", "503"),
					},
				}},
			}},
		}},
	}

	spans := ProcessOTLP(req)
	if len(spans) != 1 {
		t.Fatalf("expected one span, got %d", len(spans))
	}
	got := spans[0]
	if got.StatusCode != "ERROR" {
		t.Fatalf("expected derived ERROR status, got %q", got.StatusCode)
	}
	if got.Tags["http.method"] != "POST" {
		t.Fatalf("expected normalized http.method POST, got %q", got.Tags["http.method"])
	}
	if got.Tags["db.system"] != "postgresql" {
		t.Fatalf("expected db.system postgresql, got %q", got.Tags["db.system"])
	}
	if got.Tags["vektor.dependency.evidence"] != "port" {
		t.Fatalf("expected dependency evidence from port, got %q", got.Tags["vektor.dependency.evidence"])
	}
}

func stringKV(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: value},
		},
	}
}
