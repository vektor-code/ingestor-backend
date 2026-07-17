package model

import (
	"encoding/json"
	"time"
)

type ClickHouseSpan struct {
	Timestamp     string            `json:"timestamp"`
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id"`
	ServiceName   string            `json:"service_name"`
	OperationName string            `json:"operation_name"`
	DurationNS    int64             `json:"duration_ns"`
	StatusCode    string            `json:"status_code"`
	StatusMessage string            `json:"status_message"`
	Tags          map[string]string `json:"tags"`
	Namespace     string            `json:"namespace"`
	Cluster       string            `json:"cluster"`
	Kind          string            `json:"kind"`
	PodName       string            `json:"pod_name"`
	NodeName      string            `json:"node_name"`
	Events        string            `json:"events"`
}

// EnrichedSpan is the JSON format published by api-backend's Kafka producer.
type EnrichedSpan struct {
	TraceID      string            `json:"traceId"`
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId"`
	Name         string            `json:"name"`
	ServiceName  string            `json:"serviceName"`
	Namespace    string            `json:"namespace"`
	Cluster      string            `json:"cluster"`
	PodName      string            `json:"podName"`
	NodeName     string            `json:"nodeName"`
	StartTime    time.Time         `json:"startTime"`
	EndTime      time.Time         `json:"endTime"`
	DurationMs   float64           `json:"durationMs"`
	Status       string            `json:"status"`
	Kind         string            `json:"kind"`
	Attributes   map[string]string `json:"attributes"`
	Events       []json.RawMessage `json:"events"`
	Error        string            `json:"error"`
}
