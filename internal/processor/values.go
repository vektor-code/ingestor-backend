package processor

import (
	"fmt"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func mergeSpanTags(resourceAttrs map[string]string, spanAttrs []*commonpb.KeyValue) map[string]string {
	tags := make(map[string]string, len(resourceAttrs)+len(spanAttrs))
	for k, v := range resourceAttrs {
		tags[k] = v
	}
	for _, attr := range spanAttrs {
		tags[attr.Key] = stringVal(attr.Value)
	}
	return tags
}

func otlpStatusCode(status *tracepb.Status) string {
	if status == nil {
		return "UNSET"
	}
	switch status.Code {
	case 1:
		return "OK"
	case 2:
		return "ERROR"
	default:
		return "UNSET"
	}
}

func stringVal(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch vv := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return vv.StringValue
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", vv.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", vv.DoubleValue)
	case *commonpb.AnyValue_BoolValue:
		if vv.BoolValue {
			return "true"
		}
		return "false"
	}
	return ""
}
