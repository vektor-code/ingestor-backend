package processor

import (
	"fmt"
	"strconv"
	"strings"

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

func spanLooksErrored(tags map[string]string, events []*tracepb.Span_Event) bool {
	if code, ok := parseStatusCode(firstTag(tags, "http.response.status_code", "http.status_code")); ok && code >= 500 {
		return true
	}
	if code, ok := parseStatusCode(firstTag(tags, "rpc.grpc.status_code", "grpc.status_code")); ok && code != 0 {
		return true
	}
	for _, event := range events {
		if strings.EqualFold(event.Name, "exception") {
			return true
		}
	}
	return false
}

func enrichTelemetryTags(tags map[string]string, spanName string) {
	if tags == nil {
		return
	}

	copyIfMissing(tags, "http.method", "http.request.method")
	copyIfMissing(tags, "http.url", "url.full")
	copyIfMissing(tags, "http.target", "url.path")
	copyIfMissing(tags, "db.name", "db.namespace")
	copyIfMissing(tags, "net.peer.name", "server.address", "network.peer.address", "peer.service")
	copyIfMissing(tags, "net.peer.port", "server.port", "network.peer.port", "peer.port")
	copyIfMissing(tags, "messaging.destination", "messaging.destination.name", "messaging.destination_name")

	if tags["http.method"] != "" {
		tags["http.method"] = strings.ToUpper(tags["http.method"])
	}
	if strings.EqualFold(tags["messaging.system"], "message_bus") {
		tags["messaging.system"] = "rabbitmq"
	}

	system, kind, evidence := inferDependencySystem(tags, spanName)
	if system == "" {
		return
	}
	if kind == "messaging" {
		if tags["messaging.system"] == "" || tags["messaging.system"] == "unknown" {
			tags["messaging.system"] = system
		}
	} else {
		if tags["db.system"] == "" || tags["db.system"] == "unknown" {
			tags["db.system"] = system
		}
	}
	if tags["vektor.dependency.system"] == "" {
		tags["vektor.dependency.system"] = system
	}
	if tags["vektor.dependency.kind"] == "" {
		tags["vektor.dependency.kind"] = kind
	}
	if tags["vektor.dependency.evidence"] == "" {
		tags["vektor.dependency.evidence"] = evidence
	}
}

func inferDependencySystem(tags map[string]string, spanName string) (system, kind, evidence string) {
	if sys := strings.ToLower(strings.TrimSpace(tags["db.system"])); sys != "" && sys != "unknown" {
		return normalizeSystem(sys), "database", "db.system"
	}
	if sys := strings.ToLower(strings.TrimSpace(tags["messaging.system"])); sys != "" && sys != "unknown" {
		if sys == "message_bus" {
			sys = "rabbitmq"
		}
		return normalizeSystem(sys), "messaging", "messaging.system"
	}

	port := firstTag(tags, "server.port", "net.peer.port", "network.peer.port", "peer.port")
	host := strings.ToLower(firstTag(tags, "server.address", "net.peer.name", "network.peer.address", "peer.service", "net.peer.ip"))
	if sys, k := inferByPort(port, host); sys != "" {
		return sys, k, "port"
	}

	candidates := []string{spanName, host, tags["db.name"], tags["db.connection_string"], tags["http.url"], tags["url.full"], tags["messaging.destination"]}
	for _, value := range candidates {
		if sys, k := inferByText(strings.ToLower(value)); sys != "" {
			return sys, k, "name"
		}
	}
	for key, value := range tags {
		if strings.Contains(strings.ToLower(key), "password") || strings.Contains(strings.ToLower(key), "secret") || strings.Contains(strings.ToLower(key), "token") {
			continue
		}
		if sys, k := inferByText(strings.ToLower(value)); sys != "" {
			return sys, k, "attribute"
		}
	}

	if tags["db.name"] != "" || tags["db.statement"] != "" || tags["db.query.text"] != "" {
		return "database", "database", "db.attribute"
	}
	if tags["messaging.destination"] != "" {
		return "rabbitmq", "messaging", "messaging.destination"
	}
	return "", "", ""
}

func inferByPort(port, host string) (string, string) {
	switch strings.TrimSpace(port) {
	case "5432", "5433":
		return "postgresql", "database"
	case "3306", "33060":
		return "mysql", "database"
	case "6379", "6380":
		return "redis", "database"
	case "27017", "27018":
		return "mongodb", "database"
	case "9092", "9093", "9094", "29092", "39092":
		return "kafka", "messaging"
	case "5671", "5672", "15671", "15672":
		return "rabbitmq", "messaging"
	case "1433":
		return "mssql", "database"
	case "1521":
		return "oracle", "database"
	case "8123", "9440":
		return "clickhouse", "database"
	case "9000", "9001":
		if strings.Contains(host, "clickhouse") {
			return "clickhouse", "database"
		}
		return "minio", "database"
	case "9200", "9300":
		return "elasticsearch", "database"
	case "8200", "8201":
		if strings.Contains(host, "apm") {
			return "apm", "database"
		}
		return "vault", "database"
	}
	return "", ""
}

func inferByText(value string) (string, string) {
	switch {
	case strings.Contains(value, "postgres"), strings.Contains(value, "postgresql"):
		return "postgresql", "database"
	case strings.Contains(value, "redis"):
		return "redis", "database"
	case strings.Contains(value, "mysql"):
		return "mysql", "database"
	case strings.Contains(value, "mongo"):
		return "mongodb", "database"
	case strings.Contains(value, "clickhouse"):
		return "clickhouse", "database"
	case strings.Contains(value, "elastic"):
		return "elasticsearch", "database"
	case strings.Contains(value, "minio"), strings.Contains(value, "s3.amazonaws"):
		return "minio", "database"
	case strings.Contains(value, "vault"):
		return "vault", "database"
	case strings.Contains(value, "liquibase"):
		return "liquibase", "database"
	case strings.Contains(value, "kafka"), strings.Contains(value, "broker-"):
		return "kafka", "messaging"
	case strings.Contains(value, "rabbitmq"), strings.Contains(value, "amqp://"), strings.Contains(value, "amqps://"):
		return "rabbitmq", "messaging"
	}
	return "", ""
}

func normalizeSystem(system string) string {
	switch system {
	case "postgres":
		return "postgresql"
	case "mongo":
		return "mongodb"
	case "elastic":
		return "elasticsearch"
	default:
		return system
	}
}

func copyIfMissing(tags map[string]string, target string, sources ...string) {
	if tags[target] != "" {
		return
	}
	for _, source := range sources {
		if tags[source] != "" {
			tags[target] = tags[source]
			return
		}
	}
}

func firstTag(tags map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(tags[key]); value != "" {
			return value
		}
	}
	return ""
}

func parseStatusCode(value string) (int, bool) {
	if value == "" {
		return 0, false
	}
	code, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, false
	}
	return code, true
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
