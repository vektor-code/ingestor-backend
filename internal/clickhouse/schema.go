package clickhouse

import (
	"fmt"
	"log"
)

func (c *Client) InitDatabaseSchema(hotHours, retentionHours int) error {
	hasCold := c.coldVolumeExists()

	for i, query := range schemaQueries(hotHours, retentionHours, hasCold) {
		if err := c.exec(query); err != nil {
			return fmt.Errorf("init database query %d: %w", i+1, err)
		}
	}

	log.Println("ClickHouse database and table verified/created.")
	return nil
}

func schemaQueries(hotHours, retentionHours int, hasCold bool) []string {
	storageSetting := ""
	if hasCold {
		storageSetting = "\n\t\tSETTINGS storage_policy = 'tiered'"
	}
	ttlClause := ""
	if ttl := spansTTLExpr(hotHours, retentionHours, hasCold); ttl != "" {
		ttlClause = "\n\t\tTTL " + ttl
	}

	queries := []string{
		"CREATE DATABASE IF NOT EXISTS kubetrace;",
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS kubetrace.spans (
			timestamp DateTime64(6, 'UTC') CODEC(DoubleDelta, LZ4),
			trace_id String CODEC(ZSTD(1)),
			span_id String CODEC(ZSTD(1)),
			parent_span_id String CODEC(ZSTD(1)),
			service_name LowCardinality(String) CODEC(ZSTD(1)),
			operation_name LowCardinality(String) CODEC(ZSTD(1)),
			duration_ns Int64 CODEC(T64, LZ4),
			status_code LowCardinality(String) CODEC(ZSTD(1)),
			status_message String CODEC(ZSTD(1)),
			tags Map(String, String) CODEC(ZSTD(1)),
			namespace LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
			cluster LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
			kind LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
			pod_name String DEFAULT '' CODEC(ZSTD(1)),
			node_name String DEFAULT '' CODEC(ZSTD(1)),
			events String DEFAULT '' CODEC(ZSTD(1)),
			INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 4
		) ENGINE = ReplacingMergeTree()
		PARTITION BY toYYYYMMDD(timestamp)
		ORDER BY (service_name, operation_name, timestamp, trace_id)%s%s;`, ttlClause, storageSetting),
		"ALTER TABLE kubetrace.spans ADD COLUMN IF NOT EXISTS namespace LowCardinality(String) DEFAULT '' CODEC(ZSTD(1));",
		"ALTER TABLE kubetrace.spans ADD COLUMN IF NOT EXISTS cluster LowCardinality(String) DEFAULT '' CODEC(ZSTD(1));",
		"ALTER TABLE kubetrace.spans ADD COLUMN IF NOT EXISTS kind LowCardinality(String) DEFAULT '' CODEC(ZSTD(1));",
		"ALTER TABLE kubetrace.spans ADD COLUMN IF NOT EXISTS pod_name String DEFAULT '' CODEC(ZSTD(1));",
		"ALTER TABLE kubetrace.spans ADD COLUMN IF NOT EXISTS node_name String DEFAULT '' CODEC(ZSTD(1));",
		"ALTER TABLE kubetrace.spans ADD COLUMN IF NOT EXISTS events String DEFAULT '' CODEC(ZSTD(1));",
		"ALTER TABLE kubetrace.spans ADD INDEX IF NOT EXISTS idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 4;",
	}
	if hasCold {
		queries = append(queries, "ALTER TABLE kubetrace.spans MODIFY SETTING storage_policy = 'tiered';")
	}
	if ttl := spansTTLExpr(hotHours, retentionHours, hasCold); ttl != "" {
		queries = append(queries, "ALTER TABLE kubetrace.spans MODIFY TTL "+ttl+";")
	} else {
		queries = append(queries, "ALTER TABLE kubetrace.spans REMOVE TTL;")
	}
	return queries
}
