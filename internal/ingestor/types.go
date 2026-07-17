package ingestor

import (
	"time"

	"github.com/kubetrace/ingestor-backend/internal/clickhouse"
	"github.com/kubetrace/ingestor-backend/internal/model"
	"github.com/segmentio/kafka-go"
)

type Ingestor struct {
	kafkaReader *kafka.Reader
	clickhouse  *clickhouse.Client
	hotHours    int
	ttlHours    int
	batchSize   int
	flushWindow time.Duration
	queue       chan model.ClickHouseSpan
}
