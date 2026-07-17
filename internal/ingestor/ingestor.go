package ingestor

import (
	"time"

	"github.com/kubetrace/ingestor-backend/internal/clickhouse"
	"github.com/kubetrace/ingestor-backend/internal/model"
	"github.com/segmentio/kafka-go"
)

func New(cfg Config) *Ingestor {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.KafkaGroup,
		MinBytes: 10e3,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})

	return &Ingestor{
		kafkaReader: reader,
		clickhouse:  clickhouse.New(cfg.ClickHouseURL),
		hotHours:    cfg.ClickHouseHotHours,
		ttlHours:    cfg.ClickHouseTTLHours,
		batchSize:   cfg.BatchSize,
		flushWindow: cfg.FlushWindow,
		queue:       make(chan model.ClickHouseSpan, cfg.BatchSize*8),
	}
}

func (ing *Ingestor) InitDatabaseSchema() error {
	return ing.clickhouse.InitDatabaseSchema(ing.hotHours, ing.ttlHours)
}

func (ing *Ingestor) Close() error {
	return ing.kafkaReader.Close()
}
