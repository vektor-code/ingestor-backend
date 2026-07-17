package ingestor

import (
	"context"
	"time"

	"github.com/kubetrace/ingestor-backend/internal/model"
)

func (ing *Ingestor) RunBatcher(ctx context.Context) {
	batch := make([]model.ClickHouseSpan, 0, ing.batchSize)
	ticker := time.NewTicker(ing.flushWindow)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				ing.clickhouse.WriteBatch(batch)
			}
			return
		case span := <-ing.queue:
			batch = append(batch, span)
			if len(batch) >= ing.batchSize {
				ing.clickhouse.WriteBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				ing.clickhouse.WriteBatch(batch)
				batch = batch[:0]
			}
		}
	}
}
