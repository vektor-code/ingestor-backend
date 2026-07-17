package ingestor

import (
	"log"

	"github.com/kubetrace/ingestor-backend/internal/model"
)

func (ing *Ingestor) enqueueBatch(spans []model.ClickHouseSpan) {
	for _, span := range spans {
		ing.enqueue(span)
	}
}

func (ing *Ingestor) enqueue(span model.ClickHouseSpan) {
	select {
	case ing.queue <- span:
	default:
		log.Printf("[ingestor/warning] queue is full, dropping span: %s", span.SpanID)
	}
}
