package ingestor

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/kubetrace/ingestor-backend/internal/model"
	"github.com/kubetrace/ingestor-backend/internal/processor"
	colpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func (ing *Ingestor) ConsumeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := ing.kafkaReader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[ingestor/error] read kafka: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			ing.processMessage(msg.Value)
		}
	}
}

func (ing *Ingestor) processMessage(value []byte) {
	if len(value) > 0 && value[0] == '[' {
		var spans []model.EnrichedSpan
		if err := json.Unmarshal(value, &spans); err != nil {
			log.Printf("[ingestor/error] unmarshal enriched JSON payload: %v", err)
			return
		}
		ing.enqueueBatch(processor.ProcessEnriched(spans))
		return
	}

	var req colpb.ExportTraceServiceRequest
	if err := proto.Unmarshal(value, &req); err != nil {
		log.Printf("[ingestor/error] unmarshal protobuf payload: %v", err)
		return
	}
	ing.enqueueBatch(processor.ProcessOTLP(&req))
}
