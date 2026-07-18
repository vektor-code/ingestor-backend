package ingestor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/kubetrace/ingestor-backend/internal/model"
	"github.com/kubetrace/ingestor-backend/internal/processor"
	"github.com/segmentio/kafka-go"
	colpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func (ing *Ingestor) ConsumeLoop(ctx context.Context) {
	batch := make([]model.ClickHouseSpan, 0, ing.batchSize)
	messages := make([]kafka.Message, 0, ing.batchSize)
	flushDeadline := time.Time{}

	for {
		if ctx.Err() != nil {
			return
		}

		fetchCtx := ctx
		cancel := func() {}
		if len(batch) > 0 {
			if flushDeadline.IsZero() {
				flushDeadline = time.Now().Add(ing.flushWindow)
			}
			wait := time.Until(flushDeadline)
			if wait <= 0 {
				if ing.flushAndCommit(ctx, batch, messages) {
					batch = batch[:0]
					messages = messages[:0]
					flushDeadline = time.Time{}
				}
				continue
			}
			fetchCtx, cancel = context.WithTimeout(ctx, wait)
		}

		msg, err := ing.kafkaReader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				if len(batch) > 0 {
					_ = ing.flushAndCommit(context.Background(), batch, messages)
				}
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if ing.flushAndCommit(ctx, batch, messages) {
					batch = batch[:0]
					messages = messages[:0]
					flushDeadline = time.Time{}
				}
				continue
			}
			log.Printf("[ingestor/error] fetch kafka: %v", err)
			ing.sleep(ctx, time.Second)
			continue
		}

		spans, err := decodeMessageSpans(msg.Value)
		if err != nil {
			log.Printf("[ingestor/error] %v", err)
			ing.commitMessages(ctx, msg)
			continue
		}
		if len(spans) == 0 {
			ing.commitMessages(ctx, msg)
			continue
		}

		batch = append(batch, spans...)
		messages = append(messages, msg)
		if flushDeadline.IsZero() {
			flushDeadline = time.Now().Add(ing.flushWindow)
		}
		if len(batch) >= ing.batchSize {
			if ing.flushAndCommit(ctx, batch, messages) {
				batch = batch[:0]
				messages = messages[:0]
				flushDeadline = time.Time{}
			}
		}
	}
}

func decodeMessageSpans(value []byte) ([]model.ClickHouseSpan, error) {
	if len(value) > 0 && value[0] == '[' {
		var spans []model.EnrichedSpan
		if err := json.Unmarshal(value, &spans); err != nil {
			return nil, fmt.Errorf("unmarshal enriched JSON payload: %w", err)
		}
		return processor.ProcessEnriched(spans), nil
	}

	var req colpb.ExportTraceServiceRequest
	if err := proto.Unmarshal(value, &req); err != nil {
		return nil, fmt.Errorf("unmarshal protobuf payload: %w", err)
	}
	return processor.ProcessOTLP(&req), nil
}

func (ing *Ingestor) flushAndCommit(ctx context.Context, batch []model.ClickHouseSpan, messages []kafka.Message) bool {
	if len(batch) == 0 {
		if len(messages) > 0 {
			ing.commitMessages(ctx, messages...)
		}
		return true
	}

	for attempt := 1; ; attempt++ {
		writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := ing.clickhouse.WriteBatch(writeCtx, batch)
		cancel()
		if err == nil {
			log.Printf("[ingestor] flushed %d spans from %d kafka messages.", len(batch), len(messages))
			ing.commitMessages(ctx, messages...)
			return true
		}

		log.Printf("[ingestor/error] clickhouse batch write failed attempt=%d spans=%d: %v", attempt, len(batch), err)
		if ing.writeRetries > 0 && attempt > ing.writeRetries {
			log.Printf("[ingestor/error] giving up after %d attempts; kafka offsets remain uncommitted", attempt)
			return false
		}
		if !ing.sleep(ctx, backoffForAttempt(attempt)) {
			return false
		}
	}
}

func (ing *Ingestor) commitMessages(ctx context.Context, messages ...kafka.Message) {
	if len(messages) == 0 {
		return
	}
	for attempt := 1; attempt <= 3; attempt++ {
		commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := ing.kafkaReader.CommitMessages(commitCtx, messages...)
		cancel()
		if err == nil {
			return
		}
		log.Printf("[ingestor/error] commit kafka offsets attempt=%d: %v", attempt, err)
		if attempt < 3 && !ing.sleep(ctx, backoffForAttempt(attempt)) {
			return
		}
	}
	log.Printf("[ingestor/error] kafka offsets not committed after successful write; batch may be redelivered")
}

func (ing *Ingestor) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func backoffForAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := time.Duration(1<<min(attempt-1, 5)) * time.Second
	if backoff > 30*time.Second {
		return 30 * time.Second
	}
	return backoff
}
