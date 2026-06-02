package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	colpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
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
}

type Ingestor struct {
	kafkaReader *kafka.Reader
	chURL       string
	chClient    *http.Client
	batchSize   int
	flushWindow time.Duration
	queue       chan ClickHouseSpan
}

func main() {
	var (
		kafkaBrokers  = flag.String("kafka-brokers", "127.0.0.1:9092", "Kafka bootstrap brokers comma-separated")
		kafkaTopic    = flag.String("kafka-topic", "kubetrace-spans", "Kafka topic to consume spans from")
		kafkaGroup    = flag.String("kafka-group", "kubetrace-ingestor-group", "Kafka consumer group ID")
		clickhouseURL = flag.String("clickhouse-url", "http://127.0.0.1:8123", "ClickHouse HTTP API URL")
		batchSize     = flag.Int("batch-size", 8192, "Bulk batch insert size to ClickHouse")
		flushWindowS  = flag.Int("flush-window-seconds", 2, "Flush interval window")
	)
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("=== KubeTrace Ingestor Microservice ===")
	log.Printf("Brokers: %s | Topic: %s | Group: %s", *kafkaBrokers, *kafkaTopic, *kafkaGroup)
	log.Printf("ClickHouse: %s", *clickhouseURL)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*kafkaBrokers},
		Topic:     *kafkaTopic,
		GroupID:   *kafkaGroup,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		MaxWait:   500 * time.Millisecond,
	})

	ing := &Ingestor{
		kafkaReader: r,
		chURL:       *clickhouseURL,
		batchSize:   *batchSize,
		flushWindow: time.Duration(*flushWindowS) * time.Second,
		queue:       make(chan ClickHouseSpan, *batchSize*2),
		chClient: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize ClickHouse Spans table if not exists
	if err := ing.initDatabaseSchema(); err != nil {
		log.Fatalf("database schema init failed: %v", err)
	}

	// 2. Start bulk flushing pipeline
	go ing.runBatcher(ctx)

	// 3. Start consuming from Kafka
	go ing.consumeLoop(ctx)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down ingestor...")
	cancel()
	_ = r.Close()
	log.Println("Ingestor stopped.")
}

func (ing *Ingestor) initDatabaseSchema() error {
	query := `
	CREATE DATABASE IF NOT EXISTS kubetrace;
	CREATE TABLE IF NOT EXISTS kubetrace.spans (
		timestamp DateTime64(6, 'UTC') CODEC(DoubleDelta, LZ4),
		trace_id String CODEC(ZSTD(1)),
		span_id String CODEC(ZSTD(1)),
		parent_span_id String CODEC(ZSTD(1)),
		service_name LowCardinality(String) CODEC(ZSTD(1)),
		operation_name LowCardinality(String) CODEC(ZSTD(1)),
		duration_ns Int64 CODEC(T64, LZ4),
		status_code LowCardinality(String) CODEC(ZSTD(1)),
		status_message String CODEC(ZSTD(1)),
		tags Map(String, String) CODEC(ZSTD(1))
	) ENGINE = ReplacingMergeTree()
	PARTITION BY toYYYYMMDD(timestamp)
	ORDER BY (service_name, operation_name, timestamp, trace_id);`

	req, err := http.NewRequest("POST", ing.chURL, bytes.NewBufferString(query))
	if err != nil {
		return err
	}
	resp, err := ing.chClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, string(body))
	}
	log.Println("ClickHouse table schema verified/created.")
	return nil
}

func (ing *Ingestor) consumeLoop(ctx context.Context) {
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

			var req colpb.ExportTraceServiceRequest
			if err := proto.Unmarshal(msg.Value, &req); err != nil {
				log.Printf("[ingestor/error] unmarshal protobuf payload: %v", err)
				continue
			}

			ing.processSpans(&req)
		}
	}
}

func (ing *Ingestor) processSpans(req *colpb.ExportTraceServiceRequest) {
	for _, rs := range req.ResourceSpans {
		serviceName := "unknown"
		attrs := make(map[string]string)

		for _, attr := range rs.Resource.GetAttributes() {
			attrs[attr.Key] = stringVal(attr.Value)
			if attr.Key == "service.name" {
				serviceName = stringVal(attr.Value)
			}
		}

		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				startTime := time.Unix(0, int64(sp.StartTimeUnixNano)).UTC()
				duration := int64(sp.EndTimeUnixNano - sp.StartTimeUnixNano)

				status := "UNSET"
				if sp.Status != nil {
					switch sp.Status.Code {
					case 1: // Ok
						status = "OK"
					case 2: // Error
						status = "ERROR"
					}
				}

				// Merge resource + span attributes
				tags := make(map[string]string, len(attrs)+len(sp.Attributes))
				for k, v := range attrs {
					tags[k] = v
				}
				for _, attr := range sp.Attributes {
					tags[attr.Key] = stringVal(attr.Value)
				}

				chSpan := ClickHouseSpan{
					Timestamp:     startTime.Format("2006-01-02 15:04:05.999999"),
					TraceID:       fmt.Sprintf("%x", sp.TraceId),
					SpanID:        fmt.Sprintf("%x", sp.SpanId),
					ParentSpanID:  fmt.Sprintf("%x", sp.ParentSpanId),
					ServiceName:   serviceName,
					OperationName: sp.Name,
					DurationNS:    duration,
					StatusCode:    status,
					StatusMessage: sp.Status.GetMessage(),
					Tags:          tags,
				}

				select {
				case ing.queue <- chSpan:
				default:
					// Backpressure: drop or block if queue is completely full
					log.Printf("[ingestor/warning] queue is full, dropping span: %s", chSpan.SpanID)
				}
			}
		}
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

func (ing *Ingestor) runBatcher(ctx context.Context) {
	batch := make([]ClickHouseSpan, 0, ing.batchSize)
	ticker := time.NewTicker(ing.flushWindow)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				ing.flush(batch)
			}
			return
		case span := <-ing.queue:
			batch = append(batch, span)
			if len(batch) >= ing.batchSize {
				ing.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				ing.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (ing *Ingestor) flush(batch []ClickHouseSpan) {
	var buf bytes.Buffer
	for _, span := range batch {
		data, err := json.Marshal(span)
		if err != nil {
			continue
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	url := fmt.Sprintf("%s/?query=%s", ing.chURL, "INSERT+INTO+kubetrace.spans+FORMAT+JSONEachRow")
	
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		log.Printf("[ingestor/error] init clickhouse write: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := ing.chClient.Do(req)
	if err != nil {
		log.Printf("[ingestor/error] clickhouse post: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[ingestor/error] clickhouse bulk insert failed %d: %s", resp.StatusCode, string(body))
	} else {
		log.Printf("[ingestor] flushed %d spans to ClickHouse.", len(batch))
	}
}
