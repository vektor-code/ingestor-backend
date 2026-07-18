package ingestor

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Config struct {
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaGroup         string
	ClickHouseURL      string
	ClickHouseHotHours int
	ClickHouseTTLHours int
	BatchSize          int
	FlushWindow        time.Duration
	WriteRetries       int
}

func ParseConfig() Config {
	kafkaBrokers := flag.String("kafka-brokers", getEnv("KAFKA_BROKERS", "127.0.0.1:9092"), "Kafka bootstrap brokers comma-separated")
	kafkaTopic := flag.String("kafka-topic", getEnv("KAFKA_TOPIC", "kubetrace-spans"), "Kafka topic to consume spans from")
	kafkaGroup := flag.String("kafka-group", getEnv("KAFKA_GROUP", "kubetrace-ingestor-group"), "Kafka consumer group ID")
	clickhouseURL := flag.String("clickhouse-url", getEnv("CLICKHOUSE_URL", "http://127.0.0.1:8123"), "ClickHouse HTTP API URL")
	batchSize := flag.Int("batch-size", getEnvInt("BATCH_SIZE", 8192), "Bulk batch insert size to ClickHouse")
	flushWindowS := flag.Int("flush-window-seconds", getEnvInt("FLUSH_WINDOW_SECONDS", 2), "Flush interval window")
	writeRetries := flag.Int("write-retries", getEnvInt("WRITE_RETRIES", 0), "ClickHouse write retries per batch before giving up; 0 retries forever")
	flag.Parse()

	if *batchSize <= 0 {
		*batchSize = 8192
	}
	if *flushWindowS <= 0 {
		*flushWindowS = 2
	}
	if *writeRetries < 0 {
		*writeRetries = 0
	}

	return Config{
		KafkaBrokers:       parseBrokers(*kafkaBrokers),
		KafkaTopic:         *kafkaTopic,
		KafkaGroup:         *kafkaGroup,
		ClickHouseURL:      *clickhouseURL,
		ClickHouseHotHours: getEnvInt("CLICKHOUSE_HOT_HOURS", 48),
		ClickHouseTTLHours: getEnvInt("CLICKHOUSE_TTL_HOURS", 0),
		BatchSize:          *batchSize,
		FlushWindow:        time.Duration(*flushWindowS) * time.Second,
		WriteRetries:       *writeRetries,
	}
}

func (cfg Config) Log() {
	log.Println("=== KubeTrace Ingestor Microservice ===")
	log.Printf("Brokers: %s | Topic: %s | Group: %s", strings.Join(cfg.KafkaBrokers, ","), cfg.KafkaTopic, cfg.KafkaGroup)
	log.Printf("ClickHouse: %s", cfg.ClickHouseURL)
}

func parseBrokers(value string) []string {
	brokers := []string{}
	for _, broker := range strings.Split(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
		return n
	}
	return def
}
