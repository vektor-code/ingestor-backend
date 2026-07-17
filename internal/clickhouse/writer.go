package clickhouse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/kubetrace/ingestor-backend/internal/model"
)

func (c *Client) WriteBatch(batch []model.ClickHouseSpan) {
	var buf bytes.Buffer
	for _, span := range batch {
		data, err := json.Marshal(span)
		if err != nil {
			continue
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	url := fmt.Sprintf("%s/?query=%s", c.url, "INSERT+INTO+kubetrace.spans+FORMAT+JSONEachRow")
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		log.Printf("[ingestor/error] init clickhouse write: %v", err)
		return
	}
	applyBasicAuth(req)
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[ingestor/error] clickhouse post: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[ingestor/error] clickhouse bulk insert failed %d: %s", resp.StatusCode, string(body))
		return
	}
	log.Printf("[ingestor] flushed %d spans to ClickHouse.", len(batch))
}
