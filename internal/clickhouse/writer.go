package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/kubetrace/ingestor-backend/internal/model"
)

func (c *Client) WriteBatch(ctx context.Context, batch []model.ClickHouseSpan) error {
	var buf bytes.Buffer
	for _, span := range batch {
		data, err := json.Marshal(span)
		if err != nil {
			continue
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	if buf.Len() == 0 {
		return nil
	}

	url := fmt.Sprintf("%s/?query=%s", c.url, "INSERT+INTO+kubetrace.spans+FORMAT+JSONEachRow")
	req, err := http.NewRequestWithContext(ctx, "POST", url, &buf)
	if err != nil {
		return fmt.Errorf("init clickhouse write: %w", err)
	}
	applyBasicAuth(req)
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("clickhouse post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse bulk insert failed %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
