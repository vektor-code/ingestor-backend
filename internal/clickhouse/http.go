package clickhouse

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	url        string
	httpClient *http.Client
}

func New(url string) *Client {
	return &Client{
		url: url,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *Client) exec(query string) error {
	req, err := http.NewRequest("POST", c.url, bytes.NewBufferString(query))
	if err != nil {
		return err
	}
	applyBasicAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse error %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *Client) coldVolumeExists() bool {
	q := "SELECT count() FROM system.storage_policies WHERE policy_name = 'tiered' AND volume_name = 'cold' FORMAT TabSeparated"
	req, err := http.NewRequest("POST", c.url, bytes.NewBufferString(q))
	if err != nil {
		return false
	}
	applyBasicAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	body, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body)) != "0" && strings.TrimSpace(string(body)) != ""
}

func applyBasicAuth(req *http.Request) {
	if req.URL.User == nil {
		return
	}
	pass, _ := req.URL.User.Password()
	req.SetBasicAuth(req.URL.User.Username(), pass)
}
