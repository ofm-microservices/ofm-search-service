package elasticsearch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type client struct {
	baseURL    string
	index      string
	username   string
	password   string
	httpClient *http.Client
}

func newClient(cfg Config) (*client, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, ErrEmptyURL
	}
	if strings.TrimSpace(cfg.Index) == "" {
		return nil, ErrEmptyIndex
	}
	return &client{
		baseURL:    strings.TrimRight(cfg.URL, "/"),
		index:      cfg.Index,
		username:   strings.TrimSpace(cfg.Username),
		password:   strings.TrimSpace(cfg.Password),
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (c *client) do(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, res.StatusCode, err
	}
	return data, res.StatusCode, nil
}

func (c *client) upsert(ctx context.Context, doc SearchDocument) error {
	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/%s/_doc/%s?refresh=wait_for", c.index, url.PathEscape(doc.ID))
	data, status, err := c.do(ctx, http.MethodPut, path, payload)
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("index gig %s: status %d: %s", doc.ID, status, string(data))
	}
	return nil
}

func (c *client) exists(ctx context.Context) (bool, error) {
	_, status, err := c.do(ctx, http.MethodHead, "/"+url.PathEscape(c.index), nil)
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("check index %s: status %d", c.index, status)
	}
}

func (c *client) createIndex(ctx context.Context, body []byte) error {
	data, status, err := c.do(ctx, http.MethodPut, "/"+url.PathEscape(c.index), body)
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("create index %s: status %d: %s", c.index, status, string(data))
	}
	return nil
}

func (c *client) delete(ctx context.Context, gigID string) error {
	path := fmt.Sprintf("/%s/_doc/%s?refresh=wait_for", c.index, url.PathEscape(gigID))
	data, status, err := c.do(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusNotFound {
		return fmt.Errorf("delete gig %s: status %d: %s", gigID, status, string(data))
	}
	return nil
}

type searchHit struct {
	Source SearchDocument `json:"_source"`
	Sort   []any          `json:"sort"`
}

type searchResponse struct {
	Hits struct {
		Hits []searchHit `json:"hits"`
	} `json:"hits"`
}

func (c *client) search(ctx context.Context, body []byte) ([]searchHit, error) {
	data, status, err := c.do(ctx, http.MethodPost, fmt.Sprintf("/%s/_search", c.index), body)
	if err != nil {
		return nil, err
	}
	if status >= 300 {
		return nil, fmt.Errorf("search status %d: %s", status, string(data))
	}
	var res searchResponse
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	return res.Hits.Hits, nil
}

func encodeCursor(fields []any) string {
	data, _ := json.Marshal(fields)
	return base64.StdEncoding.EncodeToString(data)
}

func decodeCursor(cursor string) ([]any, error) {
	data, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}
	var fields []any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}
