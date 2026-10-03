package elasticsearch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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

type bulkItem struct {
	Index *struct {
		Status int             `json:"status"`
		Error  json.RawMessage `json:"error"`
	} `json:"index,omitempty"`
	Delete *struct {
		Status int             `json:"status"`
		Error  json.RawMessage `json:"error"`
	} `json:"delete,omitempty"`
}

type bulkResponse struct {
	Errors bool       `json:"errors"`
	Items  []bulkItem `json:"items"`
}

func (c *client) bulkProject(ctx context.Context, upserts []SearchDocument, deletes []string) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}
	for attempt := 1; attempt <= 3; attempt++ {
		failedUpserts, failedDeletes, err := c.bulkProjectOnce(ctx, upserts, deletes)
		if err != nil {
			if attempt == 3 {
				return err
			}
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
			continue
		}
		if len(failedUpserts) == 0 && len(failedDeletes) == 0 {
			return nil
		}
		upserts, deletes = failedUpserts, failedDeletes
	}
	return nil
}

func (c *client) bulkProjectOnce(ctx context.Context, upserts []SearchDocument, deletes []string) ([]SearchDocument, []string, error) {
	var body bytes.Buffer
	for _, doc := range upserts {
		meta, _ := json.Marshal(map[string]any{"index": map[string]string{"_index": c.index, "_id": doc.ID}})
		data, err := json.Marshal(doc)
		if err != nil {
			return nil, nil, err
		}
		body.Write(meta)
		body.WriteByte('\n')
		body.Write(data)
		body.WriteByte('\n')
	}
	for _, id := range deletes {
		meta, _ := json.Marshal(map[string]any{"delete": map[string]string{"_index": c.index, "_id": id}})
		body.Write(meta)
		body.WriteByte('\n')
	}
	data, status, err := c.doBulk(ctx, body.Bytes())
	if err != nil {
		return nil, nil, err
	}
	if status >= 300 {
		return nil, nil, fmt.Errorf("bulk projection status %d: %s", status, string(data))
	}
	var result bulkResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, nil, err
	}
	if !result.Errors {
		return nil, nil, nil
	}
	failedUpserts := make([]SearchDocument, 0)
	failedDeletes := make([]string, 0)
	for i, item := range result.Items {
		failed := (item.Index != nil && item.Index.Status >= 300) || (item.Delete != nil && item.Delete.Status >= 300 && item.Delete.Status != http.StatusNotFound)
		if !failed {
			continue
		}
		if i < len(upserts) {
			failedUpserts = append(failedUpserts, upserts[i])
		} else {
			deleteIndex := i - len(upserts)
			if deleteIndex < len(deletes) {
				failedDeletes = append(failedDeletes, deletes[deleteIndex])
			}
		}
	}
	return failedUpserts, failedDeletes, nil
}

func (c *client) doBulk(ctx context.Context, body []byte) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/_bulk?refresh=false", bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	return data, res.StatusCode, err
}

func (c *client) processedIndex() string { return c.index + "_processed_events" }

func (c *client) claimEvent(ctx context.Context, eventID string) (bool, error) {
	if strings.TrimSpace(eventID) == "" {
		return false, fmt.Errorf("event id is empty")
	}
	path := fmt.Sprintf("/%s/_doc/%s?op_type=create&refresh=false", url.PathEscape(c.processedIndex()), url.PathEscape(eventID))
	data, status, err := c.do(ctx, http.MethodPut, path, []byte(`{"processed":true}`))
	if err != nil {
		return false, err
	}
	if status == http.StatusConflict {
		return false, nil
	}
	if status >= 300 {
		return false, fmt.Errorf("claim event %s: status %d: %s", eventID, status, string(data))
	}
	return true, nil
}

func (c *client) releaseEvent(ctx context.Context, eventID string) error {
	data, status, err := c.do(ctx, http.MethodDelete, fmt.Sprintf("/%s/_doc/%s?refresh=false", url.PathEscape(c.processedIndex()), url.PathEscape(eventID)), nil)
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusNotFound {
		return fmt.Errorf("release event %s: status %d: %s", eventID, status, string(data))
	}
	return nil
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
	ctx, span := otel.Tracer("search-service/database").Start(ctx, "elasticsearch."+method, trace.WithAttributes(attribute.String("db.system", "elasticsearch"), attribute.String("db.operation", method), attribute.String("db.collection.name", c.index)))
	defer span.End()
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, 0, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, res.StatusCode, err
	}
	span.SetAttributes(attribute.Int("http.status_code", res.StatusCode))
	if res.StatusCode >= 300 {
		span.SetStatus(codes.Error, res.Status)
	}
	return data, res.StatusCode, nil
}

func (c *client) upsert(ctx context.Context, doc SearchDocument) error {
	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/%s/_doc/%s?refresh=false", c.index, url.PathEscape(doc.ID))
	data, status, err := c.do(ctx, http.MethodPut, path, payload)
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("index gig %s: status %d: %s", doc.ID, status, string(data))
	}
	return nil
}

func (c *client) updatePicture(ctx context.Context, gigID, picture string) error {
	payload, err := json.Marshal(map[string]any{
		"doc": map[string]any{
			"picture": picture,
		},
	})
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/%s/_update/%s?refresh=false", c.index, url.PathEscape(gigID))
	data, status, err := c.do(ctx, http.MethodPost, path, payload)
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("update gig picture %s: status %d: %s", gigID, status, string(data))
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

func (c *client) createProcessedIndex(ctx context.Context) error {
	data, status, err := c.do(ctx, http.MethodPut, "/"+url.PathEscape(c.processedIndex()), []byte(`{"mappings":{"properties":{"processed":{"type":"boolean"}}}`))
	if err != nil {
		return err
	}
	if status >= 300 && status != http.StatusBadRequest {
		return fmt.Errorf("create processed index: status %d: %s", status, string(data))
	}
	return nil
}

func (c *client) delete(ctx context.Context, gigID string) error {
	path := fmt.Sprintf("/%s/_doc/%s?refresh=false", c.index, url.PathEscape(gigID))
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
