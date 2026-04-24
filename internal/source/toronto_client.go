package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type TorontoClient struct {
	HTTPClient *http.Client
	SourceURL  string
	MaxRetries int
	Backoff    time.Duration
}

type ckanField struct {
	ID string `json:"id"`
}

func NewTorontoClient(httpClient *http.Client, sourceURL string) *TorontoClient {
	return &TorontoClient{
		HTTPClient: httpClient,
		SourceURL:  sourceURL,
		MaxRetries: 3,
		Backoff:    time.Second,
	}
}

func (c *TorontoClient) FetchListings(ctx context.Context) ([]map[string]any, error) {
	if c.HTTPClient == nil {
		return nil, fmt.Errorf("http client is required")
	}
	if c.SourceURL == "" {
		return nil, fmt.Errorf("source url is required")
	}

	retries := c.MaxRetries
	if retries < 1 {
		retries = 1
	}

	backoff := c.Backoff
	if backoff <= 0 {
		backoff = time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		listings, err := c.fetchOnce(ctx)
		if err == nil {
			return listings, nil
		}

		lastErr = err
		if attempt == retries {
			break
		}

		wait := time.Duration(attempt) * backoff
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("context canceled during retry wait: %w", ctx.Err())
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("fetch listings failed after %d attempt(s): %w", retries, lastErr)
}

func (c *TorontoClient) fetchOnce(ctx context.Context) ([]map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.SourceURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	listings, err := decodeListingsPayload(body)
	if err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}

	if len(listings) == 0 {
		return nil, fmt.Errorf("empty listings payload")
	}

	return listings, nil
}

func decodeListingsPayload(body []byte) ([]map[string]any, error) {
	var rows []map[string]any
	if err := json.Unmarshal(body, &rows); err == nil {
		return rows, nil
	}

	type ckanTabularPayload struct {
		Fields  []ckanField `json:"fields"`
		Records [][]any     `json:"records"`
	}

	var directTabular ckanTabularPayload
	if err := json.Unmarshal(body, &directTabular); err == nil && len(directTabular.Records) > 0 {
		converted, convErr := recordsToMaps(directTabular.Fields, directTabular.Records)
		if convErr != nil {
			return nil, convErr
		}
		return converted, nil
	}

	var wrappedObject struct {
		Result struct {
			Records []map[string]any `json:"records"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &wrappedObject); err == nil && len(wrappedObject.Result.Records) > 0 {
		return wrappedObject.Result.Records, nil
	}

	var wrappedTabular struct {
		Result ckanTabularPayload `json:"result"`
	}
	if err := json.Unmarshal(body, &wrappedTabular); err == nil && len(wrappedTabular.Result.Records) > 0 {
		converted, convErr := recordsToMaps(wrappedTabular.Result.Fields, wrappedTabular.Result.Records)
		if convErr != nil {
			return nil, convErr
		}
		return converted, nil
	}

	return nil, fmt.Errorf("unsupported payload shape")
}

func recordsToMaps(fields []ckanField, records [][]any) ([]map[string]any, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("tabular payload missing fields metadata")
	}

	rows := make([]map[string]any, 0, len(records))
	for i, record := range records {
		if len(record) > len(fields) {
			return nil, fmt.Errorf("record %d has more values than fields", i)
		}

		row := make(map[string]any, len(fields))
		for idx, field := range fields {
			if field.ID == "" {
				return nil, fmt.Errorf("field %d missing id", idx)
			}
			if idx < len(record) {
				row[field.ID] = record[idx]
				continue
			}
			row[field.ID] = nil
		}

		rows = append(rows, row)
	}

	return rows, nil
}
