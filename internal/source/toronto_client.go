package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
	log.Printf("source: fetch listings started source_url=%s", c.SourceURL)
	if c.HTTPClient == nil {
		log.Println("source: fetch listings failed: http client is nil")
		return nil, fmt.Errorf("http client is required")
	}
	if c.SourceURL == "" {
		log.Println("source: fetch listings failed: source URL is empty")
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
		log.Printf("source: fetch attempt=%d/%d", attempt, retries)
		listings, err := c.fetchOnce(ctx)
		if err == nil {
			log.Printf("source: fetch listings succeeded attempt=%d rows=%d", attempt, len(listings))
			return listings, nil
		}

		lastErr = err
		log.Printf("source: fetch attempt failed attempt=%d err=%v", attempt, err)
		if attempt == retries {
			break
		}

		wait := time.Duration(attempt) * backoff
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			log.Printf("source: context canceled during retry wait attempt=%d err=%v", attempt, ctx.Err())
			return nil, fmt.Errorf("context canceled during retry wait: %w", ctx.Err())
		case <-timer.C:
			log.Printf("source: retry wait finished attempt=%d wait=%s", attempt, wait)
		}
	}

	log.Printf("source: fetch listings exhausted retries=%d err=%v", retries, lastErr)
	return nil, fmt.Errorf("fetch listings failed after %d attempt(s): %w", retries, lastErr)
}

func (c *TorontoClient) fetchOnce(ctx context.Context) ([]map[string]any, error) {
	log.Println("source: building request")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.SourceURL, nil)
	if err != nil {
		log.Printf("source: create request failed: %v", err)
		return nil, fmt.Errorf("create request: %w", err)
	}

	log.Println("source: executing request")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		log.Printf("source: execute request failed: %v", err)
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()
	log.Printf("source: request completed status=%d", resp.StatusCode)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		log.Printf("source: unexpected response status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("source: read response body failed: %v", err)
		return nil, fmt.Errorf("read response body: %w", err)
	}
	log.Printf("source: response body read bytes=%d", len(body))

	listings, err := decodeListingsPayload(body)
	if err != nil {
		log.Printf("source: decode payload failed: %v", err)
		return nil, fmt.Errorf("decode payload: %w", err)
	}

	if len(listings) == 0 {
		log.Println("source: decoded payload empty")
		return nil, fmt.Errorf("empty listings payload")
	}
	log.Printf("source: payload decoded rows=%d", len(listings))

	return listings, nil
}

func decodeListingsPayload(body []byte) ([]map[string]any, error) {
	var rows []map[string]any
	if err := json.Unmarshal(body, &rows); err == nil {
		log.Printf("source: decoded direct array rows=%d", len(rows))
		return rows, nil
	}

	type ckanTabularPayload struct {
		Fields  []ckanField `json:"fields"`
		Records [][]any     `json:"records"`
	}

	var directTabular ckanTabularPayload
	if err := json.Unmarshal(body, &directTabular); err == nil && len(directTabular.Records) > 0 {
		log.Printf("source: decoded direct tabular records=%d fields=%d", len(directTabular.Records), len(directTabular.Fields))
		converted, convErr := recordsToMaps(directTabular.Fields, directTabular.Records)
		if convErr != nil {
			log.Printf("source: convert direct tabular failed: %v", convErr)
			return nil, convErr
		}
		log.Printf("source: converted direct tabular rows=%d", len(converted))
		return converted, nil
	}

	var wrappedObject struct {
		Result struct {
			Records []map[string]any `json:"records"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &wrappedObject); err == nil && len(wrappedObject.Result.Records) > 0 {
		log.Printf("source: decoded wrapped object rows=%d", len(wrappedObject.Result.Records))
		return wrappedObject.Result.Records, nil
	}

	var wrappedTabular struct {
		Result ckanTabularPayload `json:"result"`
	}
	if err := json.Unmarshal(body, &wrappedTabular); err == nil && len(wrappedTabular.Result.Records) > 0 {
		log.Printf("source: decoded wrapped tabular records=%d fields=%d", len(wrappedTabular.Result.Records), len(wrappedTabular.Result.Fields))
		converted, convErr := recordsToMaps(wrappedTabular.Result.Fields, wrappedTabular.Result.Records)
		if convErr != nil {
			log.Printf("source: convert wrapped tabular failed: %v", convErr)
			return nil, convErr
		}
		log.Printf("source: converted wrapped tabular rows=%d", len(converted))
		return converted, nil
	}

	log.Println("source: unsupported payload shape")
	return nil, fmt.Errorf("unsupported payload shape")
}

func recordsToMaps(fields []ckanField, records [][]any) ([]map[string]any, error) {
	if len(records) == 0 {
		log.Println("source: recordsToMaps called with zero records")
		return nil, nil
	}
	if len(fields) == 0 {
		log.Println("source: recordsToMaps failed: missing fields metadata")
		return nil, fmt.Errorf("tabular payload missing fields metadata")
	}

	rows := make([]map[string]any, 0, len(records))
	for i, record := range records {
		if len(record) > len(fields) {
			log.Printf("source: recordsToMaps failed: record=%d values=%d fields=%d", i, len(record), len(fields))
			return nil, fmt.Errorf("record %d has more values than fields", i)
		}

		row := make(map[string]any, len(fields))
		for idx, field := range fields {
			if field.ID == "" {
				log.Printf("source: recordsToMaps failed: missing field id index=%d", idx)
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
	log.Printf("source: recordsToMaps converted rows=%d", len(rows))

	return rows, nil
}
