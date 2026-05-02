package geocode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/daniil-oliynyk/go-ingest/internal/model"
)

type MapboxClient struct {
	HTTPClient   *http.Client
	AccessToken  string
	EndpointBase string
	BatchSize    int
}

func NewMapboxClient(httpClient *http.Client, accessToken string) *MapboxClient {
	return &MapboxClient{
		HTTPClient:   httpClient,
		AccessToken:  accessToken,
		EndpointBase: "https://api.mapbox.com/search/geocode/v6",
		BatchSize:    500,
	}
}

func (c *MapboxClient) GeocodeAddress(ctx context.Context, query string) (model.GeocodeResult, error) {
	results, err := c.GeocodeAddresses(ctx, []string{query})
	if err != nil {
		return model.GeocodeResult{}, err
	}
	if len(results) != 1 {
		return model.GeocodeResult{}, fmt.Errorf("expected 1 geocode result, got %d", len(results))
	}

	return results[0], nil
}

func (c *MapboxClient) GeocodeAddresses(ctx context.Context, queries []string) ([]model.GeocodeResult, error) {
	log.Printf("geocode: batch request started queries=%d", len(queries))
	if c.HTTPClient == nil {
		log.Println("geocode: geocode request failed: http client is nil")
		return nil, fmt.Errorf("http client is required")
	}
	if strings.TrimSpace(c.AccessToken) == "" {
		log.Println("geocode: geocode request failed: mapbox access token is empty")
		return nil, fmt.Errorf("mapbox access token is required")
	}
	if len(queries) == 0 {
		log.Println("geocode: batch request failed: no queries")
		return nil, fmt.Errorf("at least one geocode query is required")
	}

	batchSize := c.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	results := make([]model.GeocodeResult, 0, len(queries))
	for i, query := range queries {
		if strings.TrimSpace(query) == "" {
			log.Printf("geocode: batch request failed: empty query index=%d", i)
			return nil, fmt.Errorf("geocode query is required at index %d", i)
		}
	}

	for start := 0; start < len(queries); start += batchSize {
		end := start + batchSize
		if end > len(queries) {
			end = len(queries)
		}

		log.Printf("geocode: executing batch window start=%d end=%d", start, end)
		chunkResults, err := c.geocodeBatchChunk(ctx, queries[start:end])
		if err != nil {
			return nil, fmt.Errorf("batch geocode chunk %d-%d: %w", start, end, err)
		}

		results = append(results, chunkResults...)
	}

	log.Printf("geocode: batch request completed queries=%d results=%d", len(queries), len(results))
	return results, nil
}

func (c *MapboxClient) geocodeBatchChunk(ctx context.Context, queries []string) ([]model.GeocodeResult, error) {
	batchURL := strings.TrimRight(c.EndpointBase, "/") + "/batch"
	u, err := url.Parse(batchURL)
	if err != nil {
		log.Printf("geocode: parse batch endpoint failed endpoint=%s err=%v", batchURL, err)
		return nil, fmt.Errorf("parse batch endpoint: %w", err)
	}

	params := u.Query()
	params.Set("access_token", c.AccessToken)
	u.RawQuery = params.Encode()

	type batchForwardQuery struct {
		Q       string   `json:"q"`
		Country string   `json:"country,omitempty"`
		Limit   int      `json:"limit,omitempty"`
		Types   []string `json:"types,omitempty"`
	}

	payload := make([]batchForwardQuery, 0, len(queries))
	for _, query := range queries {
		payload = append(payload, batchForwardQuery{
			Q:       query,
			Country: "CA",
			Limit:   1,
			Types:   []string{"address"},
		})
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("geocode: marshal batch payload failed err=%v", err)
		return nil, fmt.Errorf("marshal batch payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(string(bodyBytes)))
	if err != nil {
		log.Printf("geocode: create batch request failed err=%v", err)
		return nil, fmt.Errorf("create batch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	log.Printf("geocode: executing mapbox batch request")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		sanitizedErr := sanitizeRequestError(err)
		log.Printf("geocode: execute batch request failed err=%v", sanitizedErr)
		return nil, fmt.Errorf("execute batch request: %w", sanitizedErr)
	}
	defer resp.Body.Close()
	log.Printf("geocode: mapbox batch response status=%d", resp.StatusCode)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		sanitizedBody := sanitizeSensitiveText(strings.TrimSpace(string(body)))
		log.Printf("geocode: mapbox batch non-2xx status=%d body=%s", resp.StatusCode, sanitizedBody)
		return nil, fmt.Errorf("mapbox batch status %d: %s", resp.StatusCode, sanitizedBody)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("geocode: read batch response body failed err=%v", err)
		return nil, fmt.Errorf("read batch response body: %w", err)
	}
	log.Printf("geocode: mapbox batch response read bytes=%d", len(body))

	results, err := decodeMapboxBatchResponse(body, len(queries))
	if err != nil {
		log.Printf("geocode: decode mapbox batch response failed err=%v", err)
		return nil, err
	}

	for i := range results {
		results[i].Provider = "mapbox"
	}

	return results, nil
}

func decodeMapboxBatchResponse(body []byte, expected int) ([]model.GeocodeResult, error) {
	var payload struct {
		Batch []struct {
			Features []struct {
				Properties struct {
					Coordinates struct {
						Latitude  float64 `json:"latitude"`
						Longitude float64 `json:"longitude"`
					} `json:"coordinates"`
					MatchCode struct {
						Confidence string `json:"confidence"`
					} `json:"match_code"`
				} `json:"properties"`
				Geometry struct {
					Coordinates []float64 `json:"coordinates"`
				} `json:"geometry"`
			} `json:"features"`
		} `json:"batch"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		log.Printf("geocode: decode batch payload failed err=%v", err)
		return nil, fmt.Errorf("decode mapbox batch payload: %w", err)
	}

	if len(payload.Batch) != expected {
		return nil, fmt.Errorf("mapbox batch result size mismatch expected=%d got=%d", expected, len(payload.Batch))
	}

	results := make([]model.GeocodeResult, 0, len(payload.Batch))
	for i, item := range payload.Batch {
		if len(item.Features) == 0 {
			log.Printf("no geocoding result for batch index %d", i)
			continue

			// TODO: Create a list or something to track missing geocoded addresses to then do them 1 at a time instead of a batch.
			// For some reason an address cannot get geocoded when batched but a single request works
			// return nil, fmt.Errorf("no geocoding result for batch index %d", i)
		}

		feature := item.Features[0]
		lat := feature.Properties.Coordinates.Latitude
		lng := feature.Properties.Coordinates.Longitude

		if lat == 0 && lng == 0 && len(feature.Geometry.Coordinates) >= 2 {
			lng = feature.Geometry.Coordinates[0]
			lat = feature.Geometry.Coordinates[1]
		}

		if lat == 0 && lng == 0 {
			return nil, fmt.Errorf("missing coordinates in mapbox batch response index %d", i)
		}

		results = append(results, model.GeocodeResult{
			Latitude:   lat,
			Longitude:  lng,
			Confidence: feature.Properties.MatchCode.Confidence,
		})
	}

	return results, nil
}

func decodeMapboxResponse(body []byte) (model.GeocodeResult, error) {
	var payload struct {
		Features []struct {
			Properties struct {
				Coordinates struct {
					Latitude  float64 `json:"latitude"`
					Longitude float64 `json:"longitude"`
				} `json:"coordinates"`
				MatchCode struct {
					Confidence string `json:"confidence"`
				} `json:"match_code"`
			} `json:"properties"`
			Geometry struct {
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		log.Printf("geocode: decode payload failed err=%v", err)
		return model.GeocodeResult{}, fmt.Errorf("decode mapbox payload: %w", err)
	}
	if len(payload.Features) == 0 {
		log.Println("geocode: decode payload failed: no features in response")
		return model.GeocodeResult{}, fmt.Errorf("no geocoding result")
	}

	feature := payload.Features[0]
	lat := feature.Properties.Coordinates.Latitude
	lng := feature.Properties.Coordinates.Longitude

	if lat == 0 && lng == 0 && len(feature.Geometry.Coordinates) >= 2 {
		lng = feature.Geometry.Coordinates[0]
		lat = feature.Geometry.Coordinates[1]
	}

	if lat == 0 && lng == 0 {
		log.Println("geocode: decode payload failed: missing coordinates")
		return model.GeocodeResult{}, fmt.Errorf("missing coordinates in mapbox response")
	}
	log.Printf("geocode: decoded coordinates latitude=%f longitude=%f", lat, lng)

	return model.GeocodeResult{
		Latitude:   lat,
		Longitude:  lng,
		Confidence: feature.Properties.MatchCode.Confidence,
	}, nil
}

func sanitizeRequestError(err error) error {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}

	parsed, parseErr := url.Parse(urlErr.URL)
	if parseErr != nil {
		return &url.Error{Op: urlErr.Op, URL: "[redacted]", Err: urlErr.Err}
	}

	query := parsed.Query()
	if query.Has("access_token") {
		query.Set("access_token", "REDACTED")
		parsed.RawQuery = query.Encode()
	}

	return &url.Error{Op: urlErr.Op, URL: parsed.String(), Err: urlErr.Err}
}

func sanitizeSensitiveText(input string) string {
	if input == "" {
		return input
	}

	queryTokenPattern := regexp.MustCompile(`(?i)(access_token=)[^&\s]+`)
	jsonTokenPattern := regexp.MustCompile(`(?i)("access_token"\s*:\s*")[^"]+(")`)

	output := queryTokenPattern.ReplaceAllString(input, "${1}REDACTED")
	output = jsonTokenPattern.ReplaceAllString(output, `${1}REDACTED${2}`)

	return output
}
