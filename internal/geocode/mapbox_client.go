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
}

func NewMapboxClient(httpClient *http.Client, accessToken string) *MapboxClient {
	return &MapboxClient{
		HTTPClient:   httpClient,
		AccessToken:  accessToken,
		EndpointBase: "https://api.mapbox.com/search/geocode/v6/forward",
	}
}

func (c *MapboxClient) GeocodeAddress(ctx context.Context, query string) (model.GeocodeResult, error) {
	log.Printf("geocode: geocode request started query=%q", query)
	if c.HTTPClient == nil {
		log.Println("geocode: geocode request failed: http client is nil")
		return model.GeocodeResult{}, fmt.Errorf("http client is required")
	}
	if strings.TrimSpace(c.AccessToken) == "" {
		log.Println("geocode: geocode request failed: mapbox access token is empty")
		return model.GeocodeResult{}, fmt.Errorf("mapbox access token is required")
	}
	if strings.TrimSpace(query) == "" {
		log.Println("geocode: geocode request failed: query is empty")
		return model.GeocodeResult{}, fmt.Errorf("geocode query is required")
	}

	u, err := url.Parse(c.EndpointBase)
	if err != nil {
		log.Printf("geocode: parse endpoint failed endpoint=%s err=%v", c.EndpointBase, err)
		return model.GeocodeResult{}, fmt.Errorf("parse endpoint: %w", err)
	}

	params := u.Query()
	params.Set("q", query)
	params.Set("access_token", c.AccessToken)
	params.Set("limit", "1")
	params.Set("country", "CA")
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		log.Printf("geocode: create request failed err=%v", err)
		return model.GeocodeResult{}, fmt.Errorf("create request: %w", err)
	}

	log.Printf("geocode: executing mapbox request")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		sanitizedErr := sanitizeRequestError(err)
		log.Printf("geocode: execute request failed err=%v", sanitizedErr)
		return model.GeocodeResult{}, fmt.Errorf("execute request: %w", sanitizedErr)
	}
	defer resp.Body.Close()
	log.Printf("geocode: mapbox response status=%d", resp.StatusCode)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		sanitizedBody := sanitizeSensitiveText(strings.TrimSpace(string(body)))
		log.Printf("geocode: mapbox non-2xx status=%d body=%s", resp.StatusCode, sanitizedBody)
		return model.GeocodeResult{}, fmt.Errorf("mapbox status %d: %s", resp.StatusCode, sanitizedBody)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("geocode: read response body failed err=%v", err)
		return model.GeocodeResult{}, fmt.Errorf("read response body: %w", err)
	}
	log.Printf("geocode: mapbox response read bytes=%d", len(body))

	result, err := decodeMapboxResponse(body)
	if err != nil {
		log.Printf("geocode: decode mapbox response failed err=%v", err)
		return model.GeocodeResult{}, err
	}

	result.Provider = "mapbox"
	log.Printf("geocode: geocode request completed provider=%s latitude=%f longitude=%f", result.Provider, result.Latitude, result.Longitude)
	return result, nil
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
