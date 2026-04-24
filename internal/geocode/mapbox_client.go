package geocode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	if c.HTTPClient == nil {
		return model.GeocodeResult{}, fmt.Errorf("http client is required")
	}
	if strings.TrimSpace(c.AccessToken) == "" {
		return model.GeocodeResult{}, fmt.Errorf("mapbox access token is required")
	}
	if strings.TrimSpace(query) == "" {
		return model.GeocodeResult{}, fmt.Errorf("geocode query is required")
	}

	u, err := url.Parse(c.EndpointBase)
	if err != nil {
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
		return model.GeocodeResult{}, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return model.GeocodeResult{}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return model.GeocodeResult{}, fmt.Errorf("mapbox status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return model.GeocodeResult{}, fmt.Errorf("read response body: %w", err)
	}

	result, err := decodeMapboxResponse(body)
	if err != nil {
		return model.GeocodeResult{}, err
	}

	result.Provider = "mapbox"
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
					Confidence *float64 `json:"confidence"`
				} `json:"match_code"`
			} `json:"properties"`
			Geometry struct {
				Coordinates []float64 `json:"coordinates"`
			} `json:"geometry"`
		} `json:"features"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		return model.GeocodeResult{}, fmt.Errorf("decode mapbox payload: %w", err)
	}
	if len(payload.Features) == 0 {
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
		return model.GeocodeResult{}, fmt.Errorf("missing coordinates in mapbox response")
	}

	return model.GeocodeResult{
		Latitude:   lat,
		Longitude:  lng,
		Confidence: feature.Properties.MatchCode.Confidence,
	}, nil
}
