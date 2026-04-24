package model

import "time"

type Listing struct {
	ID              string
	Address         string
	PostalCode      string
	AddressKey      string
	GeocodeQuery    string
	Latitude        *float64
	Longitude       *float64
	SourceUpdatedAt *time.Time
	IngestedAt      time.Time
	IngestionRunID  string
	RawPayload      []byte
}

type IngestionRunStatus string

const (
	IngestionRunStatusRunning IngestionRunStatus = "running"
	IngestionRunStatusSuccess IngestionRunStatus = "success"
	IngestionRunStatusFailed  IngestionRunStatus = "failed"
)

type IngestionRun struct {
	ID           string
	SourceURL    string
	Status       IngestionRunStatus
	StartedAt    time.Time
	FinishedAt   *time.Time
	RowsFetched  int
	RowsInserted int
	ErrorCode    string
	ErrorMessage string
	DurationMS   int64
}

type IngestStats struct {
	RowsFetched  int
	RowsInserted int
	DurationMS   int64
}

type GeocodeLookupKey struct {
	ListingID  string
	AddressKey string
}

type GeocodeResult struct {
	Latitude   float64
	Longitude  float64
	Provider   string
	Confidence *float64
}

type CachedGeocode struct {
	ListingID  string
	AddressKey string
	Latitude   float64
	Longitude  float64
	Provider   string
	Confidence *float64
	GeocodedAt time.Time
}
