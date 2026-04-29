package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/daniil-oliynyk/go-ingest/internal/model"
)

type Source interface {
	FetchListings(ctx context.Context) ([]map[string]any, error)
}

type Transformer interface {
	MapListings(raw []map[string]any, runID string) ([]model.Listing, error)
}

type ListingsStore interface {
	RefreshListingsTx(ctx context.Context, listings []model.Listing) error
}

type GeocodeCacheStore interface {
	GetByListingAndAddressKeys(ctx context.Context, keys []model.GeocodeLookupKey) (map[string]model.CachedGeocode, error)
	Upsert(ctx context.Context, records []model.CachedGeocode) error
}

type Geocoder interface {
	GeocodeAddress(ctx context.Context, query string) (model.GeocodeResult, error)
}

type RunsStore interface {
	StartRun(ctx context.Context, sourceURL string) (string, error)
	CompleteRun(ctx context.Context, runID string, stats model.IngestStats, runErr error) error
}

type Orchestrator struct {
	source      Source
	transformer Transformer
	listings    ListingsStore
	geocache    GeocodeCacheStore
	geocoder    Geocoder
	runs        RunsStore
	sourceURL   string
}

func NewOrchestrator(source Source, transformer Transformer, listings ListingsStore, geocache GeocodeCacheStore, geocoder Geocoder, runs RunsStore, sourceURL string) *Orchestrator {
	return &Orchestrator{
		source:      source,
		transformer: transformer,
		listings:    listings,
		geocache:    geocache,
		geocoder:    geocoder,
		runs:        runs,
		sourceURL:   sourceURL,
	}
}

func (o *Orchestrator) Run(ctx context.Context) (err error) {
	startedAt := time.Now()

	runID, err := o.runs.StartRun(ctx, o.sourceURL)
	if err != nil {
		return fmt.Errorf("start run: %w", err)
	}

	stats := model.IngestStats{}
	defer func() {
		stats.DurationMS = time.Since(startedAt).Milliseconds()
		if completeErr := o.runs.CompleteRun(ctx, runID, stats, err); completeErr != nil {
			if err != nil {
				err = fmt.Errorf("%w; complete run: %v", err, completeErr)
				return
			}
			err = fmt.Errorf("complete run: %w", completeErr)
		}
	}()

	rawListings, err := o.source.FetchListings(ctx)
	if err != nil {
		return fmt.Errorf("fetch listings: %w", err)
	}
	stats.RowsFetched = len(rawListings)

	listings, err := o.transformer.MapListings(rawListings, runID)
	if err != nil {
		return fmt.Errorf("transform listings: %w", err)
	}

	lookupKeys := make([]model.GeocodeLookupKey, 0, len(listings))
	for _, listing := range listings {
		lookupKeys = append(lookupKeys, model.GeocodeLookupKey{
			ListingID:  listing.ID,
			AddressKey: listing.AddressKey,
		})
	}

	cacheResults, err := o.geocache.GetByListingAndAddressKeys(ctx, lookupKeys)
	if err != nil {
		return fmt.Errorf("lookup geocode cache: %w", err)
	}

	newCacheRecords := make([]model.CachedGeocode, 0)
	for i := range listings {
		key := cacheLookupKey(listings[i].ID, listings[i].AddressKey)
		if cached, ok := cacheResults[key]; ok {
			lat := cached.Latitude
			lng := cached.Longitude
			listings[i].Latitude = &lat
			listings[i].Longitude = &lng
			continue
		}

		geocodeResult, geocodeErr := o.geocoder.GeocodeAddress(ctx, listings[i].GeocodeQuery)
		if geocodeErr != nil {
			return fmt.Errorf("geocode listing %s: %w", listings[i].ID, geocodeErr)
		}

		lat := geocodeResult.Latitude
		lng := geocodeResult.Longitude
		listings[i].Latitude = &lat
		listings[i].Longitude = &lng

		newCacheRecords = append(newCacheRecords, model.CachedGeocode{
			ListingID:  listings[i].ID,
			AddressKey: listings[i].AddressKey,
			Latitude:   geocodeResult.Latitude,
			Longitude:  geocodeResult.Longitude,
			Provider:   geocodeResult.Provider,
			Confidence: geocodeResult.Confidence,
			GeocodedAt: time.Now().UTC(),
		})
	}

	if err := o.geocache.Upsert(ctx, newCacheRecords); err != nil {
		return fmt.Errorf("upsert geocode cache: %w", err)
	}

	if err = o.listings.RefreshListingsTx(ctx, listings); err != nil {
		return fmt.Errorf("refresh listings: %w", err)
	}
	stats.RowsInserted = len(listings)

	return nil
}

func cacheLookupKey(listingID, addressKey string) string {
	return listingID + "|" + addressKey
}
