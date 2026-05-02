package ingest

import (
	"context"
	"fmt"
	"log"
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
	GeocodeAddresses(ctx context.Context, queries []string) ([]model.GeocodeResult, error)
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
	log.Printf("ingest: starting run source_url=%s", o.sourceURL)

	runID, err := o.runs.StartRun(ctx, o.sourceURL)
	if err != nil {
		log.Printf("ingest: start run failed: %v", err)
		return fmt.Errorf("start run: %w", err)
	}
	log.Printf("ingest: run started run_id=%s", runID)

	stats := model.IngestStats{}
	defer func() {
		stats.DurationMS = time.Since(startedAt).Milliseconds()
		if err != nil {
			log.Printf("ingest: run finishing with error run_id=%s duration_ms=%d err=%v", runID, stats.DurationMS, err)
		}
		if completeErr := o.runs.CompleteRun(ctx, runID, stats, err); completeErr != nil {
			log.Printf("ingest: complete run update failed run_id=%s err=%v", runID, completeErr)
			if err != nil {
				err = fmt.Errorf("%w; complete run: %v", err, completeErr)
				return
			}
			err = fmt.Errorf("complete run: %w", completeErr)
			return
		}
		log.Printf("ingest: run completed run_id=%s rows_fetched=%d rows_inserted=%d duration_ms=%d", runID, stats.RowsFetched, stats.RowsInserted, stats.DurationMS)
	}()

	log.Printf("ingest: fetching source listings run_id=%s", runID)
	rawListings, err := o.source.FetchListings(ctx)
	if err != nil {
		log.Printf("ingest: fetch listings failed run_id=%s err=%v", runID, err)
		return fmt.Errorf("fetch listings: %w", err)
	}
	stats.RowsFetched = len(rawListings)
	log.Printf("ingest: fetched listings run_id=%s rows=%d", runID, stats.RowsFetched)

	log.Printf("ingest: transforming listings run_id=%s", runID)
	listings, err := o.transformer.MapListings(rawListings, runID)
	if err != nil {
		log.Printf("ingest: transform listings failed run_id=%s err=%v", runID, err)
		return fmt.Errorf("transform listings: %w", err)
	}
	log.Printf("ingest: transformed listings run_id=%s rows=%d", runID, len(listings))

	lookupKeys := make([]model.GeocodeLookupKey, 0, len(listings))
	for _, listing := range listings {
		lookupKeys = append(lookupKeys, model.GeocodeLookupKey{
			ListingID:  listing.ID,
			AddressKey: listing.AddressKey,
		})
	}

	log.Printf("ingest: checking geocode cache run_id=%s keys=%d", runID, len(lookupKeys))
	cacheResults, err := o.geocache.GetByListingAndAddressKeys(ctx, lookupKeys)
	if err != nil {
		log.Printf("ingest: lookup geocode cache failed run_id=%s err=%v", runID, err)
		return fmt.Errorf("lookup geocode cache: %w", err)
	}
	log.Printf("ingest: geocode cache results run_id=%s hits=%d", runID, len(cacheResults))

	newCacheRecords := make([]model.CachedGeocode, 0)
	cacheHits := 0
	cacheMisses := 0
	missingIndexes := make([]int, 0)
	missingQueries := make([]string, 0)
	for i := range listings {
		key := cacheLookupKey(listings[i].ID, listings[i].AddressKey)
		if cached, ok := cacheResults[key]; ok {
			cacheHits++
			lat := cached.Latitude
			lng := cached.Longitude
			listings[i].Latitude = &lat
			listings[i].Longitude = &lng
			continue
		}
		cacheMisses++
		missingIndexes = append(missingIndexes, i)
		missingQueries = append(missingQueries, listings[i].GeocodeQuery)
	}

	if len(missingQueries) > 0 {
		log.Printf("ingest: geocoding cache misses in batch run_id=%s misses=%d", runID, len(missingQueries))
		batchResults, geocodeErr := o.geocoder.GeocodeAddresses(ctx, missingQueries)
		if geocodeErr != nil {
			log.Printf("ingest: batch geocode failed run_id=%s err=%v", runID, geocodeErr)
			return fmt.Errorf("batch geocode: %w", geocodeErr)
		}

		if len(batchResults) != len(missingIndexes) {
			err := fmt.Errorf("batch geocode result count mismatch expected=%d got=%d", len(missingIndexes), len(batchResults))
			log.Printf("ingest: %v run_id=%s", err, runID)
			// return err

		}

		for idx, result := range batchResults {

			if idx >= len(missingIndexes) {
				break
			}
			if result == (model.GeocodeResult{}) {
				continue
			}

			listingIndex := missingIndexes[idx]
			lat := result.Latitude
			lng := result.Longitude
			listings[listingIndex].Latitude = &lat
			listings[listingIndex].Longitude = &lng

			newCacheRecords = append(newCacheRecords, model.CachedGeocode{
				ListingID:  listings[listingIndex].ID,
				AddressKey: listings[listingIndex].AddressKey,
				Latitude:   result.Latitude,
				Longitude:  result.Longitude,
				Provider:   result.Provider,
				Confidence: result.Confidence,
				GeocodedAt: time.Now().UTC(),
			})
		}

		log.Printf("ingest: geocode assignment complete run_id=%s cache_hits=%d cache_misses=%d new_cache_records=%d", runID, cacheHits, cacheMisses, len(newCacheRecords))

		log.Printf("ingest: upserting geocode cache run_id=%s records=%d", runID, len(newCacheRecords))
		if err := o.geocache.Upsert(ctx, newCacheRecords); err != nil {
			log.Printf("ingest: upsert geocode cache failed run_id=%s err=%v", runID, err)
			return fmt.Errorf("upsert geocode cache: %w", err)
		}
		log.Printf("ingest: geocode cache upsert complete run_id=%s records=%d", runID, len(newCacheRecords))

		log.Printf("ingest: refreshing listings table run_id=%s rows=%d", runID, len(listings))
		if err = o.listings.RefreshListingsTx(ctx, listings); err != nil {
			log.Printf("ingest: refresh listings failed run_id=%s err=%v", runID, err)
			return fmt.Errorf("refresh listings: %w", err)
		}
		stats.RowsInserted = len(listings)
		log.Printf("ingest: listings refresh complete run_id=%s rows_inserted=%d", runID, stats.RowsInserted)

	} else {
		log.Printf("ingest: missingQueries=%d no need to Upsert into cache or refresh Listigns", len(missingQueries))
	}

	return nil
}

func cacheLookupKey(listingID, addressKey string) string {
	return listingID + "|" + addressKey
}
