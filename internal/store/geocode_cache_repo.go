package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/daniil-oliynyk/go-ingest/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type GeocodeCacheRepo struct {
	Pool *pgxpool.Pool
}

func NewGeocodeCacheRepo(pool *pgxpool.Pool) *GeocodeCacheRepo {
	return &GeocodeCacheRepo{Pool: pool}
}

func (r *GeocodeCacheRepo) GetByListingAndAddressKeys(ctx context.Context, keys []model.GeocodeLookupKey) (map[string]model.CachedGeocode, error) {
	log.Printf("store:geocode_cache: lookup started keys=%d", len(keys))
	result := make(map[string]model.CachedGeocode, len(keys))
	if len(keys) == 0 {
		log.Println("store:geocode_cache: lookup skipped (no keys)")
		return result, nil
	}

	hits := 0
	misses := 0
	for _, key := range keys {
		if key.ListingID == "" || key.AddressKey == "" {
			log.Printf("store:geocode_cache: skipping invalid key listing_id=%q address_key=%q", key.ListingID, key.AddressKey)
			continue
		}

		// log.Printf("store:geocode_cache: looking up key listing_id=%s address_key=%s", key.ListingID, key.AddressKey)

		var cached model.CachedGeocode
		err := r.Pool.QueryRow(ctx, `
			SELECT listing_id, address_key, latitude, longitude, provider, confidence, geocoded_at
			FROM listing_geocodes
			WHERE listing_id = $1 AND address_key = $2
		`, key.ListingID, key.AddressKey).Scan(
			&cached.ListingID,
			&cached.AddressKey,
			&cached.Latitude,
			&cached.Longitude,
			&cached.Provider,
			&cached.Confidence,
			&cached.GeocodedAt,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				misses++
				continue
			}
			log.Printf("store:geocode_cache: lookup failed listing_id=%s address_key=%s err=%v", key.ListingID, key.AddressKey, err)
			return nil, fmt.Errorf("lookup geocode cache for %s/%s: %w", key.ListingID, key.AddressKey, err)
		}

		hits++
		result[cacheMapKey(key.ListingID, key.AddressKey)] = cached
	}
	log.Printf("store:geocode_cache: lookup completed hits=%d misses=%d", hits, misses)

	return result, nil
}

func (r *GeocodeCacheRepo) Upsert(ctx context.Context, records []model.CachedGeocode) error {
	log.Printf("store:geocode_cache: upsert started records=%d", len(records))
	if len(records) == 0 {
		log.Println("store:geocode_cache: upsert skipped (no records)")
		return nil
	}

	now := time.Now().UTC()
	batch := &pgx.Batch{}
	queued := 0
	for _, record := range records {
		if record.ListingID == "" || record.AddressKey == "" {
			log.Printf("store:geocode_cache: skipping invalid record listing_id=%q address_key=%q", record.ListingID, record.AddressKey)
			continue
		}
		if !validCoordinates(record.Latitude, record.Longitude) {
			log.Printf("store:geocode_cache: invalid coordinates listing_id=%s address_key=%s latitude=%f longitude=%f", record.ListingID, record.AddressKey, record.Latitude, record.Longitude)
			return fmt.Errorf("invalid geocode coordinates for %s/%s", record.ListingID, record.AddressKey)
		}

		geocodedAt := record.GeocodedAt
		if geocodedAt.IsZero() {
			geocodedAt = now
		}

		batch.Queue(`
			INSERT INTO listing_geocodes (
				listing_id,
				address_key,
				latitude,
				longitude,
				geom,
				provider,
				confidence,
				geocoded_at
			)
			VALUES (
				$1,
				$2,
				$3,
				$4,
				ST_SetSRID(ST_MakePoint($4, $3), 4326),
				$5,
				$6,
				$7
			)
			ON CONFLICT (listing_id, address_key) DO UPDATE SET
				latitude = EXCLUDED.latitude,
				longitude = EXCLUDED.longitude,
				geom = EXCLUDED.geom,
				provider = EXCLUDED.provider,
				confidence = EXCLUDED.confidence,
				geocoded_at = EXCLUDED.geocoded_at
		`,
			record.ListingID,
			record.AddressKey,
			record.Latitude,
			record.Longitude,
			record.Provider,
			record.Confidence,
			geocodedAt,
		)
		queued++
	}
	log.Printf("store:geocode_cache: batch queued statements=%d", queued)
	if queued == 0 {
		log.Println("store:geocode_cache: upsert skipped (no valid records after filtering)")
		return nil
	}

	results := r.Pool.SendBatch(ctx, batch)
	defer results.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			log.Printf("store:geocode_cache: batch exec failed index=%d err=%v", i, err)
			return fmt.Errorf("upsert geocode cache: %w", err)
		}
	}
	log.Printf("store:geocode_cache: upsert completed statements=%d", batch.Len())

	return nil
}

func cacheMapKey(listingID, addressKey string) string {
	return listingID + "|" + addressKey
}
