package store

import (
	"context"
	"fmt"
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
	result := make(map[string]model.CachedGeocode, len(keys))
	if len(keys) == 0 {
		return result, nil
	}

	for _, key := range keys {
		if key.ListingID == "" || key.AddressKey == "" {
			continue
		}

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
				continue
			}
			return nil, fmt.Errorf("lookup geocode cache for %s/%s: %w", key.ListingID, key.AddressKey, err)
		}

		result[cacheMapKey(key.ListingID, key.AddressKey)] = cached
	}

	return result, nil
}

func (r *GeocodeCacheRepo) Upsert(ctx context.Context, records []model.CachedGeocode) error {
	if len(records) == 0 {
		return nil
	}

	now := time.Now().UTC()
	batch := &pgx.Batch{}
	for _, record := range records {
		if record.ListingID == "" || record.AddressKey == "" {
			continue
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
	}

	results := r.Pool.SendBatch(ctx, batch)
	defer results.Close()

	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("upsert geocode cache: %w", err)
		}
	}

	return nil
}

func cacheMapKey(listingID, addressKey string) string {
	return listingID + "|" + addressKey
}
