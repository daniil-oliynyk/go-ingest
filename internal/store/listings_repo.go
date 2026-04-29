package store

import (
	"context"
	"fmt"

	"github.com/daniil-oliynyk/go-ingest/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ListingsRepo struct {
	Pool *pgxpool.Pool
}

func NewListingsRepo(pool *pgxpool.Pool) *ListingsRepo {
	return &ListingsRepo{Pool: pool}
}

func (r *ListingsRepo) RefreshListingsTx(ctx context.Context, listings []model.Listing) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `TRUNCATE TABLE "Listings"`); err != nil {
		return fmt.Errorf("truncate listings: %w", err)
	}

	for _, listing := range listings {
		if listing.Latitude == nil || listing.Longitude == nil {
			return fmt.Errorf("listing %s missing coordinates", listing.ID)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO "Listings" (
				id,
				latitude,
				longitude,
				geom,
				source_updated_at,
				ingested_at,
				ingestion_run_id,
				raw_payload
			)
			VALUES (
				$1,
				$2,
				$3,
				ST_SetSRID(ST_MakePoint($3, $2), 4326),
				$4,
				$5,
				$6,
				$7
			)
		`,
			listing.ID,
			*listing.Latitude,
			*listing.Longitude,
			listing.SourceUpdatedAt,
			listing.IngestedAt,
			listing.IngestionRunID,
			listing.RawPayload,
		); err != nil {
			return fmt.Errorf("insert listing %s: %w", listing.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
