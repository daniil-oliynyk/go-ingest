package store

import (
	"context"
	"fmt"
	"log"

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
	log.Printf("store:listings: refresh transaction started rows=%d", len(listings))
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		log.Printf("store:listings: begin transaction failed err=%v", err)
		return fmt.Errorf("begin transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			log.Printf("store:listings: rollback result err=%v", rollbackErr)
		}
	}()

	log.Println("store:listings: truncating Listings table")
	if _, err := tx.Exec(ctx, `TRUNCATE TABLE "Listings"`); err != nil {
		log.Printf("store:listings: truncate failed err=%v", err)
		return fmt.Errorf("truncate listings: %w", err)
	}
	log.Println("store:listings: truncate completed")

	for i, listing := range listings {
		if listing.Latitude == nil || listing.Longitude == nil {
			log.Printf("store:listings: listing missing coordinates index=%d listing_id=%s", i, listing.ID)
			return fmt.Errorf("listing %s missing coordinates", listing.ID)
		}

		log.Printf("store:listings: inserting listing index=%d listing_id=%s", i, listing.ID)
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
			log.Printf("store:listings: insert listing failed listing_id=%s err=%v", listing.ID, err)
			return fmt.Errorf("insert listing %s: %w", listing.ID, err)
		}
	}
	log.Printf("store:listings: all inserts completed rows=%d", len(listings))

	if err := tx.Commit(ctx); err != nil {
		log.Printf("store:listings: commit transaction failed err=%v", err)
		return fmt.Errorf("commit transaction: %w", err)
	}
	committed = true
	log.Printf("store:listings: refresh transaction committed rows=%d", len(listings))

	return nil
}
