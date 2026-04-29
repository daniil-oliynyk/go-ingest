package store

import (
	"context"
	"fmt"
	"log"

	"github.com/daniil-oliynyk/go-ingest/internal/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RunsRepo struct {
	Pool *pgxpool.Pool
}

func NewRunsRepo(pool *pgxpool.Pool) *RunsRepo {
	return &RunsRepo{Pool: pool}
}

func (r *RunsRepo) StartRun(ctx context.Context, sourceURL string) (string, error) {
	log.Printf("store:runs: start run insert started source_url=%s", sourceURL)
	var runID string
	err := r.Pool.QueryRow(ctx, `
		INSERT INTO "ingestion_runs" (
			id,
			source_url,
			status
		)
		VALUES (
			(md5(random()::text || clock_timestamp()::text))::uuid,
			$1,
			'running'
		)
		RETURNING id::text
	`, sourceURL).Scan(&runID)
	if err != nil {
		log.Printf("store:runs: start run insert failed err=%v", err)
		return "", fmt.Errorf("insert ingestion run: %w", err)
	}
	log.Printf("store:runs: start run insert completed run_id=%s", runID)

	return runID, nil
}

func (r *RunsRepo) CompleteRun(ctx context.Context, runID string, stats model.IngestStats, runErr error) error {
	log.Printf("store:runs: complete run update started run_id=%s rows_fetched=%d rows_inserted=%d duration_ms=%d", runID, stats.RowsFetched, stats.RowsInserted, stats.DurationMS)
	status := model.IngestionRunStatusSuccess
	errorMessage := ""
	if runErr != nil {
		status = model.IngestionRunStatusFailed
		errorMessage = runErr.Error()
		log.Printf("store:runs: run marked failed run_id=%s err=%v", runID, runErr)
	}

	_, err := r.Pool.Exec(ctx, `
		UPDATE "ingestion_runs"
		SET
			status = $2,
			finished_at = now(),
			rows_fetched = $3,
			rows_inserted = $4,
			duration_ms = $5,
			error_message = NULLIF($6, '')
		WHERE id = $1::uuid
	`, runID, status, stats.RowsFetched, stats.RowsInserted, stats.DurationMS, errorMessage)
	if err != nil {
		log.Printf("store:runs: complete run update failed run_id=%s err=%v", runID, err)
		return fmt.Errorf("update ingestion run %s: %w", runID, err)
	}
	log.Printf("store:runs: complete run update finished run_id=%s status=%s", runID, status)

	return nil
}
