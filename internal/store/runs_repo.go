package store

import (
	"context"
	"fmt"

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
		return "", fmt.Errorf("insert ingestion run: %w", err)
	}

	return runID, nil
}

func (r *RunsRepo) CompleteRun(ctx context.Context, runID string, stats model.IngestStats, runErr error) error {
	status := model.IngestionRunStatusSuccess
	errorMessage := ""
	if runErr != nil {
		status = model.IngestionRunStatusFailed
		errorMessage = runErr.Error()
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
		return fmt.Errorf("update ingestion run %s: %w", runID, err)
	}

	return nil
}
