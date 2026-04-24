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

type RunsStore interface {
	StartRun(ctx context.Context, sourceURL string) (string, error)
	CompleteRun(ctx context.Context, runID string, stats model.IngestStats, runErr error) error
}

type Orchestrator struct {
	source      Source
	transformer Transformer
	listings    ListingsStore
	runs        RunsStore
	sourceURL   string
}

func NewOrchestrator(source Source, transformer Transformer, listings ListingsStore, runs RunsStore, sourceURL string) *Orchestrator {
	return &Orchestrator{
		source:      source,
		transformer: transformer,
		listings:    listings,
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

	if err = o.listings.RefreshListingsTx(ctx, listings); err != nil {
		return fmt.Errorf("refresh listings: %w", err)
	}
	stats.RowsInserted = len(listings)

	return nil
}
