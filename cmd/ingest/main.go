package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/daniil-oliynyk/go-ingest/internal/geocode"
	"github.com/daniil-oliynyk/go-ingest/internal/ingest"
	"github.com/daniil-oliynyk/go-ingest/internal/source"
	"github.com/daniil-oliynyk/go-ingest/internal/store"
	"github.com/daniil-oliynyk/go-ingest/internal/transform"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DatabaseURL string        `env:"SUPABASE_URL"`
	MapboxToken string        `env:"MAPBOX_TOKEN"`
	SourceURL   string        `env:"SOURCE_URL" envDefault:"https://ckan0.cf.opendata.inter.prod-toronto.ca/datastore/dump/f4659cc1-8985-4e4a-a702-ae24352271e0?format=json"`
	Timeout     time.Duration `env:"INGEST_TIMEOUT" envDefault:"5m"`
}

func main() {

	cfg := Config{}
	err := env.Parse(&cfg)
	if err != nil {
		log.Println("Error parsing config:", err)
		os.Exit(1)
	}

	dbURL := cfg.DatabaseURL
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		log.Println("Database URL is required (SUPABASE_URL or DATABASE_URL)")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	httpClient := &http.Client{Timeout: 30 * time.Second}

	sourceClient := source.NewTorontoClient(httpClient, cfg.SourceURL)
	mapper := transform.NewListingMapper()
	listingsRepo := store.NewListingsRepo(pool)
	runsRepo := store.NewRunsRepo(pool)
	geocodeCacheRepo := store.NewGeocodeCacheRepo(pool)
	mapboxClient := geocode.NewMapboxClient(httpClient, cfg.MapboxToken)

	orchestrator := ingest.NewOrchestrator(
		sourceClient,
		mapper,
		listingsRepo,
		geocodeCacheRepo,
		mapboxClient,
		runsRepo,
		cfg.SourceURL,
	)

	if err := orchestrator.Run(ctx); err != nil {
		log.Println("Ingestion run failed:", err)
		os.Exit(1)
	}

	log.Println("Ingestion run completed successfully")

}
