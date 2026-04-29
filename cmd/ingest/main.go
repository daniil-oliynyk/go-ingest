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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DatabaseURL string        `env:"SUPABASE_URL"`
	MapboxToken string        `env:"MAPBOX_TOKEN"`
	SourceURL   string        `env:"SOURCE_URL" envDefault:"https://ckan0.cf.opendata.inter.prod-toronto.ca/datastore/dump/f4659cc1-8985-4e4a-a702-ae24352271e0?format=json"`
	Timeout     time.Duration `env:"INGEST_TIMEOUT" envDefault:"10m"`
}

func main() {
	log.Println("main: starting ingestion worker")

	cfg := Config{}
	err := env.Parse(&cfg)
	if err != nil {
		log.Printf("main: parse config failed: %v", err)
		os.Exit(1)
	}
	log.Printf("main: config parsed source_url=%s timeout=%s", cfg.SourceURL, cfg.Timeout)

	dbURL := cfg.DatabaseURL
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		log.Println("main: database URL is required (SUPABASE_URL or DATABASE_URL)")
		os.Exit(1)
	}
	log.Println("main: database URL loaded")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	log.Println("main: ingestion context created")

	log.Println("main: creating database pool config")
	poolCfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		log.Printf("main: parse database config failed: %v", err)
		os.Exit(1)
	}
	log.Println("main: database pool config created")

	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	log.Println("main: creating database pool")
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Printf("main: create database pool failed: %v", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Println("main: database pool created")

	log.Println("main: pinging database")
	if err := pool.Ping(ctx); err != nil {
		log.Printf("main: database ping failed: %v", err)
		os.Exit(1)
	}
	log.Println("main: database ping successful")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	log.Println("main: http client initialized timeout=30s")

	log.Println("main: wiring dependencies")
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
	log.Println("main: dependencies wired")

	log.Println("main: starting orchestration run")
	if err := orchestrator.Run(ctx); err != nil {
		log.Printf("main: ingestion run failed: %v", err)
		os.Exit(1)
	}

	log.Println("main: ingestion run completed successfully")

}
