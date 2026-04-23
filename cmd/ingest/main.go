package main

import (
	"context"
	"log"
	"os"
	"time"

	// "net/http"

	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DatabaseURL string `env:"SUPABASE_URL"`
}

func main() {

	cfg := Config{}
	err := env.Parse(&cfg)
	if err != nil {
		log.Println("Error parsing config:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbURL := cfg.DatabaseURL
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("Connected to Supabase Postgres")

}
