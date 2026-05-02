# Toronto Airbnb Listings Ingestion Worker

Go worker that powers data ingestion for the API behind my short-term rental mapping app. It ingests Toronto short-term rental registration data, geocodes listing addresses with Mapbox, and refreshes a Postgres/PostGIS tables.

The app runs as a Railway cron job. Each run fetches the latest source data, maps it into typed listing records, reuses cached geocodes when possible, geocodes new or changed addresses, replaces `Listings` inside a transaction, and records run status in `ingestion_runs`.

## What It Does

- Fetches Toronto listing data from the City of Toronto CKAN export.
- Supports multiple CKAN JSON payload shapes.
- Normalizes listing IDs, addresses, postal codes, wards, and source timestamps.
- Builds Mapbox geocoding queries from address and postal code.
- Caches coordinates in `listing_geocodes` by `(listing_id, address_key)`.
- Refreshes the `Listings` table atomically with PostGIS point geometry.
- Tracks run lifecycle, row counts, errors, and duration in `ingestion_runs`.

## Project Layout

```text
cmd/ingest/                 Worker entrypoint and configuration
db/migrations/              Postgres/PostGIS schema
docs/railway-runbook.md     Railway deployment and scheduling notes
internal/geocode/           Mapbox geocoding client
internal/ingest/            Run orchestration
internal/model/             Shared domain types
internal/source/            Toronto CKAN source client
internal/store/             Postgres repositories
internal/transform/         Source row mapping and normalization
```

## Requirements

- Go `1.26.1` or compatible with the version in `go.mod`
- PostgreSQL with PostGIS enabled
- A Mapbox access token with geocoding enabled

## Database Setup

Apply the migration before running the worker:

```sh
psql "$DATABASE_URL" -f db/migrations/0001_init_ingestion.sql
```

The migration creates:

- `Listings`
- `listing_geocodes`
- `ingestion_runs`
- PostGIS geometry indexes for listing and geocode coordinates

## Configuration

Required environment variables:

| Variable | Description |
| --- | --- |
| `DATABASE_URL` | Postgres connection string. |
| `MAPBOX_TOKEN` | Mapbox token used for batch forward geocoding. |

Optional environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `SUPABASE_URL` | none | Fallback database URL when `DATABASE_URL` is not set. |
| `SOURCE_URL` | City of Toronto CKAN dump URL | Source JSON endpoint. |
| `INGEST_TIMEOUT` | `10m` | Total timeout for one ingestion run. |

## Run Locally

Set the required environment variables:

```sh
export DATABASE_URL='postgres://user:password@host:5432/dbname'
export MAPBOX_TOKEN='your-mapbox-token'
```

Run the worker:

```sh
go run ./cmd/ingest
```

On success, the process exits with code `0`. On failure, it logs the error, marks the run as failed when possible, and exits non-zero.

## Deployment

This repository includes a Railway runbook at `docs/railway-runbook.md`.

Typical Railway command:

```sh
go run ./cmd/ingest
```

Suggested cron schedules:

- Hourly: `0 * * * *`
- Every 6 hours: `0 */6 * * *`
- Daily at 2 AM UTC: `0 2 * * *`

## Validation

After a run, check:

```sql
SELECT status, rows_fetched, rows_inserted, duration_ms, error_message
FROM ingestion_runs
ORDER BY started_at DESC
LIMIT 5;

SELECT count(*) FROM "Listings";

SELECT count(*)
FROM "Listings"
WHERE geom IS NOT NULL;

SELECT count(*) FROM listing_geocodes;
```

`rows_inserted` should match the current number of rows in `Listings`, and every listing should have populated latitude, longitude, and `geom`.
