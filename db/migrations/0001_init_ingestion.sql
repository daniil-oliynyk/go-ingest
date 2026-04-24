CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS "ingestion_runs" (
    id uuid PRIMARY KEY,
    source_url text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    rows_fetched integer,
    rows_inserted integer,
    error_code text,
    error_message text,
    duration_ms bigint
);

CREATE INDEX IF NOT EXISTS ingestion_runs_started_at_idx
    ON "ingestion_runs" (started_at DESC);

CREATE TABLE IF NOT EXISTS "Listings" (
    id text PRIMARY KEY,
    latitude double precision NOT NULL CHECK (latitude >= -90 AND latitude <= 90),
    longitude double precision NOT NULL CHECK (longitude >= -180 AND longitude <= 180),
    geom geometry(Point, 4326) NOT NULL,
    source_updated_at timestamptz,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    ingestion_run_id uuid NOT NULL REFERENCES "ingestion_runs"(id),
    raw_payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS listings_geom_gix
    ON "Listings" USING GIST (geom);

CREATE INDEX IF NOT EXISTS listings_ingestion_run_id_idx
    ON "Listings" (ingestion_run_id);
