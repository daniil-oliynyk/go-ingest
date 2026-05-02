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
    address text NOT NULL,
    postal_code text NOT NULL,
    property_type text,
    ward_number text,
    ward_name text,
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

CREATE TABLE IF NOT EXISTS listing_geocodes (
    listing_id text NOT NULL,
    address_key text NOT NULL,
    latitude double precision NOT NULL CHECK (latitude >= -90 AND latitude <= 90),
    longitude double precision NOT NULL CHECK (longitude >= -180 AND longitude <= 180),
    geom geometry(Point, 4326) NOT NULL,
    provider text NOT NULL,
    confidence text,
    geocoded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (listing_id, address_key)
);

CREATE INDEX IF NOT EXISTS listing_geocodes_geom_gix
    ON listing_geocodes USING GIST (geom);
