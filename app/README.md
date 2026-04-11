# LandIntel Scotland Ingestion Worker

Production-oriented Stage 1 ingestion worker for Scottish land parcel data. The worker discovers metadata from SpatialData.gov.scot, fetches Registers of Scotland INSPIRE cadastral parcel downloads directly over HTTP, clips parcels to LandIntel's target council areas, classifies land objects by size, and writes both audit artifacts and processed records into Supabase.

## What This Stage Delivers

- Direct source discovery from SpatialData.gov.scot GeoNetwork and Spatial Hub metadata
- Direct RoS INSPIRE cadastral parcel downloads over HTTP
- Boundary ingestion for the 20 target Scottish authorities
- Geometry repair, standardisation, clipping, area calculation, and size bucketing
- Rerunnable upserts into Supabase Postgres with PostGIS
- Raw source artifact uploads into Supabase Storage for audit
- Railway-ready container entrypoint for manual or scheduled refreshes

## Data Understanding

### Primary legal parcel source

This worker uses the Registers of Scotland INSPIRE Cadastral Parcels dataset as the base legal parcel geometry layer for Scotland.

- Publisher: Registers of Scotland
- Dataset role: indicative ownership polygons at ground level
- Key identifier: `inspire id`
- Working CRS: `EPSG:27700`
- Update cadence assumption: quarterly
- Bulk access pattern: county-based download archives exposed by the RoS INSPIRE download service

This project intentionally does not use the ScotLIS API for bulk parcel acquisition. ScotLIS is reserved for later title-level enrichment once title numbers are known.

### Metadata discovery role

SpatialData.gov.scot is treated as a discovery and metadata registry layer.

- Use it to find dataset records, UUIDs, service endpoints, and download URLs
- Do not treat it as the parcel geometry source of record

### Boundary source role

The worker discovers Scottish local authority boundary metadata from Spatial Hub / SSDI and then requests boundary features over HTTP. If the live WFS endpoint is access-controlled, you can provide either:

- `BOUNDARY_AUTHKEY`
- `BOUNDARY_GEOJSON_URL`

The code still keeps the workflow fully server-side and does not require manual downloads.

## Architecture

### Runtime flow

1. `discover-sources`
   Search GeoNetwork for parcel and boundary metadata, enrich with record detail, and upsert `public.source_registry`.

2. `load-boundaries`
   Download authority boundaries, standardise names to the LandIntel canonical list, dissolve to one polygon per authority, simplify for previews, and upsert `public.authority_aoi`.

3. `ingest-ros-cadastral`
   Download RoS county archives, extract spatial files, normalise attributes and geometries, store raw and clean staging records, clip to target authorities, calculate area metrics, classify size buckets, and upsert production tables.

4. `full-refresh`
   Runs the full Stage 1 sequence end-to-end.

### Storage design

- Supabase Postgres with PostGIS is the system of record for structured spatial data
- Supabase Storage keeps raw downloaded files for audit and replay
- `staging` holds append-only raw and cleaned parcel loads
- `public` holds canonical operational tables
- `analytics` exposes monitoring views

## Repository Layout

```text
app/
  config/
  sql/
  src/
  .env.example
  README.md
  requirements.txt
Dockerfile
.dockerignore
```

## Supabase Schema

### Core schemas

- `public`
- `staging`
- `analytics`

### Core operational tables

- `public.authority_aoi`
- `public.source_registry`
- `public.ingest_runs`
- `staging.ros_cadastral_parcels_raw`
- `staging.ros_cadastral_parcels_clean`
- `public.ros_cadastral_parcels`
- `public.land_objects`

### Future scaffold tables

- `public.land_object_toid_enrichment`
- `public.land_object_title_matches`
- `public.land_object_address_links`

### Operational views

- `analytics.v_ingest_run_summary`
- `analytics.v_ros_parcels_summary_by_authority_size`
- `analytics.v_source_registry_latest`

## PostGIS In Supabase

This project assumes your Supabase database already supports PostGIS. The migration set still enables required extensions:

- `postgis`
- `pgcrypto`
- `pg_trgm`

Run the SQL files in order or use the built-in migration command from this worker.

## Environment

Copy `app/.env.example` to `app/.env` and set values for your environment.

Required:

- `SUPABASE_DB_URL`
- `SUPABASE_URL`

Strongly recommended:

- `SUPABASE_SERVICE_ROLE_KEY`

Optional but useful:

- `BOUNDARY_AUTHKEY`
- `BOUNDARY_GEOJSON_URL`
- `ROS_API_BASE_URL`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Install And Run Locally

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r app/requirements.txt
cp app/.env.example app/.env
```

Run migrations:

```bash
cd app
python -m src.main run-migrations
```

Discover sources:

```bash
cd app
python -m src.main discover-sources
```

Load boundaries:

```bash
cd app
python -m src.main load-boundaries
```

Ingest RoS parcels:

```bash
cd app
python -m src.main ingest-ros-cadastral
```

Full refresh:

```bash
cd app
python -m src.main full-refresh
```

## Railway Deployment

The root `Dockerfile` packages the worker for Railway. It copies the `app/` project into `/app` and starts `python -m src.entrypoint`. The root `railway.toml` marks the service as a worker-style process with an explicit start command and failure restart policy.

### Default process mode

By default the container runs one full refresh and exits:

```env
ENABLE_INTERNAL_SCHEDULER=false
```

This is the recommended Railway pattern when you trigger the worker via Railway cron or a scheduled deployment job.

### Optional persistent scheduler mode

If you want the same image to stay alive and schedule quarterly refreshes internally:

```env
ENABLE_INTERNAL_SCHEDULER=true
QUARTERLY_CRON=0 6 2 3,6,9,12 *
```

For Railway, external cron triggering is still the simpler production option.

## Idempotency And Upserts

The pipeline is designed to be rerunnable.

- `public.source_registry` upserts on `metadata_uuid`
- `public.authority_aoi` upserts on `authority_name`
- `public.ros_cadastral_parcels` upserts on `(ros_inspire_id, authority_name)`
- `public.land_objects` upserts on `(source_system, source_key, authority_name)`

Staging tables remain append-only per run so you can audit what was downloaded and cleaned for each execution.

## Failure Handling

- Every command creates an `ingest_runs` row
- County-level RoS failures are logged and do not stop other counties from processing
- Fatal stage failures are recorded in `ingest_runs.error_message`
- Structured logs go to stdout and to `LOG_FILE_PATH`

## Future Enrichment Fit

The schema and code are designed so later stages can plug in without reshaping the parcel core.

- TOID enrichment belongs in `public.land_object_toid_enrichment`
- Title-number matching belongs in `public.land_object_title_matches`
- ScotLIS title retrieval can use known title numbers from title matches
- Address linkage belongs in `public.land_object_address_links`
- Planning and constraint layers can add more `land_objects` sources or adjacent domain tables

## Concise Cloud Runbook

1. Create a Railway worker service from this repository.
2. Set the service root to the repository root so Railway uses the root `Dockerfile`.
3. Add environment variables from `app/.env.example`.
4. Ensure the Supabase database user can create extensions, schemas, tables, indexes, functions, and views.
5. Run an initial deploy with `ENABLE_INTERNAL_SCHEDULER=false`.
6. Trigger `python -m src.main full-refresh` once to seed the database.
7. Check `analytics.v_ingest_run_summary` and `analytics.v_ros_parcels_summary_by_authority_size`.
8. Add a quarterly Railway cron trigger for the service, or switch to internal scheduler mode if you prefer a persistent worker.

## Notes

- The RoS parcel ingestion intentionally downloads all published county archives, then clips to the 20 target authority AOIs in PostGIS-compatible British National Grid.
- Boundary delivery can vary by endpoint permissions; the worker supports a direct GeoJSON override so the pipeline can remain server-side even if the default WFS endpoint changes.
- Raw downloads are uploaded to Supabase Storage using idempotent object paths with upsert semantics.
