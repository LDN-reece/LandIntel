# LandIntel Scotland Ingestion + Site Qualification MVP

This repository now contains two deliberately separated layers:

- Stage 1 ingestion for Scottish parcel and authority data
- A site qualification MVP that creates canonical internal sites, links separate datasets to them, derives traceable signals, applies explicit rules, and exposes lean review surfaces for senior human review

The detailed site architecture lives in [docs/site-qualification-mvp.md](docs/site-qualification-mvp.md). The Scottish scoring logic is documented in [docs/scottish-scoring-handbook.md](docs/scottish-scoring-handbook.md). The canonical-site bridge and source stack are documented in [docs/scottish-source-stack-and-reference-bridge.md](docs/scottish-source-stack-and-reference-bridge.md). Current strengths and remaining gaps are documented in [docs/scottish-mvp-gap-analysis.md](docs/scottish-mvp-gap-analysis.md). Future extension notes live in [docs/scottish-reasoning-future-notes.md](docs/scottish-reasoning-future-notes.md). The operational storage rules for raw versus structured data remain in [docs/operational-architecture.md](docs/operational-architecture.md). The manifest-first retention design and subscription guidance live in [docs/lean-storage-retention.md](docs/lean-storage-retention.md).

Production-oriented Stage 1 ingestion still discovers metadata from SpatialData.gov.scot, fetches Registers of Scotland INSPIRE cadastral parcel downloads directly over HTTP, clips parcels to LandIntel's target council areas, classifies land objects by size, and writes structured operational outputs into Supabase.

## What This Stage Delivers

- Direct source discovery from SpatialData.gov.scot GeoNetwork and Spatial Hub metadata
- Direct RoS INSPIRE cadastral parcel downloads over HTTP
- Boundary ingestion for the 20 target Scottish authorities
- Geometry repair, standardisation, clipping, area calculation, and size bucketing
- Rerunnable upserts into Supabase Postgres with PostGIS
- Precomputed analytics surfaces for lean frontend and API reads
- Usage visibility views for database and storage footprint tracking
- Railway-ready container entrypoint for manual or scheduled refreshes

## Hard Requirement

Supabase is the operational data layer, not the default raw archive layer.

- Do not store raw source files or bulk dumps in Supabase by default
- Do not persist bulk staging rows in Supabase by default
- Frontend and API delivery must read lean precomputed outputs, not raw operational tables
- Heavy aggregation must refresh during ingestion, not on every request
- Growth in storage and table size must stay visible

The detailed rules are documented in [docs/operational-architecture.md](docs/operational-architecture.md).

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
   Download RoS county archives, extract spatial files, normalise attributes and geometries, optionally persist debug staging rows, clip to target authorities, calculate area metrics, classify size buckets, upsert production tables, and refresh cached analytics outputs.

4. `full-refresh`
   Runs the full Stage 1 sequence end-to-end.

### Storage design

- Supabase Postgres with PostGIS is the system of record for structured operational data
- Supabase Storage is the heavy-file layer when audit retention is explicitly enabled
- `public.source_artifacts` keeps the full artifact audit trail even after a stored file expires
- `landintel-working` is for short-lived working artifacts
- `landintel-ingest-audit` is for longer-lived archive copies
- `staging` is a temporary debug layer and is disabled by default via `PERSIST_STAGING_ROWS=false`
- `public` holds canonical operational tables
- `analytics` exposes delivery views, incremental site search cache rows, and usage monitoring views

## Site Qualification MVP

The qualification layer is intentionally not a generic search portal and not a black-box score.

- `public.sites` is the canonical internal site object
- `public.site_reference_aliases`, `public.site_geometry_versions`, and `public.site_reconciliation_matches` preserve how external references and geometries reconcile back to that canonical site
- External datasets remain modular in their own tables and are linked to a site instead of merged into a single master record
- `public.site_geometry_components` shows how a site footprint is assembled from linked geometry components
- `src/site_engine/site_reference_reconciliation_engine.py` bridges site references across LDP, HLA, VDL, planning, and title-style identifiers before scoring happens
- `src/site_engine/source_normalisers/` plus the boundary, use, utility, and BGS engines build a Scotland-specific evidence object before scoring happens
- `public.site_signals` stores atomic facts derived from linked source rows
- `public.site_assessments` and `public.site_assessment_scores` store the seven core scores, hard-fail gates, bucket route, horizon, blocker, and explanation
- `public.site_interpretations` stores analyst-facing positives, risks, possible fatal issues, and unknowns generated only after signals and scores exist
- `public.evidence_references` plus link tables keep every surfaced conclusion traceable
- `public.site_refresh_queue` and `analytics.site_search_cache` make updates incremental per site instead of full-dataset recomputation

Key qualification commands:

```bash
cd app
python -m src.main seed-mvp-sites
python -m src.main refresh-site-qualifications
python -m src.main serve-review-ui --host 127.0.0.1 --port 8000
```

## GitHub Actions First

This repo is set up to validate and run on GitHub Actions rather than depending on a local machine.

- `LandIntel CI` now starts a real PostGIS service, runs the full SQL migration set, smoke-checks the CLI, and then runs the unit tests.
- `Run LandIntel` is the single manual operational workflow for real Supabase-backed runs.
- The manual workflow now supports:
  - `audit-operational-footprint`
  - `cleanup-operational-footprint`
  - `bootstrap-scottish-mvp`
  - `ingest-bgs-boreholes`
  - optional CLI arguments such as `--limit 6`
  - a reusable `source_file_url` field so hosted source files can be supplied to workflows without storing them in the repo
  - an optional `source_file_name` override so Google Drive links keep the correct filename and extension
  - an `audit_backend` choice so you can keep raw files out of Storage by default and only retain them when needed
  - targeted qualification refreshes such as `--site-code SC-A-001`

Recommended GitHub workflow usage:

- `bootstrap-scottish-mvp`
  Run migrations, seed the curated Scottish MVP sites, and refresh queued qualifications in one Action run.
- `audit-operational-footprint`
  Print the current live parcel and site footprint before or after a cleanup run.
- `cleanup-operational-footprint`
  Remove legacy sub-threshold parcels and the duplicate `land_objects` mirror from older parcel-led runs.
- `refresh-site-qualifications`
  Use this with `arguments` like `--site-code SC-C-003` when you want to rerun a specific site.
- `ingest-bgs-boreholes`
  Use this for the one-off January 26, 2026 BGS borehole load. Upload the original ZIP shapefile package to Google Drive or another direct-download file host, paste that share or download link into `source_file_url`, optionally set `source_file_name` to the original ZIP filename, and use `arguments` such as `--process-site-refresh-queue --site-refresh-limit 200` if you want GitHub to recalculate affected sites in the same run.

Reusable source intake pattern:

- GitHub Actions does not provide a direct file-upload box during a manual run
- The standard LandIntel operator flow is therefore:
  - upload the source file or ZIP to Google Drive or another direct-download host
  - paste that link into `source_file_url`
  - optionally set `source_file_name` when the shared link does not preserve the file extension cleanly
- When a source file is supplied, the workflow now exposes standard runtime paths for future import jobs:
  - `LANDINTEL_SOURCE_INPUT_PATH`
  - `LANDINTEL_SOURCE_INPUT_DIR`
  - `LANDINTEL_SOURCE_INPUT_EXTRACTED_DIR` for ZIP packages
  - `LANDINTEL_SOURCE_INPUT_FILENAME`
- New import commands should use those standard runtime paths instead of inventing a new upload pattern each time

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
- `public.source_artifacts`
- `public.ros_cadastral_parcels`
- `public.land_objects` when the optional mirror is enabled

### Optional debug tables

- `staging.ros_cadastral_parcels_raw`
- `staging.ros_cadastral_parcels_clean`

### Future scaffold tables

- `public.land_object_toid_enrichment`
- `public.land_object_title_matches`
- `public.land_object_address_links`

### Operational views

- `analytics.v_ingest_run_summary`
- `analytics.v_frontend_authority_summary`
- `analytics.v_frontend_authority_size_summary`
- `analytics.v_ros_parcels_summary_by_authority_size`
- `analytics.v_source_registry_latest`
- `analytics.v_relation_storage_usage`
- `analytics.v_storage_bucket_usage`
- `analytics.v_source_artifact_storage_summary`
- `analytics.v_source_artifact_expiry_queue`

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
- `SUPABASE_WORKING_BUCKET_NAME`
- `SUPABASE_ARCHIVE_BUCKET_NAME`
- `BGS_BOREHOLE_ARCHIVE_PATH`
- `PROCESS_SITE_REFRESH_QUEUE_AFTER_BGS`
- `BGS_SITE_REFRESH_LIMIT`

Cost-control defaults:

- `AUDIT_ARTIFACT_BACKEND=none`
- `PERSIST_STAGING_ROWS=false`
- `STAGING_RETENTION_DAYS=14`
- `ARTIFACT_WORKING_RETENTION_DAYS=30`
- `ARTIFACT_ARCHIVE_RETENTION_DAYS=365`
- `MIN_OPERATIONAL_AREA_ACRES=4`
- `MIRROR_LAND_OBJECTS=false`

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

Ingest the authoritative BGS borehole archive, merge it into the master borehole table, refresh borehole-derived `site_constraints`, and leave affected sites queued for recalculation:

```bash
cd app
python -m src.main ingest-bgs-boreholes --archive-path /absolute/path/to/single-onshore-borehole-index-dataset-26-01-26.zip
```

If you want the same run to immediately process the queued site refreshes:

```bash
cd app
python -m src.main ingest-bgs-boreholes --archive-path /absolute/path/to/single-onshore-borehole-index-dataset-26-01-26.zip --process-site-refresh-queue --site-refresh-limit 200
```

Full refresh:

```bash
cd app
python -m src.main full-refresh
```

Prune expired stored artifacts while keeping their manifest rows:

```bash
cd app
python -m src.main prune-audit-artifacts --limit 200
```

Audit the current live footprint:

```bash
cd app
python -m src.main audit-operational-footprint
```

Clean up a legacy parcel-heavy footprint:

```bash
cd app
python -m src.main cleanup-operational-footprint --min-area-acres 4
```

Seed or refresh canonical site scenarios for the qualification MVP:

```bash
cd app
python -m src.main seed-mvp-sites --limit 6
```

Process queued per-site recalculations:

```bash
cd app
python -m src.main refresh-site-qualifications --limit 20
```

Refresh specific sites only:

```bash
cd app
python -m src.main refresh-site-qualifications --site-code MVP-WL-004
```

Run the internal review UI:

```bash
cd app
python -m src.main serve-review-ui --host 127.0.0.1 --port 8000
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

To run the BGS borehole ingest as a one-off startup job in Railway:

```env
STARTUP_COMMAND=ingest-bgs-boreholes
BGS_BOREHOLE_ARCHIVE_PATH=/app/uploads/single-onshore-borehole-index-dataset-26-01-26.zip
PROCESS_SITE_REFRESH_QUEUE_AFTER_BGS=true
BGS_SITE_REFRESH_LIMIT=200
```

## GitHub Actions

If you want GitHub to run the worker for you instead of your local machine, use the manual workflow in [../.github/workflows/run-landintel.yml](../.github/workflows/run-landintel.yml).

Add these GitHub repository secrets before running it:

- `SUPABASE_DB_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Optional secrets:

- `BOUNDARY_AUTHKEY`
- `BOUNDARY_GEOJSON_URL`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

Then in GitHub:

1. Open **Actions**
2. Open **Run LandIntel**
3. Click **Run workflow**
4. Choose one command:
   - `audit-operational-footprint`
   - `cleanup-operational-footprint`
   - `bootstrap-scottish-mvp`
   - `ingest-bgs-boreholes`
   - `run-migrations`
   - `discover-sources`
   - `load-boundaries`
   - `ingest-ros-cadastral`
   - `full-refresh`
   - `seed-mvp-sites`
   - `refresh-site-qualifications`
5. If the run needs an external source file, upload that file or ZIP package to Google Drive or another direct-download file host and paste the file link into `source_file_url`.
6. If the shared link does not preserve the file name cleanly, enter the original filename in `source_file_name`.
7. If you choose `ingest-bgs-boreholes`, do not upload a CSV. Upload the original BGS ZIP shapefile package.
8. If needed, pass extra CLI flags in the `arguments` box, for example:
   - `--limit 6`
   - `--site-code SC-E-005`
   - `--min-area-acres 4`
   - `--process-site-refresh-queue --site-refresh-limit 200`

GitHub also runs automatic unit-test validation through `LandIntel CI` on pushes and pull requests that touch `app/**` or the relevant workflow files.

## Idempotency And Upserts

The pipeline is designed to be rerunnable.

- `public.source_registry` upserts on `metadata_uuid`
- `public.authority_aoi` upserts on `authority_name`
- `public.ros_cadastral_parcels` upserts on `(ros_inspire_id, authority_name)`
- `public.land_objects` upserts on `(source_system, source_key, authority_name)`

`staging` persistence is opt-in only. By default the worker skips raw and clean staging writes so Supabase does not become the long-term home for bulk debug data.

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
7. Check `analytics.v_ingest_run_summary`, `analytics.v_frontend_authority_summary`, and `analytics.v_relation_storage_usage`.
8. Add a quarterly Railway cron trigger for the service, or switch to internal scheduler mode if you prefer a persistent worker.

## Notes

- The RoS parcel ingestion intentionally downloads all published county archives, then clips to the 20 target authority AOIs in PostGIS-compatible British National Grid.
- Boundary delivery can vary by endpoint permissions; the worker supports a direct GeoJSON override so the pipeline can remain server-side even if the default WFS endpoint changes.
- Raw downloads are not uploaded into Supabase by default. If a future archive workflow needs object storage, it should be an explicit cold-storage decision with retention controls.
