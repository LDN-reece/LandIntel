# GitHub Actions Runbook

This repository now has three GitHub Actions workflows only.

## Workflows

### `LandIntel CI`

Runs automatically on pushes and pull requests to `main`.

Use it to confirm:

- the app installs cleanly
- the source runner still parses manifests and resource metadata correctly
- unit tests pass before manual ingestion runs

### `Run LandIntel Lean`

This is the lean parcel-and-boundary foundation workflow.

Use it for:

- `audit-operational-footprint`
- `cleanup-operational-footprint`
- `ingest-ros-cadastral-lean`
- `full-refresh-lean`

### `Run LandIntel Sources`

This is the source-intelligence workflow that populates the private `landintel` schema.

Use it for:

- `audit-source-footprint`
- `ingest-planning-history`
- `ingest-hla`
- `reconcile-canonical-sites`
- `ingest-bgs`
- `full-refresh-core-sources`

## Required GitHub secrets

- `SUPABASE_DB_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `BOUNDARY_AUTHKEY`

## Additional secrets already supported

- `BOUNDARY_GEOJSON_URL`
- `OS_API_KEY`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Recommended run order for MVP

### Foundation

1. `Run LandIntel Lean` -> `audit-operational-footprint`
2. `Run LandIntel Lean` -> `full-refresh-lean`
3. `Run LandIntel Lean` -> `audit-operational-footprint`

### Source intelligence

1. `Run LandIntel Sources` -> `audit-source-footprint`
2. `Run LandIntel Sources` -> `ingest-planning-history`
3. `Run LandIntel Sources` -> `ingest-hla`
4. `Run LandIntel Sources` -> `reconcile-canonical-sites`
5. `Run LandIntel Sources` -> `ingest-bgs`
6. `Run LandIntel Sources` -> `audit-source-footprint`

## Current design rule

GitHub Actions is the execution surface.
Supabase is the operational data store.
The app should not depend on Docker or manual local execution to run the MVP path.
