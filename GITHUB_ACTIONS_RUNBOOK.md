# GitHub Actions Runbook

LandIntel Phase One is repo-first and Supabase-backed.

Do not run, store, load, or patch source work locally. GitHub is the source of truth. Supabase is the orchestration and storage layer.

## Active workflows

### `LandIntel CI`

Runs automatically on pushes and pull requests to `main`.

Use it to confirm:

- the app installs cleanly
- workflow contracts still hold
- runner modules still compile
- unit tests pass before manual ingestion runs

### `Run LandIntel Sources`

This is the only active Phase One source orchestration workflow.

Use it for:

- `run-migrations`
- `source-estate-maintenance`
- `audit-source-estate`
- `audit-source-freshness`
- `publish-planning-links`
- `ingest-hla`
- `process-reconcile-queue`
- `refresh-affected-sites`
- `ingest-bgs`
- `audit-source-footprint`

## Retired workflow

### `Run LandIntel Lean (Retired)`

This workflow is retained only as an audit marker. It loads no Supabase secrets and performs no Supabase writes.

The old lean runner `app/src/lean_ops.py` is also retired and hard-fails if called.

## Required GitHub secrets

- `SUPABASE_DB_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `BOUNDARY_AUTHKEY`
- `IMPROVEMENT_SERVICE_AUTHKEY`
- `OS_API_KEY`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Recommended Phase One run order

1. `Run LandIntel Sources` -> `run-migrations`
2. `Run LandIntel Sources` -> `source-estate-maintenance`
3. `Run LandIntel Sources` -> `audit-source-estate`
4. `Run LandIntel Sources` -> `audit-source-freshness`
5. `Run LandIntel Sources` -> `publish-planning-links`
6. `Run LandIntel Sources` -> `ingest-hla`
7. `Run LandIntel Sources` -> `process-reconcile-queue`
8. `Run LandIntel Sources` -> `refresh-affected-sites`
9. `Run LandIntel Sources` -> `ingest-bgs`
10. `Run LandIntel Sources` -> `audit-source-footprint`
11. `Run LandIntel Sources` -> `audit-source-freshness`
12. `Run LandIntel Sources` -> `audit-source-estate`

## Current design rule

Phase One must run from the repo through GitHub Actions and prove itself in Supabase. Old full-refresh, lean parcel, and local execution paths are intentionally blocked.
