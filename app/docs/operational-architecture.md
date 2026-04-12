# Operational Data Architecture

This project treats Supabase as the operational system of record for structured, queryable outputs. It is not the default archive for every source file, raw dump, or replay artifact.

## Hard requirements

1. Supabase stores structured operational data and precomputed delivery surfaces.
2. Raw source files and bulk archives are excluded from Supabase by default.
3. Frontend-facing reads must come from lean analytics surfaces, not large operational tables.
4. Expensive spatial aggregation and summarisation must be refreshed during ingestion, not recomputed on request.
5. Staging persistence is opt-in and temporary. It exists for debugging, not permanent retention.
6. Storage and database growth must remain observable through usage views and dashboard checks.

## Current defaults

- `AUDIT_ARTIFACT_BACKEND=none`
- `PERSIST_STAGING_ROWS=false`
- `STAGING_RETENTION_DAYS=14`

These defaults make the GitHub workflow avoid raw-file uploads to Supabase Storage and skip append-only raw staging writes unless someone explicitly opts in.

## Approved Supabase surfaces

- `public.authority_aoi`
- `public.source_registry`
- `public.ingest_runs`
- `public.ros_cadastral_parcels`
- `public.land_objects`
- `analytics.v_frontend_authority_summary`
- `analytics.v_frontend_authority_size_summary`
- `analytics.v_ingest_run_summary`
- `analytics.v_relation_storage_usage`
- `analytics.v_storage_bucket_usage`

## Frontend rule

Frontend and API delivery layers should read from precomputed analytics views or other deliberately slim delivery tables. They should not query `staging.*`, should not stream raw parcel attributes unnecessarily, and should not run broad spatial aggregation live per request.

## Operational rule

If a future feature needs raw archive retention, it should add an explicit cold-storage/archive layer and a documented retention policy. It should not silently reuse Supabase Storage as the default dumping ground.
