# Operational Data Architecture

This project treats Supabase as the operational system of record for structured, queryable outputs. It is not the default archive for every source file, raw dump, or replay artifact.

## Hard requirements

1. Supabase stores structured operational data and precomputed delivery surfaces.
2. Raw source files and bulk archives are excluded from Supabase by default.
3. Frontend-facing reads must come from lean analytics surfaces, not large operational tables.
4. Expensive spatial aggregation and summarisation must be refreshed during ingestion, not recomputed on request.
5. Staging persistence is opt-in and temporary. It exists for debugging, not permanent retention.
6. Storage and database growth must remain observable through usage views and dashboard checks.
7. The live source truth and the analyst-facing browse layer must stay clearly separated.

## Current defaults

- `AUDIT_ARTIFACT_BACKEND=none`
- `PERSIST_STAGING_ROWS=false`
- `STAGING_RETENTION_DAYS=14`

These defaults make the GitHub workflow avoid raw-file uploads to Supabase Storage and skip append-only raw staging writes unless someone explicitly opts in.

## Approved Supabase surfaces

### Live source truth

These are the current live sourcing surfaces and provenance tables:

- `public.authority_aoi`
- `public.source_registry`
- `public.ingest_runs`
- `public.ros_cadastral_parcels`
- `landintel.canonical_sites`
- `landintel.site_reference_aliases`
- `landintel.site_source_links`
- `landintel.evidence_references`
- `landintel.planning_application_records`
- `landintel.hla_site_records`
- `landintel.bgs_records`
- `landintel.v_source_ingest_summary`
- `landintel.v_site_traceability`

### Analyst-facing audit layer

These are the supported manual browse surfaces for the live sourcing phase:

- `analytics.v_live_ingest_audit`
- `analytics.v_live_source_coverage`
- `analytics.v_live_site_summary`
- `analytics.v_live_site_sources`
- `analytics.v_live_site_readiness`

### Legacy parcel-era operational summaries

These remain useful for parcel operations, but they are not the current live-source site audit truth:

- `analytics.v_frontend_authority_summary`
- `analytics.v_frontend_authority_size_summary`
- `analytics.v_ros_parcels_summary_by_authority_size`
- `analytics.v_ingest_run_summary`
- `analytics.v_relation_storage_usage`
- `analytics.v_storage_bucket_usage`

## Frontend rule

Frontend and API delivery layers should read from precomputed analytics views or other deliberately slim delivery tables. They should not query `staging.*`, should not stream raw parcel attributes unnecessarily, and should not run broad spatial aggregation live per request.

For the live source phase:
- Supabase manual browsing should start with `analytics.v_live_source_coverage` and `analytics.v_live_site_summary`
- frontend and operator reads should not start with raw `landintel` tables unless the task is lineage/debugging

## Operational rule

If a future feature needs raw archive retention, it should add an explicit cold-storage/archive layer and a documented retention policy. It should not silently reuse Supabase Storage as the default dumping ground.

## Current browse rule

Use this order when manually browsing live source data in Supabase:

1. `analytics.v_live_source_coverage`
2. `analytics.v_live_site_summary`
3. `analytics.v_live_site_sources`
4. `analytics.v_live_site_readiness`
5. `landintel.v_site_traceability` only for deep lineage/debugging
