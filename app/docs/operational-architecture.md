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

### Live site spine for constraints and qualification

The live site spine for site qualification and the Constraints tab is:

- `public.sites`
- `public.site_locations`

Constraint and qualification logic should anchor on `public.site_locations.geometry`.

This is an extension of the repo architecture, not a second canonical site system.

### Constraints measurement architecture

These are the approved measurement-layer tables for the Constraints tab:

- `public.site_spatial_links`
- `public.site_title_validation`
- `public.constraint_layer_registry`
- `public.constraint_source_features`
- `public.site_constraint_measurements`
- `public.site_constraint_group_summaries`
- `public.site_commercial_friction_facts`

These are the approved analyst-facing Constraints tab surfaces:

- `analytics.v_constraints_tab_overview`
- `analytics.v_constraints_tab_measurements`
- `analytics.v_constraints_tab_group_summaries`
- `analytics.v_constraints_tab_commercial_friction`

This layer is measurement-first. It stores overlap, distance, grouped summaries, and commercially relevant friction facts only.

It does not add scoring, pass/fail logic, or RAG logic.

### Legacy parcel-era operational summaries

These remain useful for parcel operations, but they are not the current live-source site audit truth:

- `analytics.v_frontend_authority_summary`
- `analytics.v_frontend_authority_size_summary`
- `analytics.v_ros_parcels_summary_by_authority_size`
- `analytics.v_ingest_run_summary`
- `analytics.v_relation_storage_usage`
- `analytics.v_storage_bucket_usage`

### Legacy constraints path

- `public.site_constraints`

`public.site_constraints` is the legacy severity-style path.
It can remain for backward compatibility, but new Constraints tab and qualification logic should not be built on it.

## Frontend rule

Frontend and API delivery layers should read from precomputed analytics views or other deliberately slim delivery tables. They should not query `staging.*`, should not stream raw parcel attributes unnecessarily, and should not run broad spatial aggregation live per request.

For the live source phase:
- Supabase manual browsing should start with `analytics.v_live_source_coverage` and `analytics.v_live_site_summary`
- frontend and operator reads should not start with raw `landintel` tables unless the task is lineage/debugging

For the Constraints tab:
- Supabase manual browsing should start with `analytics.v_constraints_tab_overview`
- detailed investigation should then move to `analytics.v_constraints_tab_group_summaries`, `analytics.v_constraints_tab_measurements`, and `analytics.v_constraints_tab_commercial_friction`
- frontend and operator reads should not start from raw `public.constraint_source_features` or raw `public.site_constraints`

## Operational rule

If a future feature needs raw archive retention, it should add an explicit cold-storage/archive layer and a documented retention policy. It should not silently reuse Supabase Storage as the default dumping ground.

## Current browse rule

Use this order when manually browsing live source data in Supabase:

1. `analytics.v_live_source_coverage`
2. `analytics.v_live_site_summary`
3. `analytics.v_live_site_sources`
4. `analytics.v_live_site_readiness`
5. `landintel.v_site_traceability` only for deep lineage/debugging

Use this order when manually browsing constraints data in Supabase:

1. `analytics.v_constraints_tab_overview`
2. `analytics.v_constraints_tab_group_summaries`
3. `analytics.v_constraints_tab_measurements`
4. `analytics.v_constraints_tab_commercial_friction`
