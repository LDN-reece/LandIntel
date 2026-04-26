# GitHub Actions source phase runbook

This is the repo-first operating runbook for LandIntel Phase One source orchestration.

Phase One source work must run through GitHub Actions and Supabase only. Nothing should be run, stored, loaded, or patched locally.

## Workflow to use

Use one workflow only:

- `Run LandIntel Sources`

Retired workflow:

- `Run LandIntel Lean (Retired)`

The retired lean workflow is kept only as an audit marker. It loads no Supabase secrets, performs no Supabase writes, and must not be used for Phase One source work.

## Strategic position

LandIntel is not an HLA ingest tool.

HLA is a useful supporting source, but it is not the sourcing spine and must not dominate opportunity generation. The source engine should prioritise parcel truth, planning movement, settlement/policy context, geometry, location, access, constraints, and overlooked evidence. HLA should validate, enrich, explain stalled-site context, and sometimes surface opportunities, but a site should not be treated as strong solely because it is in HLA.

## What this workflow currently does

The active workflow supports controlled Phase One source operations:

1. source estate registration and endpoint probing
2. LDP and settlement discovery through Scottish SDI GeoNetwork
3. planning link publishing from existing Supabase planning records
4. Housing Land Supply ingest as a supporting evidence layer
5. BGS enrichment
6. incremental reconciliation to `canonical_site_id`
7. affected-site refresh
8. source, freshness, and footprint audits

The runner resolves Spatial Hub downloads from published resource pages and WFS capabilities, rather than trusting brittle CKAN `typeName` hints directly.

## Live target tables and views

The workflow builds and audits:

- `landintel.planning_application_records`
- `landintel.hla_site_records`
- `landintel.canonical_sites`
- `landintel.site_reference_aliases`
- `landintel.site_source_links`
- `landintel.evidence_references`
- `landintel.bgs_records`
- `analytics.v_live_source_coverage`
- `analytics.v_live_site_summary`
- `analytics.v_live_site_sources`
- `analytics.v_live_site_readiness`

## Required GitHub secrets

Required now:

- `SUPABASE_DB_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `BOUNDARY_AUTHKEY`
- `IMPROVEMENT_SERVICE_AUTHKEY`

Used by later promoted source packs:

- `OS_API_KEY`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Controlled run order

Use this order for the current Phase One source estate:

1. `run-migrations`
2. `source-estate-maintenance`
3. `audit-source-estate`
4. `audit-source-freshness`
5. `publish-planning-links`
6. `ingest-hla`
7. `process-reconcile-queue`
8. `refresh-affected-sites`
9. `ingest-bgs`
10. `audit-source-footprint`
11. `audit-source-freshness`
12. `audit-source-estate`

## Planning command rule

Use:

- `publish-planning-links`

This uses planning application records already populated in Supabase, queues planning reconciliation, processes the queue, and refreshes affected sites. It does not run the full national SpatialHub planning WFS pull.

`ingest-planning-history` is retained as a compatibility alias during burn-in and also skips the full WFS pull.

Only use `full-ingest-planning-history` when a deliberate full SpatialHub planning reload is required.

## Retired or blocked commands

Do not use these old command paths:

- `full-reconcile-canonical-sites`
- `full-refresh-core-sources`
- `full-refresh-lean`
- `ingest-ros-cadastral-lean`
- `audit-operational-footprint`
- `cleanup-operational-footprint`

`full-reconcile-canonical-sites` and `full-refresh-core-sources` are not exposed as dispatch options in the active workflow. Defensive branches remain only to fail direct/API-triggered attempts.

## How to browse results in Supabase

The live source truth is split clearly:

- `landintel.*` = raw, reconciled, and provenance-aware source layer
- `analytics.v_live_*` = analyst-facing browse and audit layer
- `landintel.v_site_traceability` = deep lineage/debug view only

Start with:

1. `analytics.v_live_source_coverage`
2. `analytics.v_live_site_summary`
3. `analytics.v_live_site_sources`
4. `analytics.v_live_site_readiness`
5. `landintel.v_site_traceability`

Older parcel/operations views may still exist, but they are not the current live-source site audit surface.

## Completion discipline

Phase One is not complete because a workflow passes. A source is only complete when Supabase proves populated source rows, linked canonical-site rows or measurements, evidence rows, signals where applicable, review-facing output rows, and change/resurfacing events where the dataset changed.
