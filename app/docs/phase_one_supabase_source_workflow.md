# LandIntel Phase One Supabase Source Workflow

This is the controlled GitHub Actions runbook for pushing the Phase One source estate into Supabase.

## Operating Rule

Do not manually patch the live database. Do not run, store, or load source work locally. All schema, source registration, discovery, ingestion, reconciliation, refresh, and audit work must run through the repository and GitHub Actions.

GitHub is the source of truth. Supabase is the orchestration and storage layer.

Phase One is not complete until live Supabase proves populated source rows, linked canonical-site rows, evidence rows, signals, review-output rows, and change events.

## Active and Retired Workflows

Active workflow:

- `Run LandIntel Sources`

Retired workflow:

- `Run LandIntel Lean (Retired)`

The retired lean workflow is intentionally inert. It loads no Supabase secrets, performs no Supabase writes, and exists only to stop accidental use of the old lean source path.

## Required GitHub Secrets

- `SUPABASE_DB_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `BOUNDARY_AUTHKEY`
- `IMPROVEMENT_SERVICE_AUTHKEY`
- `OS_API_KEY`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

Optional when Dtechtive is promoted:

- `DTECHTIVE_API_KEY`

## Workflow

Use GitHub Actions -> `Run LandIntel Sources`.

Run in this order:

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

## What Each Step Proves

- `run-migrations` applies the Supabase schema for source freshness, source estate registry, and analytics proof views.
- `source-estate-maintenance` registers all source families, registers topography/adopted-road/utility/Section 75 gap sources, probes live endpoints, discovers LDP records through Scottish SDI GeoNetwork, discovers settlement boundary records through Scottish SDI GeoNetwork, and writes source freshness states.
- `audit-source-estate` proves every source is either live-wired, explicitly deferred, discovery-only, static-registered, blocked, or unproven.
- `audit-source-freshness` proves the current freshness gate for ranking/review surfaces.
- `publish-planning-links` publishes existing live planning records into the canonical-site reconcile queue and refreshes affected sites without running the long national SpatialHub WFS pull.
- `ingest-planning-history` is a compatibility alias for `publish-planning-links` during burn-in. It must not run a full planning pull by default.
- `full-ingest-planning-history` is the only command that runs the full SpatialHub planning WFS pull and should be used only when a deliberate full reload is needed.
- `ingest-hla` loads Housing Land Supply/HLA records as a supporting future-context and stalled-site evidence layer, then queues and refreshes affected canonical sites.
- `process-reconcile-queue` resolves source records into canonical-site links.
- `refresh-affected-sites` recalculates affected site outputs.
- `ingest-bgs` loads BGS ground-context records against canonical sites.
- Final audits prove live Supabase state after data movement.

## HLA Positioning

HLA is not the engine. It is one evidence source inside the wider site sourcing automation engine.

Use HLA to:

- identify stalled or delayed future supply
- corroborate planning/future-context evidence
- detect likely control, builder activity, or availability risk
- explain why an opportunity may already be known or partially de-risked

Do not use HLA to:

- make HLA presence the sole source route unless HLA genuinely surfaced the site
- promote a site solely because it appears in HLA
- let HLA dominate parcel, planning movement, geometry, access, settlement, location, or constraint evidence

## LDP and Settlement Policy

LDP allocations and settlement boundaries are Phase One critical.

The discovery spine is Scottish SDI GeoNetwork:

- `https://www.spatialdata.gov.scot/geonetwork/srv/api/search/records/_search`

GeoNetwork discovery does not automatically promote a source into ranking. Each authority source must be validated before promotion because the estate mixes WFS, ArcGIS, ZIP, PDF, and authority-specific schemas.

Until promoted:

- ranking eligible: false
- review-output eligible: false
- status: explicitly deferred
- reason: authority adapter not validated

After promotion:

- reconcile geometry to `canonical_site_id`
- create `site_source_links`
- create `site_reference_aliases`
- create `evidence_references`
- generate planning/future-context or settlement-position signals
- write `site_change_events`
- refresh affected sites

## Topography

Topography is part of the source estate.

Primary baseline:

- OS Terrain 50 through OS Downloads API.

Higher-resolution override:

- Scottish Remote Sensing LiDAR where coverage exists.

All derived slope/terrain outputs must be labelled `indicative_only`. They may affect geometry/constraint warnings only. They must not create appraisal, pricing, spread, or viability conclusions.

## Blocked Legacy Paths

The active workflow must not expose:

- `full-reconcile-canonical-sites`
- `full-refresh-core-sources`

The retired lean workflow must not expose:

- `audit-operational-footprint`
- `cleanup-operational-footprint`
- `ingest-ros-cadastral-lean`
- `full-refresh-lean`

Contract tests enforce these boundaries.

## Completion Gate

A source may only be marked live-wired when Supabase proves:

- non-zero raw/source rows
- non-zero canonical links or constraint measurements
- non-zero evidence rows
- non-zero signals where applicable
- non-zero review-facing analytics rows
- non-zero change/resurfacing events where the dataset changed
- passing GitHub Actions run

If any of those are missing, the source remains registered, monitored, deferred, or blocked. It is not complete.
