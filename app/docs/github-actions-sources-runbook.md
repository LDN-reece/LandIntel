# GitHub Actions source phase runbook

This workflow is the next live source phase for the Scottish MVP.

It does **not** replace the lean parcel workflow.
It sits on top of the lean parcel base and populates the private `landintel` schema.
It is the only source-intelligence workflow now. There is no separate runtime variant.

## Workflow to use

GitHub Actions workflow name:

- `Run LandIntel Sources`

## What this workflow currently does

This first source phase is intentionally focused on the highest-value live inputs that can be wired cleanly now:

1. `Planning Applications: Official - Scotland`
2. `Housing Land Supply - Scotland`
3. `BGS OpenGeoscience API` enrichment
4. canonical site reconciliation across those records

The runner resolves Spatial Hub downloads from the published resource pages and WFS capabilities, rather than trusting brittle CKAN `typeName` hints directly.

This means the workflow now starts to build:

- `landintel.planning_application_records`
- `landintel.hla_site_records`
- `landintel.canonical_sites`
- `landintel.site_reference_aliases`
- `landintel.site_source_links`
- `landintel.evidence_references`
- `landintel.bgs_records`

## What this workflow does not do yet

Not in this first source phase:

- live LDP ingestion
- live settlement boundary ingestion
- live flood ingestion
- full score generation into `landintel.site_assessments`
- TOID to title selective enrichment

Those are the next source packs after this one.

## Required GitHub secrets

Already required:

- `SUPABASE_DB_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `BOUNDARY_AUTHKEY`

Present for later phases but not yet heavily used in this workflow:

- `OS_API_KEY`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Exact run order

### 1. Audit the source footprint first

Run:

- `command = audit-source-footprint`

This confirms the current counts in `landintel` before the new ingest phase.

### 2. Ingest national planning history

Run:

- `command = ingest-planning-history`

This loads the national planning applications polygons feed into `landintel.planning_application_records` for the configured target councils.

### 3. Ingest Housing Land Supply

Run:

- `command = ingest-hla`

This loads HLA records into `landintel.hla_site_records` for the configured target councils.

### 4. Build canonical sites and source links

Run:

- `command = reconcile-canonical-sites`

This creates or refreshes `landintel.canonical_sites`, alias rows, source links, and evidence rows.

### 5. Enrich reconciled sites with BGS evidence

Run:

- `command = ingest-bgs`

This adds first-pass BGS evidence into `landintel.bgs_records` for the reconciled canonical sites.

### 6. Audit the source footprint again

Run:

- `command = audit-source-footprint`

This lets you confirm that records, canonical sites, and evidence were actually created.

## One-shot refresh option

If you want to run the whole pack end to end, use:

- `command = full-refresh-core-sources`

That runs:

1. planning ingest
2. HLA ingest
3. reconciliation
4. BGS enrichment

## Recommended first live test sequence

Use this order the first time:

1. `audit-source-footprint`
2. `ingest-planning-history`
3. `ingest-hla`
4. `reconcile-canonical-sites`
5. `ingest-bgs`
6. `audit-source-footprint`

This makes it much easier to see which step fails or under-loads.

## How to browse the results in Supabase

The live source truth is now split clearly:

- `landintel.*` = raw, reconciled, and provenance-aware source layer
- `analytics.v_live_*` = analyst-facing browse and audit layer
- `landintel.v_site_traceability` = deep lineage/debug view only

### Start with these views

1. `analytics.v_live_source_coverage`
   Use this to see what data exists by authority and source family.
2. `analytics.v_live_site_summary`
   Use this to see what sites exist, why they exist, how complete they are, and whether they are review-ready.
3. `analytics.v_live_site_sources`
   Use this to see what planning/HLA/BGS/source records are attached to each site.
4. `analytics.v_live_site_readiness`
   Use this for the fastest readiness triage.
5. `landintel.v_site_traceability`
   Use this only for deep lineage and debugging.

### Views that are not the live source audit truth

These older parcel/operations views may still be useful, but they are not the current live-source site audit surface:

- `analytics.v_frontend_authority_summary`
- `analytics.v_frontend_authority_size_summary`
- `analytics.v_ros_parcels_summary_by_authority_size`
- `analytics.v_ingest_run_summary`

## Strategic meaning of this phase

This phase gets the MVP beyond parcel-only sourcing.

It starts to answer:

- what planning history exists
- whether a site is already in HLA
- whether prior ground investigation is visible in BGS
- how those references start rolling into one `canonical_site`
- whether the resulting site is partial, core, enriched, review-ready, or still blocked

That is the bridge from a lean parcel base to a real evidence-led sourcing engine.
