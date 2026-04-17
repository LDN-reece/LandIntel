# Live Source Audit Runbook

This runbook defines the supported manual browse path for the Scotland live sourcing phase in Supabase.

It exists because the raw `landintel` tables are technically correct but not intended to be the first thing a human browses.

## Current rule

- `landintel.*` remains the raw, reconciled, and provenance-aware live source layer.
- `analytics.v_live_*` is the analyst-facing browse and audit layer.
- `landintel.v_site_traceability` is the deep lineage/debug surface.
- parcel-era `analytics.v_frontend_*` views are legacy parcel summaries, not live-source site audit views.

## Start here in Supabase

### 1. `analytics.v_live_source_coverage`

Use this first.

This answers:
- what source datasets are loaded for each authority
- how much planning, HLA, BGS, flood, LDP, or settlement data exists
- how much of that source data has already been linked to canonical sites
- whether there is still a large unlinked raw-data backlog
- the latest ingest state for each authority/source family

If you want to answer `what do we actually have for Glasgow City?`, this is the first view to open.

### 2. `analytics.v_live_site_summary`

Use this second.

This is the main site browse view.

This answers:
- what the site is
- which authority it belongs to
- why it exists in the live sourcing system
- what source families are attached
- whether it is still partial or already enriched
- whether it is traceable
- whether it is ready for review or commercial analysis

Important fields:
- `data_completeness_status`
- `traceability_status`
- `site_stage`
- `review_ready_flag`
- `commercial_ready_flag`
- `missing_core_inputs`
- `why_not_ready`

### 3. `analytics.v_live_site_sources`

Use this third.

This answers:
- what source systems are attached to a specific site
- how many source records are linked from each family
- what the key references are
- how the site was linked
- how confident the linkage is

This is the best view when a site exists but you want to understand `what exactly is attached to it?`.

### 4. `analytics.v_live_site_readiness`

Use this fourth.

This is the operational readiness view.

This answers:
- is the site ready for human review
- is it ready for commercial analysis
- what minimum readiness band it has reached
- what is still missing

Readiness bands:
- `not_ready`
- `review_ready`
- `commercial_ready`

## Deep lineage and debugging only

### 5. `landintel.v_site_traceability`

Use this only after the `analytics.v_live_*` views.

This is the deep lineage view for:
- source record ids
- alias references
- link methods
- evidence metadata
- ingest run ids

It is useful for debugging and audit trail work, but it is not the first browse surface for normal operational review.

## How to read the status fields

### `data_completeness_status`

- `raw_only`
  The canonical site exists, but there are no meaningful linked source families yet.
- `linked_partial`
  Some source linkage exists, but core planning/HLA/site context is still missing.
- `linked_core`
  The site has core planning or HLA context, area, surfaced reason, and evidence.
- `linked_enriched`
  The site has core context plus BGS enrichment and deeper evidence.

### `traceability_status`

- `clear`
  Linkage and evidence are in a good enough state for confident manual browsing.
- `review_needed`
  Linkage exists, but alias or confidence issues still need attention.
- `unresolved_links`
  The site is not yet well-linked enough to trust operationally.

### Why many sites may still not be commercially ready

The commercial readiness logic is intentionally conservative.

At this stage, many sites will still be `not_ready` or `review_ready` only because:
- flood/constraints coverage is still limited
- live LDP and settlement packs are not yet fully in place
- evidence depth is still growing
- some sites are still partially linked

This is expected in the MVP.

## Legacy views to avoid for live-source auditing

These remain useful for parcel operations, but they are not the live-source truth:
- `analytics.v_frontend_authority_summary`
- `analytics.v_frontend_authority_size_summary`
- `analytics.v_ros_parcels_summary_by_authority_size`
- `analytics.v_ingest_run_summary`

These are legacy parcel/operations summaries.
They should not be used to answer:
- what live source data we have
- what sites currently exist in the live source layer
- why a site exists
- whether a site is ready for review or commercial analysis

## Recommended manual browse sequence

Always use this order:

1. `analytics.v_live_source_coverage`
2. `analytics.v_live_site_summary`
3. `analytics.v_live_site_sources`
4. `analytics.v_live_site_readiness`
5. `landintel.v_site_traceability` only if you need deep lineage/debug detail
