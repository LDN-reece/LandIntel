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

HLA is useful supporting evidence, but it is not the sourcing spine and must not dominate opportunity generation. The engine should prioritise parcel truth, planning movement, settlement and policy context, geometry, location, access, constraints, and overlooked evidence. HLA should validate, enrich, explain stalled-site context, and sometimes surface opportunities, but a site must not be treated as strong solely because it is in HLA.

The same rule applies to every source: source data is only valuable when it is populated in Supabase, linked or measured against `canonical_site_id`, evidenced, reflected in signals or review outputs, and capable of creating a resurfacing event when it changes.

## What this workflow now does

The active workflow supports controlled Phase One source operations:

1. source estate registration and endpoint probing
2. LDP and settlement discovery through Scottish SDI GeoNetwork
3. planning link publishing from existing Supabase planning records
4. future-context ingest for HLA, ELA, and VDL
5. canonical constraint ingest for SEPA flood, Coal Authority, HES, NatureScot, contaminated land, TPO, culverts, conservation areas, green belt, topography, OS Places, and OS Features
6. incremental reconciliation to `canonical_site_id` where the source family uses the planning/HLA queue
7. direct canonical publication for ELA and VDL
8. affected-site refresh and source proof audits

The runner resolves Spatial Hub downloads from published resource pages and WFS capabilities, rather than trusting brittle CKAN `typeName` hints directly.

## Live target tables and views

The workflow builds and audits:

- `landintel.canonical_sites`
- `landintel.planning_application_records`
- `landintel.hla_site_records`
- `landintel.ela_site_records`
- `landintel.vdl_site_records`
- `landintel.site_reference_aliases`
- `landintel.site_source_links`
- `landintel.evidence_references`
- `landintel.site_signals`
- `landintel.site_change_events`
- `landintel.source_expansion_events`
- `public.constraint_source_features`
- `public.site_constraint_measurements`
- `public.site_constraint_group_summaries`
- `public.site_commercial_friction_facts`
- `analytics.v_live_source_coverage`
- `analytics.v_live_site_summary`
- `analytics.v_live_site_sources`
- `analytics.v_live_site_readiness`
- `analytics.v_phase_one_source_expansion_readiness`

## Required GitHub secrets

Required now:

- `SUPABASE_DB_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `BOUNDARY_AUTHKEY`
- `IMPROVEMENT_SERVICE_AUTHKEY`
- `OS_API_KEY`

Used by later title/parcel source packs:

- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Controlled run order

Use this order. Do not keep rerunning HLA or planning unless those feeds actually changed.

1. `run-migrations`
2. `source-estate-maintenance`
3. `audit-source-expansion`
4. `ingest-ela`
5. `ingest-vdl`
6. `ingest-greenbelt`
7. `ingest-conservation-areas`
8. `ingest-tpo`
9. `ingest-culverts`
10. `ingest-contaminated-land`
11. `ingest-sepa-flood`
12. `ingest-coal-authority`
13. `ingest-hes-designations`
14. `ingest-naturescot`
15. `ingest-os-topography`
16. `ingest-os-places`
17. `ingest-os-features`
18. `discover-ldp-sources`
19. `discover-settlement-sources`
20. `refresh-affected-sites`
21. `audit-source-expansion`
22. `audit-source-footprint`
23. `audit-source-freshness`
24. `audit-source-estate`

Run `publish-planning-links` only when Supabase planning records have changed and need publishing into canonical sites. Run `ingest-hla` only when HLA/HLS needs refreshing. HLA is a supporting source, not the default next step.

## Planning command rule

Use:

- `publish-planning-links`

This uses planning application records already populated in Supabase, queues planning reconciliation, processes the queue, and refreshes affected sites. It does not run the full national SpatialHub planning WFS pull.

`ingest-planning-history` is retained as a compatibility alias during burn-in and also skips the full WFS pull.

Only use `full-ingest-planning-history` when a deliberate full SpatialHub planning reload is required.

## LDP and settlement rule

LDP and settlement boundaries are Phase One critical, but they remain ranking-deferred until authority-specific adapters are validated.

The workflow must still discover, register, monitor, and report them. They become ranking-active only after an authority source is promoted from deferred to live with evidence that the adapter is reliable and that unvalidated feeds are not affecting review outputs.

Use:

- `discover-ldp-sources`
- `discover-settlement-sources`
- `promote-ldp-authority-source`
- `promote-settlement-authority-source`

Promotion commands currently record the explicit deferred state unless an authority adapter is available. That is correct behaviour and protects the ranking layer from false policy certainty.

## Source proof rule

A source is not complete because an ingest command passes.

A source is `live_wired_proven` only when `analytics.v_phase_one_source_expansion_readiness` proves:

- non-zero raw or feature rows
- non-zero linked or measured rows
- non-zero evidence rows
- non-zero signal rows
- non-zero review-output rows
- non-zero site change events

LDP and settlement may show `explicitly_deferred_until_authority_adapter_validated`. That is acceptable only while registry monitoring exists and they are proven absent from live ranking impact.

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
- `public.constraint_*` and `public.site_constraint_*` = canonical constraint measurement layer
- `analytics.v_live_*` = analyst-facing browse and audit layer
- `analytics.v_phase_one_source_expansion_readiness` = source-universe proof layer
- `landintel.v_site_traceability` = deep lineage/debug view only

Start with:

1. `analytics.v_phase_one_source_expansion_readiness`
2. `analytics.v_live_source_coverage`
3. `analytics.v_live_site_summary`
4. `analytics.v_live_site_sources`
5. `analytics.v_live_site_readiness`
6. `landintel.v_site_traceability`

Older parcel/operations views may still exist, but they are not the current live-source site audit surface.

## Completion discipline

Phase One is not complete because a workflow passes. A source is only complete when Supabase proves populated source rows, linked canonical-site rows or measurements, evidence rows, signals where applicable, review-facing output rows, and change/resurfacing events where the dataset changed.
