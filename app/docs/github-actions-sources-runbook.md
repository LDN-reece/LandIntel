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

HLA is useful supporting evidence, but it is not the sourcing spine and must not dominate opportunity generation. The engine should prioritise control first: title number, LDP, then settlement logic. Parcel truth, planning movement, geometry, location, access, constraints, and overlooked evidence sit behind that commercial spine. HLA should validate, enrich, explain stalled-site context, and sometimes surface opportunities, but a site must not be treated as strong solely because it is in HLA.

The same rule applies to every source: source data is only valuable when it is populated in Supabase, linked or measured against `canonical_site_id`, evidenced, reflected in signals or review outputs, and capable of creating a resurfacing event when it changes.

## What this workflow now does

The active workflow supports controlled Phase One source operations:

1. source estate registration and endpoint probing
2. title number control audit through `public.site_title_validation`
3. LDP and settlement discovery through Scottish SDI GeoNetwork
4. planning link publishing from existing Supabase planning records
5. future-context ingest for HLA, ELA, and VDL
6. canonical constraint ingest for SEPA flood, Coal Authority, HES, NatureScot, contaminated land, TPO, culverts, conservation areas, green belt, topography, OS Places, and OS Features
7. incremental reconciliation to `canonical_site_id` where the source family uses the planning/HLA queue
8. direct canonical publication for ELA and VDL
9. affected-site refresh and source proof audits

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
- `analytics.v_phase_one_control_policy_priority`
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
3. `audit-title-number-control`
4. `discover-ldp-sources`
5. `discover-settlement-sources`
6. `audit-source-expansion`
7. `ingest-ela`
8. `ingest-vdl`
9. `ingest-greenbelt`
10. `ingest-conservation-areas`
11. `ingest-tpo`
12. `ingest-culverts`
13. `ingest-contaminated-land`
14. `ingest-sepa-flood`
15. `ingest-coal-authority`
16. `ingest-hes-designations`
17. `ingest-naturescot`
18. `ingest-os-topography`
19. `ingest-os-places`
20. `ingest-os-features`
21. `refresh-affected-sites`
22. `audit-source-expansion`
23. `audit-source-footprint`
24. `audit-source-freshness`
25. `audit-source-estate`

Run `publish-planning-links` only when Supabase planning records have changed and need publishing into canonical sites. Run `ingest-hla` only when HLA/HLS needs refreshing. HLA is a supporting source, not the default next step.

## Planning command rule

Use:

- `publish-planning-links`

This uses planning application records already populated in Supabase, queues planning reconciliation, processes the queue, and refreshes affected sites. It does not run the full national SpatialHub planning WFS pull.

`ingest-planning-history` is retained as a compatibility alias during burn-in and also skips the full WFS pull.

Only use `full-ingest-planning-history` when a deliberate full SpatialHub planning reload is required.

## Control and policy spine

The commercial priority spine is:

1. Title Number
2. LDP
3. Settlement

Title number is the control layer. LDP and settlement boundaries are Phase One critical policy sources, but they remain ranking-protected until authority-specific adapters are validated.

The workflow must still discover, register, monitor, and report LDP and settlement sources. They become ranking-active only after an authority source is promoted from pending adapter to live with evidence that the adapter is reliable and that unvalidated feeds are not affecting review outputs.

Use:

- `audit-title-number-control`
- `discover-ldp-sources`
- `discover-settlement-sources`
- `promote-ldp-authority-source`
- `promote-settlement-authority-source`

Promotion commands currently record the core pending-adapter state unless an authority adapter is available. That is correct behaviour and protects the ranking layer from false policy certainty while keeping the source commercially first-class.

## Source proof rule

A source is not complete because an ingest command passes.

A source is `live_wired_proven` only when `analytics.v_phase_one_source_expansion_readiness` proves:

- non-zero raw or feature rows
- non-zero linked or measured rows
- non-zero evidence rows
- non-zero signal rows
- non-zero review-output rows
- non-zero site change events

LDP and settlement may show `core_policy_pending_authority_adapter`. That is acceptable only while registry monitoring exists and they are proven absent from live ranking impact.

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
- `analytics.v_phase_one_control_policy_priority` = title number, LDP, and settlement priority spine
- `analytics.v_phase_one_source_expansion_readiness` = source-universe proof layer
- `landintel.v_site_traceability` = deep lineage/debug view only

Start with:

1. `analytics.v_phase_one_control_policy_priority`
2. `analytics.v_phase_one_source_expansion_readiness`
3. `analytics.v_live_source_coverage`
4. `analytics.v_live_site_summary`
5. `analytics.v_live_site_sources`
6. `analytics.v_live_site_readiness`
7. `landintel.v_site_traceability`

Older parcel/operations views may still exist, but they are not the current live-source site audit surface.

## Completion discipline

Phase One is not complete because a workflow passes. A source is only complete when Supabase proves populated source rows, linked canonical-site rows or measurements, evidence rows, signals where applicable, review-facing output rows, and change/resurfacing events where the dataset changed.
