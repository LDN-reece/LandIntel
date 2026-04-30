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
3. LDP package registration and storage from Spatial Hub CKAN ZIP resources
4. settlement boundary registration and storage from the National Records of Scotland WFS
5. planning link publishing from existing Supabase planning records
6. future-context ingest for HLA, ELA, and VDL
7. canonical constraint ingest for SEPA flood, Coal Authority, HES, NatureScot, contaminated land, TPO, culverts, conservation areas, green belt, topography, OS Places, and OS Features
8. incremental reconciliation to `canonical_site_id` where the source family uses the planning/HLA queue
9. direct canonical publication for ELA and VDL
10. affected-site refresh and source proof audits

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
- `OS_PLACES_API_KEY` or `OS_PLACES_API` as fallback names for the OS Places API key

Used by later title/parcel source packs:

- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Controlled run order

Use this order. Do not keep rerunning HLA or planning unless those feeds actually changed.

1. `run-migrations`
2. `source-estate-maintenance`
3. `audit-title-number-control`
4. `discover-ldp-sources`
5. `ingest-ldp`
6. `discover-settlement-sources`
7. `ingest-settlement-boundaries`
8. `audit-source-expansion`
9. `ingest-ela`
10. `ingest-vdl`
11. `ingest-greenbelt`
12. `ingest-conservation-areas`
13. `ingest-tpo`
14. `ingest-culverts`
15. `ingest-contaminated-land`
16. `ingest-sepa-flood`
17. `ingest-coal-authority`
18. `ingest-hes-designations`
19. `ingest-naturescot`
20. `ingest-os-topography`
21. `ingest-os-places`
22. `ingest-os-features`
23. `refresh-affected-sites`
24. `audit-source-expansion`
25. `audit-source-footprint`
26. `audit-source-freshness`
27. `audit-source-estate`

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

Title number is the control layer. LDP is a Spatial Hub package source and stores direct ZIP resource features into `landintel.ldp_site_records`. It remains ranking-protected until commercial-use rights and the policy interpreter are validated. Settlement boundaries are now the NRS `NRS:SettlementBoundaries` WFS and store polygons in `landintel.settlement_boundary_records`; they remain ranking-protected until the canonical inside/outside/edge overlay is promoted.

The workflow must still discover, register, monitor, and report LDP and settlement sources. LDP becomes storage-live through `ingest-ldp`; settlement becomes storage-live through `ingest-settlement-boundaries`. Neither source is allowed to create DD conclusions until the relevant interpreter gates are passed.

Use:

- `audit-title-number-control`
- `discover-ldp-sources`
- `ingest-ldp`
- `discover-settlement-sources`
- `ingest-settlement-boundaries`

`ingest-ldp` and `ingest-settlement-boundaries` are storage-first. They prove data capture, not commercial ranking eligibility. That protects the ranking layer from false policy certainty while keeping the priority sources commercially first-class.

## Source proof rule

A source is not complete because an ingest command passes.

A source is `live_wired_proven` only when `analytics.v_phase_one_source_expansion_readiness` proves:

- non-zero raw or feature rows
- non-zero linked or measured rows
- non-zero evidence rows
- non-zero signal rows
- non-zero review-output rows
- non-zero site change events

LDP may show `core_policy_storage_proven_licence_gated`. That means the Spatial Hub package is stored but not allowed to influence ranking or DD conclusions yet. Settlement may show `core_policy_storage_proven_interpreter_gated`. That means NRS boundaries are stored but not allowed to influence ranking or DD conclusions until the canonical settlement-position overlay is promoted.

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
