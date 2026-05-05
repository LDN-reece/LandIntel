# Site Title And Measurement Orchestration

## Purpose

This layer gives LandIntel a single operating spine for sourcing DD:

- every canonical site can be traced to its RoS parcel candidates, safe title-number candidates, title-order workflow and human title review state;
- every priority site can be checked against constraint scan-state, measured constraint rows and the next bounded measurement batch;
- every operator can see the next safe workflow step without creating duplicate truth tables.

It does not move data. It does not ingest data. It does not run broad spatial scans. It does not confirm ownership.

## Why this matters commercially

LDN cannot scale sourcing if a site is only “East Lothian, possible land.” The system needs to show what the site is, how it is traceable, what title evidence exists, what constraints have actually been measured, and what still needs to happen before money or time is spent.

The commercial value is speed with control: title/parcel traceability first, then measured DD facts, then review.

## Views

### `landintel_reporting.v_site_title_traceability_matrix`

One row per `landintel.canonical_sites` record.

It links:

- canonical site spine;
- `public.site_ros_parcel_link_candidates`;
- `public.ros_cadastral_parcels`;
- `public.site_title_resolution_candidates`;
- `landintel_reporting.v_title_candidates_operator_safe`;
- `landintel.title_order_workflow`;
- `landintel.title_review_records`;
- `landintel_reporting.v_title_control_status`.

Operator meaning:

- `needs_ros_parcel_linking` means run `link-sites-to-ros-parcels`;
- `parcel_linked_no_safe_title_candidate` means a parcel candidate exists but no safe title-number candidate exists;
- `parcel_linked_needs_licensed_title_bridge` means the RoS parcel route needs licensed/manual title bridge work;
- `safe_title_candidate_available` means a title-number-shaped candidate is available for manual check;
- `title_review_recorded` means the human title review layer has a record.

Ownership remains unconfirmed unless `landintel.title_review_records` supports the position. RoS parcel references are not title numbers. SCT-like cadastral references are not title numbers.

### `landintel_reporting.v_site_measurement_readiness_matrix`

One row per canonical site with DD measurement readiness.

It links:

- title traceability;
- `landintel_reporting.v_constraint_priority_sites`;
- `landintel_reporting.v_constraint_priority_layers`;
- `landintel_reporting.v_constraint_priority_measurement_queue`;
- `public.site_constraint_measurements`;
- `public.site_constraint_measurement_scan_state`;
- `public.site_commercial_friction_facts`;
- evidence and signal counts.

Operator meaning:

- `priority_constraint_measurement_backlog` means the site has unscanned priority site/layer pairs;
- `priority_constraints_partially_scanned` means some scan-state exists, but priority coverage is incomplete;
- `priority_constraints_scanned` means priority scan-state is currently complete for active priority layers;
- `blocked_no_site_geometry` means geometry must be fixed before DD measurement.

Constraint outputs remain measured facts. They are not RAG/pass/fail scoring, legal certainty, planning certainty or engineering certainty.

### `landintel_reporting.v_site_dd_orchestration_queue`

The mass-orchestration queue.

It produces the next step per site:

- `link_site_to_ros_parcel`;
- `resolve_title_candidate`;
- `measure_next_constraint_layer`;
- `manual_title_review_or_title_spend_decision`;
- `ready_for_operator_review`.

It recommends existing workflow commands:

- `site-title-traceability-proof`;
- `link-sites-to-ros-parcels`;
- `resolve-title-numbers`;
- `constraint-measurement-proof-title-spend-source-family`;
- `measure-constraints-duckdb`;
- `refresh-title-readiness`;
- `audit-site-dd-orchestration`.

This is a guidance surface only. It does not execute measurement.

### `landintel_reporting.v_site_dd_orchestration_summary`

Aggregated proof surface by:

- site priority band;
- title traceability status;
- measurement readiness status.

Use this after each bounded run to prove whether title traceability, scan-state or measurement coverage improved.

## En Masse Pattern

The scalable pattern is:

1. Run `audit-site-dd-orchestration`.
2. Read `v_site_dd_orchestration_queue`.
3. Run title/parcel linking first where needed using the bounded proof command:
   - `site-title-traceability-proof`;
   - `audit-site-parcel-links`;
   - `audit-title-number-control`.
4. Use `link-sites-to-ros-parcels` and `resolve-title-numbers` only for an intentional full rebuild window. Those commands are broader rebuild tools; the bounded proof command is the default operating pattern because it preserves existing rows and processes a small priority batch.
5. Run constraint measurement only through bounded source-family or layer batches:
   - `constraint-measurement-proof-title-spend-source-family`;
   - or `measure-constraints-duckdb` with source family / layer / authority inputs.
6. Run:
   - `audit-constraint-measurements`;
   - `audit-site-dd-orchestration`;
   - `audit-source-completion-matrix`.
7. Repeat priority batch by priority batch, source-family by source-family, not all-layer/all-site.

This is how LandIntel scales without drifting into unsafe “grab everything and analyse everything at once” behaviour.

## Bounded Title Traceability Proof

`site-title-traceability-proof` is the safe linking pattern.

It:

- selects a small priority batch, default `auto`;
- in `auto` mode it tries `title_spend_candidates`, then `review_queue`, then `ldn_candidate_screen`, then `prove_it_candidates`, then `wider_canonical_sites`;
- skips sites already holding non-rejected RoS parcel candidates;
- calls `public.refresh_site_ros_parcel_link_candidates_for_sites`;
- then calls `public.refresh_site_title_resolution_bridge_for_sites` for the same batch;
- reports before/after title traceability counts;
- preserves existing `site_ros_parcel_link_candidates`, `site_title_resolution_candidates` and `site_title_validation` rows.

It does not delete existing candidates. It does not claim ownership. It does not treat RoS parcel references or SCT-like cadastral references as title numbers. The output is traceability evidence so LDN can decide whether title spend or human title review is justified.

GitHub Actions controls:

- `SITE_TITLE_TRACEABILITY_PROOF_SITE_BATCH_SIZE=10`;
- `SITE_TITLE_TRACEABILITY_PROOF_PRIORITY_BAND=auto`;
- `SITE_TITLE_TRACEABILITY_PROOF_INCLUDE_TITLE_RESOLUTION=true`.

## Title Safety Rules

- `title_review_records=0` means ownership remains unconfirmed.
- RoS parcel references are not title numbers.
- SCT-like cadastral references are not title numbers.
- Rejected SCT-like values remain audit-only.
- Companies House, FCA and control signals are hypotheses until title review.
- Title candidates support review and spend decisions; they do not prove ownership.

## Measurement Safety Rules

- Existing constraint engine remains the source of truth.
- This PR does not create a second constraint measurement system.
- This PR does not run measurement.
- No broad all-site/all-layer scans.
- No RAG/pass/fail/scoring.
- Use layer-by-layer and source-family-by-source-family batches.
- Scan-state matters because no-hit scans are still evidence that the site/layer pair has been checked.

## Operational Priority

Order of work:

1. title spend candidates;
2. review queue;
3. LDN candidate screen;
4. Prove It candidates;
5. wider canonical estate.

Constraint order:

1. flood;
2. coal/mining;
3. green belt;
4. contaminated land;
5. culverts;
6. heritage/conservation;
7. ecology/NatureScot;
8. TPO/landscape.

## What This Does Not Do

- It does not upload Google Drive files.
- It does not ingest new source data.
- It does not run `measure-constraints-duckdb`.
- It does not confirm legal ownership.
- It does not create a physical sourced-sites table.
- It does not move or delete existing data.

## Next Operational Move

After merge and migration:

1. Run `audit-site-dd-orchestration`.
2. If many sites show `needs_ros_parcel_linking`, run `site-title-traceability-proof`.
3. Run `audit-site-parcel-links`.
4. Run `audit-title-number-control`.
5. Run one bounded constraint source-family batch from `v_site_dd_orchestration_queue`.
6. Re-run `audit-site-dd-orchestration` and compare counts.

## Performance Note

`audit-site-dd-orchestration` is intentionally bounded. It proves that the views exist and reports direct title/parcel, measurement and constraint-source-family counts from the source tables. It does not force full all-site summary counts through the orchestration views because that would recreate the broad-scan behaviour this layer is designed to prevent.
