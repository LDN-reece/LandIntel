# Bounded Constraint Measurement Execution Proof

## Purpose

This PR proves that the existing constraint measurement engine can safely increase measured coverage from the constraint scaler controls.

It does not create a new constraint engine. It does not create duplicate constraint truth tables. It does not ingest new sources. It does not run a broad all-site/all-layer scan.

Guardrail: no broad all-site/all-layer scan.

Guardrail: not a new constraint engine.

## Exact Cohort And Layer

Layer family: flood only.

Site cohort: `title_spend_candidates` only.

Plain rule: flood only, title_spend_candidates only.

Selection surface:

`landintel_reporting.v_constraint_priority_measurement_queue`

Runtime selection correction:

The reporting queue remains useful for operators, audits and backlog visibility, but live proof runs no longer select
directly from the full queue view. The first completion run proved the view is too expensive to sort repeatedly at
execution time. The runner now uses the same underlying priority surfaces:

- `landintel_reporting.v_constraint_priority_sites`;
- `landintel_reporting.v_constraint_priority_layers`;
- `public.site_constraint_measurement_scan_state`.

It filters those by one site-priority band and one source family or layer before joining. That preserves the same
commercial ordering and scan-state safeguards while avoiding a national queue sort for every 25-pair batch.

Queue correction:

The first source-family proof run showed that the global 5,000-pair queue cap was commercially too narrow. Flood backlog consumed the queue, which meant `coal_authority` had backlog in `v_constraint_measurement_backlog` but zero queued rows in `v_constraint_priority_measurement_queue`.

Migration `073_constraint_source_family_queue_fix.sql` keeps the same queue surface but caps it per source family. That means flood remains first priority, while coal/mining, green belt, contaminated land, culverts, heritage/conservation, ecology/NatureScot and TPO/landscape can also surface bounded candidate pairs without creating a second constraint engine.

Audit correction:

Once the queue became source-family aware, exact `count(*)` over the full queue became too expensive for the GitHub audit timeout. The audit now reports bounded/estimated queue counts and keeps exact proof to the before/after measurement output and queue samples.

Execution command:

`constraint-measurement-proof-flood-title-spend`

The command filters the queue to:

- `site_priority_band = 'title_spend_candidates'`
- `constraint_priority_family = 'flood'`

Reusable source-family command:

`constraint-measurement-proof-title-spend-source-family`

This command is the source completion programme adapter for the rest of the priority constraint estate. It defaults to
the `title_spend_candidates` cohort, but the direct `Run LandIntel Sources` path can reuse `constraint_measure_authority`
as the proof site-priority band for bounded `review_queue` and `ldn_candidate_screen` proof runs. It refuses to run
unless one of these existing workflow filters is provided:

- `constraint_measure_source_family`
- `constraint_measure_layer_key`

That means coal/mining, green belt, contaminated land, culverts, heritage/conservation, ecology/NatureScot and TPO/landscape can follow the same bounded proof pattern without creating a new constraint engine or a broad all-layer button.

For heavy layers such as NatureScot SAC/SPA, the runner uses an exact no-hit prefilter before the full finalizer. If a
site/layer pair has no exact intersecting or within-buffer source feature, and there is no existing measurement or
summary to clean up, the runner records scan-state directly with `measurement_method=exact_spatial_no_hit_prefilter`.
Only pairs with a real spatial candidate or existing measurement state go through the full measurement finalizer. This
keeps negative checks fast without inventing a second constraint truth table.

Example next run:

- command: `constraint-measurement-proof-title-spend-source-family`
- `constraint_measure_source_family=coal_authority`
- `constraint_measure_authority=title_spend_candidates`
- batch: 10 site-layer pairs

## Why Flood First

Flood is the highest-priority constraint family because it can materially affect:

- net developable area;
- layout efficiency;
- title-spend timing;
- buyer appetite;
- early desktop due diligence confidence.

Flood evidence is also measurable, auditable and already present in the constraint scaler priority taxonomy.

## Why Title Spend Candidates First

`title_spend_candidates` are the sites closest to a paid human/title action. Measuring these first protects LDN from spending on sites where a basic high-priority physical constraint has not been checked.

This keeps measurement aligned with commercial decision pressure rather than widening into the full canonical estate.

## Batch Size

Default batch:

`CONSTRAINT_PROOF_PAIR_BATCH_SIZE=10`

Hard cap:

Manual proof default: `25` site-layer pairs per run.

Completion workflow cap: `250` site-layer pairs per bounded constraint step.

Absolute runner ceiling: `250` site-layer pairs. A workflow can lower the cap, but cannot raise it above that ceiling.

The batch unit is a site-layer pair, not an all-layer site sweep. If the first queued site has multiple unscanned flood layers, the command may process several flood layers for one site before moving to another site.

This is a bounded batch by design, not a standing full-refresh workflow.

## Runtime Controls

The GitHub Actions command uses the existing `SOURCE_EXPANSION_COMMAND_TIMEOUT` wrapper.

The command itself:

- reads a capped batch from filtered priority sites and filtered priority layers;
- runs only flood/title-spend pairs;
- for the reusable command, runs only explicitly filtered title-spend source-family or layer pairs;
- groups by layer key;
- chunks heavy layers into narrower site batches before calling the finalizer;
- calls the existing `public.refresh_constraint_measurements_for_layer_sites` finalizer;
- prints before/after proof;
- exits non-zero if any layer batch errors.

Heavy layer safeguard:

The review-queue proof runs showed that NatureScot SAC and SPA layers can time out when 13 candidate sites are passed
to the finalizer in one call. The runner now keeps the same source-family/cohort filters, but splits configured heavy
layers into one-site chunks by default:

- `naturescot:protectedareas_sac`;
- `naturescot:protectedareas_spa`.

This is still bounded measurement. It does not add a second constraint engine, a new truth table, RAG scoring or a broad
scan. It simply reduces the per-call work for expensive ecology layers so scan-state can advance safely.

Finalizer performance correction:

The first heavy-layer retry proved a second bottleneck inside the existing PostGIS finalizer: the scan-state write was
calling `public.constraints_site_anchor()` and expanding every canonical site before filtering back to the requested
site batch. Migration `085_constraint_finalizer_requested_anchor.sql` keeps the same finalizer and same truth tables, but
restricts that scan-state anchor to `p_site_location_ids`. It does not execute measurement during migration.

The proof runner also excludes active layers with no source features from candidate selection. Those layers remain in
the registry for governance, but they are not useful measurement work.

## Measurement Path

The existing DuckDB command can filter by layer or source family, but it does not currently target the `title_spend_candidates` queue cleanly.

For this proof, the bounded wrapper uses the existing authoritative PostGIS finalizer:

`public.refresh_constraint_measurements_for_layer_sites`

That function already writes:

- `public.site_constraint_measurements`;
- `public.site_constraint_group_summaries`;
- `public.site_commercial_friction_facts`;
- `landintel.evidence_references`;
- `landintel.site_signals`;
- `landintel.site_change_events`;
- `public.site_constraint_measurement_scan_state`.

This preserves the existing constraint truth model.

## Scan State

`public.site_constraint_measurement_scan_state` prevents repeated no-hit scans.

For every requested site-layer pair, the finalizer records scan state for `canonical_site_geometry`, including whether a constraint relationship was found.

This matters because no-hit scans are still useful proof. They stop the same empty relationship from being repeatedly reprocessed unless a future refresh intentionally forces it.

## Before/After Proof

The command prints:

- candidate sites in the title-spend cohort;
- active flood layer count;
- selected site-layer pair count;
- selected site count;
- selected layer count;
- sites with flood measurements before;
- flood measurement rows before;
- sites with flood scan state before;
- flood scan-state rows before;
- sites measured in the run;
- site-layer pairs processed;
- per-layer finalizer results;
- chunk index and chunk count for heavy layers;
- sites with flood measurements after;
- flood measurement rows after;
- sites with flood scan state after;
- flood scan-state rows after;
- errors;
- runtime seconds.

## Rollback And Safety

Migration `073_constraint_source_family_queue_fix.sql` only replaces a reporting view. It does not move data, create a duplicate truth table or run measurement.

The command uses existing update semantics inside the measurement engine. For the requested layer/site pairs, the finalizer refreshes measurements and summaries, then records scan state. It does not touch unrelated layers or unrelated sites.

If a layer batch errors, the command reports the failed layer and exits non-zero. Existing transaction boundaries are handled by the database helper and finalizer function.

## What Is Deliberately Not Measured

This PR does not measure:

- coal/mining;
- green belt;
- contaminated land;
- culverts;
- heritage/conservation;
- ecology/NatureScot;
- TPO/landscape;
- review queue sites outside the selected flood/title-spend batch;
- LDN candidate screen sites outside the selected flood/title-spend batch;
- wider canonical sites.

It does not add RAG scoring, pass/fail status, planning certainty, legal certainty or engineering certainty.

The reusable command makes those later families measurable, but they are not measured until the command is explicitly dispatched with a source-family or layer filter. A run without a filter fails closed.

## Post-Merge Run

Run through GitHub Actions:

1. `run-migrations`
2. `constraint-measurement-proof-flood-title-spend`
3. `audit-constraint-measurements`

Initial batch setting:

`CONSTRAINT_PROOF_PAIR_BATCH_SIZE = 10`

Do not use this as a broad measurement button. It is a proof-grade bounded execution path for one commercial cohort and one constraint family.

Next source-family proof run after this PR:

1. `run-migrations`
2. `constraint-measurement-proof-title-spend-source-family`
   - `constraint_measure_source_family=coal_authority`
3. `audit-constraint-measurements`
4. `audit-source-completion-matrix`

This should prove whether coal/mining can move through the same safe coverage pattern as flood, while leaving BGS, Apex, source ingestion and planning extraction untouched.
