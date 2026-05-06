# Constraint Coverage Scaler

## Purpose

This phase adds operator-safe reporting controls for scaling constraint measurement.

It does not create a second constraint engine, run measurements, move data, or change constraint truth. The existing constraint model remains the source of truth:

- `public.constraint_layer_registry`
- `public.constraint_source_features`
- `public.site_constraint_measurements`
- `public.site_constraint_group_summaries`
- `public.site_commercial_friction_facts`
- `public.site_constraint_measurement_scan_state`

## Commercial Meaning

LDN needs constraint coverage to move from proof-of-life to useful DD coverage without burning compute on broad scans.

The commercial goal is simple:

- measure the sites most likely to justify title spend first;
- measure one constraint layer at a time;
- keep outputs as measured facts, not RAG, pass/fail or legal/engineering certainty;
- expose the backlog clearly enough that GitHub Actions can be run safely.

This helps LDN avoid spending title or analyst time on sites where a high-priority constraint has not yet been measured.

## New Reporting Views

### `landintel_reporting.v_constraint_coverage_by_layer`

Shows coverage by existing constraint layer:

- source feature count;
- measured row count;
- measured site count;
- scan-state count;
- backlog site count;
- missing overlap-character count;
- latest measured/scanned timestamps;
- priority family such as flood, coal/mining, green belt, contaminated land, culverts, heritage/conservation, ecology/NatureScot and TPO/landscape.

This view is for coverage control only. It does not run spatial measurement.

### `landintel_reporting.v_constraint_coverage_by_site_priority`

Shows whether constraint coverage is reaching the right sites first.

Site priority order is:

1. title spend candidates;
2. review queue;
3. LDN candidate screen;
4. Prove It candidates;
5. wider canonical sites.

### `landintel_reporting.v_constraint_measurement_backlog`

Aggregates unscanned site-layer pairs by site priority and constraint priority.

Use it to choose the next bounded layer/site-priority batch.

### `landintel_reporting.v_constraint_priority_measurement_queue`

Exposes a bounded first 5,000 unscanned site-layer pairs.

This is a queue-guidance view only. It does not write scan state and it does not execute measurement.

## Performance Follow-Up

Live verification after PR #14 proved the views were readable, but `v_constraint_coverage_by_site_priority` and backlog sampling were too slow for an operator surface.

Codex challenge and evidence:

- object/workflow: `landintel_reporting.v_constraint_priority_sites` feeding the constraint scaler views;
- assumed status: use the already polished `landintel_sourced` views directly;
- challenged status: use the underlying current tables for the constraint priority spine, because the sourced-site views are heavier operator surfaces;
- evidence: live count/sample verification returned, but site-priority coverage took several minutes and backlog sampling was slower than an operational control should be;
- recommended action: keep the same reporting outputs, but derive priority bands from `landintel.site_urgent_address_title_pack`, `landintel.site_prove_it_assessments`, `landintel.site_ldn_candidate_screen`, `landintel.title_order_workflow` and `landintel.canonical_sites`;
- proceed or wait: proceed as a narrow Phase E performance fix because no data is moved, no measurement is run and no truth table changes.

The operator sourced-site views remain valid decision surfaces. The constraint scaler uses cheaper source tables so it can guide bounded measurement runs without becoming the bottleneck.

## Bounded Scale-Up Rule

Run constraints in this order unless live evidence justifies a documented challenge:

1. flood;
2. coal/mining;
3. green belt;
4. contaminated land;
5. culverts;
6. heritage/conservation;
7. ecology/NatureScot;
8. TPO/landscape.

Within each layer, run:

1. title spend candidates;
2. review queue;
3. LDN candidate screen;
4. Prove It candidates;
5. wider canonical sites.

## What This PR Does Not Do

- It does not create a second constraint measurement system.
- It does not run broad all-site/all-layer measurement.
- It does not run spatial joins in the migration.
- It does not create RAG scoring.
- It does not create pass/fail conclusions.
- It does not state that a constraint kills or clears a site.
- It does not replace human DD.

## Operator Caveat

Constraint outputs remain measured facts and commercial context only.

Overlap, distance and overlap-character evidence can inform review priority, title-spend timing and layout questions, but it is not legal certainty, not engineering certainty and not a development verdict.

## Recommended Use

After migration, check:

```sql
select * from landintel_reporting.v_constraint_coverage_by_layer;
select * from landintel_reporting.v_constraint_coverage_by_site_priority;
select * from landintel_reporting.v_constraint_measurement_backlog;
select * from landintel_reporting.v_constraint_priority_measurement_queue;
```

Then run bounded GitHub Actions using one layer at a time, small site batches, and the highest site-priority band first.

## Bounded Proof Caveat

Workflow proof and `audit-constraint-measurements` must not expand `v_constraint_priority_measurement_queue` just to print a sample after a run. That queue is useful for bounded execution, but it can be large enough to waste the run after measurement has already completed.

Post-run proof should sample `v_constraint_coverage_by_layer` instead of expanding either the full queue or the site-layer backlog view. This keeps the measurement loop operational: run the existing measurement engine, update scan-state/measurements, then print cheap layer-level backlog evidence.

## Next Phase

Phase F should create the source completion matrix and live workflow gap audit.
