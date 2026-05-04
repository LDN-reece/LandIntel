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

## Next Phase

Phase F should create the source completion matrix and live workflow gap audit.
