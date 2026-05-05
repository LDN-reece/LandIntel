# Measure Link Completion Orchestrator

## Purpose

`Run LandIntel Measure Link Completion` is the GitHub Actions control loop for turning the current LandIntel estate from partially measured into operationally readable.

It does not create a new truth system. It repeatedly runs the existing bounded linkers, measurement functions, context refreshers and audits.

## Why This Matters Commercially

LandIntel only becomes useful at scale when every site can be read through:

- legal title/location identity;
- parcel and safe title-number candidate traceability;
- measured constraints;
- open-location context;
- amenity, market, demographic, power and abnormal-risk context;
- source/evidence audit surfaces.

The commercial benefit is speed with discipline: LDN can see what a site is, where it is, what affects it, what is still missing, and whether the next pound or hour is justified.

## Workflow

Workflow file:

`.github/workflows/run-landintel-measure-link-completion.yml`

The workflow runs serial bounded cycles. Each cycle does:

1. title/parcel traceability proof;
2. outside-register title/parcel traceability proof;
3. parcel/title audits;
4. source reconcile catchup, queue processing and affected-site refresh;
5. bounded open-location corpus/context completion;
6. bounded constraint measurement by source family and site priority band;
7. constraint measurement audit;
8. site amenity, demographic, market, power and abnormal-risk context refresh;
9. DD orchestration audit;
10. source completion matrix audit.

## Constraint Families

Default source-family order:

- `sepa_flood`
- `coal_authority`
- `greenbelt`
- `contaminated_land`
- `culverts`
- `hes`
- `conservation_areas`
- `naturescot`
- `tpo`

This order follows LDN’s DD priority: physical/site-killing constraints first, then policy/heritage/ecology/tree context.

## Site Priority Bands

Default priority bands:

- `title_spend_candidates`
- `review_queue`
- `ldn_candidate_screen`

The workflow can be rerun later for `prove_it_candidates` and `wider_canonical_sites`, but those should not be the default until the priority commercial estate is moving cleanly.

## Safety Rules

The workflow:

- uses existing `landintel_reporting.v_constraint_priority_measurement_queue`;
- runs one source family and priority band at a time;
- uses `constraint-measurement-proof-title-spend-source-family`;
- preserves scan-state logic;
- records no-hit scans as progress;
- fails closed on measurement errors;
- audits after each cycle.

It does not:

- create a second constraint engine;
- does not create a second constraint engine;
- run an unbounded all-site/all-layer scan;
- delete data;
- move data;
- confirm ownership;
- does not confirm ownership;
- treat RoS parcel references as title numbers;
- score sites as pass/fail.

## How To Run

Start with:

- `completion_cycles`: `3`
- `constraint_pair_batch_size`: `25`
- `title_traceability_site_batch_size`: `25`
- `include_open_location_context`: `true`
- `include_phase2_context_refresh`: `true`

If the audits show queues still have backlog, rerun the workflow with more cycles. The target is zero priority backlog, not one heroic unsafe full-table scan.

## What Complete Means

Completion is proven when:

- `audit-site-dd-orchestration` shows no priority title/parcel/indexing backlog;
- `audit-constraint-measurements` shows priority constraint scan-state coverage;
- `audit-open-location-spine-completion` shows context coverage materially advanced;
- `audit-source-completion-matrix` no longer marks core DD sources as only registered/stubbed;
- operator views can read each site with title/location, area/access, location context, measured constraints and relevant source caveats.

## Known Limitation

This workflow is bounded by GitHub Actions runtime. If the estate still has backlog at the end of a run, rerun it. That is deliberate: repeated bounded runs are safer than a single unbounded database-wide scan.
