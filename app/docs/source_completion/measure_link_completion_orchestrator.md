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

- uses existing constraint priority sites, priority layers and scan-state surfaces;
- keeps `landintel_reporting.v_constraint_priority_measurement_queue` as the operator/audit reporting surface;
- avoids sorting the full national measurement queue inside every live proof batch;
- runs one source family and priority band at a time;
- uses `constraint-measurement-proof-title-spend-source-family`;
- preserves scan-state logic;
- records no-hit scans as progress;
- records source-family layer errors in the proof output;
- allows the orchestrator to continue past isolated layer timeouts while leaving the failed layer in the audit trail;
- audits after each cycle.

It does not:

- does not create a second constraint engine;
- run an unbounded all-site/all-layer scan;
- delete data;
- move data;
- does not confirm ownership;
- treat RoS parcel references as title numbers;
- score sites as pass/fail.

Manual constraint proof commands still fail closed by default. The completion workflow sets
`CONSTRAINT_PROOF_ALLOW_LAYER_ERRORS=true` so a heavy layer such as SAC/SPA ecology timing out does not stop flood,
coal, access/context and other DD measurement from continuing. Those errors must be reviewed from the workflow log and
backlog/audit views before calling the relevant source family fully complete.

Heavy ecology layer correction:

NatureScot SAC and SPA layers proved too expensive when all remaining review-queue sites were sent to the finalizer in
one layer call. The completion workflow now sets:

- `CONSTRAINT_PROOF_HEAVY_LAYER_KEYS=naturescot:protectedareas_sac,naturescot:protectedareas_spa`
- `CONSTRAINT_PROOF_HEAVY_LAYER_SITE_BATCH_SIZE=1`

That keeps the same bounded source-family pattern, but measures those heavy layers one site at a time so the workflow
can keep draining scan-state without creating a second constraint engine.

The follow-up live proof also showed that the finalizer itself was doing too much work at the scan-state write: it used
the full canonical site anchor before filtering back to the requested batch. Migration
`085_constraint_finalizer_requested_anchor.sql` restricts that final scan-state anchor to the requested site IDs and does
not run measurement during migration. This keeps the measurement engine intact but removes the avoidable full-spine scan
from bounded proof runs.

For surgical constraint retries, `Run LandIntel Sources` exposes the same proof runner directly through:

- `constraint_measure_source_family`
- `constraint_measure_site_batch_size`
- `constraint_measure_authority` as the proof site-priority band for this command

Use that direct path when only one source family and one cohort needs draining. Use the full measure/link completion
workflow when title traceability, reconcile, open-location and Phase 2 context refreshes also need to run.

## How To Run

Start with:

- `completion_cycles`: `3`
- `constraint_pair_batch_size`: `25`
- `title_traceability_site_batch_size`: `25`
- `include_open_location_context`: `true`
- `include_phase2_context_refresh`: `true`

If the audits show queues still have backlog, rerun the workflow with more cycles. The completion workflow explicitly
allows a higher proof cap of `250` site-layer pairs per bounded constraint step, while manual proof commands remain
conservative by default. The target is zero priority backlog, not one heroic unsafe full-table scan.

## What Complete Means

Completion is proven when:

- `audit-site-dd-orchestration` shows no priority title/parcel/indexing backlog;
- `audit-constraint-measurements` shows priority constraint scan-state coverage;
- `audit-open-location-spine-completion` shows context coverage materially advanced;
- `audit-source-completion-matrix` no longer marks core DD sources as only registered/stubbed;
- operator views can read each site with title/location, area/access, location context, measured constraints and relevant source caveats.

## Known Limitation

This workflow is bounded by GitHub Actions runtime. If the estate still has backlog at the end of a run, rerun it. That is deliberate: repeated bounded runs are safer than a single unbounded database-wide scan.

Some very heavy constraint layers may need narrower layer-specific batches or indexing work before they can be treated as cleanly operational. They should not block all other DD measurement.

Runtime proof note:

Run `25401595259` completed successfully but proved that queue selection itself was a scale bottleneck. The runner was
therefore tightened to select directly from filtered priority sites and filtered priority layers before applying
scan-state. This does not change constraint truth. It makes repeated GitHub Actions cycles materially faster and keeps
the measurement programme commercially usable. The 2026-05-06 follow-up raised the completion workflow cap to `250`
site-layer pairs while preserving the runner's absolute ceiling and source/cohort filters. The next correction keeps
that higher cap, but chunks configured heavy layers by site to avoid statement timeouts on SAC/SPA ecology measurements.
The finalizer anchor correction then removes the full-canonical scan-state step that blocked those one-site chunks.
