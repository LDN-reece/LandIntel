# Site Qualification MVP

## Objective

This MVP exists to answer one operational question:

`Why might this site deserve senior review?`

It is not a final decision engine, not a valuation model, and not a generic property portal.

## Architectural Lock

The architecture follows six non-negotiable rules:

1. `public.sites` is the canonical internal site object.
2. External datasets stay modular in their own structured models.
3. Signals are derived from linked site data before any interpretations are created.
4. Every surfaced signal and interpretation must be linked to evidence rows.
5. The frontend reads only site summaries, signals, interpretations, and evidence.
6. Reprocessing is incremental per affected site, not a full-dataset recompute by default.

## Truth Model

A site is an internal aggregate, not an imported polygon.

- RoS parcels remain in `public.ros_cadastral_parcels`
- Planning rows remain in `public.planning_records`
- Planning context rows remain in `public.planning_context_records`
- Constraints remain in `public.site_constraints`
- Market comparables remain in `public.comparable_market_records`
- Buyer profiles remain in `public.buyer_profiles`

The site object links to those rows.

### How site construction is stored

- `public.site_parcels` links parcels and title references to the site
- `public.site_geometry_components` records which external geometry records are currently used to construct the site footprint
- `public.site_locations.geometry` is the internal aggregated geometry derived from linked parcel components, not the source of truth for external datasets

This means the system can always answer:

- what the current site footprint is
- which linked source geometries were used to build it
- which source tables and source ids those components came from

## Data Flow

The MVP runs in this order:

1. Raw ingestion
   Stage 1 pulls and standardises parcel and authority data into `staging` and `public`.
2. Site-linked source records
   A site is created in `public.sites`, then linked records are inserted into the dataset-specific tables.
3. Canonical reference bridge
   `src/site_engine/site_reference_reconciliation_engine.py` rolls site codes, planning refs, title numbers, and other aliases into one canonical site spine before evidence normalisation.
4. Normalised Scottish evidence
   `src/site_engine/source_normalisers/` plus the dedicated boundary, use, utility, and BGS engines turn linked source rows into a stable site evidence object before any opinions are formed.
5. Incremental queue
   `public.site_refresh_queue` records which sites must be recalculated and why.
6. Signals
   `src/site_engine/signal_engine.py` derives atomic signals into `public.site_signals`.
7. Scores and portfolio assessment
   The Scottish rules engine creates the seven scores, hard-fail flags, bucket, horizon, blocker, and explanation into `public.site_assessments` and `public.site_assessment_scores`.
8. Interpretations
   `src/site_engine/rule_engine.py` then creates analyst-facing positives, risks, possible fatal issues, and unknowns from the persisted scorecard and routing result.
9. Delivery cache
   `analytics.upsert_site_search_cache_row(uuid)` updates only the affected row in `analytics.site_search_cache`.
10. UI / API
   The frontend uses the cache for shortlist search and scoped per-site reads for the analyst brief.

## Schema Summary

### Existing Stage 1 base

- `public.authority_aoi`
- `public.source_registry`
- `public.ingest_runs`
- `public.source_artifacts`
- `public.ros_cadastral_parcels`
- `public.land_objects` when the optional geometry mirror is enabled

### Canonical site layer

- `public.sites`
- `public.site_reference_aliases`
- `public.site_geometry_versions`
- `public.site_reconciliation_matches`
- `public.site_reconciliation_review_queue`
- `public.site_locations`
- `public.site_parcels`
- `public.site_geometry_components`
- `public.site_review_status_history`
- `public.site_refresh_queue`

### Site-linked dataset models

- `public.planning_records`
- `public.planning_context_records`
- `public.site_constraints`
- `public.site_infrastructure_records`
- `public.site_control_records`
- `public.comparable_market_records`
- `public.buyer_profiles`
- `public.site_buyer_matches`

### Reasoning and traceability

- `public.site_analysis_runs`
- `public.site_signals`
- `public.site_assessments`
- `public.site_assessment_scores`
- `public.site_assessment_evidence`
- `public.site_assessment_score_evidence`
- `public.site_assessment_overrides`
- `public.site_interpretations`
- `public.evidence_references`
- `public.site_signal_evidence`
- `public.site_interpretation_evidence`

### Lean delivery surfaces

- `analytics.v_canonical_sites`
- `analytics.v_site_reference_index`
- `analytics.v_site_current_analysis_runs`
- `analytics.v_site_current_signals`
- `analytics.v_site_current_interpretations`
- `analytics.v_site_fact_summary`
- `analytics.site_search_cache`
- `analytics.v_site_search_summary`

## Evidence Model

Every meaningful output must be traceable.

`public.evidence_references` stores:

- source dataset name
- source table
- source record id
- source identifier where useful
- source URL where useful
- observed date / import version where available
- the actual assertion used by the engine

Signals link to evidence through `public.site_signal_evidence`.
Interpretations link to evidence through `public.site_interpretation_evidence`.

Interpretations never bypass the signal layer and never invent unsupported evidence.

## Rule Engine

The first Scottish portfolio ruleset is explicit and deterministic.

Implemented layers:

- site reference reconciliation
- boundary position classification
- source normalisers
- previous-use and current-building-use inference
- utility burden inference
- BGS reasoning
- signal extraction
- seven-score engine
- hard fail gates
- bucket router
- explanation builder
- analyst-facing interpretation grouping

Implemented portfolio buckets:

- `A` Clean Strategic Greenfield
- `B` Emerging / Coming Forward
- `C` Stalled / Re-Entry
- `D` Messy But Workable
- `E` Infrastructure-Locked
- `F` Dead / Do Not Chase

Analyst-facing interpretation categories:

- `positive`
- `risk`
- `possible_fatal`
- `unknown`

Rules live in [src/site_engine/rule_engine.py](../src/site_engine/rule_engine.py).
Signals live in [src/site_engine/signal_engine.py](../src/site_engine/signal_engine.py).
The scoring handbook lives in [scottish-scoring-handbook.md](scottish-scoring-handbook.md).

### Adding a new rule

1. Add or extend a signal in `signal_engine.py`.
2. Make sure the signal has evidence references.
3. Add a deterministic interpretation rule in `rule_engine.py` that only consumes signals.
4. Add or update a unit test in `app/tests/site_engine/`.
5. Reprocess affected sites only by queue or explicit site selection.

## Incremental Update Model

The default path is not full recomputation.

- Source rows linked to a site enqueue that site in `public.site_refresh_queue`
- The queue tracks cause, scope, status, and error state
- `refresh-site-qualifications` processes pending sites one by one
- Each successful refresh writes one new analysis run and updates one shortlist cache row

This keeps recalculation scoped and makes the operational cost visible.

## Frontend Discipline

The internal UI is thin by design.

- Search reads `analytics.v_site_search_summary`
- Site detail reads one site at a time from the current signal, interpretation, and evidence views plus site-linked fact tables
- No client-side reasoning is performed
- No raw bulk datasets are sent to the browser

The review UI lives in [src/web/app.py](../src/web/app.py).

## Seeded MVP Data

The current MVP now seeds six Scottish portfolio scenarios:

- clean strategic greenfield
- emerging / coming forward
- stalled / re-entry
- messy but workable brownfield
- infrastructure-locked
- dead / do not chase

What is real today:

- Scottish authority boundaries
- RoS parcel geometry and area data
- canonical site aggregation mechanics
- source-aware Scottish evidence normalisation
- signals, scorecards, interpretations, evidence wiring
- incremental queue and search cache
- internal review UI

What is still seeded / placeholder:

- planning records
- planning context records
- comparable market records
- buyer fit matches

Those rows are clearly marked with `mvp_seed.*` datasets so they cannot be mistaken for production ingest.

## Out Of Scope For This MVP

- residual valuation
- pricing engine
- negotiation strategy
- autonomous site ranking model
- opaque AI scoring
- override editing UI, even though the override table scaffold now exists
- large raw file retention in Supabase
- browser access to raw source tables
- full ingestion pipelines for planning, constraints, comps, and buyer intel

## Next After MVP

1. Replace seeded planning and constraint records with real ingestion pipelines that still preserve dataset separation.
2. Add site-linking jobs that spatially relate new datasets to affected sites and enqueue only those sites.
3. Add manual site creation and editing for land team users.
4. Add authenticated workflow actions for changing site review status from the UI.
5. Add buyer-specific routing overlays, authority nuance overlays, and eventually an England-specific reasoning layer as separate extensions.
