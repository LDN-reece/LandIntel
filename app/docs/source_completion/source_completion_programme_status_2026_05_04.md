# LandIntel Source Completion Programme Status - 2026-05-04

## Current Repo State

Implementation base: GitHub `main` at merge commit `07dbfa3ef7f7aee7379caa2b9167af102f2fcd41`.

Recent cleanup and operationalisation PRs merged:

- PR #7 object ownership and schema clarity.
- PR #9 operator-safe sourced-site views.
- PR #10 title/control operator safety views.
- PR #12 BGS borehole clean normalisation views.
- PR #13 parcel model decision reporting.
- PRs #14-#21 constraint coverage scaler controls, performance and audit proof.
- PRs #22-#23 source completion matrix and rerun hardening.
- PRs #24-#29 bounded BGS context, scan registry, queue and asset manifests.
- PR #30 bounded flood constraint measurement proof.

Open PRs observed:

- PR #2 `[codex] Publish Phase One implementation` - old draft from April 2026.
- PR #3 `Phase One canonical opportunity engine rebase` - old draft from April 2026.

Recommendation: treat PR #2 and PR #3 as stale/superseded for this source completion programme unless a human explicitly reactivates them. They predate the post-audit schema clarity, sourced views, title safety, constraint scaler and source completion matrix work.

## Live Source Completion Counts

Live Supabase view: `landintel_reporting.v_source_completion_matrix`.

Observed status counts:

- `blocked`: 17
- `live_partial`: 37
- `manual_only`: 9
- `registered_only`: 132
- `live_complete`: 0

Interpretation: LandIntel has useful live partial capability, but no material source should yet be called `live_complete`. The programme must move families through bounded workflow proof, freshness, evidence/signal output and live Supabase verification before promotion.

## Current Live Surfaces

Core operator and audit surfaces now exist:

- `landintel_store.object_ownership_registry`
- `landintel_reporting.v_object_ownership_matrix`
- `landintel_sourced.v_sourced_sites`
- `landintel_sourced.v_review_queue`
- `landintel_sourced.v_title_spend_candidates`
- `landintel_reporting.v_title_control_status`
- `landintel_reporting.v_title_candidates_operator_safe`
- `landintel_reporting.v_sites_needing_title_review`
- `landintel_reporting.v_title_spend_queue`
- `landintel_reporting.v_parcel_model_status`
- `landintel_reporting.v_constraint_coverage_by_layer`
- `landintel_reporting.v_constraint_coverage_by_site_priority`
- `landintel_reporting.v_constraint_measurement_backlog`
- `landintel_reporting.v_constraint_priority_measurement_queue`
- `landintel_reporting.v_source_completion_matrix`

These are control and operating surfaces. They do not, by themselves, prove source completion.

## Blockers

Main blockers observed in the live matrix:

- many P0 LDP and settlement sources are registered but have no source rows;
- title/control sources remain partial until freshness and title-review lifecycle proof is present;
- constraint sources are partial because ingestion exists but priority-cohort measurement coverage is thin;
- planning document and appeal sources remain blocked or registered-only until bounded metadata workflows are run;
- corporate, power, market and terrain sources remain partial or blocked pending bounded source-family workflows;
- open-location spine sources are mostly partial and need context refresh hardening before they should drive decisions.

## Recommended Source-Family Order

Recommended order remains:

1. Constraints, using bounded priority queue measurement.
2. Planning extraction from existing planning records.
3. Planning documents, Section 75 and appeals metadata.
4. Pre-SI ground intelligence, bounded candidate-first only.
5. Title/control and parcel dependency hardening.
6. Corporate enrichment.
7. Market, EPC, comparables and buyer evidence.
8. Power/DNO/infrastructure.
9. Terrain/slope.
10. Open-location spine hardening.

## Current PR Scope

This programme continues Priority 1 constraints.

The flood proof showed the existing engine can move a bounded title-spend batch safely. PR #31 added the reusable title-spend source-family proof command so the same pattern can be repeated for:

- `coal_authority`
- `greenbelt`
- `contaminated_land`
- `culverts`
- `hes`
- `conservation_areas`
- `naturescot`
- `tpo`

The command fails closed unless an explicit `constraint_measure_source_family` or `constraint_measure_layer_key` filter is provided.

Post-merge live proof found one remaining queue flaw: `v_constraint_priority_measurement_queue` used a single global 5,000-pair cap, so SEPA flood consumed the entire queue and `coal_authority` returned zero candidate pairs even though `v_constraint_measurement_backlog` showed live title-spend coal backlog.

The next narrow fix is therefore to keep the same queue surface but make the cap source-family aware. That is a controls fix only; it should then be followed by a repeat `coal_authority` proof run and audit.

No BGS, Apex, source ingestion or planning extraction work is included in this PR.
