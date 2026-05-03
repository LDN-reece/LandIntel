# LandIntel Cleanup Structure Plan

## Purpose

This PR creates an object ownership and schema clarity layer. It does not move, delete, re-upload, rename, truncate or physically reorganise live data.

The point is to make the estate readable and governable before heavier scaling work continues. LandIntel already has the right live architecture: canonical site spine, source evidence, measured context and review/action outputs. The gap is that live Supabase also contains legacy, duplicate, empty-stub and manually uploaded warehouse assets that are not clearly labelled for operators or future implementation work.

## Why This PR Does Not Move Data

Moving live tables now would create avoidable break risk. Existing GitHub Actions, SQL functions, analytics views and Python runners still reference a mixture of `landintel` and `public` objects. This PR therefore adds:

- new target schemas;
- an object ownership registry;
- compatibility views;
- reporting surfaces;
- documentation and tests.

It deliberately does not alter source table ownership, table names, row contents or runner behaviour.

## Target Schemas

| Schema | Purpose |
| --- | --- |
| `landintel` | Core canonical spine and universal evidence/signal layer. |
| `landintel_store` | LandIntel Data Store, the warehouse/source estate/raw and normalised data layer. |
| `landintel_sourced` | LandIntel Sourced Sites, the polished commercial opportunity register for LDN review. |
| `landintel_reporting` | Human and machine-readable views for dashboards, audits, UI and operator review. |
| `staging` | Temporary import and transformation. |
| `public` | Legacy compatibility for LandIntel domain objects; new LandIntel domain tables should not be added here unless technically unavoidable. |

## Object Ownership Registry

`landintel_store.object_ownership_registry` is the control table for object clarity. It records what each audited object is, whether it exists in GitHub and Supabase, which layer owns it, whether it is safe for operator use, and what should happen next.

It is not a source of commercial truth. It is a governance map for the estate.

Important fields:

- `current_status`: current classification such as `current_keep`, `duplicate_candidate`, `known_origin_manual_bulk_upload` or `stub_future_module`.
- `owner_layer`: intended layer, such as `landintel`, `landintel_store`, `landintel_sourced`, `landintel_reporting` or `public_legacy_compatibility`.
- `safe_to_read`, `safe_to_write`, `safe_for_operator`, `safe_to_retire`: immediate operational safety flags.
- `risk_summary` and `recommended_action`: plain-English guidance.

## Status Taxonomy

| Status | Meaning |
| --- | --- |
| `current_keep` | Current object that should stay in service. |
| `current_but_expensive_scale_risk` | Current and useful, but too large or expensive for broad scans. |
| `duplicate_candidate` | Likely duplicate of another model or storage path; investigate before retirement. |
| `known_origin_manual_bulk_upload` | Known manually supplied source with useful rows, but missing repo-governed provenance/refresh/enrichment controls. |
| `orphaned_in_supabase` | Live object not represented by current repo definitions and without confirmed provenance. |
| `legacy_candidate_retire` | Legacy/empty/compatibility object that may be retired after dependency proof. |
| `repo_defined_empty_stub` | Repo-defined object present but empty; usually awaiting workflow/data. |
| `stub_future_module` | Intentional future module schema with no live data yet. |
| `staging_only` | Temporary import/transformation object. |
| `reporting_surface` | View or reporting object intended for human or machine consumption. |

## Public As Legacy Compatibility

`public` currently contains important LandIntel objects, especially RoS parcels and constraint measurement tables. These are not moved in this PR because existing functions and workflows rely on them.

The intended direction is:

1. keep physical tables where they are for now;
2. expose stable views in `landintel_store`;
3. update new code to read from the clearer layer;
4. retire legacy physical objects only after dependency proof and compatibility views.

## Why `public.land_objects` Is A Duplicate Candidate

`public.land_objects` has roughly the same scale as `public.ros_cadastral_parcels` and appears to be a parcel-era normalised object store. It may still have dependencies, so this PR does not retire it.

Its audit status is `duplicate_candidate` because it likely overlaps with the current RoS parcel store while adding storage and interpretation complexity. The recommended action is dependency mapping, then a human decision on whether it remains a legacy cache or is archived later.

## BGS Borehole Master Provenance And Governance

`landintel.bgs_borehole_master` is not unknown-origin junk.

It is a known-origin manual bulk upload:

- source: BGS Single Onshore Borehole Index;
- source form: master raw CSV downloaded by Reece and supplied manually;
- upload: performed by Codex into Supabase;
- current live row count: approximately `1,350,790`;
- source date / file date / exact loaded row count: to be confirmed from available metadata and the upload tracker.

The correct classification is:

- `current_status`: `known_origin_manual_bulk_upload`;
- risk classification: `high_value_governance_incomplete`.

The earlier off-repo/uncontrolled risk label is corrected here: the origin is known, but governance is incomplete. The issue is not origin uncertainty. The issue is missing repo-governed provenance, table contract, upload manifest, refresh policy and enrichment workflow.

Safe use now:

- safe for borehole proximity intelligence;
- safe for borehole coverage and density intelligence;
- safe for identifying whether borehole evidence exists near a site;
- not safe as final ground condition interpretation;
- not safe as piling, grouting, remediation or abnormal cost evidence without further borehole records, site investigation or engineering review.

Refresh policy:

- manual refresh for now;
- no automated re-download;
- no re-upload until an intentional BGS connector/import workflow is built and reviewed.

Future enrichment workflow:

- site-to-borehole proximity;
- nearest borehole signal;
- borehole density signal;
- evidence reference creation;
- feed into `landintel.site_ground_risk_context`;
- feed into `landintel.site_abnormal_cost_flags`;
- keep all outputs framed as desktop context, not engineering certainty.

See `app/docs/schema/bgs_borehole_master_upload_manifest.md` and `app/docs/schema/bgs_borehole_master_schema_contract.md`.

## Title Review Status

`landintel.title_review_records=0` means ownership remains unconfirmed.

The current title/control layer can support title spend decisions and candidate visibility, but it must not claim legal ownership certainty before human title review. RoS parcel IDs, SCT references, title candidates, Companies House data and control signals are all evidence or leads. They are not a substitute for reviewed title evidence.

## Constraint Coverage Status

The constraint model is current but not yet estate-scale. The audit found `public.site_constraint_measurements` at low coverage relative to the canonical site spine.

The next constraint work should be layer-by-layer and bounded:

- flood first;
- then priority environmental/planning constraints;
- then infrastructure and abnormal context;
- no broad all-layer scans by default;
- overlap character must stay populated and visible.

## Empty Phase 2 Tables

Empty Phase 2 tables are not failed work by themselves. They are schemas waiting for trusted adapters, access or extraction workflows.

They must be labelled honestly as `stub_future_module` or `repo_defined_empty_stub`, not presented as implemented or trusted.

Examples:

- planning appeals;
- planning documents;
- Section 75 obligations;
- power infrastructure;
- corporate enrichments;
- market transactions;
- buyer evidence;
- local intelligence.

## Compatibility Views

This PR adds `landintel_store` compatibility views over existing major objects. The views are a readability layer only. They let future work refer to clearer schema names without moving physical data.

## Recommended Next PR Sequence

1. Object ownership and drift guard in CI.
2. Title output hardening and operator-safe title views.
3. Open-data spine safety: isolate OSM failure and separate Boundary-Line containment from proximity context.
4. Constraint coverage scaler: bounded, flood-first, layer-by-layer.
5. Legacy model labelling and dependency map for `public.land_objects`, `public.land_parcels`, old frontend materialized views and `public.site_spatial_links`.
6. Planning decision extraction and document adapter framework.
7. RLS/index hygiene using Supabase advisor output.

## Codex Challenges And Evidence

| Object | Assumed status | Challenged status | Evidence from repo/Supabase | Recommended action | Proceed or wait |
| --- | --- | --- | --- | --- | --- |
| `landintel.bgs_borehole_master` | `orphaned_in_supabase` | `known_origin_manual_bulk_upload` with `high_value_governance_incomplete` risk | User confirmed it came from the BGS Single Onshore Borehole Index master raw CSV supplied manually and uploaded by Codex. Supabase metadata shows approximately `1,350,790` rows and a related upload tracker. Current repo lacks a full governed manifest/contract. | Govern, document and enrich. Do not delete, re-upload or use as final interpreted ground evidence. | Proceed with classification update and compatibility view; wait for human decision before automated refresh or enrichment workflow. |
| `public.land_objects` | current parcel object | `duplicate_candidate` | Supabase audit shows approximately `989,741` rows, close to `public.ros_cadastral_parcels` at approximately `990,059` rows. Current canonical/title candidate workflows use RoS parcel candidates rather than this as the sole anchor. | Dependency-map before any retirement. | Proceed with registry label only. |
| `public.site_spatial_links` | current spatial lineage | `legacy_candidate_retire` | Supabase audit shows `0` rows and table comment references absent `public.sites` / `public.site_locations.geometry` model. | Keep until dependency proof, then retire/archive. | Proceed with registry label only. |
