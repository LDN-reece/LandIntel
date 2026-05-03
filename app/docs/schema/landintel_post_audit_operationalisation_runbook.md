# LandIntel Post-Audit Operationalisation Runbook

## Purpose

This runbook turns the post-audit cleanup layer into an operating control.

It does not authorise ingestion, data movement, deletion, table retirement, title certainty, broad spatial scans or new scoring models.

The rule is:

Clean, classify, prove and expose before scaling.

## Immediate Sequence After PR 1 Merges

1. Run migrations.
   - Workflow: `Run LandIntel Sources`
   - Command: `run-migrations`

2. Prove the estate matrix still works.
   - Workflow: `Run LandIntel Sources`
   - Command: `audit-full-source-estate`

3. Check object ownership.
   - View: `landintel_reporting.v_object_ownership_matrix`
   - Confirm current, legacy, duplicate, stub and manual-bulk-upload objects are visible.

4. Check BGS governance.
   - Object: `landintel.bgs_borehole_master`
   - Expected status: `known_origin_manual_bulk_upload`
   - Expected risk: `high_value_governance_incomplete`
   - Do not re-upload.
   - Do not treat as interpreted ground-condition evidence.

5. Check title/control caveat.
   - Object: `landintel.title_review_records`
   - If row count remains `0`, ownership remains unconfirmed.
   - Title/control outputs remain title-readiness and control-signal support only.

6. Check constraint scaling status.
   - Object: `public.site_constraint_measurements`
   - Constraint coverage must be scaled layer-by-layer.
   - Do not run broad all-layer scans as a cleanup task.

## Operating Controls

The object ownership registry is a governance map. It is not a source of commercial truth and must not become a duplicate decision engine.

### Do Not Move Data Yet

The new target schemas are naming and governance layers. Existing physical objects remain where they are until dependency proof exists.

### Do Not Retire Legacy Tables Yet

Objects marked `legacy_candidate_retire` or `duplicate_candidate` are not approved for deletion. They require:

- dependency mapping;
- row-count proof;
- code-reference proof;
- compatibility plan;
- human decision.

### Do Not Treat Stubs As Implemented

Objects marked `stub_future_module` or `repo_defined_empty_stub` are useful because they describe planned modules. They are not live evidence engines until rows, links, evidence, signals and freshness exist.

### Do Not Treat BGS As Engineering Certainty

`landintel.bgs_borehole_master` is a high-value warehouse asset. Safe use is proximity, density and coverage intelligence. It is not safe for piling, remediation, grouting, abnormal-cost quantification or safe-ground claims.

### Do Not Treat Registers As Commercial Proof

HLA, ELA and VDL remain discovery/context layers. A register-origin site still needs corroboration from title/control, constraints, access/frontage, planning precedent, market context or other source-backed evidence.

## Drift Guard

Future schema work must keep the ownership model current.

Any new compatibility view should have its source object represented in `landintel_store.object_ownership_registry`.

Any future migration that creates a new LandIntel domain table should answer:

- which owner layer owns it;
- whether it is source, evidence, signal, sourced-site or reporting;
- whether it is safe for operator use;
- whether it duplicates an existing object;
- what proof is required before it becomes trusted.

## Next PR Sequence

### PR 2: Object Ownership Drift Guard

Add CI-backed tests and documentation so new compatibility views, major source objects and manual bulk uploads cannot drift outside the ownership registry.

### PR 3: Title Output Hardening

Expose title candidates, rejected SCT-like identifiers and title review status through operator-safe views. Keep confirmed ownership locked behind human title review.

### PR 4: Open Data Spine Safety

Separate landing from context measurement. Prevent Boundary-Line and other broad administrative layers being treated as noisy generic proximity positives.

### PR 5: Constraint Coverage Scaler

Run bounded layer-by-layer coverage. Flood first, then priority constraints. Keep overlap character visible.

### PR 6: Legacy Dependency Map

Map dependencies for `public.land_objects`, `public.land_parcels`, `public.site_spatial_links` and old public compatibility objects before any retirement plan.

## Success Criteria

LandIntel is operationally cleaner when LDN can see:

- which object is canonical;
- which object is source storage;
- which object is a decision surface;
- which object is legacy;
- which object is duplicate risk;
- which object is a labelled stub;
- which object is a manual high-value upload requiring governance;
- which outputs are safe for operators;
- which outputs remain evidence-only.
