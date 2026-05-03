# BGS Borehole Clean Normalisation

## Purpose

This PR makes the existing `landintel.bgs_borehole_master` asset usable without changing the data.

It does not re-upload the BGS CSV, download a new dataset, create a second truth table, move rows, delete rows or infer engineering conclusions.

## Business Meaning

The BGS Single Onshore Borehole Index is valuable because it tells LandIntel where borehole records exist and whether logs may be available near a site.

That helps LDN decide whether a site needs Pre-SI ground review before spending further time, title money or buyer effort.

It does not prove ground conditions. It does not prove piling, grouting, remediation or abnormal-cost requirements.

## Existing Source Asset

- Warehouse table: `landintel.bgs_borehole_master`
- Source: BGS Single Onshore Borehole Index
- Origin: known-origin manual bulk upload
- Risk classification: high_value_governance_incomplete
- Current use: proximity, borehole density and log-availability context
- Current non-use: final ground-condition interpretation

The source was manually supplied and uploaded. It should not be re-uploaded or replaced by automation until a deliberate source connector/import PR exists.

## New Views

### `landintel_store.v_bgs_borehole_master_clean`

Clean governed warehouse view over the master table.

It exposes:

- source provenance fields;
- registration/reference fields;
- parsed coordinates and geometries;
- borehole name/reference fields;
- depth and height fields;
- AGS/log availability;
- data-quality flags;
- operator use status;
- safe-use caveat.

### `landintel_reporting.v_bgs_borehole_operator_index`

Operator-safe lookup surface for borehole context.

This is the view a future Pre-SI workflow should read before creating site evidence.

### `landintel_reporting.v_bgs_borehole_data_quality`

Source quality summary grouped by source snapshot and file.

It exposes:

- row count;
- geometry parse count;
- missing geometry count;
- confidential count;
- depth parse count;
- log-availability count;
- known-year coverage;
- latest row update timestamp;
- safe-use caveat.

## Safe Use Rules

Safe:

- nearest borehole context;
- borehole density context;
- borehole log availability;
- evidence that BGS index records exist near a site;
- manual Pre-SI triage.

Not safe:

- final ground-condition interpretation;
- piling conclusions;
- grouting conclusions;
- remediation conclusions;
- abnormal-cost quantification;
- replacement for site investigation;
- claims that ground is safe or unsafe.

## Why This Matters Commercially

Before this PR, the BGS master was high-value but too raw for controlled operator use.

After this PR, LandIntel has a governed route to convert borehole availability into disciplined DD context:

1. A site can be flagged as having nearby ground records.
2. LDN can decide whether Pre-SI review is worth the next pound or hour.
3. The system avoids fake engineering certainty.
4. Later workflows can create source-backed evidence and signals without touching the raw table.

## Object Ownership

The migration also refreshes `landintel_store.object_ownership_registry` for:

- `landintel.bgs_borehole_master`
- `landintel_store.v_bgs_borehole_master_clean`
- `landintel_reporting.v_bgs_borehole_operator_index`
- `landintel_reporting.v_bgs_borehole_data_quality`

The master table remains a high-value governed warehouse asset, not a retire candidate.

## Future PRs

The next BGS-related PR should be a bounded enrichment workflow:

1. Select canonical sites in small batches.
2. Measure nearest borehole distance.
3. Count boreholes within agreed buffers.
4. Create `landintel.evidence_references`.
5. Create restrained `landintel.site_signals`.
6. Feed only contextual outputs into `landintel.site_ground_risk_context`.
7. Add review flags to `landintel.site_abnormal_cost_flags` where justified.

OCR or borehole log extraction remains opt-in and should only be built when the target subset is commercially justified.
