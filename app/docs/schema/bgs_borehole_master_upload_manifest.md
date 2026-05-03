# BGS Borehole Master Upload Manifest

## Current Classification

`landintel.bgs_borehole_master` is a known-origin manual bulk upload, not unknown-origin junk.

Status:

- object: `landintel.bgs_borehole_master`
- classification: `known_origin_manual_bulk_upload`
- risk classification: `high_value_governance_incomplete`
- source family: `ground_abnormal`
- safe to delete: no
- safe to re-upload automatically: no
- safe for final interpreted ground-condition evidence: no

## Source Provenance

- Source: BGS Single Onshore Borehole Index.
- Source form: master raw CSV downloaded by Reece and supplied manually.
- Upload route: manual file supplied to Codex and uploaded into Supabase.
- Current live row count: approximately `1,350,790`.
- Upload tracker table: `landintel.bgs_borehole_master_uploads`.
- Source date: to be confirmed from `source_snapshot_date`.
- File date: to be confirmed from `source_file_name`, `original_filename`, `uploaded_at` and any retained local/source metadata.
- Exact loaded row count: to be confirmed from `landintel.bgs_borehole_master_uploads.row_count`.

## Current Governance Gaps

The asset is valuable, but governance is incomplete:

- no repo-owned migration currently creates the physical master table;
- no repo-owned upload manifest previously existed;
- no documented refresh policy previously existed;
- no automated source connector is intentionally approved;
- no enrichment workflow currently turns the master table into source-backed site facts;
- no operator view should treat raw borehole presence as engineering conclusion.

## Refresh Policy

Manual refresh only for now.

Do not:

- re-upload the CSV automatically;
- schedule re-downloads;
- overwrite or replace the current table;
- infer ground-condition certainty from the index alone.

Future refresh requires a deliberate connector/import PR with:

- source URL and licensing notes;
- file naming and snapshot date;
- checksum;
- row count reconciliation;
- schema contract validation;
- idempotent load strategy;
- evidence/freshness event recording.

## Safe Use

Safe now:

- borehole proximity intelligence;
- borehole density and coverage context;
- identifying whether borehole records exist near a candidate site;
- supporting a manual ground-risk review decision.

Not safe now:

- not safe as final ground condition interpretation;
- final ground-condition interpretation;
- piling, grouting or remediation conclusions;
- abnormal cost quantification;
- site investigation replacement;
- claims that a site is safe or unsafe.

## Future Enrichment Workflow

The intended workflow is:

0. Define site-to-borehole proximity as the primary safe enrichment.
1. Select candidate canonical sites in bounded batches.
2. Measure nearest borehole distance.
3. Count boreholes within agreed buffers, such as 250m, 500m and 1km.
4. Record borehole density context.
5. Create an evidence reference in `landintel.evidence_references` for each source-backed proximity fact.
6. Create restrained `landintel.site_signals` for borehole proximity/density.
7. Feed only contextual outputs into `landintel.site_ground_risk_context`.
8. Feed review flags into `landintel.site_abnormal_cost_flags` where appropriate.

All outputs must remain desktop-context wording.

## Required Before Trusting For Review

- Confirm source snapshot date.
- Confirm original filename.
- Confirm loaded row count from upload tracker.
- Confirm checksum if available.
- Add repo-owned table contract or migration/commentary.
- Add bounded enrichment workflow.
- Add proof views showing row count, linked site count, evidence count and signal count.
