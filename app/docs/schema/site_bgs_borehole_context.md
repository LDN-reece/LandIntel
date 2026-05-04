# Site BGS Borehole Context

## Purpose

This phase adds the first bounded Pre-SI ground intelligence workflow.

It converts the governed BGS Single Onshore Borehole Index into site-level context for candidate sites only.

It does not download borehole scans, run OCR, scrape planning documents, re-upload BGS data, or infer final ground conditions.

## New Objects

### `landintel_store.site_bgs_borehole_context`

Warehouse table storing measured site-to-borehole context:

- nearest borehole distance;
- boreholes inside the site;
- boreholes within 100m, 250m, 500m and 1km;
- deep borehole counts;
- log availability counts;
- confidential borehole counts;
- evidence density signal;
- ground uncertainty signal;
- safe-use caveat.

### `landintel_reporting.v_site_bgs_borehole_context`

Operator-safe reporting view joining the context back to the canonical site spine.

This is the view for human review and future DD surfaces.

## Workflow Commands

`refresh-site-bgs-borehole-context`

Refreshes a bounded candidate-site batch and then prints audit proof.

`audit-site-bgs-borehole-context`

Reads the live context table and reporting view without changing data.

## Bounded Selection

The refresh command uses `landintel_reporting.v_constraint_priority_sites`.

Priority order:

1. title spend candidates
2. review queue
3. LDN candidate screen
4. Prove It candidates
5. wider canonical sites

Default batch size is 25. The workflow has explicit controls for batch size, priority band, authority and force refresh.

## Safe Use

Safe:

- borehole proximity context;
- borehole evidence density;
- log availability context;
- ground uncertainty triage;
- deciding whether manual Pre-SI review is worth the next action.

Not safe:

- final ground-condition interpretation;
- piling conclusions;
- grouting conclusions;
- remediation conclusions;
- abnormal-cost quantification;
- replacement for intrusive site investigation.

## Commercial Meaning

This helps LDN avoid wasting time on sites where ground uncertainty needs early attention, while avoiding false engineering certainty.

The point is not to answer "what is the ground condition?"

The point is to answer:

"Is there enough nearby ground-record evidence to justify the next pound or hour on Pre-SI review?"

## No Broad Scan Rule

This workflow is candidate-site first and batch-limited.

It must not be turned into a broad all-site scan by default.

Future phases may add scan registry, scan fetch and extraction, but those are separate opt-in phases.
