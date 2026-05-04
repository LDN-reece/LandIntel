# BGS Borehole Scan Registry And Queue

## What This Builds

Phase G2 adds a governed source-link registry and a bounded candidate-site queue for BGS borehole scan/log review.

It does not fetch scans, run OCR, download PDFs, store PDF blobs in Postgres, re-upload BGS data, or interpret ground conditions.

## Why It Matters Commercially

This gives LDN a clean way to see where source ground records appear worth manual Pre-SI review, without confusing availability of a borehole log with engineering certainty.

The output helps decide whether the next pound or hour should go into reviewing ground source records for an already interesting site.

## Objects

`landintel_store.bgs_borehole_scan_registry`

Source-link registry over `landintel_store.v_bgs_borehole_master_clean`. It stores BGS records that advertise an AGS/log URL and keeps them as `linked_not_downloaded`.

`landintel_store.bgs_borehole_scan_fetch_queue`

Candidate-site-first queue linking priority sites to nearby BGS records with available logs. Queue rows stay `linked_not_downloaded` until a separate bounded fetch workflow is approved.

`landintel_reporting.v_bgs_scan_registry`

Operator-safe registry surface.

`landintel_reporting.v_bgs_scan_queue`

Operator-safe queue surface for manual Pre-SI review planning.

## Commands

`refresh-bgs-borehole-scan-registry`

Refreshes the source-link registry from the governed clean BGS master. Default batch is fixed in GitHub Actions and bounded.

`queue-bgs-borehole-scans`

Queues nearby log-bearing BGS records for candidate sites with measured BGS context. Default queueing is candidate-site first, site-batch limited, max-records-per-site limited, and max-rows-per-run limited.

`audit-bgs-borehole-scan-queue`

Prints proof counts for registry rows, queued sites, queue status and an operator sample.

## Safe Use

Safe:

- identifying BGS records with source log links;
- identifying candidate sites with nearby log-bearing boreholes;
- planning manual Pre-SI source review;
- proving the queue is bounded and auditable.

Not safe:

- final ground-condition interpretation;
- piling, grouting, remediation or abnormal-cost conclusions;
- automatic site pass/fail language;
- treating a scan/log link as reviewed evidence.

## No Broad Scan Rule

The queue uses already-measured candidate-site BGS context and caps the number of sites and records per run.

It must not become an all-site/all-record fetch engine.

## Next Phase Boundary

Future Phase G3 may add bounded scan/log fetching, but only as a separately scoped workflow with explicit caps and storage manifest rules.

Until then, all scan/log assets remain linked-not-downloaded.
