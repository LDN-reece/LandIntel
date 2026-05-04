# BGS Borehole Scan Asset Manifests

## What This Builds

Phase G3 adds a bounded asset-manifest workflow for queued BGS borehole scan/log links.

It creates:

- `landintel_store.bgs_borehole_scan_assets`
- `landintel_reporting.v_bgs_scan_assets`
- `fetch-bgs-borehole-scans`
- `audit-bgs-borehole-scan-assets`

## Commercial Purpose

This gives LDN a controlled Pre-SI evidence path: when a sourced site is commercially interesting, LandIntel can show which BGS source records are worth reviewing next.

It helps decide whether manual ground-source review is worth time or spend. It does not produce engineering certainty.

## Safety Rules

The database stores URI and storage-manifest metadata only.

It must not store:

- PDF blobs in Postgres;
- OCR output;
- interpreted ground-condition facts;
- abnormal-cost values.

When storage is not configured or downloads are disabled, rows stay `linked_not_downloaded`.

## Workflow Behaviour

`fetch-bgs-borehole-scans`

Default GitHub Actions behaviour keeps downloads disabled and creates manifest rows only.

The command is:

- candidate-site first;
- queue-based;
- max-assets-per-run capped;
- storage-manifest only;
- safe to run without storage configured.

`audit-bgs-borehole-scan-assets`

Reports asset row counts, status counts and an operator sample.

## Safe Use

Safe:

- source-availability triage;
- deciding whether to manually review BGS logs/scans;
- identifying sites with queued source records;
- proving that file handling is bounded.

Not safe:

- final ground-condition interpretation;
- piling, grouting, remediation or abnormal-cost conclusions;
- automatic pursuit/rejection;
- saying a ground issue exists because a source file exists.

## Next Boundary

Phase G4 can extract text from stored assets later, but OCR must remain opt-in and extraction must still be candidate-site first.
