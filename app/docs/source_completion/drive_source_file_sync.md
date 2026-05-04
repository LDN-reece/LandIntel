# Drive Source File Sync

## What This Adds

LandIntel now has a repo-governed control layer for the Google Drive folder:

`Scotland`
`1aXGeNM6AeqJ6IDH-jaVrZkhmjczYVTHt`

This is a source-file registry and update workflow. It records what source files exist, which ones are ready for controlled storage, and which ones must remain documentation, paused, or human-review items.

It does not ingest, parse, spatially measure, score, or interpret the datasets.

## Why It Matters Commercially

LDN has a lot of useful source material sitting in Drive. Without a registry, the system cannot distinguish between:

- a ready spatial file that should feed LandIntel;
- a PDF that is only supporting evidence;
- a manually supplied high-value asset that must not be re-uploaded;
- a loose shapefile component that is not safe to load;
- a misfiled item that needs review.

This stops source completion drifting into memory and guesswork.

## Objects

- `landintel_store.drive_source_file_registry`
- `landintel_reporting.v_drive_source_ready_upload_files`
- `landintel_reporting.v_drive_source_sync_status`
- `app/config/scotland_drive_source_manifest.yaml`
- `.github/workflows/run-landintel-drive-source-sync.yml`

## Workflow Commands

Use the GitHub Action:

`Run LandIntel Drive Source Sync`

Commands:

- `audit-drive-source-manifest`
- `sync-drive-source-manifest`
- `sync-drive-ready-upload-files`

The scheduled run is weekly and metadata-first.

## Ready-To-Upload Meaning

`ready_to_upload_flag = true` means the file is suitable for controlled source storage or document-corpus storage.

It does not mean:

- the source is ingested;
- the source is normalised;
- the source is linked to canonical sites;
- the source is evidence-grade;
- the source is commercially interpreted;
- the source is `live_complete`.

Source completion still requires the normal LandIntel standard: access, gather, store, normalise, link/measure where relevant, evidence/signals where appropriate, freshness, audit proof, bounded workflow and tests.

## Main Ready Files Found

High-value ready file groups now tracked:

- settlement boundary shapefiles and support tables;
- local planning authority boundaries;
- conservation areas;
- local landscape areas;
- school catchments;
- nature reserves and local nature conservation sites;
- forestry and woodland strategy layer;
- culverts;
- council asset register;
- VDL CSV, ODS and spatial ZIP;
- local authority boundaries;
- TPOs;
- green belt;
- contaminated land;
- ELA spatial ZIP;
- RoS cadastral authority ZIPs including `STG`, `REN`, `LAN`, `KNC`, `FFE`, `DMB`, `ELN`, `AYR`, `WLN`, `GLA`;
- LDP Glasgow PDF and spatial ZIP;
- HLA spatial ZIP and selected HLA PDFs/workbooks.

## Deliberate Caveats

### BGS Remains Paused

`single-onshore-borehole-index-dataset-26-01-26.zip` is recorded as:

- `asset_role = known_origin_manual_bulk_upload`
- `upload_status = paused`

This reflects the existing rule: BGS / Pre-SI work remains paused. The file is known-origin and high value, but this workflow must not re-upload it or treat it as final ground evidence.

### Loose Shapefile Components Are Not Ready

Loose files such as `WLN_bng.shp`, `WLN_bng.shx`, `WLN_bng.dbf`, `WLN_bng.prj`, and equivalent `GLA` components are tracked but marked not ready.

They should be packaged into a complete ZIP before controlled upload.

### Misfiled Planning Folder Item

`BritishGeologicalSurvey.github.io-master.zip` appears inside the Planning Applications folder. It is marked `misfiled_review`, not ready.

### Google Docs And Sheets

Google-native Docs and Sheets are metadata-tracked. They need export or explicit document-processing workflows before being treated as upload files.

## Optional Downloads

The workflow defaults to metadata-only.

To take bounded copies of ready files as workflow artifacts:

- run `sync-drive-ready-upload-files`;
- set `drive_source_sync_enable_downloads = true`;
- keep `drive_source_sync_max_files_per_run` small;
- keep `drive_source_sync_max_download_bytes` capped;
- provide `GOOGLE_DRIVE_API_KEY` if the folder/files are accessible to that key.

This does not upload files into source truth tables.

## What This Does Not Do

This PR does not:

- run source ingestion;
- parse ZIPs, CSVs, PDFs, ODS or XLSX files;
- move files from Drive;
- delete files from Drive;
- create duplicate source truth tables;
- run constraint measurements;
- start BGS / Pre-SI work;
- start Apex;
- build prediction models.

## Operator Use

After merge, run:

1. `run-migrations`
2. `sync-drive-source-manifest`

Then verify:

```sql
select * from landintel_reporting.v_drive_source_sync_status;

select
    source_family,
    asset_role,
    count(*) as ready_files
from landintel_reporting.v_drive_source_ready_upload_files
group by source_family, asset_role
order by source_family, asset_role;
```

## Next Commercial Move

Use `v_drive_source_ready_upload_files` to choose which source family should become `live_complete` next.

Recommended order:

1. constraints already in the bounded scaler pattern: green belt, contaminated land, culverts, TPO, conservation, landscape, ecology;
2. HLA / ELA / VDL document and table extraction, keeping register-origin weighting low;
3. RoS cadastral authority ZIP governance and parcel-source alignment;
4. LDP document/spatial policy layer completion;
5. council asset register for public ownership exclusion.
