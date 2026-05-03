# BGS Borehole Master Schema Contract

## Scope

This document describes the observed live schema for `landintel.bgs_borehole_master` and `landintel.bgs_borehole_master_uploads`.

It is a contract for governance and future enrichment. It is not a licence to re-upload, reinterpret or automate the source.

## Table: `landintel.bgs_borehole_master`

Observed live columns from Supabase metadata:

| Column | Type | Required | Meaning |
| --- | --- | --- | --- |
| `bgs_id` | bigint | yes | Internal loaded row identifier / BGS record identifier used as primary record key. |
| `source_upload_id` | uuid | no | Link to upload tracker row where available. |
| `source_snapshot_date` | date | yes | Source snapshot date recorded during load. |
| `source_file_name` | text | yes | Source file name used during load. |
| `source_row_number` | bigint | yes | Original/source row number. |
| `source_storage_bucket` | text | no | Storage bucket where source file was retained, if available. |
| `source_storage_path` | text | no | Storage path where source file was retained, if available. |
| `qs` | text | no | Source field retained from BGS index. |
| `numb_raw` | text | no | Raw borehole number value. |
| `numb` | integer | no | Parsed borehole number value. |
| `bsuff` | text | no | Borehole suffix/source field. |
| `regno` | text | no | Registration/reference number. |
| `rt` | text | no | Source record/type field. |
| `grid_reference_raw` | text | no | Raw grid reference. |
| `grid_reference` | text | no | Normalised grid reference. |
| `easting_raw` | text | no | Raw easting value. |
| `northing_raw` | text | no | Raw northing value. |
| `x_raw` | text | no | Raw X coordinate value. |
| `y_raw` | text | no | Raw Y coordinate value. |
| `easting` | double precision | no | Parsed British National Grid easting. |
| `northing` | double precision | no | Parsed British National Grid northing. |
| `geom_27700` | geometry | no | British National Grid point geometry. |
| `geom_wgs84` | geometry | no | WGS84 point geometry. |
| `confidentiality_raw` | text | no | Raw confidentiality marker. |
| `is_confidential` | boolean | yes | Parsed confidentiality flag. |
| `start_height_raw` | text | no | Raw start height value. |
| `start_height_m` | double precision | no | Parsed start height in metres. |
| `name_raw` | text | no | Raw borehole name. |
| `name_normalised` | text | no | Normalised borehole name. |
| `depth_raw` | text | no | Raw depth value. |
| `depth_m` | double precision | no | Parsed depth in metres. |
| `depth_status` | text | yes | Depth parsing/status marker. |
| `ags_log_url_raw` | text | no | Raw AGS/log URL value. |
| `ags_log_url` | text | no | Normalised AGS/log URL where available. |
| `has_ags_log` | boolean | yes | Whether an AGS/log URL is present. |
| `date_known_raw` | text | no | Raw known date value. |
| `date_known_year` | integer | no | Parsed known year. |
| `date_known_type_raw` | text | no | Raw date-known type marker. |
| `date_entered_raw` | text | no | Raw entered-date value. |
| `date_entered` | date | no | Parsed entered date. |
| `api_last_seen_at` | timestamptz | no | API/source last-seen timestamp if enriched later. |
| `api_raw_payload` | jsonb | yes | Retained raw/API payload metadata. |
| `created_at` | timestamptz | yes | Load/create timestamp. |
| `updated_at` | timestamptz | yes | Last update timestamp. |

## Table: `landintel.bgs_borehole_master_uploads`

Observed live columns from Supabase metadata:

| Column | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | uuid | yes | Upload manifest row identifier. |
| `storage_bucket` | text | yes | Storage bucket for uploaded source file. |
| `storage_path` | text | yes | Storage path for uploaded source file. |
| `original_filename` | text | yes | Original filename supplied for upload. |
| `source_format` | text | yes | Source file format, expected CSV for current manual upload. |
| `source_snapshot_date` | date | no | Source snapshot date if recorded. |
| `upload_status` | text | yes | Upload/load state. |
| `file_size_bytes` | bigint | no | File size where recorded. |
| `row_count` | bigint | no | Loaded row count where recorded. |
| `uploaded_at` | timestamptz | yes | Upload timestamp. |
| `loaded_at` | timestamptz | no | Load completion timestamp. |
| `error_message` | text | no | Load error, if any. |
| `created_at` | timestamptz | yes | Manifest row creation timestamp. |
| `updated_at` | timestamptz | yes | Manifest row update timestamp. |

## Contract Rules

1. The table is source/warehouse data, not interpreted ground evidence.
2. The table should be read through `landintel_store.bgs_borehole_master` where possible.
3. Source rows should not be overwritten without a deliberate refresh PR.
4. Future enrichment must preserve `source_upload_id`, source file metadata and row-level provenance.
5. Future site context must create source-backed evidence references and restrained site signals.
6. No output may claim safe ground, piling requirement, grouting requirement or remediation cost from this table alone.

## Future Validation Checks

Before any automated refresh exists, a later PR should add checks for:

- non-null `source_snapshot_date`;
- non-null `source_file_name`;
- non-null `source_row_number`;
- coordinate parse rate;
- geometry parse rate;
- duplicate `bgs_id`;
- upload tracker row count reconciliation;
- source file checksum where available.
