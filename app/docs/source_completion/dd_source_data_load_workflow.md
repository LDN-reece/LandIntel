# LandIntel DD Source Data Load Workflow

This workflow is the data-first reset for LandIntel's desktop due diligence source estate.

It does not score sites, interpret opportunity quality, run Apex, or measure all site/layer intersections. Its job is narrower and more important:

1. register source ownership;
2. sync the Google Drive source-file registry;
3. load source data into Supabase where a bounded loader exists;
4. keep failures isolated by source family;
5. produce audit proof of what landed and what is still blocked.

The workflow is:

`Run LandIntel DD Source Data Load`

File:

`.github/workflows/run-landintel-dd-source-data-load.yml`

## Why This Exists

LandIntel had started to combine collection, measurement, and interpretation in the same operating motion. That caused partial states to look more advanced than they were.

The correct chain is now:

`registered -> data_loaded -> geometry_ready -> measured -> interpreted`

This workflow only targets the first three stages.

## Source Groups

The workflow supports these scopes:

- `all`
- `planning_policy_registers`
- `core_constraints`
- `open_location_context`
- `commercial_context`
- `drive_registry_only`

## Core DD Sources Covered

Planning and policy:

- LDP
- settlement boundaries
- HLA
- ELA
- VDL

Constraints:

- SEPA flood
- Coal Authority
- green belt
- contaminated land
- culverts
- Tree Preservation Orders
- conservation areas
- HES heritage designations
- NatureScot ecology/designations

Open location context:

- OS Boundary-Line
- OS Open Roads
- OS Open Rivers
- OS Open Names
- OS Open Greenspace
- OS Open Built Up Areas
- NaPTAN
- statistics.gov.scot
- OpenTopography SRTM

Commercial/source context:

- amenities
- demographics
- market context
- power infrastructure
- planning appeals
- planning documents
- Companies House
- FCA

Google Drive:

- the curated Scotland Drive source manifest is always synced;
- bounded Drive downloads are optional and artifact-only;
- Drive downloads do not by themselves create source truth tables;
- Drive files must enrich the appropriate existing source family rather than create duplicate truth systems.

## Safety Rules

The workflow deliberately sets:

- `SOURCE_EXPANSION_CONSTRAINT_MEASURE_MODE=off`
- `SOURCE_EXPANSION_MAX_MEASURE_FEATURES=0`
- `SOURCE_EXPANSION_MAX_MEASURE_LAYERS=0`
- `OPEN_LOCATION_SPINE_CONTEXT_REFRESH_MODE=disabled`

That means the workflow is for source loading, not broad measurement.

It does not:

- run Apex;
- build prediction models;
- run broad all-site/all-layer scans;
- perform OS Places bulk address spend;
- claim ownership certainty;
- interpret BGS boreholes as final ground truth.

## How To Run

Use GitHub Actions:

Workflow:

`Run LandIntel DD Source Data Load`

Recommended overnight inputs:

- `dd_source_scope = all`
- `dd_step_timeout_minutes = 35`
- `dd_continue_on_source_error = true`
- `dd_enable_drive_downloads = false`
- `dd_open_data_max_download_bytes = 90000000`
- `dd_open_data_max_features_per_source = 2500`
- `dd_open_data_max_csv_scan_rows = 500000`

## How To Read The Result

The workflow uploads:

- `dd-source-data-load-status`
- `drive-source-sync-summary`
- optionally `drive-source-ready-files`

Then check:

```sql
select current_status, count(*)
from landintel_reporting.v_source_completion_matrix
group by current_status
order by current_status;

select source_family, source_key, current_status, row_count, freshness_record_count, known_blocker, next_action
from landintel_reporting.v_source_completion_matrix
order by priority, source_family, source_key;

select *
from landintel_reporting.v_drive_source_enrichment_queue
order by immediate_add_flag desc, priority_rank nulls last, source_family, file_name;
```

## What Remains After This Workflow

After source data is loaded, LandIntel still needs separate measurement and interpretation phases:

1. layer-by-layer site measurements;
2. yes/no/affected area/percentage outputs;
3. commercial DD interpretation;
4. final operator view per site.

This workflow is the foundation for that, not the whole DD answer.
