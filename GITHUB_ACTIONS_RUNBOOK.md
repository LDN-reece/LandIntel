# GitHub Actions Runbook

This project is set up to run the Scotland land ingestion worker in GitHub Actions.

## What To Use

Use this workflow:

- `.github/workflows/landintel-ingest.yml`

It supports:

- manual runs from the GitHub website
- automatic quarterly runs

## Secrets To Add In GitHub

In GitHub, open your repository, then go to:

- `Settings`
- `Secrets and variables`
- `Actions`
- `New repository secret`

Create these secrets:

### Required

- `SUPABASE_DB_URL`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

### Optional

- `BOUNDARY_AUTHKEY`
- `BOUNDARY_GEOJSON_URL`
- `ROS_API_BASE_URL`
- `ROS_CLIENT_ID`
- `ROS_CLIENT_SECRET`

## Where To Find The Supabase Values

### SUPABASE_DB_URL

In Supabase:

- `Project Settings`
- `Database`
- `Connection string`

Use the Postgres connection string.

### SUPABASE_URL

In Supabase:

- `Settings`
- `API`
- `Project URL`

### SUPABASE_SERVICE_ROLE_KEY

In Supabase:

- `Settings`
- `API`
- `service_role`

Use the service role key, not the anon key.

## How To Run It Manually

1. Open the repo in GitHub.
2. Click `Actions`.
3. Click `LandIntel Scotland Ingestion`.
4. Click `Run workflow`.
5. Choose branch `main`.
6. Click the green `Run workflow` button.

## How The Schedule Works

The workflow is already set to run automatically on this UTC schedule:

- `0 6 2 3,6,9,12 *`

That means:

- 06:00 UTC
- on the 2nd day of March, June, September, and December

## How To Check If It Worked

After the workflow finishes, go to Supabase SQL Editor and run:

```sql
select *
from analytics.v_ingest_run_summary
order by started_at desc
limit 10;
```

```sql
select count(*) as authority_count
from public.authority_aoi;
```

```sql
select count(*) as ros_parcel_count
from public.ros_cadastral_parcels;
```

## What Success Looks Like

- `authority_count = 20`
- `ros_parcel_count > 0`
- latest runs show success for:
  - `discover_sources`
  - `load_boundaries`
  - `ingest_ros_cadastral`

## If The Workflow Fails

### Database connection error

Update `SUPABASE_DB_URL` in GitHub Secrets.
If direct connection fails, use the Supabase pooler connection string instead.

### Boundary download error

Add one of these secrets:

- `BOUNDARY_AUTHKEY`
- `BOUNDARY_GEOJSON_URL`

### Storage upload error

Check that `SUPABASE_SERVICE_ROLE_KEY` is the real service role key.

### Old Python workflow still shows warnings

The older default workflow file in this repo is separate from the ingestion workflow.
The ingestion workflow to use is:

- `LandIntel Scotland Ingestion`
