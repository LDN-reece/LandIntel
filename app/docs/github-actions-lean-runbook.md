# GitHub Actions Lean Runbook

Use this workflow instead of the legacy `Run LandIntel` workflow when you need to control Supabase storage growth.

## Workflow

Open GitHub Actions and run `Run LandIntel Lean`.

## Commands

### `audit-operational-footprint`
Reports the current operational footprint in Supabase:
- authority count
- parcel count
- parcel rows under the minimum acreage threshold
- parcel rows at or above the threshold
- mirrored `land_objects` count
- parcel counts by authority

Recommended first step after any parcel ingest.

### `cleanup-operational-footprint`
Deletes:
- `public.ros_cadastral_parcels` rows below the minimum acreage threshold
- mirrored RoS parcel rows from `public.land_objects` when `MIRROR_LAND_OBJECTS=false`

Recommended arguments:
- `--min-area-acres 4`

### `ingest-ros-cadastral-lean`
Runs the RoS parcel ingest but only persists parcel rows at or above the minimum acreage threshold.
By default it does **not** mirror parcel geometries into `public.land_objects`.

Recommended arguments:
- leave blank to use the workflow defaults
- or `--min-area-acres 4`

### `full-refresh-lean`
Runs the lean sequence in one pass:
1. migrations
2. source discovery
3. boundary load
4. lean parcel ingest
5. footprint cleanup

Use this instead of the legacy `full-refresh` workflow command.

## Safe first-pass sequence

1. `audit-operational-footprint`
2. `cleanup-operational-footprint --min-area-acres 4`
3. `audit-operational-footprint`
4. `full-refresh-lean`
5. `audit-operational-footprint`

## Notes

- Default minimum operational acreage: `4`
- Default parcel mirroring into `public.land_objects`: `false`
- Use `audit_backend = none` until the operational footprint is stable
