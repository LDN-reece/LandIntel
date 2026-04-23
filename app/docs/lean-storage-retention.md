# Lean Storage And Retention

This project now uses a manifest-first storage design so LandIntel keeps a full audit trail without turning Supabase Postgres into a raw data lake.

## Core rule

- Postgres stores operational truth.
- Supabase Storage stores heavy source files only when they are worth retaining.
- `public.source_artifacts` stores the audit trail for both retained and non-retained files.

That means every downloaded or derived artifact can still be traced through:

- ingest run
- source name
- authority
- file role
- checksum
- size
- storage location if retained
- retention class
- expiry and deletion timestamps

## Supabase split

### Postgres / PostGIS

Keep only:

- canonical sites
- site links and reconciliation state
- evidence, signals, assessments, interpretations
- lean search cache
- authority AOIs
- source registry
- ingest runs
- `public.source_artifacts`
- candidate-linked parcel and title records

Do not keep nationwide raw archives or repeat intermediate extracts here.

### Supabase Storage

Use two buckets:

- `landintel-working`
  Short-lived downloads and intermediate artifacts.
- `landintel-ingest-audit`
  Longer-lived source copies that are worth retaining for replay or audit.

## Retention classes

- `working`
  Default for temporary source downloads and intermediate outputs.
- `archive`
  Keep when replay or external audit matters.
- `permanent`
  Use sparingly for irreplaceable evidence or reference files.

The current defaults are:

- `ARTIFACT_WORKING_RETENTION_DAYS=30`
- `ARTIFACT_ARCHIVE_RETENTION_DAYS=365`

Expired Storage objects should be deleted with:

```bash
cd app
python -m src.main prune-audit-artifacts
```

The manifest row remains in Postgres after deletion, so the audit trail survives.

If an older run already filled the database with low-value parcel rows, clean it back down with:

```bash
cd app
python -m src.main cleanup-operational-footprint --min-area-acres 4
```

## Scotland-wide sourcing strategy

Do not load all Scotland-wide raw geometry into Postgres first.

Use this order instead:

1. Hold raw nationwide parcels, OS datasets, PDFs, schedules, and large extracts in Supabase Storage.
2. Let GitHub Actions read those files and run reconciliation, filtering, and scoring.
3. Promote only operational records into Postgres:
   - candidate sites
   - linked parcel subsets
   - title and TOID bridges for touched candidates
   - evidence refs
   - signals
   - assessments

This is the only realistic way to keep Supabase lean while still supporting a national Scottish audit trail.

## Subscription guidance

As checked on April 13, 2026, Supabase documentation states:

- Pro includes `8 GB` database disk per project, then `$0.125/GB/month`
- Pro includes `100 GB` Storage, then `$0.021/GB/month`

Sources:

- [Billing overview](https://supabase.com/docs/guides/platform/billing-on-supabase)
- [Manage Disk size usage](https://supabase.com/docs/guides/platform/manage-your-usage/disk-size)
- [Manage Storage size usage](https://supabase.com/docs/guides/platform/manage-your-usage/storage-size)

### Practical recommendation

- `Pro` is enough to start if Postgres is kept operational-only and heavy files live in Storage with retention.
- `Pro` is not enough if you try to keep all Scotland-wide raw parcels, OS geometry, intermediate joins, and derived archives in Postgres.
- `Team` does not solve this by itself, because the included database and file quotas are not fundamentally larger than Pro. It mainly adds collaboration and governance.

For MVP:

- target Postgres under `8 GB`
- treat Storage as the heavy layer
- prune working artifacts aggressively
- only archive files that matter for replay or evidence
- set `MIN_OPERATIONAL_AREA_ACRES=4`
- keep `MIRROR_LAND_OBJECTS=false` unless you have a specific downstream need for the duplicate geometry mirror

## What this changes commercially

This storage design keeps the expensive part focused on the reasoning engine rather than wasted database space. The value sits in:

- canonical site identity
- reconciliation
- evidence
- signals
- scoring
- routing

not in paying to keep every raw bulk file in Postgres forever.

## Cheapest practical route to first-pass sourcing

For the current MVP, the friendliest storage route is:

1. Keep `AUDIT_ARTIFACT_BACKEND=none` for normal runs.
2. Keep `PERSIST_STAGING_ROWS=false`.
3. Keep `MIN_OPERATIONAL_AREA_ACRES=4`.
4. Keep `MIRROR_LAND_OBJECTS=false`.

That means:

- files are manifested for audit but not retained in Storage by default
- staging tables stay temporary
- parcels under `4 acres` never enter the operational parcel table
- the duplicate `public.land_objects` geometry mirror is not populated

This is the cheapest route that still supports a real first pass at sourcing based on:

- size
- location
- planning context and history
- LDP status
- constraints

It is not the final candidate-led architecture yet, but it is the least expensive route that still gets you into live sourcing work.
