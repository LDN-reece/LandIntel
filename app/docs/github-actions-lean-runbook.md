# GitHub Actions Lean Runbook (Retired)

`Run LandIntel Lean` is retired for Phase One.

Do not use it for source orchestration, parcel loading, footprint cleanup, or Supabase writes.

## Current status

- Workflow: `Run LandIntel Lean (Retired)`
- Purpose: audit marker only
- Supabase secrets loaded: no
- Supabase writes performed: no
- Runner: `app/src/lean_ops.py` hard-fails if called

## Replacement workflow

Use GitHub Actions -> `Run LandIntel Sources`.

Controlled Phase One run order:

1. `run-migrations`
2. `source-estate-maintenance`
3. `audit-source-estate`
4. `audit-source-freshness`
5. `publish-planning-links`
6. `ingest-hla`
7. `process-reconcile-queue`
8. `refresh-affected-sites`
9. `ingest-bgs`
10. `audit-source-footprint`
11. `audit-source-freshness`
12. `audit-source-estate`

## Reason

The old lean path belonged to the parcel-foundation MVP. Phase One now runs against the canonical-site model and live analytics surfaces. Keeping the lean workflow active would risk old source assumptions interfering with the full Phase One build.
