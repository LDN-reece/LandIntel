# LDN Simple Sourcing Control

Use this when you want to run LandIntel without touching command-line inputs.

## Where to click

1. Open GitHub.
2. Go to `LDN-reece/LandIntel`.
3. Click `Actions`.
4. Click `LDN - Simple Sourcing Control`.
5. Click `Run workflow`.
6. Pick one `stage`.
7. Leave the other fields alone unless you deliberately want a focused run.

## Safest option

Pick:

`SAFE - Run 01 to 07 without OS Places spend`

This runs the normal sourcing/DD preparation sequence without spending OS Places trial calls.

## Stage guide

- `01 - Open data location spine`
  Lands open-data files first, then runs a small light-touch site context pass. If the context pass is deferred, the raw data landing still counts and later sourcing stages can continue.

- `02 - Flood constraints`
  Measures SEPA flood against sites.

- `03 - Core constraints`
  Measures Coal Authority, HES and NatureScot constraints.

- `04 - Constraint audit`
  Shows proof of measured constraint coverage.

- `05 - Refresh sourcing intelligence`
  Refreshes planning, title readiness, market, amenities, demographics, power, abnormal risk, assessments, Prove It and LDN candidate screen.

- `06 - Audit sourcing outputs without address spend`
  Audits the sourcing outputs without using OS Places.

- `07 - Final proof audit`
  Runs the final proof checks.

- `08 - Small OS Places urgent address test`
  Runs a tiny OS Places test only. Use sparingly because it spends trial calls.

## Defaults

- Leave `authority_filter` blank for Scotland-wide.
- Leave `batch_size` at `250`.
- Leave `runtime_minutes` at `15`.

The open-data location spine intentionally uses a smaller internal context batch than the main sourcing batch. That stops heavy OS boundary/amenity geometry refreshes from blocking the whole run.

## Rules

- Run one workflow at a time.
- If a run fails, do not repeatedly rerun it.
- Do not use the OS Places test unless you specifically want address enrichment.
- The proof is in the completed Actions summary and Supabase analytics views.
