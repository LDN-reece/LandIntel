# LandIntel MVP Worker

This app is the GitHub Actions worker for the LandIntel Scotland MVP.

The operating model is:

- Codex writes and maintains the code
- GitHub Actions runs the workflows
- Supabase stores the operational data

## Workflow split

### Lean foundation workflow

`Run LandIntel Lean` keeps the parcel-and-boundary foundation clean and cost-controlled.

It is responsible for:

- authority boundaries
- operational RoS parcel retention
- storage cleanup
- audit footprint checks

### Source-intelligence workflow

`Run LandIntel Sources` populates the private `landintel` schema.

It is responsible for:

- planning history
- HLA
- canonical site reconciliation
- BGS enrichment
- evidence and source links

## Supabase data design

### Public operational base

- `public.authority_aoi`
- `public.source_registry`
- `public.ingest_runs`
- `public.ros_cadastral_parcels`

### Private reasoning layer

- `landintel.canonical_sites`
- `landintel.site_reference_aliases`
- `landintel.site_geometry_versions`
- `landintel.site_source_links`
- `landintel.planning_application_records`
- `landintel.ldp_site_records`
- `landintel.settlement_boundary_records`
- `landintel.hla_site_records`
- `landintel.bgs_records`
- `landintel.flood_records`
- `landintel.evidence_references`
- `landintel.site_signals`
- `landintel.site_assessments`

### Traceability views

- `landintel.v_source_ingest_summary`
- `landintel.v_site_traceability`

## Current MVP source packs

Live now:

- Scottish local authority boundaries
- RoS cadastral parcels on the lean path
- Planning Applications: Official - Scotland
- Housing Land Supply - Scotland
- BGS OGC API enrichment

Planned next:

- LDP and settlement boundaries
- flood
- TOID and title enrichment
- scoring and assessment refresh

## Key design rules

- `canonical_site` is the internal object of record
- source datasets stay separate and link back to canonical sites
- every meaningful conclusion must be traceable to source rows
- GitHub Actions should be the normal execution path
- Supabase should hold operational truth, not endless raw bulk copies
