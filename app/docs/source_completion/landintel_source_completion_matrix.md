# LandIntel Source Completion Matrix

## Purpose

This phase creates the operator-safe source completion surface for LandIntel.

It does not ingest data, move data, delete data, or promote a source to trusted status. It exposes what is currently known about each source across:

- `landintel.source_estate_registry`
- `landintel.source_catalog`
- `landintel.source_endpoint_catalog`
- `public.source_registry`
- `analytics.v_landintel_source_estate_matrix`
- `analytics.v_phase_one_source_estate_matrix`
- GitHub Actions workflow command availability
- repo config files under `app/config`

The live reporting surface is:

`landintel_reporting.v_source_completion_matrix`

## Status Taxonomy

`live_complete` means the source has enough live proof to be treated as complete for its current LandIntel role. This is deliberately hard to reach.

`live_partial` means the source has some live proof, such as rows, freshness, workflow coverage, measurements, evidence, or signals, but one or more completion gates remain missing.

`registered_only` means the source exists in a registry or catalog, but live execution proof is not strong enough yet.

`discovery_only` means the source helps discovery but is not an operational ingest/source of review evidence.

`manual_only` means the source is intentionally human-controlled or manually supplied.

`blocked` means access, adapter, licence, workflow, or lifecycle evidence currently prevents reliable operational use.

`retired_or_replaced` means the source is no longer intended to be an active operational source.

## Completion Standard

A source is not complete because it has a table, config row, command name, or registered endpoint.

A source is complete only when the relevant parts of this chain are proven:

- discover/access
- gather
- store
- normalise
- link/measure where relevant
- evidence/signals where relevant
- freshness state
- audit proof
- bounded workflow
- tests or contract checks

## Commercial Meaning

This matters because LandIntel must not scale on unclear source ownership. LDN needs to know which data can support a sourcing or DD decision and which data is still only context, a stub, or a manual workflow.

The matrix prevents false certainty. It lets us prioritise the next source work by commercial value:

- title/control and planning spine first
- constraints and open location spine second
- source adapters and document workflows only after the underlying source estate is readable

## Current High-Level Read

The live system now has the right source architecture, but not every source is complete.

Strongest live/partial areas:

- canonical/source/evidence/signal spine
- title readiness and operator-safe title/control views
- Prove It and sourced-site operator surfaces
- BGS borehole master governance and clean exposure
- RoS parcel model decision surface
- constraint coverage scaler/reporting views

Still incomplete or gated:

- DPEA and LRB appeal adapters
- council planning documents and Section 75 adapters
- local press and council agenda intelligence
- several power/network capacity sources
- market context beyond partial open context
- source-level tests for every individual adapter
- full open-data universe freshness and context coverage

## Key Design Choices

The matrix is a reporting view, not a new truth table.

It does not replace:

- `landintel.source_estate_registry`
- `landintel.source_catalog`
- `public.source_registry`
- `analytics.v_landintel_source_estate_matrix`

Instead, it makes their overlap readable.

`public.source_registry` remains legacy compatibility. New LandIntel source ownership should be governed through `landintel_store.object_ownership_registry`, `landintel.source_estate_registry`, and reporting views.

## Workflow Safety

Some commands exist but are broad-run risky. The matrix marks those through `broad_run_risk`.

Examples:

- `ingest-bulk-download-universe`
- open-data source landing commands
- constraint ingestion and measurement commands
- large planning/HLA/HLS ingestion commands

These must remain bounded by workflow inputs and should not be treated as casual operator buttons.

## Source Completion CSV

Static repo-side matrix:

`app/docs/source_completion/landintel_source_completion_matrix.csv`

Live database matrix:

`landintel_reporting.v_source_completion_matrix`

The CSV is a repo audit companion. The live view is the actual database truth surface.

## Live Proof Command

GitHub Actions command:

`audit-source-completion-matrix`

This command runs migrations, reads the reporting view, and prints bounded proof:

- source counts by completion status
- source counts by priority and status
- workflow gap sample
- completion-ready sample

It does not ingest data, download data, move rows, or run broad spatial scans.

## Next PR Sequence

1. Run `audit-source-completion-matrix` after migration to prove the live matrix.
2. Tighten per-source workflow/test evidence where the matrix says `registered_only` or `blocked`.
3. Build bounded source-specific adapters only for the highest-value gaps.
4. Do not broaden ingestion until the relevant source row is visible in this matrix with a bounded workflow command.
