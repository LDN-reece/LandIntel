# Register Context Cleanse And Merge

## Purpose

HLA, ELA, VDL, LDP and settlement records are already useful in Supabase, but they must not be read as direct commercial proof. This layer keeps the raw source tables intact and gives LandIntel one clean register/context surface for sourcing and DD.

This does not move data, remove data, re-upload Drive files, or create a second register truth table.

## What Exists Now

- `landintel.hla_site_records` is already populated. In plain terms, HLA_site_records is already populated, but the source-completion matrix can understate its live rows and linked-site status.
- `landintel.ela_site_records` is the strongest linked register source currently.
- `landintel.vdl_site_records` is already populated. In plain terms, VDL_site_records is already populated. The recent operational issue was invalid source geometry during linking, not missing VDL data.
- `landintel.ldp_site_records` and `landintel.settlement_boundary_records` are policy/location context layers, not final sourcing proof.
- Drive register files are governance and refresh inputs. They are not duplicate truth and should not be uploaded again without a source-specific reason.

## Clean Read Surfaces

- `landintel_store.v_register_context_records_clean`
- `landintel_store.v_register_context_records_current`
- `landintel_sourced.v_site_register_context`

Use these for operator and sourcing views instead of reading HLA, ELA, VDL, LDP and settlement raw tables directly.

## Reporting Surfaces

- `landintel_reporting.v_register_context_merge_status`
- `landintel_reporting.v_register_context_duplicate_diagnostics`
- `landintel_reporting.v_register_context_source_completion_overlay`
- `landintel_reporting.v_register_context_freshness`

These views show whether the system is loaded, duplicated, stale, matrix-misaligned, or ready for a targeted refresh.

## Evidence Weighting

Register presence remains context only.

- HLA supports housing land supply visibility.
- ELA supports candidate/emerging land context.
- VDL supports regeneration or underuse visibility.
- LDP supports planning policy context.
- Settlement boundaries support settlement-edge reasoning.

None of these prove availability, deliverability, clean ownership, buyer demand, acceptable abnormal risk or commercial viability. Independent corroboration is still required before a site becomes a strong review or pursue candidate.

## Freshness Workflow

Refresh register/context sources through the repo workflows:

- `ingest-hla` uses `src.source_phase_runner`.
- `ingest-ela`, `ingest-vdl`, `ingest-ldp` and `ingest-settlement-boundaries` use the source expansion runner.
- `audit-register-context` proves merge status, matrix alignment, duplicates and freshness.

The DD source data load workflow now routes HLA through the correct core runner and finishes with `audit-register-context`.

## Geometry Safety

VDL and other register layers can contain invalid polygons. The site-linking path now repairs polygonal geometry before intersection checks and skips unrepairable rows. This avoids one bad source geometry blocking a full bounded run.

## Commercial Meaning

This gives LDN cleaner evidence discipline:

- registers tell LandIntel where to look;
- the clean surface prevents duplicate register confidence;
- raw source tables stay preserved for audit;
- operator views can show why a site surfaced without pretending the register proves a deal.

## Next Operational Sequence

After merge:

1. Run `run-migrations`.
2. Run `audit-register-context`.
3. Run targeted `ingest-hla` only if HLA freshness requires it.
4. Run targeted `ingest-vdl` only if VDL freshness requires it.
5. Re-run `audit-register-context`.
6. Use `landintel_sourced.v_site_register_context` in sourced-site DD surfaces.

Do not reload Drive register files just because they exist. Enrich existing source families only where the audit proves a real gap.
