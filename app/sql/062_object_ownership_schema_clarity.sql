create schema if not exists extensions;
create extension if not exists "uuid-ossp" with schema extensions;
create extension if not exists pgcrypto;

do $$
begin
    if to_regprocedure('extensions.uuid_generate_v4()') is null then
        execute 'create function extensions.uuid_generate_v4() returns uuid language sql volatile as ''select gen_random_uuid()''';
    end if;
end $$;

create schema if not exists landintel_store;
create schema if not exists landintel_sourced;
create schema if not exists landintel_reporting;

comment on schema landintel_store
    is 'LandIntel Data Store, the warehouse/source estate/raw and normalised data layer.';

comment on schema landintel_sourced
    is 'LandIntel Sourced Sites, the polished commercial opportunity register for LDN review.';

comment on schema landintel_reporting
    is 'Human and machine-readable views for dashboards, audits, UI and operator review.';

comment on schema public
    is 'Legacy compatibility for LandIntel domain objects; new LandIntel domain tables should not be added here unless technically unavoidable.';

create table if not exists landintel_store.object_ownership_registry (
    id uuid primary key default extensions.uuid_generate_v4(),
    schema_name text not null,
    object_name text not null,
    object_type text not null default 'table',
    row_count_estimate bigint,
    current_status text not null,
    owner_layer text not null,
    canonical_role text,
    source_family_or_module text,
    exists_in_github boolean,
    exists_in_supabase boolean,
    represented_in_repo boolean,
    safe_to_read boolean default true,
    safe_to_write boolean default false,
    safe_for_operator boolean default false,
    safe_to_retire boolean default false,
    replacement_object text,
    risk_summary text,
    recommended_action text,
    reviewed_at timestamptz default now(),
    reviewed_by text default 'codex_audit',
    metadata jsonb default '{}'::jsonb,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create unique index if not exists object_ownership_registry_object_uidx
    on landintel_store.object_ownership_registry (schema_name, object_name, object_type);

create index if not exists object_ownership_registry_status_idx
    on landintel_store.object_ownership_registry (current_status, owner_layer);

create index if not exists object_ownership_registry_module_idx
    on landintel_store.object_ownership_registry (source_family_or_module);

comment on table landintel_store.object_ownership_registry
    is 'LandIntel object ownership and schema clarity registry. It labels current, legacy, duplicate, manual-bulk-upload, reporting and future-stub objects without moving or deleting live data.';

insert into landintel_store.object_ownership_registry (
    schema_name,
    object_name,
    object_type,
    row_count_estimate,
    current_status,
    owner_layer,
    canonical_role,
    source_family_or_module,
    exists_in_github,
    exists_in_supabase,
    represented_in_repo,
    safe_to_read,
    safe_to_write,
    safe_for_operator,
    safe_to_retire,
    replacement_object,
    risk_summary,
    recommended_action,
    metadata
)
values
    ('landintel', 'canonical_sites', 'table', 38769, 'current_keep', 'landintel', 'canonical site spine', 'core_canonical', true, true, true, true, false, true, false, null, 'Primary canonical site grain for sourcing and desktop DD.', 'Keep as sole operational site anchor.', '{"audit_source":"2026_05_03_structure_audit"}'::jsonb),
    ('landintel', 'site_source_links', 'table', 8166, 'current_keep', 'landintel', 'source-to-site lineage', 'core_canonical', true, true, true, true, false, false, false, null, 'Technical lineage between source records and canonical sites.', 'Keep; browse through reporting views first.', '{}'::jsonb),
    ('landintel', 'site_reference_aliases', 'table', 8166, 'current_keep', 'landintel', 'source reference aliases', 'core_canonical', true, true, true, true, false, false, false, null, 'Reference bridge for matching and lineage.', 'Keep as technical lineage.', '{}'::jsonb),
    ('landintel', 'site_geometry_versions', 'table', 2864, 'current_keep', 'landintel', 'site geometry history', 'core_canonical', true, true, true, true, false, false, false, null, 'Stores geometry versions for source-linked sites.', 'Keep; do not use as primary browse surface.', '{}'::jsonb),
    ('landintel', 'evidence_references', 'table', 113611, 'current_keep', 'landintel', 'universal evidence layer', 'evidence_signal', true, true, true, true, false, false, false, null, 'Core source-backed evidence table.', 'Keep; ensure evidence stays idempotent and source-backed.', '{}'::jsonb),
    ('landintel', 'site_signals', 'table', 114377, 'current_keep', 'landintel', 'universal signal layer', 'evidence_signal', true, true, true, true, false, false, false, null, 'Core signal table; audit noted RLS/policy review is still required.', 'Keep; review RLS policy and duplicate signal controls in a later PR.', '{"policy_review_required":true}'::jsonb),
    ('landintel', 'site_prove_it_assessments', 'table', 38773, 'current_keep', 'landintel_sourced', 'conviction layer', 'sourced_sites', true, true, true, true, false, true, false, null, 'Commercial attention-allocation layer.', 'Keep as decision layer; do not treat as raw source truth.', '{}'::jsonb),
    ('landintel', 'site_ldn_candidate_screen', 'table', 12250, 'current_keep', 'landintel_sourced', 'LDN candidate screen', 'sourced_sites', true, true, true, true, false, true, false, null, 'Private/no-builder candidate screen; not legal ownership proof.', 'Keep as sourced-site decision support.', '{}'::jsonb),

    ('landintel', 'source_catalog', 'table', 31, 'current_keep', 'landintel_store', 'source catalog', 'source_estate', true, true, true, true, false, false, false, null, 'Source catalog used by source estate.', 'Keep; align with source estate registry over time.', '{}'::jsonb),
    ('landintel', 'source_endpoint_catalog', 'table', 31, 'current_keep', 'landintel_store', 'endpoint catalog', 'source_estate', true, true, true, true, false, false, false, null, 'Endpoint catalog for managed source estate.', 'Keep.', '{}'::jsonb),
    ('landintel', 'source_estate_registry', 'table', 166, 'current_keep', 'landintel_store', 'source lifecycle registry', 'source_estate', true, true, true, true, false, true, false, null, 'Current lifecycle registry; zero trusted-for-review sources in latest proof.', 'Keep; source trust must remain proof-gated.', '{"trusted_for_review_count":0}'::jsonb),
    ('landintel', 'source_freshness_states', 'table', 184, 'current_keep', 'landintel_store', 'source freshness state', 'source_estate', true, true, true, true, false, false, false, null, 'Freshness tracking for source estate.', 'Keep; review RLS policy later.', '{"policy_review_required":true}'::jsonb),
    ('landintel', 'source_reconcile_state', 'table', 309842, 'current_but_expensive_scale_risk', 'landintel_store', 'reconcile state', 'source_estate', true, true, true, true, false, false, false, null, 'Large reconcile state table.', 'Keep; avoid broad manual reads.', '{}'::jsonb),
    ('landintel', 'source_reconcile_queue', 'table', 309841, 'current_but_expensive_scale_risk', 'landintel_store', 'reconcile queue', 'source_estate', true, true, true, true, false, false, false, null, 'Large reconcile queue table.', 'Keep; monitor queue health and bounded workers.', '{}'::jsonb),
    ('landintel', 'source_expansion_events', 'table', 900, 'current_keep', 'landintel_store', 'source expansion audit log', 'source_estate', true, true, true, true, false, false, false, null, 'Audit trail for source expansion runs.', 'Keep; review RLS policy later.', '{"policy_review_required":true}'::jsonb),
    ('public', 'source_registry', 'table', 25, 'current_keep', 'public_legacy_compatibility', 'legacy source registry', 'source_estate', true, true, true, true, false, false, false, 'landintel.source_estate_registry', 'Original source registry remains for compatibility.', 'Keep for compatibility until all code paths use landintel source estate objects.', '{}'::jsonb),
    ('public', 'ingest_runs', 'table', 342, 'current_keep', 'public_legacy_compatibility', 'legacy ingest run log', 'source_estate', true, true, true, true, false, false, false, null, 'Operational ingest/run log used by current workflows.', 'Keep; do not retire until replacement run log is proven.', '{}'::jsonb),

    ('landintel', 'planning_application_records', 'table', 378334, 'current_but_expensive_scale_risk', 'landintel_store', 'planning application source records', 'planning', true, true, true, true, false, false, false, null, 'Large planning corpus.', 'Keep; improve extraction before further broad ingestion.', '{}'::jsonb),
    ('landintel', 'planning_decision_facts', 'table', 8250, 'current_keep', 'landintel_store', 'planning decision facts', 'planning', true, true, true, true, false, false, false, null, 'Extracted decision facts from planning records.', 'Keep; expand extraction coverage.', '{}'::jsonb),
    ('landintel', 'site_planning_decision_context', 'table', 317, 'current_keep', 'landintel_store', 'site planning decision context', 'planning', true, true, true, true, false, false, false, null, 'Site-level planning decision context is present but narrow.', 'Keep; scale context carefully.', '{}'::jsonb),
    ('landintel', 'planning_appeal_records', 'table', 0, 'stub_future_module', 'landintel_store', 'appeal source records', 'planning_appeals', true, true, true, true, false, false, false, null, 'Schema exists but no appeal rows are landed.', 'Keep as labelled stub until DPEA/LRB adapters land data.', '{}'::jsonb),
    ('landintel', 'planning_appeal_documents', 'table', 0, 'stub_future_module', 'landintel_store', 'appeal documents', 'planning_appeals', true, true, true, true, false, false, false, null, 'Schema exists but no appeal documents are landed.', 'Keep as labelled stub.', '{}'::jsonb),
    ('landintel', 'planning_document_records', 'table', 0, 'stub_future_module', 'landintel_store', 'planning document records', 'planning_documents', true, true, true, true, false, false, false, null, 'Schema exists but no planning document rows are landed.', 'Keep as labelled stub until adapters are built.', '{}'::jsonb),
    ('landintel', 'planning_document_extractions', 'table', 0, 'stub_future_module', 'landintel_store', 'planning document extractions', 'planning_documents', true, true, true, true, false, false, false, null, 'Schema exists but no document facts are extracted.', 'Keep as labelled stub.', '{}'::jsonb),
    ('landintel', 'section75_obligation_records', 'table', 0, 'stub_future_module', 'landintel_store', 'Section 75 obligations', 'planning_documents', true, true, true, true, false, false, false, null, 'Schema exists but no Section 75 obligations are extracted.', 'Keep as labelled stub.', '{}'::jsonb),

    ('landintel', 'ldp_site_records', 'table', 3336, 'current_keep', 'landintel_store', 'LDP/policy source records', 'land_supply_policy', true, true, true, true, false, false, false, null, 'Policy/allocation source records.', 'Keep as evidence layer, not automatic positive sourcing proof.', '{}'::jsonb),
    ('landintel', 'hla_site_records', 'table', 2864, 'current_keep', 'landintel_store', 'HLA register records', 'land_supply_policy', true, true, true, true, false, false, false, null, 'HLA is register/context evidence, not availability or commercial proof.', 'Keep; require independent corroboration for REVIEW/PURSUE confidence.', '{"corroboration_required":true}'::jsonb),
    ('landintel', 'ela_site_records', 'table', 1799, 'current_keep', 'landintel_store', 'ELA register records', 'land_supply_policy', true, true, true, true, false, false, false, null, 'ELA is register/context evidence, not commercial proof.', 'Keep; require corroboration and review RLS policy.', '{"corroboration_required":true,"policy_review_required":true}'::jsonb),
    ('landintel', 'vdl_site_records', 'table', 3142, 'current_keep', 'landintel_store', 'VDL register records', 'land_supply_policy', true, true, true, true, false, false, false, null, 'VDL is regeneration/underuse context, not deliverability proof.', 'Keep; require corroboration and review RLS policy.', '{"corroboration_required":true,"policy_review_required":true}'::jsonb),
    ('landintel', 'settlement_boundary_records', 'table', 514, 'current_keep', 'landintel_store', 'settlement boundaries', 'land_supply_policy', true, true, true, true, false, false, false, null, 'Settlement boundary context for planning/location interpretation.', 'Keep.', '{}'::jsonb),

    ('public', 'constraint_layer_registry', 'table', 63, 'current_keep', 'public_legacy_compatibility', 'constraint layer registry', 'constraints', true, true, true, true, false, false, false, 'landintel_store.constraint_layer_registry', 'Current constraint registry still physically lives in public.', 'Keep; expose via landintel_store compatibility view.', '{}'::jsonb),
    ('public', 'constraint_source_features', 'table', 202778, 'current_but_expensive_scale_risk', 'public_legacy_compatibility', 'constraint feature store', 'constraints', true, true, true, true, false, false, false, 'landintel_store.constraint_source_features', 'Large feature table; broad scans are expensive.', 'Keep; use layer-by-layer bounded measurement.', '{}'::jsonb),
    ('public', 'site_constraint_measurements', 'table', 1638, 'current_keep', 'public_legacy_compatibility', 'measured constraint facts', 'constraints', true, true, true, true, false, false, false, 'landintel_store.site_constraint_measurements', 'Current measured facts but coverage is still thin.', 'Keep; scale coverage layer-by-layer.', '{"coverage_needs_scaling":true}'::jsonb),
    ('public', 'site_constraint_group_summaries', 'table', 688, 'current_keep', 'public_legacy_compatibility', 'constraint group summaries', 'constraints', true, true, true, true, false, false, false, 'landintel_store.site_constraint_group_summaries', 'Derived summaries from measured facts.', 'Keep.', '{}'::jsonb),
    ('public', 'site_commercial_friction_facts', 'table', 688, 'current_keep', 'public_legacy_compatibility', 'constraint friction facts', 'constraints', true, true, true, true, false, true, false, 'landintel_store.site_commercial_friction_facts', 'Operator-readable constraint facts.', 'Keep; ensure facts remain source-backed.', '{}'::jsonb),
    ('public', 'site_constraint_measurement_scan_state', 'table', 1614, 'current_keep', 'public_legacy_compatibility', 'constraint scan state', 'constraints', true, true, true, true, false, false, false, null, 'Resumable scan state for constraints.', 'Keep; do not treat as evidence surface.', '{}'::jsonb),

    ('public', 'ros_cadastral_parcels', 'table', 990059, 'current_but_expensive_scale_risk', 'public_legacy_compatibility', 'RoS parcel source store', 'title_parcel', true, true, true, true, false, false, false, 'landintel_store.ros_cadastral_parcels', 'Large parcel store used for candidate linking; not ownership proof.', 'Keep; use bounded candidate linking and expose via compatibility view.', '{}'::jsonb),
    ('public', 'land_objects', 'table', 989741, 'duplicate_candidate', 'public_legacy_compatibility', 'legacy land object store', 'title_parcel', true, true, true, true, false, false, false, 'public.ros_cadastral_parcels', 'Near-duplicate parcel-era store with high storage cost.', 'Investigate dependencies before any retire/archive plan.', '{"duplicate_of_candidate":"public.ros_cadastral_parcels"}'::jsonb),
    ('public', 'land_parcels', 'table', 0, 'legacy_candidate_retire', 'public_legacy_compatibility', 'legacy parcel stub', 'title_parcel', false, true, false, true, false, false, true, 'public.ros_cadastral_parcels', 'Empty Supabase-only legacy/stub table.', 'Retire only after dependency proof.', '{}'::jsonb),
    ('public', 'site_ros_parcel_link_candidates', 'table', 18209, 'current_keep', 'public_legacy_compatibility', 'site-to-RoS parcel candidates', 'title_parcel', true, true, true, true, false, false, false, 'landintel_store.site_ros_parcel_link_candidates', 'Candidate parcel links; not ownership proof.', 'Keep; expose candidate status clearly.', '{}'::jsonb),
    ('public', 'site_title_resolution_candidates', 'table', 1864, 'current_keep', 'public_legacy_compatibility', 'title candidate bridge', 'title_parcel', true, true, true, true, false, false, false, 'landintel_store.site_title_resolution_candidates', 'Title-number candidate bridge; SCT identifiers should not be surfaced as title numbers.', 'Keep; only valid title-shaped candidates should reach operator views.', '{"sct_as_title_blocked":true}'::jsonb),
    ('public', 'site_title_validation', 'table', 1864, 'current_keep', 'public_legacy_compatibility', 'title validation evidence', 'title_parcel', true, true, true, true, false, false, false, 'landintel_store.site_title_validation', 'Contains rejected SCT-like values as audit evidence; not ownership proof.', 'Keep but operators should use filtered views.', '{"ownership_confirmed":false}'::jsonb),
    ('landintel', 'title_order_workflow', 'table', 2250, 'current_keep', 'landintel_store', 'title order workflow', 'title_control', true, true, true, true, false, true, false, null, 'Pre-title workflow; title spend decision support only.', 'Keep; human title review remains required.', '{}'::jsonb),
    ('landintel', 'title_review_records', 'table', 0, 'repo_defined_empty_stub', 'landintel_store', 'human title review records', 'title_control', true, true, true, true, false, true, false, null, 'No human title reviews are recorded; ownership remains unconfirmed.', 'Keep as human layer; do not infer ownership before rows exist.', '{"ownership_confirmed":false}'::jsonb),
    ('landintel', 'ownership_control_signals', 'table', 2250, 'current_keep', 'landintel', 'pre-title control signals', 'title_control', true, true, true, true, false, true, false, null, 'Pre-title control signal layer; not legal ownership proof.', 'Keep with explicit caveats.', '{}'::jsonb),
    ('landintel', 'site_urgent_address_title_pack', 'table', 184, 'current_keep', 'landintel_sourced', 'urgent address/title pack', 'title_control', true, true, true, true, false, true, false, null, 'Urgent evidence pack for address/title candidate visibility; ownership remains unconfirmed.', 'Keep; review through operator views only.', '{}'::jsonb),
    ('public', 'land_object_toid_enrichment', 'table', 0, 'legacy_candidate_retire', 'public_legacy_compatibility', 'legacy TOID enrichment', 'title_parcel', true, true, true, true, false, false, true, null, 'Empty legacy land_objects enrichment table.', 'Retire only after dependency proof.', '{}'::jsonb),
    ('public', 'land_object_title_matches', 'table', 0, 'legacy_candidate_retire', 'public_legacy_compatibility', 'legacy title matches', 'title_parcel', true, true, true, true, false, false, true, 'public.site_title_resolution_candidates', 'Empty legacy title match table.', 'Retire only after dependency proof.', '{}'::jsonb),
    ('public', 'land_object_address_links', 'table', 0, 'legacy_candidate_retire', 'public_legacy_compatibility', 'legacy address links', 'title_parcel', true, true, true, true, false, false, true, null, 'Empty legacy address link table.', 'Retire only after dependency proof.', '{}'::jsonb),
    ('public', 'site_spatial_links', 'table', 0, 'legacy_candidate_retire', 'public_legacy_compatibility', 'legacy spatial links', 'title_parcel', true, true, true, true, false, false, true, 'landintel.site_source_links', 'Empty table whose comment references absent public.sites/site_locations model.', 'Retire only after dependency proof.', '{}'::jsonb),

    ('landintel', 'bgs_records', 'table', 4830, 'current_keep', 'landintel_store', 'BGS enrichment records', 'ground_abnormal', true, true, true, true, false, false, false, 'landintel_store.bgs_records', 'Current BGS enrichment table.', 'Keep.', '{}'::jsonb),
    ('landintel', 'bgs_borehole_master', 'table', 1350790, 'known_origin_manual_bulk_upload', 'landintel_store', 'BGS borehole master', 'ground_abnormal', false, true, false, true, false, false, false, 'landintel_store.bgs_borehole_master', 'High-value manual bulk upload from BGS Single Onshore Borehole Index CSV; governance incomplete.', 'Govern and enrich; do not delete, re-upload or treat as final ground-condition interpretation.', '{"risk_classification":"high_value_governance_incomplete","source":"BGS Single Onshore Borehole Index","manual_bulk_upload":true,"trusted_interpreted_ground_evidence":false}'::jsonb),
    ('landintel', 'bgs_borehole_master_uploads', 'table', 1, 'known_origin_manual_bulk_upload', 'landintel_store', 'BGS upload manifest tracker', 'ground_abnormal', false, true, false, true, false, false, false, null, 'Upload tracker for manually supplied BGS borehole master CSV.', 'Backfill repo-governed manifest and refresh policy.', '{"risk_classification":"high_value_governance_incomplete","manual_bulk_upload":true}'::jsonb),
    ('landintel', 'site_ground_risk_context', 'table', 613, 'current_keep', 'landintel_store', 'site ground risk context', 'ground_abnormal', true, true, true, true, false, false, false, null, 'Derived desktop ground risk context; not engineering certainty.', 'Keep with caveats.', '{}'::jsonb),
    ('landintel', 'site_abnormal_cost_flags', 'table', 613, 'current_keep', 'landintel_store', 'abnormal cost flags', 'ground_abnormal', true, true, true, true, false, false, false, null, 'Desktop abnormal review flags; not QS/engineering proof.', 'Keep with caveats.', '{}'::jsonb),

    ('landintel', 'open_location_spine_features', 'table', 340224, 'current_but_expensive_scale_risk', 'landintel_store', 'open location feature corpus', 'open_location_context', true, true, true, true, false, false, false, 'landintel_store.open_location_spine_features', 'Large open-data feature corpus.', 'Keep; use bounded landing and targeted context refresh.', '{}'::jsonb),
    ('landintel', 'site_open_location_spine_context', 'table', 34815, 'current_keep', 'landintel_store', 'site open-location context', 'open_location_context', true, true, true, true, false, false, false, 'landintel_store.site_open_location_spine_context', 'Nearest/proximity context rows.', 'Keep; avoid generic noisy promotion to operator verdicts.', '{}'::jsonb),
    ('landintel', 'open_location_spine_ingest_progress', 'table', 73, 'current_keep', 'landintel_store', 'open location progress state', 'open_location_context', true, true, true, true, false, false, false, null, 'Progress/resume state for open-data landing.', 'Keep.', '{}'::jsonb),
    ('landintel', 'amenity_assets', 'table', 1000, 'current_keep', 'landintel_store', 'amenity assets', 'amenities', true, true, true, true, false, false, false, null, 'Amenity asset subset exists.', 'Keep; convert to location-strength facts where useful.', '{}'::jsonb),
    ('landintel', 'site_amenity_context', 'table', 3206, 'current_keep', 'landintel_store', 'site amenity context', 'amenities', true, true, true, true, false, false, false, null, 'Amenity context exists.', 'Keep; avoid unsupported location claims.', '{}'::jsonb),
    ('landintel', 'market_area_metrics', 'table', 96, 'current_keep', 'landintel_store', 'market area metrics', 'market_context', true, true, true, true, false, false, false, null, 'Market context metrics exist, not final valuation.', 'Keep; do not infer RLV/demand certainty.', '{}'::jsonb),
    ('landintel', 'site_market_context', 'table', 1644, 'current_keep', 'landintel_store', 'site market context', 'market_context', true, true, true, true, false, false, false, null, 'Site market context exists, not buyer proof.', 'Keep; improve evidence extraction.', '{}'::jsonb),
    ('landintel', 'demographic_area_metrics', 'table', 160, 'current_keep', 'landintel_store', 'demographic metrics', 'demographics', true, true, true, true, false, false, false, null, 'Demographic context metrics exist.', 'Keep as context only.', '{}'::jsonb),
    ('landintel', 'site_demographic_context', 'table', 1961, 'current_keep', 'landintel_store', 'site demographic context', 'demographics', true, true, true, true, false, false, false, null, 'Site demographic context exists.', 'Keep as context only; do not claim buyer demand certainty.', '{}'::jsonb)
on conflict (schema_name, object_name, object_type) do update set
    row_count_estimate = excluded.row_count_estimate,
    current_status = excluded.current_status,
    owner_layer = excluded.owner_layer,
    canonical_role = excluded.canonical_role,
    source_family_or_module = excluded.source_family_or_module,
    exists_in_github = excluded.exists_in_github,
    exists_in_supabase = excluded.exists_in_supabase,
    represented_in_repo = excluded.represented_in_repo,
    safe_to_read = excluded.safe_to_read,
    safe_to_write = excluded.safe_to_write,
    safe_for_operator = excluded.safe_for_operator,
    safe_to_retire = excluded.safe_to_retire,
    replacement_object = excluded.replacement_object,
    risk_summary = excluded.risk_summary,
    recommended_action = excluded.recommended_action,
    reviewed_at = now(),
    reviewed_by = excluded.reviewed_by,
    metadata = landintel_store.object_ownership_registry.metadata || excluded.metadata,
    updated_at = now();

do $$
declare
    compatibility_views text[][] := array[
        array['landintel_store.planning_application_records', 'landintel.planning_application_records'],
        array['landintel_store.planning_decision_facts', 'landintel.planning_decision_facts'],
        array['landintel_store.hla_site_records', 'landintel.hla_site_records'],
        array['landintel_store.ela_site_records', 'landintel.ela_site_records'],
        array['landintel_store.vdl_site_records', 'landintel.vdl_site_records'],
        array['landintel_store.ldp_site_records', 'landintel.ldp_site_records'],
        array['landintel_store.settlement_boundary_records', 'landintel.settlement_boundary_records'],
        array['landintel_store.evidence_references', 'landintel.evidence_references'],
        array['landintel_store.site_signals', 'landintel.site_signals'],
        array['landintel_store.ros_cadastral_parcels', 'public.ros_cadastral_parcels'],
        array['landintel_store.land_objects', 'public.land_objects'],
        array['landintel_store.constraint_layer_registry', 'public.constraint_layer_registry'],
        array['landintel_store.constraint_source_features', 'public.constraint_source_features'],
        array['landintel_store.site_constraint_measurements', 'public.site_constraint_measurements'],
        array['landintel_store.site_constraint_group_summaries', 'public.site_constraint_group_summaries'],
        array['landintel_store.site_commercial_friction_facts', 'public.site_commercial_friction_facts'],
        array['landintel_store.site_title_validation', 'public.site_title_validation'],
        array['landintel_store.site_ros_parcel_link_candidates', 'public.site_ros_parcel_link_candidates'],
        array['landintel_store.site_title_resolution_candidates', 'public.site_title_resolution_candidates'],
        array['landintel_store.bgs_records', 'landintel.bgs_records'],
        array['landintel_store.bgs_borehole_master', 'landintel.bgs_borehole_master'],
        array['landintel_store.open_location_spine_features', 'landintel.open_location_spine_features'],
        array['landintel_store.site_open_location_spine_context', 'landintel.site_open_location_spine_context']
    ];
    view_pair text[];
    target_relation text;
    source_relation text;
begin
    foreach view_pair slice 1 in array compatibility_views loop
        if to_regclass(view_pair[2]) is not null then
            target_relation :=
                quote_ident(split_part(view_pair[1], '.', 1))
                || '.'
                || quote_ident(split_part(view_pair[1], '.', 2));
            source_relation :=
                quote_ident(split_part(view_pair[2], '.', 1))
                || '.'
                || quote_ident(split_part(view_pair[2], '.', 2));

            execute
                'create or replace view '
                || target_relation
                || ' with (security_invoker = true) as select * from '
                || source_relation;
        else
            raise notice 'Skipping compatibility view because source relation is not present in this database.';
        end if;
    end loop;
end $$;

create or replace view landintel_reporting.v_object_ownership_matrix with (security_invoker = true) as
select
    schema_name,
    object_name,
    object_type,
    current_status,
    owner_layer,
    canonical_role,
    safe_to_read,
    safe_to_write,
    safe_for_operator,
    safe_to_retire,
    replacement_object,
    risk_summary,
    recommended_action,
    reviewed_at
from landintel_store.object_ownership_registry;

comment on view landintel_reporting.v_object_ownership_matrix
    is 'Human and machine-readable matrix showing LandIntel object ownership, status, safety flags, risks and recommended actions.';

grant usage on schema landintel_store to authenticated;
grant usage on schema landintel_sourced to authenticated;
grant usage on schema landintel_reporting to authenticated;

grant select on landintel_store.object_ownership_registry to authenticated;
grant select on all tables in schema landintel_store to authenticated;
grant select on all tables in schema landintel_reporting to authenticated;
