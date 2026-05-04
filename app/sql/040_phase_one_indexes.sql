create unique index if not exists landintel_canonical_sites_site_code_uidx
    on landintel.canonical_sites (site_code);

create index if not exists landintel_canonical_sites_authority_idx
    on landintel.canonical_sites (authority_name, workflow_status);

create unique index if not exists landintel_planning_application_records_source_uidx
    on landintel.planning_application_records (authority_name, source_record_id);

create index if not exists landintel_planning_application_records_canonical_idx
    on landintel.planning_application_records (canonical_site_id, updated_at desc);

create unique index if not exists landintel_hla_site_records_source_uidx
    on landintel.hla_site_records (authority_name, source_record_id);

create index if not exists landintel_hla_site_records_canonical_idx
    on landintel.hla_site_records (canonical_site_id, updated_at desc);

create unique index if not exists landintel_ldp_site_records_source_uidx
    on landintel.ldp_site_records (authority_name, source_record_id);

create index if not exists landintel_ldp_site_records_canonical_idx
    on landintel.ldp_site_records (canonical_site_id, updated_at desc);

create unique index if not exists landintel_settlement_boundary_records_source_uidx
    on landintel.settlement_boundary_records (authority_name, source_record_id);

create unique index if not exists landintel_bgs_records_source_uidx
    on landintel.bgs_records (authority_name, source_record_id);

create index if not exists landintel_bgs_records_canonical_idx
    on landintel.bgs_records (canonical_site_id, updated_at desc);

create unique index if not exists landintel_flood_records_source_uidx
    on landintel.flood_records (authority_name, source_record_id);

create index if not exists landintel_flood_records_canonical_idx
    on landintel.flood_records (canonical_site_id, updated_at desc);

create unique index if not exists landintel_ela_site_records_source_uidx
    on landintel.ela_site_records (authority_name, source_record_id);

create index if not exists landintel_ela_site_records_canonical_idx
    on landintel.ela_site_records (canonical_site_id, updated_at desc);

create unique index if not exists landintel_vdl_site_records_source_uidx
    on landintel.vdl_site_records (authority_name, source_record_id);

create index if not exists landintel_vdl_site_records_canonical_idx
    on landintel.vdl_site_records (canonical_site_id, updated_at desc);

create unique index if not exists landintel_site_source_links_active_uidx
    on landintel.site_source_links (canonical_site_id, source_family, source_dataset, source_record_id)
    where active_flag = true;

create index if not exists landintel_site_source_links_family_idx
    on landintel.site_source_links (source_family, source_dataset, canonical_site_id);

create index if not exists landintel_site_source_links_record_idx
    on landintel.site_source_links (source_record_id, active_flag);

create unique index if not exists landintel_site_reference_aliases_active_uidx
    on landintel.site_reference_aliases (
        canonical_site_id,
        source_family,
        source_dataset,
        authority_name,
        coalesce(plan_period, ''),
        coalesce(raw_reference_value, '')
    )
    where active_flag = true;

create index if not exists landintel_site_reference_aliases_reference_idx
    on landintel.site_reference_aliases (normalized_reference_value, authority_name, active_flag);

create index if not exists landintel_evidence_references_site_idx
    on landintel.evidence_references (canonical_site_id, created_at desc);

create index if not exists landintel_evidence_references_source_idx
    on landintel.evidence_references (source_family, source_record_id, active_flag);

create unique index if not exists landintel_source_reconcile_state_source_uidx
    on landintel.source_reconcile_state (source_family, authority_name, source_record_id);

create unique index if not exists landintel_source_reconcile_queue_state_uidx
    on landintel.source_reconcile_queue (state_id);

create index if not exists landintel_source_reconcile_queue_status_idx
    on landintel.source_reconcile_queue (status, priority desc, created_at asc);

create unique index if not exists landintel_site_signals_site_key_uidx
    on landintel.site_signals (canonical_site_id, signal_key);

create index if not exists landintel_site_signals_group_idx
    on landintel.site_signals (signal_group, source_family);

create unique index if not exists landintel_site_assessments_site_version_uidx
    on landintel.site_assessments (canonical_site_id, assessment_version);

create index if not exists landintel_site_assessments_rank_idx
    on landintel.site_assessments (overall_tier, overall_rank_score desc, latest_assessment_at desc);

create index if not exists landintel_canonical_site_refresh_queue_status_idx
    on landintel.canonical_site_refresh_queue (status, next_attempt_at, created_at);

create index if not exists landintel_canonical_site_refresh_queue_site_idx
    on landintel.canonical_site_refresh_queue (canonical_site_id, created_at desc);

create index if not exists landintel_site_review_events_site_idx
    on landintel.site_review_events (canonical_site_id, created_at desc);

create index if not exists landintel_site_manual_overrides_site_idx
    on landintel.site_manual_overrides (canonical_site_id, created_at desc);

create index if not exists landintel_site_change_events_site_idx
    on landintel.site_change_events (canonical_site_id, created_at desc);

create index if not exists landintel_site_change_events_priority_idx
    on landintel.site_change_events (alert_priority, created_at desc);

create unique index if not exists landintel_site_geometry_diagnostics_site_uidx
    on landintel.site_geometry_diagnostics (canonical_site_id);

create index if not exists public_site_spatial_links_site_idx
    on public.site_spatial_links (site_id, linked_record_table, linked_record_id);

create index if not exists public_site_title_validation_site_idx
    on public.site_title_validation (site_id, created_at desc);

create unique index if not exists public_constraint_layer_registry_layer_key_uidx
    on public.constraint_layer_registry (layer_key);

create index if not exists public_constraint_source_features_layer_idx
    on public.constraint_source_features (constraint_layer_id, authority_name);

create index if not exists public_site_constraint_measurements_site_idx
    on public.site_constraint_measurements (site_id, site_location_id, measured_at desc);

create index if not exists public_site_constraint_group_summaries_site_idx
    on public.site_constraint_group_summaries (site_id, site_location_id, measured_at desc);

create index if not exists public_site_commercial_friction_facts_site_idx
    on public.site_commercial_friction_facts (site_id, site_location_id, created_at desc);
