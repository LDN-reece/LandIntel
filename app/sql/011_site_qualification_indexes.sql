create index if not exists sites_workflow_status_idx
    on public.sites (workflow_status);

create index if not exists sites_primary_land_object_idx
    on public.sites (primary_land_object_id);

create index if not exists sites_primary_ros_parcel_idx
    on public.sites (primary_ros_parcel_id);

create index if not exists site_locations_authority_idx
    on public.site_locations (authority_name);

create index if not exists site_locations_settlement_idx
    on public.site_locations (nearest_settlement);

create index if not exists site_locations_within_boundary_idx
    on public.site_locations (within_settlement_boundary);

create index if not exists site_locations_geometry_gix
    on public.site_locations using gist (geometry);

create index if not exists site_locations_centroid_gix
    on public.site_locations using gist (centroid);

create unique index if not exists site_parcels_primary_uidx
    on public.site_parcels (site_id, is_primary)
    where is_primary = true;

create index if not exists site_parcels_site_idx
    on public.site_parcels (site_id);

create index if not exists site_parcels_land_object_idx
    on public.site_parcels (land_object_id);

create index if not exists site_parcels_ros_parcel_idx
    on public.site_parcels (ros_parcel_id);

create index if not exists site_geometry_components_site_idx
    on public.site_geometry_components (site_id);

create index if not exists site_geometry_components_source_idx
    on public.site_geometry_components (source_table, source_record_id);

create unique index if not exists site_geometry_components_primary_uidx
    on public.site_geometry_components (site_id, source_table, source_record_id, relation_type);

create index if not exists planning_records_site_idx
    on public.planning_records (site_id, decision_date desc);

create index if not exists planning_records_outcome_idx
    on public.planning_records (application_outcome);

create index if not exists planning_context_records_site_idx
    on public.planning_context_records (site_id);

create index if not exists planning_context_records_type_status_idx
    on public.planning_context_records (context_type, context_status);

create index if not exists site_constraints_site_idx
    on public.site_constraints (site_id);

create index if not exists site_constraints_type_severity_idx
    on public.site_constraints (constraint_type, severity);

create index if not exists comparable_market_records_site_idx
    on public.comparable_market_records (site_id, sale_date desc);

create index if not exists comparable_market_records_strength_idx
    on public.comparable_market_records (record_strength);

create index if not exists site_buyer_matches_site_idx
    on public.site_buyer_matches (site_id);

create unique index if not exists site_buyer_matches_uidx
    on public.site_buyer_matches (site_id, buyer_profile_id);

create index if not exists site_analysis_runs_site_created_idx
    on public.site_analysis_runs (site_id, created_at desc);

create index if not exists site_analysis_runs_status_idx
    on public.site_analysis_runs (status);

create unique index if not exists site_signals_run_key_uidx
    on public.site_signals (analysis_run_id, signal_key);

create index if not exists site_signals_site_key_idx
    on public.site_signals (site_id, signal_key);

create unique index if not exists site_interpretations_run_key_uidx
    on public.site_interpretations (analysis_run_id, interpretation_key);

create index if not exists site_interpretations_site_category_idx
    on public.site_interpretations (site_id, category, priority);

create index if not exists evidence_references_site_idx
    on public.evidence_references (site_id, created_at desc);

create index if not exists evidence_references_source_idx
    on public.evidence_references (source_table, source_record_id);

create index if not exists site_review_status_history_site_idx
    on public.site_review_status_history (site_id, created_at desc);

create index if not exists site_refresh_queue_status_requested_idx
    on public.site_refresh_queue (status, requested_at);

create unique index if not exists site_refresh_queue_pending_uidx
    on public.site_refresh_queue (site_id, refresh_scope)
    where status in ('pending', 'processing');

create index if not exists analytics_site_search_cache_workflow_idx
    on analytics.site_search_cache (workflow_status);

create index if not exists analytics_site_search_cache_authority_idx
    on analytics.site_search_cache (authority_name);

create index if not exists analytics_site_search_cache_surface_idx
    on analytics.site_search_cache (possible_fatal_count, positive_count, updated_at desc);
