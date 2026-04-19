create unique index if not exists site_spatial_links_site_record_role_uidx
    on public.site_spatial_links (site_location_id, linked_record_table, linked_record_id, link_role);

create index if not exists site_spatial_links_site_idx
    on public.site_spatial_links (site_id);

create index if not exists site_spatial_links_location_idx
    on public.site_spatial_links (site_location_id);

create unique index if not exists site_title_validation_site_title_method_uidx
    on public.site_title_validation (site_location_id, normalized_title_number, validation_method);

create index if not exists site_title_validation_site_idx
    on public.site_title_validation (site_id);

create index if not exists site_title_validation_status_idx
    on public.site_title_validation (validation_status, validation_method);

create index if not exists constraint_layer_registry_group_idx
    on public.constraint_layer_registry (constraint_group, constraint_type);

create index if not exists constraint_layer_registry_active_idx
    on public.constraint_layer_registry (is_active, layer_key);

create unique index if not exists constraint_source_features_layer_feature_uidx
    on public.constraint_source_features (constraint_layer_id, source_feature_key);

create index if not exists constraint_source_features_layer_idx
    on public.constraint_source_features (constraint_layer_id);

create index if not exists constraint_source_features_authority_idx
    on public.constraint_source_features (authority_name);

create index if not exists constraint_source_features_severity_idx
    on public.constraint_source_features (severity_label);

create index if not exists constraint_source_features_source_reference_idx
    on public.constraint_source_features (source_reference);

create index if not exists constraint_source_features_geometry_gix
    on public.constraint_source_features using gist (geometry);

create unique index if not exists site_constraint_measurements_site_layer_feature_source_uidx
    on public.site_constraint_measurements (site_location_id, constraint_layer_id, constraint_feature_id, measurement_source);

create index if not exists site_constraint_measurements_site_idx
    on public.site_constraint_measurements (site_id);

create index if not exists site_constraint_measurements_location_idx
    on public.site_constraint_measurements (site_location_id);

create index if not exists site_constraint_measurements_layer_idx
    on public.site_constraint_measurements (constraint_layer_id);

create index if not exists site_constraint_measurements_feature_idx
    on public.site_constraint_measurements (constraint_feature_id);

create index if not exists site_constraint_measurements_intersects_idx
    on public.site_constraint_measurements (intersects, within_buffer);

create unique index if not exists site_constraint_group_summaries_site_layer_group_scope_uidx
    on public.site_constraint_group_summaries (site_location_id, constraint_layer_id, constraint_group, summary_scope);

create index if not exists site_constraint_group_summaries_site_idx
    on public.site_constraint_group_summaries (site_id);

create index if not exists site_constraint_group_summaries_group_idx
    on public.site_constraint_group_summaries (constraint_group, constraint_layer_id);

create index if not exists site_constraint_group_summaries_nearest_feature_idx
    on public.site_constraint_group_summaries (nearest_feature_id);

create unique index if not exists site_commercial_friction_facts_site_layer_group_key_uidx
    on public.site_commercial_friction_facts (site_location_id, constraint_layer_id, constraint_group, fact_key);

create index if not exists site_commercial_friction_facts_site_idx
    on public.site_commercial_friction_facts (site_id);

create index if not exists site_commercial_friction_facts_group_idx
    on public.site_commercial_friction_facts (constraint_group, constraint_layer_id);
