create schema if not exists landintel_reporting;

create or replace view landintel_reporting.v_source_completion_matrix
with (security_invoker = true) as
with workflow_commands(command_name, workflow_file, command_family, broad_run_risk) as (
    values
        ('run-migrations', 'run-landintel-sources.yml', 'system', false),
        ('audit-source-estate', 'run-landintel-sources.yml', 'audit', false),
        ('audit-source-freshness', 'run-landintel-sources.yml', 'audit', false),
        ('audit-source-expansion', 'run-landintel-sources.yml', 'audit', false),
        ('audit-source-completion-matrix', 'run-landintel-sources.yml', 'audit', false),
        ('audit-full-source-estate', 'run-landintel-sources.yml', 'audit', false),
        ('audit-constraint-measurements', 'run-landintel-sources.yml', 'audit', false),
        ('audit-open-location-spine-completion', 'run-landintel-sources.yml', 'audit', false),
        ('audit-title-number-control', 'run-landintel-sources.yml', 'audit', false),
        ('audit-urgent-address-title-pack', 'run-landintel-sources.yml', 'audit', false),
        ('audit-scotland-parcel-use-context', 'run-landintel-sources.yml', 'audit', false),
        ('ingest-planning-history', 'run-landintel-sources.yml', 'planning', true),
        ('ingest-hla', 'run-landintel-sources.yml', 'land_supply', true),
        ('ingest-ela', 'run-landintel-sources.yml', 'land_supply', true),
        ('ingest-vdl', 'run-landintel-sources.yml', 'land_supply', true),
        ('ingest-ldp', 'run-landintel-sources.yml', 'policy', true),
        ('ingest-settlement-boundaries', 'run-landintel-sources.yml', 'policy', true),
        ('ingest-sepa-flood', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-coal-authority', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-hes-designations', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-naturescot', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-contaminated-land', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-tpo', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-culverts', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-conservation-areas', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-greenbelt', 'run-landintel-sources.yml', 'constraints', true),
        ('measure-constraints-duckdb', 'run-landintel-sources.yml', 'constraints', true),
        ('ingest-bgs', 'run-landintel-sources.yml', 'ground', false),
        ('refresh-planning-decisions', 'run-landintel-sources.yml', 'planning', false),
        ('audit-planning-decisions', 'run-landintel-sources.yml', 'planning', false),
        ('ingest-planning-appeals', 'run-landintel-sources.yml', 'planning', false),
        ('ingest-planning-documents', 'run-landintel-sources.yml', 'documents', false),
        ('ingest-intelligence-events', 'run-landintel-sources.yml', 'intelligence', false),
        ('refresh-title-readiness', 'run-landintel-sources.yml', 'title_control', false),
        ('refresh-title-reviews', 'run-landintel-sources.yml', 'title_control', false),
        ('ingest-companies-house', 'run-landintel-sources.yml', 'corporate_control', false),
        ('ingest-fca-entities', 'run-landintel-sources.yml', 'corporate_control', false),
        ('ingest-power-infrastructure', 'run-landintel-sources.yml', 'power', false),
        ('ingest-amenities', 'run-landintel-sources.yml', 'amenities', false),
        ('ingest-demographics', 'run-landintel-sources.yml', 'demographics', false),
        ('ingest-market-context', 'run-landintel-sources.yml', 'market', false),
        ('refresh-site-market-context', 'run-landintel-sources.yml', 'market', false),
        ('refresh-site-amenity-context', 'run-landintel-sources.yml', 'amenities', false),
        ('refresh-site-demographic-context', 'run-landintel-sources.yml', 'demographics', false),
        ('refresh-site-power-context', 'run-landintel-sources.yml', 'power', false),
        ('refresh-site-abnormal-risk', 'run-landintel-sources.yml', 'ground', false),
        ('refresh-site-assessments', 'run-landintel-sources.yml', 'assessment', false),
        ('refresh-site-prove-it-assessments', 'run-landintel-sources.yml', 'assessment', false),
        ('audit-site-prove-it-assessments', 'run-landintel-sources.yml', 'assessment', false),
        ('refresh-ldn-candidate-screen', 'run-landintel-sources.yml', 'assessment', false),
        ('audit-ldn-candidate-screen', 'run-landintel-sources.yml', 'assessment', false),
        ('refresh-urgent-address-title-pack', 'run-landintel-sources.yml', 'title_control', false),
        ('refresh-scotland-parcel-use-context', 'run-landintel-sources.yml', 'address_property_base', false),
        ('ingest-os-openmap-local', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-roads', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-rivers', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-boundary-line', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-names', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-greenspace', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-zoomstack', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-toid', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-built-up-areas', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-uprn', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-os-open-usrn', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-osm-overpass', 'run-landintel-open-data-completion.yml', 'open_location_spine', true),
        ('ingest-naptan', 'run-landintel-sources.yml', 'amenities', true),
        ('ingest-statistics-gov-scot', 'run-landintel-sources.yml', 'demographics', true),
        ('ingest-opentopography-srtm', 'run-landintel-sources.yml', 'terrain', true),
        ('ingest-bulk-download-universe', 'run-landintel-open-data-completion.yml', 'open_location_spine', true)
),
endpoint_rollup as (
    select
        endpoint.source_key,
        count(*)::bigint as endpoint_count,
        min(endpoint.endpoint_url) as catalog_endpoint_url,
        array_remove(array_agg(distinct endpoint.endpoint_type order by endpoint.endpoint_type), null::text) as endpoint_types,
        bool_or(coalesce(endpoint.auth_required, false)) as auth_required
    from landintel.source_endpoint_catalog as endpoint
    group by endpoint.source_key
),
source_catalog as (
    select
        catalog.source_key,
        catalog.domain,
        catalog.source_name,
        catalog.source_role,
        catalog.scope,
        catalog.actionable_endpoint,
        catalog.access_pattern,
        catalog.auth_type,
        catalog.primary_output_object,
        catalog.primary_join_method,
        catalog.secondary_join_method,
        catalog.refresh_cadence,
        catalog.workflow_stage,
        catalog.workflow_ready,
        catalog.critical_notes,
        catalog.metadata
    from landintel.source_catalog as catalog
),
legacy_public_sources as (
    select
        coalesce(registry.metadata_uuid, md5(registry.source_name || coalesce(registry.endpoint_url, ''))) as legacy_source_key,
        count(*)::bigint as legacy_registry_rows,
        max(registry.updated_at) as legacy_latest_updated_at,
        max(registry.last_checked_at) as legacy_latest_checked_at,
        max(registry.last_success_at) as legacy_latest_success_at,
        max(registry.freshness_status) as legacy_freshness_status
    from public.source_registry as registry
    group by coalesce(registry.metadata_uuid, md5(registry.source_name || coalesce(registry.endpoint_url, '')))
),
estate_sources as (
    select
        estate.source_key,
        estate.source_family,
        estate.source_name,
        estate.source_group,
        estate.programme_phase,
        estate.module_key,
        estate.geography,
        estate.source_status,
        estate.orchestration_mode,
        estate.endpoint_url,
        estate.target_table,
        estate.reconciliation_path,
        estate.evidence_path,
        estate.signal_output,
        estate.data_age_basis,
        estate.ranking_eligible,
        estate.review_output_eligible,
        estate.access_status,
        estate.ingest_status,
        estate.normalisation_status,
        estate.site_link_status,
        estate.measurement_status,
        estate.evidence_status,
        estate.signal_status,
        estate.assessment_status,
        estate.trusted_for_review as registry_trusted_for_review,
        estate.limitation_notes,
        estate.next_action,
        estate.lifecycle_metadata
    from landintel.source_estate_registry as estate
),
combined_sources as (
    select
        coalesce(estate.source_key, catalog.source_key) as source_key,
        estate.source_family,
        coalesce(estate.source_name, catalog.source_name) as source_name,
        coalesce(estate.source_group, catalog.domain, 'unknown') as source_category,
        coalesce(estate.geography, catalog.scope, 'Scotland') as jurisdiction,
        estate.programme_phase,
        estate.module_key,
        estate.source_status,
        estate.orchestration_mode,
        coalesce(estate.endpoint_url, catalog.actionable_endpoint, endpoint_rollup.catalog_endpoint_url) as endpoint_url,
        coalesce(estate.target_table, catalog.primary_output_object) as target_table,
        estate.reconciliation_path,
        estate.evidence_path,
        estate.signal_output,
        estate.data_age_basis,
        estate.ranking_eligible,
        estate.review_output_eligible,
        estate.access_status,
        estate.ingest_status,
        estate.normalisation_status,
        estate.site_link_status,
        estate.measurement_status,
        estate.evidence_status,
        estate.signal_status,
        estate.assessment_status,
        estate.registry_trusted_for_review,
        estate.limitation_notes,
        estate.next_action,
        catalog.source_role,
        catalog.access_pattern,
        catalog.auth_type,
        catalog.primary_join_method,
        catalog.secondary_join_method,
        catalog.refresh_cadence,
        catalog.workflow_stage,
        catalog.workflow_ready,
        catalog.critical_notes,
        endpoint_rollup.endpoint_count,
        endpoint_rollup.endpoint_types,
        endpoint_rollup.auth_required,
        coalesce(estate.lifecycle_metadata, '{}'::jsonb) || coalesce(catalog.metadata, '{}'::jsonb) as metadata
    from estate_sources as estate
    full join source_catalog as catalog
      on catalog.source_key = estate.source_key
    left join endpoint_rollup
      on endpoint_rollup.source_key = coalesce(estate.source_key, catalog.source_key)
),
workflow_mapping as (
    select
        combined_sources.source_key,
        case
            when combined_sources.source_key = 'planning_applications_spatialhub' then 'ingest-planning-history'
            when combined_sources.source_key = 'housing_land_supply_spatialhub' then 'ingest-hla'
            when combined_sources.source_key = 'employment_land_supply_spatialhub' then 'ingest-ela'
            when combined_sources.source_key = 'vacant_derelict_land_spatialhub' then 'ingest-vdl'
            when combined_sources.source_key = 'ldp_spatialhub_package' then 'ingest-ldp'
            when combined_sources.source_key = 'nrs_settlement_boundaries' then 'ingest-settlement-boundaries'
            when combined_sources.source_key = 'sepa_flood_maps' then 'ingest-sepa-flood'
            when combined_sources.source_key = 'coal_authority_layers' then 'ingest-coal-authority'
            when combined_sources.source_key = 'hes_designations' then 'ingest-hes-designations'
            when combined_sources.source_key = 'naturescot_designations' then 'ingest-naturescot'
            when combined_sources.source_key = 'contaminated_land_spatialhub' then 'ingest-contaminated-land'
            when combined_sources.source_key = 'tpo_spatialhub' then 'ingest-tpo'
            when combined_sources.source_key = 'culverts_spatialhub' then 'ingest-culverts'
            when combined_sources.source_key = 'conservation_areas_spatialhub' then 'ingest-conservation-areas'
            when combined_sources.source_key = 'green_belt_spatialhub' then 'ingest-greenbelt'
            when combined_sources.source_key = 'bgs_boreholes_ogc' then 'ingest-bgs'
            when combined_sources.source_key = 'planning_decision_engine' then 'refresh-planning-decisions'
            when combined_sources.source_key in ('dpea_planning_appeals', 'local_review_body_decisions', 'planning_application_appeal_signals') then 'ingest-planning-appeals'
            when combined_sources.source_key = 'title_readiness_internal' then 'refresh-title-readiness'
            when combined_sources.source_key = 'title_review_manual' then 'refresh-title-reviews'
            when combined_sources.source_key = 'urgent_address_title_pack' then 'refresh-urgent-address-title-pack'
            when combined_sources.source_key = 'scotland_parcel_use_spine' then 'refresh-scotland-parcel-use-context'
            when combined_sources.source_key in ('companies_house_control_context', 'companies_house_charges') then 'ingest-companies-house'
            when combined_sources.source_key = 'fca_entity_enrichment' then 'ingest-fca-entities'
            when combined_sources.source_key in ('sp_energy_networks_assets', 'ssen_assets', 'national_grid_public_assets', 'osm_power_context') then 'ingest-power-infrastructure'
            when combined_sources.source_key in ('naptan_public_transport', 'os_places_amenity_context', 'scottish_schools_amenities', 'nhs_scotland_amenities', 'overture_places_open', 'geolytix_supermarket_points', 'geolytix_bank_points', 'retail_centre_boundaries_open') then 'ingest-amenities'
            when combined_sources.source_key in ('statistics_gov_scot_demographics', 'simd_demographic_context') then 'ingest-demographics'
            when combined_sources.source_key in ('ros_market_statistics', 'uk_hpi_market_context') then 'ingest-market-context'
            when combined_sources.source_key in ('council_planning_documents', 'section75_records') then 'ingest-planning-documents'
            when combined_sources.source_key in ('council_agenda_intelligence', 'local_press_intelligence') then 'ingest-intelligence-events'
            when combined_sources.source_key in ('terrain_abnormal_context', 'os_terrain_context') then 'refresh-site-abnormal-risk'
            when combined_sources.source_key = 'site_assessment_refresh' then 'refresh-site-assessments'
            when combined_sources.source_key = 'prove_it_conviction_layer' then 'refresh-site-prove-it-assessments'
            when combined_sources.source_key = 'ldn_candidate_screen' then 'refresh-ldn-candidate-screen'
            when combined_sources.source_key = 'os_open_zoomstack' then 'ingest-os-open-zoomstack'
            when combined_sources.source_key = 'os_open_toid' then 'ingest-os-open-toid'
            when combined_sources.source_key = 'os_open_built_up_areas' then 'ingest-os-open-built-up-areas'
            when combined_sources.source_key like 'os_downloads_openmap%%' then 'ingest-os-openmap-local'
            when combined_sources.source_key like 'os_downloads_open_roads%%' then 'ingest-os-open-roads'
            when combined_sources.source_key like 'os_downloads_open_rivers%%' then 'ingest-os-open-rivers'
            when combined_sources.source_key like 'os_downloads_boundary%%' then 'ingest-os-boundary-line'
            when combined_sources.source_key like 'os_downloads_open_names%%' then 'ingest-os-open-names'
            when combined_sources.source_key like 'os_downloads_open_greenspace%%' then 'ingest-os-open-greenspace'
            when combined_sources.source_key like 'osm%%' then 'ingest-osm-overpass'
            else null
        end as workflow_command
    from combined_sources
),
matrix as (
    select
        combined_sources.*,
        live.row_count,
        live.linked_site_count,
        live.measured_site_count,
        live.evidence_count,
        live.signal_count,
        live.assessment_ready_count,
        live.freshness_record_count,
        live.freshness_status,
        live.current_lifecycle_stage,
        live.trust_block_reason,
        live.trusted_for_review as live_trusted_for_review,
        phase_one.operational_status as phase_one_operational_status,
        phase_one.source_freshness_status as phase_one_freshness_status,
        phase_one.freshness_records_observed as phase_one_freshness_records_observed,
        workflow_mapping.workflow_command,
        workflow_commands.workflow_file,
        coalesce(workflow_commands.broad_run_risk, false) as broad_run_risk,
        coalesce(legacy_public_sources.legacy_registry_rows, 0)::bigint as legacy_registry_rows,
        legacy_public_sources.legacy_latest_updated_at,
        legacy_public_sources.legacy_latest_success_at,
        legacy_public_sources.legacy_freshness_status
    from combined_sources
    left join analytics.v_landintel_source_estate_matrix as live
      on live.source_key = combined_sources.source_key
    left join analytics.v_phase_one_source_estate_matrix as phase_one
      on phase_one.source_key = combined_sources.source_key
    left join workflow_mapping
      on workflow_mapping.source_key = combined_sources.source_key
    left join workflow_commands
      on workflow_commands.command_name = workflow_mapping.workflow_command
    left join legacy_public_sources
      on legacy_public_sources.legacy_source_key = (combined_sources.metadata ->> 'metadata_uuid')
)
select
    source_key,
    source_name,
    source_family,
    source_category,
    jurisdiction,
    case
        when source_status in ('retired', 'replaced') then 'retired_or_replaced'
        when coalesce(access_status, source_status) in ('access_required', 'gated', 'failed')
          or coalesce(freshness_status, phase_one_freshness_status, legacy_freshness_status) in ('failed', 'access_required', 'gated') then 'blocked'
        when source_status = 'discovery_only' then 'discovery_only'
        when source_status = 'static_snapshot'
          or source_key = 'title_review_manual'
          or orchestration_mode ilike '%%manual%%' then 'manual_only'
        when coalesce(live_trusted_for_review, false) then 'live_complete'
        when coalesce(row_count, 0) > 0
          or coalesce(linked_site_count, 0) > 0
          or coalesce(measured_site_count, 0) > 0
          or coalesce(freshness_record_count, 0) > 0
          or phase_one_operational_status in ('live_wired', 'static_registered')
          or coalesce(legacy_registry_rows, 0) > 0 then 'live_partial'
        else 'registered_only'
    end as current_status,
    case
        when source_status in ('retired', 'replaced') then 'retired_or_replaced'
        when source_status = 'discovery_only' then 'discovery_only'
        when source_status = 'static_snapshot'
          or source_key = 'title_review_manual'
          or orchestration_mode ilike '%%manual%%' then 'manual_only'
        else 'live_complete'
    end as target_status,
    target_table as current_table_or_view,
    target_table as target_table_or_view,
    workflow_command,
    (workflow_file is not null) as github_actions_command_available,
    coalesce('endpoint_or_manifest: ' || nullif(endpoint_url, ''), 'catalog_or_manual_discovery') as source_discovery_method,
    coalesce(orchestration_mode, access_pattern, workflow_stage, 'not_documented') as ingestion_method,
    coalesce(target_table, 'not_documented') as storage_method,
    case
        when source_family in ('amenities', 'demographics', 'market_context', 'power_infrastructure', 'terrain_abnormal', 'address_property_base') then 'phase2_context_enrichment'
        when source_category = 'constraints' or source_family in ('sepa_flood', 'coal_authority', 'hes', 'naturescot', 'contaminated_land', 'tpo', 'culverts', 'conservation_areas', 'greenbelt') then 'constraint_measurement_engine'
        when source_family in ('planning', 'planning_decisions', 'planning_appeals', 'planning_documents') then 'planning_extraction_and_site_context'
        when source_family in ('title_control', 'title_number', 'corporate_control') then 'title_control_hypothesis_until_manual_review'
        else coalesce(source_role, 'not_documented')
    end as enrichment_method,
    coalesce(reconciliation_path, primary_join_method, 'not_documented') as site_linking_method,
    coalesce(evidence_path, 'not_documented') as evidence_method,
    coalesce(signal_output, 'not_documented') as signal_method,
    coalesce(data_age_basis, refresh_cadence, 'not_documented') as freshness_method,
    case
        when source_family in ('sepa_flood', 'coal_authority', 'hes', 'naturescot', 'contaminated_land', 'tpo', 'culverts', 'conservation_areas', 'greenbelt') then 'audit-constraint-measurements / landintel_reporting.v_constraint_coverage_by_layer'
        when source_family in ('title_control', 'title_number', 'corporate_control') then 'audit-title-number-control / landintel_reporting.v_title_control_status'
        when source_family in ('planning_decisions', 'planning_appeals') then 'audit-planning-decisions / audit-full-source-estate'
        when source_family in ('amenities', 'demographics', 'market_context', 'power_infrastructure', 'address_property_base') then 'audit-full-source-estate'
        when source_key like 'os_downloads%%' or source_family in ('base_geometry', 'settlement_context', 'access_context') then 'audit-open-location-spine-completion'
        else coalesce('audit-source-freshness / ' || nullif(phase_one_operational_status, ''), 'audit-source-estate')
    end as audit_view_or_audit_command,
    case
        when source_family in ('planning', 'hla', 'ela', 'vdl', 'sepa_flood', 'coal_authority', 'title_control', 'title_number', 'corporate_control', 'site_conviction', 'amenities', 'demographics', 'market_context', 'power_infrastructure', 'address_property_base')
          or source_key in ('site_assessment_refresh', 'prove_it_conviction_layer', 'ldn_candidate_screen', 'urgent_address_title_pack', 'scotland_parcel_use_spine')
            then true
        else false
    end as tests_present,
    case
        when trust_block_reason is not null then trust_block_reason
        when limitation_notes is not null and btrim(limitation_notes) <> '' then limitation_notes
        when critical_notes is not null and btrim(critical_notes) <> '' then critical_notes
        when workflow_command is null then 'no_bounded_github_actions_command_mapped'
        when broad_run_risk then 'workflow_exists_but_must_be_bounded_by_inputs'
        when current_lifecycle_stage is null and phase_one_operational_status is null then 'not_yet_proven_in_live_matrix'
        else null
    end as known_blocker,
    coalesce(next_action, critical_notes, 'Prove current lifecycle, row counts, freshness, evidence and signal outputs before promotion.') as next_action,
    case
        when source_family in ('title_control', 'title_number', 'planning', 'planning_decisions', 'hla', 'ldp', 'settlement') then 'P0'
        when source_family in ('sepa_flood', 'coal_authority', 'greenbelt', 'contaminated_land', 'culverts', 'amenities', 'demographics', 'market_context', 'power_infrastructure', 'site_conviction') then 'P1'
        when source_family in ('planning_appeals', 'planning_documents', 'local_intelligence', 'terrain_abnormal', 'bgs', 'address_property_base') then 'P2'
        else 'P3'
    end as priority,
    case
        when target_table like 'public.%%' then 'public_legacy_compatibility'
        when target_table like 'landintel_store.%%' then 'landintel_store'
        when target_table like 'landintel_sourced.%%' then 'landintel_sourced'
        when target_table like 'landintel_reporting.%%' then 'landintel_reporting'
        when target_table like 'landintel.%%' then 'landintel'
        else 'source_catalog_or_registry'
    end as owner_layer,
    coalesce(row_count, 0)::bigint as row_count,
    coalesce(linked_site_count, 0)::bigint as linked_site_count,
    coalesce(measured_site_count, 0)::bigint as measured_site_count,
    coalesce(evidence_count, 0)::bigint as evidence_count,
    coalesce(signal_count, 0)::bigint as signal_count,
    coalesce(assessment_ready_count, 0)::bigint as assessment_ready_count,
    coalesce(freshness_record_count, phase_one_freshness_records_observed, 0)::bigint as freshness_record_count,
    current_lifecycle_stage,
    phase_one_operational_status,
    phase_one_freshness_status,
    legacy_registry_rows,
    legacy_latest_updated_at,
    workflow_file,
    broad_run_risk,
    now() as matrix_generated_at
from matrix;

comment on view landintel_reporting.v_source_completion_matrix
    is 'LandIntel source completion and workflow gap matrix. It classifies sources as live_complete, live_partial, registered_only, discovery_only, manual_only, blocked, or retired_or_replaced without ingesting data or moving source truth.';

do $$
begin
    if to_regclass('landintel_store.object_ownership_registry') is not null then
        insert into landintel_store.object_ownership_registry (
            schema_name,
            object_name,
            object_type,
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
        values (
            'landintel_reporting',
            'v_source_completion_matrix',
            'view',
            'reporting_surface',
            'landintel_reporting',
            'source completion matrix',
            'source_estate',
            true,
            true,
            true,
            true,
            false,
            true,
            false,
            null,
            'Reports source completion and workflow gaps without changing source tables or claiming unproven completion.',
            'Use before scaling ingestion; promote sources only when lifecycle proof, bounded workflow and tests are present.',
            '{"phase":"F","data_movement":false,"broad_ingestion":false}'::jsonb
        )
        on conflict (schema_name, object_name, object_type) do update set
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
            metadata = landintel_store.object_ownership_registry.metadata || excluded.metadata,
            reviewed_at = now(),
            updated_at = now();
    end if;
end $$;

grant usage on schema landintel_reporting to authenticated;
grant select on landintel_reporting.v_source_completion_matrix to authenticated;
