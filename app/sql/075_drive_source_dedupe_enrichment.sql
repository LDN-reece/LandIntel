create schema if not exists landintel_store;
create schema if not exists landintel_reporting;

create table if not exists landintel_store.drive_source_use_case_catalog (
    id uuid primary key default extensions.uuid_generate_v4(),
    source_family text not null,
    use_case_key text not null,
    use_case_label text not null,
    commercial_use text not null,
    source_of_record_policy text not null,
    intended_table_or_view text,
    completion_method text not null,
    duplicate_policy text not null,
    bounded_next_action text not null,
    priority_band text not null default 'P2',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint drive_source_use_case_catalog_priority_check
        check (priority_band in ('P0', 'P1', 'P2', 'P3', 'paused')),
    unique (source_family, use_case_key)
);

comment on table landintel_store.drive_source_use_case_catalog is
    'Readable use-case map for Drive-held source files. It links each Drive source family to the LandIntel use case, target object and duplicate handling policy without ingesting file contents.';

insert into landintel_store.drive_source_use_case_catalog (
    source_family,
    use_case_key,
    use_case_label,
    commercial_use,
    source_of_record_policy,
    intended_table_or_view,
    completion_method,
    duplicate_policy,
    bounded_next_action,
    priority_band,
    metadata
)
values
    ('source_catalogue', 'source_control', 'Source catalogue control', 'Keeps source estate decisions governed and auditable.', 'Registry/control document only; do not treat workbook presence as source completion.', 'landintel.source_catalog / landintel.source_estate_registry', 'metadata_control', 'Compare workbook file_id/name against source_corpus_assets before uploading a new copy.', 'Use to reconcile source registry gaps, not as a data ingestion source.', 'P1', '{}'::jsonb),
    ('settlement', 'settlement_context', 'Settlement and boundary context', 'Supports settlement-edge logic and location spine context.', 'Existing settlement records remain source truth where populated; Drive files are source snapshots.', 'landintel.settlement_boundary_records', 'bounded_source_completion', 'Do not create a second settlement truth table; enrich existing settlement source family only.', 'Compare against source completion matrix, then complete missing settlement storage/linking if not already live.', 'P1', '{}'::jsonb),
    ('coal_authority', 'mining_constraint_docs', 'Coal and mining constraint documentation', 'Explains mining layer meaning and informs bounded coal measurement.', 'Coal Authority source layers/measurements remain the constraint truth; Drive PDFs are documentation only.', 'public.constraint_source_features / public.site_constraint_measurements', 'documentation_context', 'Avoid duplicate uploads where guidance PDFs are already registered as corpus assets.', 'Keep as reference docs unless missing layer guidance is needed for operators.', 'P1', '{}'::jsonb),
    ('sgn_assets', 'gas_infrastructure_context', 'Gas infrastructure context', 'Supports infrastructure and abnormal-cost context where source access is proven.', 'Drive workbook is a catalogue/control asset, not live asset geometry.', 'landintel.site_power_context', 'metadata_control', 'Do not treat catalogue rows as asset data until an explicit connector/parser exists.', 'Hold as source catalogue; build bounded infrastructure adapter only when commercially needed.', 'P2', '{}'::jsonb),
    ('bgs', 'bgs_borehole_governance', 'BGS borehole governance', 'Supports future borehole proximity and evidence-density intelligence.', 'BGS master is a known-origin manual bulk upload; Drive ZIP must not be re-uploaded by this flow.', 'landintel.bgs_borehole_master / landintel_store.bgs_borehole_*', 'paused_governance', 'Treat as paused known-origin manual bulk upload, not duplicate junk and not final ground evidence.', 'Do not enrich until BGS / Pre-SI pause is lifted.', 'paused', '{}'::jsonb),
    ('local_planning_authorities', 'lpa_boundary_context', 'Local planning authority boundary context', 'Supports authority routing and source coverage checks.', 'Boundary/control layer only; avoid duplicating existing boundary context.', 'landintel_store.local_planning_authority_boundaries', 'bounded_source_completion', 'Compare with local authority boundary/open-location coverage before upload.', 'Create or enrich an authority-boundary source adapter if not already represented.', 'P2', '{}'::jsonb),
    ('conservation_areas', 'conservation_constraint', 'Conservation area constraint', 'Flags design/planning sensitivity for settlement/infill opportunities.', 'Existing constraint engine remains truth; Drive ZIP is a source snapshot for that engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a second conservation constraint model; enrich existing constraint layer registry.', 'Store source file, register layer, then run bounded priority-site measurements.', 'P1', '{}'::jsonb),
    ('landscape', 'landscape_constraint', 'Local landscape constraint', 'Identifies design/policy sensitivity that can affect layout and buyer risk.', 'Existing constraint engine remains truth; Drive ZIP is a source snapshot for that engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a second landscape constraint model; enrich existing constraint layer registry.', 'Store source file, register layer, then run bounded priority-site measurements.', 'P1', '{}'::jsonb),
    ('school_catchments', 'school_location_context', 'School catchment/location context', 'Supports market and NPF4-compatible location logic.', 'School catchments are context, not ownership/planning proof.', 'landintel.open_location_spine_features / landintel.site_open_location_spine_context', 'bounded_location_context_completion', 'Do not let catchments inflate site conviction without planning/control corroboration.', 'Store source file and expose as location context only.', 'P2', '{}'::jsonb),
    ('naturescot', 'ecology_constraint', 'Nature and ecology constraint', 'Flags ecology/design risk and manual review triggers.', 'Existing constraint engine remains truth; Drive ZIPs are source snapshots for that engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a second ecology constraint model; enrich existing constraint layer registry.', 'Store source file, register layer, then run bounded priority-site measurements.', 'P1', '{}'::jsonb),
    ('forestry', 'forestry_context', 'Forestry and woodland context', 'Identifies woodland/policy sensitivity and possible design constraints.', 'Forestry layer is context/constraint support until measured through the constraint engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a parallel forestry model; use constraint/source registry enrichment only.', 'Keep in ready backlog after the immediate constraint set.', 'P2', '{}'::jsonb),
    ('culverts', 'culvert_constraint', 'Culvert and buried infrastructure constraint', 'Highlights hidden infrastructure risk and abnormal-cost review need.', 'Existing constraint engine remains truth; Drive ZIP is a source snapshot for that engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a second culvert model; enrich existing constraint layer registry.', 'Store source file, register layer, then run bounded priority-site measurements.', 'P1', '{}'::jsonb),
    ('council_assets', 'public_ownership_exclusion', 'Council asset/public ownership exclusion', 'Filters out public ownership and protects LDN from false private-site leads.', 'Council asset register is ownership/control context, not legal title confirmation.', 'landintel_store.council_asset_register / landintel.ownership_control_signals', 'bounded_public_ownership_completion', 'Do not replace title review; use as public-owner exclusion/context only.', 'Store source file, normalise authority assets, and link as public ownership exclusion signal.', 'P1', '{}'::jsonb),
    ('vdl', 'register_context_vdl', 'Vacant and derelict land context', 'Identifies regeneration/underuse leads without over-weighting register presence.', 'VDL remains a register/context source and does not prove availability or viability.', 'landintel.vdl_site_records', 'register_context_completion', 'Do not duplicate VDL rows already in landintel.vdl_site_records; enrich missing docs/tables only.', 'Use as context, requiring independent corroboration before review/pursue.', 'P1', '{}'::jsonb),
    ('local_authority_boundaries', 'authority_boundary_context', 'Local authority boundary context', 'Supports authority routing, coverage and open-location spine joins.', 'Existing boundary/open-location tables remain source truth where populated.', 'landintel.open_location_spine_features', 'bounded_location_context_completion', 'Avoid duplicate boundary proximity context; use containment where relevant.', 'Use for coverage/routing, not as commercial evidence.', 'P2', '{}'::jsonb),
    ('planning', 'planning_document_review', 'Planning source/document review', 'May support planning evidence where correctly filed and source-linked.', 'Planning application records remain the planning truth; misfiled files require review before use.', 'landintel.planning_application_records / landintel.planning_document_records', 'manual_review', 'Do not upload misfiled planning-folder assets without human review.', 'Review file placement before any enrichment.', 'P3', '{}'::jsonb),
    ('tpo', 'tpo_constraint', 'Tree preservation order constraint', 'Flags layout/design/legal sensitivity.', 'Existing constraint engine remains truth; Drive ZIP is a source snapshot for that engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a second TPO model; enrich existing constraint layer registry.', 'Store source file, register layer, then run bounded priority-site measurements.', 'P1', '{}'::jsonb),
    ('greenbelt', 'greenbelt_constraint', 'Green belt constraint', 'Major planning risk layer for quick site triage.', 'Existing constraint engine remains truth; Drive ZIP is a source snapshot for that engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a second green belt model; enrich existing constraint layer registry.', 'Store source file, register layer, then run bounded priority-site measurements.', 'P1', '{}'::jsonb),
    ('contaminated_land', 'contaminated_land_constraint', 'Contaminated land constraint', 'Flags abnormal-cost and buyer/lender risk.', 'Existing constraint engine remains truth; Drive ZIP is a source snapshot for that engine.', 'public.constraint_source_features / public.site_constraint_measurements', 'bounded_constraint_completion', 'Do not create a second contaminated-land model; enrich existing constraint layer registry.', 'Store source file, register layer, then run bounded priority-site measurements.', 'P1', '{}'::jsonb),
    ('ela', 'register_context_ela', 'Employment land audit context', 'Identifies employment/policy context without over-weighting register presence.', 'ELA remains a register/context source and does not prove availability or viability.', 'landintel.ela_site_records', 'register_context_completion', 'Do not duplicate ELA rows already in landintel.ela_site_records; enrich missing docs/tables only.', 'Use as context, requiring independent corroboration before review/pursue.', 'P1', '{}'::jsonb),
    ('ros_cadastral', 'ros_parcel_context', 'RoS cadastral parcel context', 'Supports parcel/title candidate linkage but does not prove ownership.', 'RoS cadastral parcels remain canonical parcel source; parcel IDs are not title numbers.', 'public.ros_cadastral_parcels', 'parcel_source_completion', 'Do not revive public.land_objects as a duplicate truth model; enrich ros_cadastral only.', 'Use source ZIPs for governed parcel source completion where gaps remain.', 'P1', '{}'::jsonb),
    ('ldp', 'ldp_policy_context', 'LDP policy and spatial context', 'Supports planning journey and allocation/policy evidence.', 'LDP records remain policy context and need corroboration before commercial promotion.', 'landintel.ldp_site_records', 'policy_context_completion', 'Do not duplicate LDP rows already in landintel.ldp_site_records; enrich missing docs/spatial layers only.', 'Store missing LDP docs/spatial layers and classify as policy context.', 'P1', '{}'::jsonb),
    ('hla', 'register_context_hla', 'Housing land audit context', 'Identifies housing supply context without treating register presence as strong positive evidence.', 'HLA remains a register/context source and does not prove availability or viability.', 'landintel.hla_site_records', 'register_context_completion', 'Do not duplicate HLA rows already in landintel.hla_site_records; enrich missing docs/tables only.', 'Use as context, requiring independent corroboration before review/pursue.', 'P1', '{}'::jsonb)
on conflict (source_family, use_case_key) do update set
    use_case_label = excluded.use_case_label,
    commercial_use = excluded.commercial_use,
    source_of_record_policy = excluded.source_of_record_policy,
    intended_table_or_view = excluded.intended_table_or_view,
    completion_method = excluded.completion_method,
    duplicate_policy = excluded.duplicate_policy,
    bounded_next_action = excluded.bounded_next_action,
    priority_band = excluded.priority_band,
    metadata = excluded.metadata,
    updated_at = now();

create or replace view landintel_reporting.v_drive_source_use_case_schema
with (security_invoker = true) as
select
    catalog.source_family,
    catalog.use_case_key,
    catalog.use_case_label,
    catalog.commercial_use,
    catalog.source_of_record_policy,
    catalog.intended_table_or_view,
    catalog.completion_method,
    catalog.duplicate_policy,
    catalog.bounded_next_action,
    catalog.priority_band,
    catalog.updated_at
from landintel_store.drive_source_use_case_catalog as catalog
order by catalog.priority_band, catalog.source_family, catalog.use_case_key;

create or replace view landintel_reporting.v_drive_source_dedupe_enrichment
with (security_invoker = true) as
with drive_files as (
    select
        registry.*,
        lower(regexp_replace(registry.file_name, '\s+', ' ', 'g')) as normalized_file_name
    from landintel_store.drive_source_file_registry as registry
    where registry.file_or_folder = 'file'
),
drive_name_counts as (
    select
        normalized_file_name,
        count(*)::integer as same_name_in_drive_count
    from drive_files
    group by normalized_file_name
),
corpus_matches as (
    select
        drive_files.file_id,
        count(distinct asset.id)::integer as corpus_asset_match_count,
        bool_or(asset.drive_url ilike '%' || drive_files.file_id || '%') as exact_drive_file_match,
        bool_or(lower(asset.file_name) = drive_files.normalized_file_name) as file_name_match,
        array_remove(array_agg(distinct asset.source_key order by asset.source_key), null::text) as matched_source_keys,
        array_remove(array_agg(distinct estate.source_family order by estate.source_family), null::text) as matched_source_families,
        array_remove(array_agg(distinct asset.asset_role order by asset.asset_role), null::text) as matched_asset_roles
    from drive_files
    left join landintel.source_corpus_assets as asset
      on asset.drive_url ilike '%' || drive_files.file_id || '%'
      or lower(asset.file_name) = drive_files.normalized_file_name
    left join landintel.source_estate_registry as estate
      on estate.source_key = asset.source_key
    group by drive_files.file_id
),
source_completion as (
    select distinct on (matrix.source_family)
        matrix.source_family,
        matrix.source_key,
        matrix.source_name,
        matrix.current_status,
        matrix.target_status,
        matrix.current_table_or_view,
        matrix.workflow_command,
        matrix.github_actions_command_available,
        matrix.row_count,
        matrix.linked_site_count,
        matrix.measured_site_count,
        matrix.evidence_count,
        matrix.signal_count,
        matrix.freshness_record_count,
        matrix.known_blocker,
        matrix.next_action,
        matrix.priority
    from landintel_reporting.v_source_completion_matrix as matrix
    order by
        matrix.source_family,
        case matrix.current_status
            when 'live_complete' then 1
            when 'live_partial' then 2
            when 'manual_only' then 3
            when 'registered_only' then 4
            when 'discovery_only' then 5
            when 'blocked' then 6
            else 7
        end,
        matrix.source_key
)
select
    drive_files.root_folder_id,
    drive_files.root_folder_name,
    drive_files.folder_path,
    drive_files.folder_id,
    drive_files.file_id,
    drive_files.file_name,
    drive_files.mime_type,
    drive_files.file_extension,
    drive_files.drive_url,
    drive_files.source_family,
    drive_files.asset_role,
    drive_files.operator_priority,
    drive_files.priority_rank,
    drive_files.immediate_add_flag,
    drive_files.ready_to_upload_flag,
    drive_files.ready_to_upload_reason,
    drive_files.source_completion_next_action,
    use_case.use_case_key,
    use_case.use_case_label,
    use_case.commercial_use,
    use_case.source_of_record_policy,
    use_case.intended_table_or_view,
    use_case.completion_method,
    use_case.duplicate_policy,
    use_case.bounded_next_action,
    use_case.priority_band as use_case_priority_band,
    coalesce(corpus_matches.corpus_asset_match_count, 0) as corpus_asset_match_count,
    coalesce(corpus_matches.exact_drive_file_match, false) as exact_drive_file_match,
    coalesce(corpus_matches.file_name_match, false) as file_name_match,
    coalesce(corpus_matches.matched_source_keys, '{}'::text[]) as matched_source_keys,
    coalesce(corpus_matches.matched_source_families, '{}'::text[]) as matched_source_families,
    coalesce(corpus_matches.matched_asset_roles, '{}'::text[]) as matched_asset_roles,
    coalesce(drive_name_counts.same_name_in_drive_count, 1) as same_name_in_drive_count,
    source_completion.source_key as linked_source_key,
    source_completion.source_name as linked_source_name,
    source_completion.current_status as linked_source_completion_status,
    source_completion.target_status as linked_source_target_status,
    source_completion.current_table_or_view as linked_current_table_or_view,
    source_completion.workflow_command as linked_workflow_command,
    source_completion.github_actions_command_available as linked_workflow_available,
    source_completion.row_count as linked_row_count,
    source_completion.linked_site_count,
    source_completion.measured_site_count,
    source_completion.evidence_count,
    source_completion.signal_count,
    source_completion.freshness_record_count,
    source_completion.known_blocker as linked_known_blocker,
    case
        when coalesce(corpus_matches.exact_drive_file_match, false) then 'exact_drive_asset_duplicate'
        when coalesce(corpus_matches.file_name_match, false) then 'file_name_matches_existing_source_asset'
        when coalesce(drive_name_counts.same_name_in_drive_count, 1) > 1 then 'duplicate_name_inside_drive_registry'
        when drive_files.asset_role = 'known_origin_manual_bulk_upload' then 'known_origin_manual_upload_governance_only'
        when drive_files.asset_role = 'loose_shapefile_component' then 'not_upload_ready_loose_component'
        when drive_files.asset_role = 'misfiled_review' then 'manual_review_required'
        when source_completion.current_status in ('live_complete', 'live_partial', 'manual_only')
             and drive_files.ready_to_upload_flag is true then 'source_family_has_existing_database_coverage_review_before_upload'
        when drive_files.ready_to_upload_flag is true then 'not_known_duplicate_ready_for_enrichment'
        else 'metadata_or_reference_only_not_for_enrichment'
    end as duplicate_status,
    case
        when coalesce(corpus_matches.exact_drive_file_match, false) then 'high'
        when coalesce(corpus_matches.file_name_match, false) then 'medium'
        when source_completion.current_status in ('live_complete', 'live_partial', 'manual_only') then 'medium'
        else 'low'
    end as duplicate_confidence,
    (
        drive_files.ready_to_upload_flag is true
        and coalesce(corpus_matches.corpus_asset_match_count, 0) = 0
        and drive_files.asset_role not in ('known_origin_manual_bulk_upload', 'loose_shapefile_component', 'misfiled_review', 'documentation', 'documentation_archive')
    ) as safe_to_enrich_flag,
    case
        when coalesce(corpus_matches.corpus_asset_match_count, 0) > 0 then 'do_not_upload_duplicate_without_review'
        when drive_files.asset_role = 'known_origin_manual_bulk_upload' then 'paused_governance_only'
        when drive_files.asset_role = 'loose_shapefile_component' then 'package_complete_shapefile_before_upload'
        when drive_files.asset_role = 'misfiled_review' then 'manual_review_before_any_upload'
        when drive_files.ready_to_upload_flag is true
             and source_completion.current_status in ('live_complete', 'live_partial', 'manual_only') then 'enrich_existing_source_family_do_not_create_new_truth'
        when drive_files.ready_to_upload_flag is true then 'create_bounded_source_completion_adapter'
        else 'keep_as_metadata_or_reference_document'
    end as recommended_action,
    now() as assessed_at
from drive_files
left join drive_name_counts
  on drive_name_counts.normalized_file_name = drive_files.normalized_file_name
left join corpus_matches
  on corpus_matches.file_id = drive_files.file_id
left join source_completion
  on source_completion.source_family = drive_files.source_family
left join landintel_store.drive_source_use_case_catalog as use_case
  on use_case.source_family = drive_files.source_family;

create or replace view landintel_reporting.v_drive_source_duplicate_review_queue
with (security_invoker = true) as
select *
from landintel_reporting.v_drive_source_dedupe_enrichment
where duplicate_status in (
        'exact_drive_asset_duplicate',
        'file_name_matches_existing_source_asset',
        'duplicate_name_inside_drive_registry',
        'source_family_has_existing_database_coverage_review_before_upload',
        'known_origin_manual_upload_governance_only',
        'not_upload_ready_loose_component',
        'manual_review_required'
    )
order by
    case duplicate_status
        when 'exact_drive_asset_duplicate' then 1
        when 'file_name_matches_existing_source_asset' then 2
        when 'duplicate_name_inside_drive_registry' then 3
        when 'source_family_has_existing_database_coverage_review_before_upload' then 4
        else 5
    end,
    priority_rank nulls last,
    source_family,
    file_name;

create or replace view landintel_reporting.v_drive_source_enrichment_queue
with (security_invoker = true) as
select *
from landintel_reporting.v_drive_source_dedupe_enrichment
where safe_to_enrich_flag is true
order by
    immediate_add_flag desc,
    priority_rank nulls last,
    case use_case_priority_band
        when 'P0' then 1
        when 'P1' then 2
        when 'P2' then 3
        else 4
    end,
    source_family,
    file_name;

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
    risk_summary,
    recommended_action,
    metadata,
    reviewed_at,
    updated_at
)
values
    (
        'landintel_store',
        'drive_source_use_case_catalog',
        'table',
        'current_keep',
        'LandIntel Data Store',
        'Readable source-family and use-case map for Drive-held files',
        'source_completion',
        true,
        true,
        true,
        true,
        true,
        false,
        false,
        'Use-case metadata only; does not ingest, duplicate, or replace source truth.',
        'Maintain as the control map for Drive file enrichment decisions.',
        '{"created_by_migration":"075_drive_source_dedupe_enrichment.sql"}'::jsonb,
        now(),
        now()
    ),
    (
        'landintel_reporting',
        'v_drive_source_use_case_schema',
        'view',
        'reporting_surface',
        'LandIntel Reporting',
        'Readable use-case schema for Drive-held source families',
        'source_completion',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'Schema view only; source files still require duplicate checks and bounded source-completion work.',
        'Use to explain how each Drive file family should be used commercially.',
        '{"created_by_migration":"075_drive_source_dedupe_enrichment.sql"}'::jsonb,
        now(),
        now()
    ),
    (
        'landintel_reporting',
        'v_drive_source_dedupe_enrichment',
        'view',
        'reporting_surface',
        'LandIntel Reporting',
        'Duplicate-risk and enrichment matrix for Drive-held source files',
        'source_completion',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'Metadata-level duplicate assessment only; exact content duplicate proof needs checksum/live file metadata.',
        'Use before uploading or parsing any Drive-held source file.',
        '{"created_by_migration":"075_drive_source_dedupe_enrichment.sql"}'::jsonb,
        now(),
        now()
    ),
    (
        'landintel_reporting',
        'v_drive_source_duplicate_review_queue',
        'view',
        'reporting_surface',
        'LandIntel Reporting',
        'Drive files needing duplicate or manual review before enrichment',
        'source_completion',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'Flags probable duplicates, paused files, loose shapefile components and source families already represented in DB.',
        'Review before copying or parsing files.',
        '{"created_by_migration":"075_drive_source_dedupe_enrichment.sql"}'::jsonb,
        now(),
        now()
    ),
    (
        'landintel_reporting',
        'v_drive_source_enrichment_queue',
        'view',
        'reporting_surface',
        'LandIntel Reporting',
        'Drive files that are not known duplicates and can be considered for bounded enrichment',
        'source_completion',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'Queue indicates safe-to-consider enrichment, not automatic ingestion approval.',
        'Use to sequence the next bounded source-family PR.',
        '{"created_by_migration":"075_drive_source_dedupe_enrichment.sql"}'::jsonb,
        now(),
        now()
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
    risk_summary = excluded.risk_summary,
    recommended_action = excluded.recommended_action,
    metadata = landintel_store.object_ownership_registry.metadata || excluded.metadata,
    reviewed_at = now(),
    updated_at = now();
