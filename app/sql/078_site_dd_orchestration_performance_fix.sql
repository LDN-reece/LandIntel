create or replace view landintel_reporting.v_site_title_traceability_matrix
with (security_invoker = true) as
with best_parcel_link as (
    select distinct on (link.site_id)
        link.site_id,
        link.site_location_id,
        link.ros_parcel_id,
        link.ros_inspire_id,
        link.authority_name as parcel_authority_name,
        link.link_status,
        link.match_method,
        link.confidence,
        link.candidate_rank,
        link.overlap_area_sqm,
        link.overlap_pct_of_site,
        link.overlap_pct_of_parcel,
        link.nearest_distance_m,
        link.centroid_inside_site,
        link.updated_at as parcel_link_updated_at
    from public.site_ros_parcel_link_candidates as link
    where link.link_status <> 'rejected'
    order by
        link.site_id,
        case link.link_status
            when 'primary' then 1
            when 'candidate' then 2
            when 'manual_review' then 3
            else 9
        end,
        link.candidate_rank,
        link.confidence desc nulls last,
        link.updated_at desc nulls last
),
parcel_link_counts as (
    select
        link.site_id,
        count(*) filter (where link.link_status <> 'rejected')::integer as parcel_candidate_count,
        count(*) filter (where link.link_status = 'primary')::integer as primary_parcel_candidate_count,
        count(*) filter (where link.link_status = 'rejected')::integer as rejected_parcel_candidate_count,
        max(link.updated_at) as latest_parcel_link_at
    from public.site_ros_parcel_link_candidates as link
    group by link.site_id
),
best_title_resolution as (
    select distinct on (candidate.site_id)
        candidate.site_id,
        candidate.site_location_id,
        candidate.ros_parcel_id,
        candidate.ros_inspire_id,
        candidate.cadastral_unit_identifier,
        candidate.candidate_title_number,
        candidate.normalized_title_number,
        candidate.candidate_source,
        candidate.resolution_status,
        candidate.match_method,
        candidate.confidence,
        candidate.overlap_pct_of_site,
        candidate.nearest_distance_m,
        candidate.updated_at as title_resolution_updated_at
    from public.site_title_resolution_candidates as candidate
    where candidate.resolution_status <> 'rejected'
    order by
        candidate.site_id,
        case candidate.resolution_status
            when 'validated_title' then 1
            when 'probable_title' then 2
            when 'needs_licensed_bridge' then 3
            when 'manual_review' then 4
            else 9
        end,
        candidate.confidence desc nulls last,
        candidate.updated_at desc nulls last
),
title_resolution_counts as (
    select
        candidate.site_id,
        count(*) filter (where candidate.resolution_status <> 'rejected')::integer as title_resolution_candidate_count,
        count(*) filter (where candidate.resolution_status in ('probable_title', 'validated_title'))::integer as probable_title_candidate_count,
        count(*) filter (where candidate.resolution_status = 'needs_licensed_bridge')::integer as licensed_bridge_required_count,
        count(*) filter (where candidate.resolution_status = 'rejected')::integer as rejected_title_resolution_count,
        max(candidate.updated_at) as latest_title_resolution_at
    from public.site_title_resolution_candidates as candidate
    group by candidate.site_id
),
latest_title_workflow as (
    select distinct on (workflow.canonical_site_id)
        workflow.*
    from landintel.title_order_workflow as workflow
    order by workflow.canonical_site_id, workflow.updated_at desc nulls last, workflow.created_at desc nulls last
),
latest_title_review as (
    select distinct on (review.canonical_site_id)
        review.*
    from landintel.title_review_records as review
    order by
        review.canonical_site_id,
        review.review_date desc nulls last,
        review.updated_at desc nulls last,
        review.created_at desc nulls last
),
safe_title_candidates as (
    select *
    from landintel_reporting.v_title_candidates_operator_safe
)
select
    site.id as canonical_site_id,
    site.id::text as site_location_id,
    coalesce(nullif(site.site_name_primary, ''), site.site_code, site.id::text) as site_label,
    site.authority_name,
    site.area_acres as gross_area_acres,
    site.primary_ros_parcel_id,
    best_parcel.ros_parcel_id as best_ros_parcel_id,
    coalesce(parcel.ros_inspire_id, best_parcel.ros_inspire_id, best_resolution.ros_inspire_id) as best_ros_inspire_id,
    coalesce(
        public.extract_ros_cadastral_identifier(parcel.raw_attributes, parcel.ros_inspire_id),
        best_resolution.cadastral_unit_identifier
    ) as best_cadastral_unit_identifier,
    best_parcel.link_status as best_parcel_link_status,
    best_parcel.match_method as best_parcel_match_method,
    best_parcel.confidence as best_parcel_confidence,
    best_parcel.candidate_rank as best_parcel_candidate_rank,
    best_parcel.overlap_area_sqm as best_parcel_overlap_area_sqm,
    best_parcel.overlap_pct_of_site as best_parcel_overlap_pct_of_site,
    best_parcel.overlap_pct_of_parcel as best_parcel_overlap_pct_of_parcel,
    best_parcel.nearest_distance_m as best_parcel_nearest_distance_m,
    coalesce(parcel_counts.parcel_candidate_count, 0) as parcel_candidate_count,
    coalesce(parcel_counts.primary_parcel_candidate_count, 0) as primary_parcel_candidate_count,
    coalesce(parcel_counts.rejected_parcel_candidate_count, 0) as rejected_parcel_candidate_count,
    safe_title.title_number_candidate_for_manual_check as safe_title_number_candidate,
    safe_title.normalized_title_number as safe_normalized_title_number,
    safe_title.title_candidate_source as safe_title_candidate_source,
    safe_title.title_candidate_status as safe_title_candidate_status,
    safe_title.title_candidate_confidence as safe_title_candidate_confidence,
    coalesce(safe_title.safe_title_candidate_count, 0) as safe_title_candidate_count,
    best_resolution.candidate_title_number as bridge_title_number_candidate,
    best_resolution.normalized_title_number as bridge_normalized_title_number,
    best_resolution.candidate_source as bridge_candidate_source,
    best_resolution.resolution_status as bridge_resolution_status,
    best_resolution.match_method as bridge_match_method,
    best_resolution.confidence as bridge_confidence,
    coalesce(title_counts.title_resolution_candidate_count, 0) as title_resolution_candidate_count,
    coalesce(title_counts.probable_title_candidate_count, 0) as probable_title_candidate_count,
    coalesce(title_counts.licensed_bridge_required_count, 0) as licensed_bridge_required_count,
    coalesce(title_counts.rejected_title_resolution_count, 0) as rejected_title_resolution_count,
    title_workflow.title_order_status,
    title_workflow.title_review_status,
    title_workflow.title_required_flag,
    title_workflow.next_action as title_workflow_next_action,
    case
        when title_review.id is not null
         and coalesce(title_review.ownership_outcome, '') ~* '(confirmed|known|clear|reviewed|attractive)'
         and coalesce(title_review.ownership_outcome, '') !~* '(unclear|issue|problem|blocked|complex|dispute|adverse)' then 'title_reviewed_confirmed'
        when title_review.id is not null then 'title_reviewed_issue'
        when coalesce(title_workflow.title_order_status, '') not in ('', 'not_ordered', 'not ordered', 'not_ordered_yet')
          or coalesce(title_workflow.title_review_status, '') not in ('', 'not_reviewed', 'not reviewed') then 'title_ordered'
        when coalesce(safe_title.safe_title_candidate_count, 0) > 0 then 'title_candidate_available'
        when coalesce(title_workflow.title_required_flag, false) = false then 'title_not_required_yet'
        else 'ownership_unconfirmed'
    end as title_control_status,
    case
        when title_review.id is not null then 'human_title_review_recorded'
        else 'ownership_unconfirmed'
    end as ownership_control_status,
    title_review.title_number as reviewed_title_number,
    title_review.normalized_title_number as reviewed_normalized_title_number,
    title_review.registered_proprietor,
    title_review.proprietor_type,
    title_review.ownership_outcome,
    title_review.review_date,
    greatest(
        site.updated_at,
        best_parcel.parcel_link_updated_at,
        parcel_counts.latest_parcel_link_at,
        best_resolution.title_resolution_updated_at,
        title_counts.latest_title_resolution_at,
        safe_title.latest_candidate_activity_at,
        title_workflow.updated_at,
        title_review.updated_at
    ) as latest_title_traceability_activity_at,
    case
        when site.geometry is null then 'no_site_geometry'
        when title_review.id is not null then 'title_review_recorded'
        when coalesce(safe_title.safe_title_candidate_count, 0) > 0 then 'safe_title_candidate_available'
        when coalesce(title_counts.licensed_bridge_required_count, 0) > 0 then 'parcel_linked_needs_licensed_title_bridge'
        when coalesce(parcel_counts.parcel_candidate_count, 0) > 0
          or site.primary_ros_parcel_id is not null then 'parcel_linked_no_safe_title_candidate'
        else 'needs_ros_parcel_linking'
    end as title_traceability_status,
    case
        when site.geometry is null then 'Add or repair site geometry before title/parcel linking.'
        when title_review.id is not null then 'Use title_review_records as the ownership/control interpretation source.'
        when coalesce(safe_title.safe_title_candidate_count, 0) > 0 then 'Review candidate title number manually; this is not ownership confirmation.'
        when coalesce(parcel_counts.parcel_candidate_count, 0) > 0
          or site.primary_ros_parcel_id is not null then 'Run resolve-title-numbers or licensed/manual title bridge for linked RoS parcel candidates.'
        else 'Run link-sites-to-ros-parcels before title spend or title review.'
    end as next_title_link_action,
    'Ownership is unconfirmed unless landintel.title_review_records supports it. RoS parcel references, SCT-like cadastral references and Companies House/FCA/control signals are not title numbers or legal ownership proof.'::text as title_traceability_caveat
from landintel.canonical_sites as site
left join best_parcel_link as best_parcel
  on best_parcel.site_id = site.id::text
left join parcel_link_counts as parcel_counts
  on parcel_counts.site_id = site.id::text
left join public.ros_cadastral_parcels as parcel
  on parcel.id = coalesce(best_parcel.ros_parcel_id, site.primary_ros_parcel_id)
left join best_title_resolution as best_resolution
  on best_resolution.site_id = site.id::text
left join title_resolution_counts as title_counts
  on title_counts.site_id = site.id::text
left join latest_title_workflow as title_workflow
  on title_workflow.canonical_site_id = site.id
left join latest_title_review as title_review
  on title_review.canonical_site_id = site.id
left join safe_title_candidates as safe_title
  on safe_title.canonical_site_id = site.id;

comment on view landintel_reporting.v_site_title_traceability_matrix is
    'Per-site title traceability matrix. Performance-fixed to avoid joining the heavier all-site title/control status view; ownership remains unconfirmed unless title_review_records supports it.';

create or replace view landintel_reporting.v_site_dd_orchestration_queue
with (security_invoker = true) as
with ranked_inputs as (
    select
        measurement.*,
        case
            when measurement.site_priority_band = 'title_spend_candidates' then 1
            when measurement.site_priority_band = 'review_queue' then 2
            when measurement.site_priority_band = 'ldn_candidate_screen' then 3
            when measurement.site_priority_band = 'prove_it_candidates' then 4
            when measurement.site_priority_band = 'wider_canonical_sites' then 5
            else 9
        end as site_band_rank,
        case
            when measurement.measurement_readiness_status = 'below_minimum_area' then 90
            when measurement.title_traceability_status = 'no_site_geometry' then 10
            when measurement.title_traceability_status = 'needs_ros_parcel_linking' then 20
            when measurement.title_traceability_status in ('parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge') then 30
            when measurement.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 40
            when measurement.title_traceability_status = 'safe_title_candidate_available' then 50
            else 80
        end as step_rank
    from landintel_reporting.v_site_measurement_readiness_matrix as measurement
    where measurement.measurement_readiness_status <> 'below_minimum_area'
       or measurement.site_priority_band in ('title_spend_candidates', 'review_queue')
)
select
    (
        ranked_inputs.site_band_rank * 100000000
        + ranked_inputs.step_rank * 1000000
        + coalesce(ranked_inputs.next_constraint_queue_rank, 999999)
    )::bigint as orchestration_queue_rank,
    ranked_inputs.canonical_site_id,
    ranked_inputs.site_location_id,
    ranked_inputs.site_label,
    ranked_inputs.authority_name,
    ranked_inputs.gross_area_acres,
    ranked_inputs.site_priority_band,
    ranked_inputs.title_traceability_status,
    ranked_inputs.measurement_readiness_status,
    ranked_inputs.safe_title_candidate_count,
    ranked_inputs.parcel_candidate_count,
    ranked_inputs.licensed_bridge_required_count,
    ranked_inputs.constraint_measurement_row_count,
    ranked_inputs.constraint_scan_state_row_count,
    ranked_inputs.unscanned_priority_pair_count,
    ranked_inputs.next_constraint_priority_family,
    ranked_inputs.next_constraint_source_family,
    ranked_inputs.next_constraint_layer_key,
    ranked_inputs.next_constraint_layer_name,
    case
        when ranked_inputs.measurement_readiness_status = 'below_minimum_area' then 'hold_below_minimum_area'
        when ranked_inputs.title_traceability_status = 'no_site_geometry' then 'repair_site_geometry'
        when ranked_inputs.title_traceability_status = 'needs_ros_parcel_linking' then 'link_site_to_ros_parcel'
        when ranked_inputs.title_traceability_status in ('parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge') then 'resolve_title_candidate'
        when ranked_inputs.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 'measure_next_constraint_layer'
        when ranked_inputs.title_traceability_status = 'safe_title_candidate_available' then 'manual_title_review_or_title_spend_decision'
        else 'ready_for_operator_review'
    end as orchestration_step,
    case
        when ranked_inputs.title_traceability_status = 'needs_ros_parcel_linking' then 'link-sites-to-ros-parcels'
        when ranked_inputs.title_traceability_status in ('parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge') then 'resolve-title-numbers'
        when ranked_inputs.measurement_readiness_status = 'priority_constraint_measurement_backlog'
          and ranked_inputs.site_priority_band = 'title_spend_candidates' then 'constraint-measurement-proof-title-spend-source-family'
        when ranked_inputs.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 'measure-constraints-duckdb'
        when ranked_inputs.title_traceability_status = 'safe_title_candidate_available' then 'refresh-title-readiness'
        else 'audit-site-dd-orchestration'
    end as recommended_workflow_command,
    case
        when ranked_inputs.measurement_readiness_status = 'priority_constraint_measurement_backlog'
          and ranked_inputs.next_constraint_source_family is not null
            then 'constraint_measure_source_family=' || ranked_inputs.next_constraint_source_family
        when ranked_inputs.title_traceability_status in ('needs_ros_parcel_linking', 'parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge')
            then 'Use existing bounded title/parcel workflow inputs; do not manually mutate title truth.'
        else null
    end as recommended_workflow_input_hint,
    case
        when ranked_inputs.title_traceability_status = 'needs_ros_parcel_linking' then 'Site has no non-rejected RoS parcel link candidate yet.'
        when ranked_inputs.title_traceability_status = 'parcel_linked_needs_licensed_title_bridge' then 'RoS parcel candidate exists but title-number candidate needs licensed/manual bridge.'
        when ranked_inputs.title_traceability_status = 'parcel_linked_no_safe_title_candidate' then 'RoS parcel candidate exists but no operator-safe title-number candidate is available.'
        when ranked_inputs.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 'Priority constraint scan-state is incomplete for this site.'
        when ranked_inputs.title_traceability_status = 'safe_title_candidate_available' then 'Operator-safe title candidate exists, but ownership is still unconfirmed without human review.'
        else 'Site has traceability and current priority measurement coverage suitable for operator review.'
    end as orchestration_reason,
    'Run en masse through bounded workflow batches only: parcel/title linking first, then one constraint source family or layer at a time, then audit. Do not run broad all-site/all-layer scans.'::text as bounded_orchestration_caveat
from ranked_inputs;

comment on view landintel_reporting.v_site_dd_orchestration_queue is
    'Operator queue for mass DD orchestration. Performance-fixed to avoid a global row_number sort; queue rank is deterministic guidance only.';
