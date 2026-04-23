drop view if exists analytics.v_live_opportunity_queue;
drop view if exists analytics.v_live_priority_alerts;
drop view if exists analytics.v_live_site_change_log;
drop view if exists analytics.v_live_site_review_state;
drop view if exists analytics.v_live_site_assessment;
drop view if exists analytics.v_live_site_constraints;
drop view if exists analytics.v_live_site_title;
drop view if exists analytics.v_constraints_tab_overview;
drop view if exists analytics.v_constraints_tab_group_summaries;
drop view if exists analytics.v_constraints_tab_measurements;
drop view if exists analytics.v_constraints_tab_commercial_friction;
drop view if exists analytics.v_live_site_sources;
drop view if exists analytics.v_live_site_readiness;
drop view if exists analytics.v_live_site_summary;
drop view if exists analytics.v_live_site_summary_base;

create or replace view analytics.v_constraints_tab_measurements
with (security_invoker = true) as
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    layer.constraint_group,
    layer.constraint_type,
    layer.measurement_mode,
    measurement.buffer_distance_m,
    measurement.constraint_feature_id,
    feature.source_feature_key,
    feature.feature_name,
    feature.source_reference,
    feature.severity_label,
    measurement.measurement_source,
    measurement.intersects,
    measurement.within_buffer,
    measurement.site_inside_feature,
    measurement.feature_inside_site,
    measurement.overlap_area_sqm,
    measurement.overlap_pct_of_site,
    measurement.overlap_pct_of_feature,
    measurement.nearest_distance_m,
    measurement.measured_at
from public.site_constraint_measurements as measurement
join public.constraints_site_anchor() as anchor
  on anchor.site_id = measurement.site_id
 and anchor.site_location_id = measurement.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = measurement.constraint_layer_id
left join public.constraint_source_features as feature
  on feature.id = measurement.constraint_feature_id;

create or replace view analytics.v_constraints_tab_group_summaries
with (security_invoker = true) as
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    summary.constraint_group,
    summary.summary_scope,
    summary.intersecting_feature_count,
    summary.buffered_feature_count,
    summary.total_overlap_area_sqm,
    summary.max_overlap_pct_of_site,
    summary.min_distance_m,
    summary.nearest_feature_id,
    summary.nearest_feature_name,
    summary.measured_at
from public.site_constraint_group_summaries as summary
join public.constraints_site_anchor() as anchor
  on anchor.site_id = summary.site_id
 and anchor.site_location_id = summary.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = summary.constraint_layer_id;

create or replace view analytics.v_constraints_tab_commercial_friction
with (security_invoker = true) as
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    friction.constraint_group,
    friction.fact_key,
    friction.fact_label,
    friction.fact_value_text,
    friction.fact_value_numeric,
    friction.fact_unit,
    friction.fact_basis,
    friction.created_at
from public.site_commercial_friction_facts as friction
join public.constraints_site_anchor() as anchor
  on anchor.site_id = friction.site_id
 and anchor.site_location_id = friction.site_location_id
left join public.constraint_layer_registry as layer
  on layer.id = friction.constraint_layer_id;

create or replace view analytics.v_constraints_tab_overview
with (security_invoker = true) as
with measurement_rollup as (
    select
        measurement.site_id,
        measurement.site_location_id,
        count(distinct measurement.constraint_group)::bigint as constraint_groups_measured,
        count(distinct measurement.constraint_group) filter (where measurement.intersects or measurement.within_buffer)::bigint as constraint_groups_intersecting,
        array_agg(distinct measurement.layer_key order by measurement.layer_key) as measured_layer_keys,
        max(measurement.measured_at) as latest_measurement_at
    from analytics.v_constraints_tab_measurements as measurement
    group by measurement.site_id, measurement.site_location_id
),
friction_rollup as (
    select
        friction.site_id,
        friction.site_location_id,
        count(*)::bigint as friction_fact_count,
        array_agg(distinct friction.fact_label order by friction.fact_label) as friction_fact_labels
    from analytics.v_constraints_tab_commercial_friction as friction
    group by friction.site_id, friction.site_location_id
)
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_sqm as site_area_sqm,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    coalesce(measurement_rollup.constraint_groups_measured, 0) as constraint_groups_measured,
    coalesce(measurement_rollup.constraint_groups_intersecting, 0) as constraint_groups_intersecting,
    coalesce(measurement_rollup.measured_layer_keys, '{}'::text[]) as measured_layer_keys,
    coalesce(friction_rollup.friction_fact_count, 0) as friction_fact_count,
    coalesce(friction_rollup.friction_fact_labels, '{}'::text[]) as friction_fact_labels,
    measurement_rollup.latest_measurement_at
from public.constraints_site_anchor() as anchor
left join measurement_rollup
  on measurement_rollup.site_id = anchor.site_id
 and measurement_rollup.site_location_id = anchor.site_location_id
left join friction_rollup
  on friction_rollup.site_id = anchor.site_id
 and friction_rollup.site_location_id = anchor.site_location_id;

create or replace view analytics.v_live_site_title
with (security_invoker = true) as
with parcel_links as (
    select
        spatial.site_id::uuid as canonical_site_id,
        count(*) filter (where spatial.linked_record_table = 'public.ros_cadastral_parcels')::bigint as parcel_link_count,
        array_agg(distinct spatial.linked_record_id order by spatial.linked_record_id) filter (where spatial.linked_record_table = 'public.ros_cadastral_parcels') as linked_parcel_ids,
        max(spatial.updated_at) as latest_spatial_update_at
    from public.site_spatial_links as spatial
    where spatial.site_id ~* '^[0-9a-f-]{36}$'
    group by spatial.site_id::uuid
),
title_rollup as (
    select
        title.site_id::uuid as canonical_site_id,
        count(*)::bigint as title_validation_count,
        count(*) filter (where lower(coalesce(title.validation_status, '')) in ('title_reviewed', 'validated', 'confirmed'))::bigint as title_reviewed_count,
        array_agg(distinct coalesce(title.matched_title_number, title.title_number) order by coalesce(title.matched_title_number, title.title_number)) filter (
            where coalesce(title.matched_title_number, title.title_number) is not null
        ) as title_numbers,
        max(title.updated_at) as latest_title_update_at
    from public.site_title_validation as title
    where title.site_id ~* '^[0-9a-f-]{36}$'
    group by title.site_id::uuid
),
latest_title as (
    select
        ranked.canonical_site_id,
        ranked.validation_status,
        ranked.validation_method,
        ranked.title_source,
        ranked.confidence
    from (
        select
            title.site_id::uuid as canonical_site_id,
            title.validation_status,
            title.validation_method,
            title.title_source,
            title.confidence,
            row_number() over (
                partition by title.site_id::uuid
                order by title.updated_at desc nulls last, title.created_at desc, title.id desc
            ) as row_number
        from public.site_title_validation as title
        where title.site_id ~* '^[0-9a-f-]{36}$'
    ) as ranked
    where ranked.row_number = 1
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    coalesce(parcel_links.parcel_link_count, case when site.primary_ros_parcel_id is not null then 1 else 0 end, 0) as parcel_link_count,
    coalesce(parcel_links.linked_parcel_ids, case when site.primary_ros_parcel_id is not null then array[site.primary_ros_parcel_id::text] else '{}'::text[] end) as linked_parcel_ids,
    coalesce(title_rollup.title_validation_count, 0) as title_validation_count,
    coalesce(title_rollup.title_numbers, '{}'::text[]) as title_numbers,
    coalesce(title_rollup.title_reviewed_count, 0) > 0 as title_reviewed_flag,
    case
        when coalesce(title_rollup.title_reviewed_count, 0) > 0 then 'title_reviewed'
        else 'commercial_inference'
    end as title_state,
    case
        when coalesce(title_rollup.title_reviewed_count, 0) > 0 then 'title_reviewed'
        else 'commercial_inference'
    end as ownership_control_fact_label,
    latest_title.validation_status,
    latest_title.validation_method,
    latest_title.title_source,
    latest_title.confidence,
    greatest(
        coalesce(parcel_links.latest_spatial_update_at, site.updated_at),
        coalesce(title_rollup.latest_title_update_at, site.updated_at)
    ) as latest_title_update_at,
    case
        when coalesce(title_rollup.title_reviewed_count, 0) > 0 then 'Legal ownership certainty is only shown after title review.'
        when coalesce(title_rollup.title_validation_count, 0) > 0 then 'Title-related evidence exists but ownership remains a commercial inference until title is reviewed.'
        when site.primary_ros_parcel_id is not null then 'Parcel linkage exists, but ownership remains a commercial inference until title is reviewed.'
        else 'No title review has been completed yet; ownership remains a commercial inference.'
    end as title_summary
from landintel.canonical_sites as site
left join parcel_links
  on parcel_links.canonical_site_id = site.id
left join title_rollup
  on title_rollup.canonical_site_id = site.id
left join latest_title
  on latest_title.canonical_site_id = site.id;

create or replace view analytics.v_live_site_constraints
with (security_invoker = true) as
with overview as (
    select
        overview.site_id::uuid as canonical_site_id,
        overview.constraint_groups_measured,
        overview.constraint_groups_intersecting,
        overview.measured_layer_keys,
        overview.friction_fact_count,
        overview.friction_fact_labels,
        overview.latest_measurement_at
    from analytics.v_constraints_tab_overview as overview
    where overview.site_id ~* '^[0-9a-f-]{36}$'
),
group_rollup as (
    select
        summary.site_id::uuid as canonical_site_id,
        count(*)::bigint as group_summary_count,
        count(*) filter (where coalesce(summary.intersecting_feature_count, 0) > 0)::bigint as active_group_count,
        coalesce(max(summary.max_overlap_pct_of_site), 0) as max_overlap_pct_of_site,
        coalesce(min(summary.min_distance_m), 0) as nearest_constraint_distance_m,
        array_agg(distinct summary.constraint_group order by summary.constraint_group) as constraint_groups_present
    from analytics.v_constraints_tab_group_summaries as summary
    where summary.site_id ~* '^[0-9a-f-]{36}$'
    group by summary.site_id::uuid
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    coalesce(overview.constraint_groups_measured, 0) as constraint_groups_measured,
    coalesce(overview.constraint_groups_intersecting, 0) as constraint_groups_intersecting,
    coalesce(group_rollup.group_summary_count, 0) as group_summary_count,
    coalesce(group_rollup.active_group_count, 0) as active_group_count,
    coalesce(group_rollup.constraint_groups_present, '{}'::text[]) as constraint_groups_present,
    coalesce(overview.measured_layer_keys, '{}'::text[]) as measured_layer_keys,
    coalesce(overview.friction_fact_count, 0) as friction_fact_count,
    coalesce(overview.friction_fact_labels, '{}'::text[]) as friction_fact_labels,
    coalesce(group_rollup.max_overlap_pct_of_site, 0) as max_overlap_pct_of_site,
    coalesce(group_rollup.nearest_constraint_distance_m, 0) as nearest_constraint_distance_m,
    case
        when coalesce(group_rollup.active_group_count, 0) = 0 and coalesce(overview.constraint_groups_measured, 0) = 0 then 'unknown'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 25 then 'severe'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 10 then 'material'
        when coalesce(group_rollup.active_group_count, 0) > 0 then 'conditional'
        else 'limited'
    end as constraint_severity,
    case
        when coalesce(group_rollup.active_group_count, 0) = 0 and coalesce(overview.constraint_groups_measured, 0) = 0 then 'No measured constraints have been linked yet.'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 25 then 'A material part of the site overlaps severe constraints.'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 10 then 'Constraints overlap a meaningful part of the site and need review.'
        when coalesce(group_rollup.active_group_count, 0) > 0 then 'Constraints are present but may still be workable.'
        else 'Measured constraints are currently limited.'
    end as constraint_summary,
    overview.latest_measurement_at as latest_constraint_update_at
from landintel.canonical_sites as site
left join overview
  on overview.canonical_site_id = site.id
left join group_rollup
  on group_rollup.canonical_site_id = site.id;

create or replace view analytics.v_live_site_assessment
with (security_invoker = true) as
with latest_assessment as (
    select
        ranked.*
    from (
        select
            assessment.*,
            row_number() over (
                partition by assessment.canonical_site_id
                order by coalesce(assessment.latest_assessment_at, assessment.created_at) desc,
                    assessment.assessment_version desc,
                    assessment.created_at desc,
                    assessment.id desc
            ) as row_number
        from landintel.site_assessments as assessment
    ) as ranked
    where ranked.row_number = 1
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    assessment.id as site_assessment_id,
    coalesce(assessment.overall_tier, 'Unranked') as overall_tier,
    assessment.overall_rank_score,
    coalesce(assessment.queue_recommendation, 'New Candidates') as queue_recommendation,
    assessment.why_it_surfaced,
    assessment.why_it_survived,
    coalesce(assessment.good_items, '[]'::jsonb) as good_items,
    coalesce(assessment.bad_items, '[]'::jsonb) as bad_items,
    coalesce(assessment.ugly_items, '[]'::jsonb) as ugly_items,
    coalesce(assessment.subrank_summary, '{}'::jsonb) as subrank_summary,
    assessment.subrank_summary ->> 'size_rank' as size_rank,
    assessment.subrank_summary ->> 'planning_context_rank' as planning_context_rank,
    assessment.subrank_summary ->> 'location_rank' as location_rank,
    assessment.subrank_summary ->> 'constraints_rank' as constraints_rank,
    assessment.subrank_summary ->> 'access_rank' as access_rank,
    assessment.subrank_summary ->> 'geometry_rank' as geometry_rank,
    assessment.subrank_summary ->> 'ownership_control_rank' as ownership_control_rank,
    assessment.subrank_summary ->> 'utilities_burden_rank' as utilities_burden_rank,
    assessment.subrank_summary ->> 'redevelopment_angle_rank' as redevelopment_angle_rank,
    assessment.subrank_summary ->> 'stalled_site_angle_rank' as stalled_site_angle_rank,
    assessment.subrank_summary ->> 'settlement_position' as settlement_position,
    assessment.subrank_summary ->> 'location_band' as location_band,
    assessment.subrank_summary ->> 'access_strength' as access_strength,
    assessment.subrank_summary ->> 'geometry_quality' as geometry_quality,
    assessment.subrank_summary ->> 'ownership_control_state' as ownership_control_state,
    assessment.subrank_summary ->> 'constraint_severity' as constraint_severity,
    assessment.subrank_summary ->> 'planning_context_band' as planning_context_band,
    coalesce(assessment.title_state, 'commercial_inference') as title_state,
    coalesce(assessment.ownership_control_fact_label, 'commercial_inference') as ownership_control_fact_label,
    coalesce(assessment.resurfaced_reason, '') as resurfaced_reason,
    coalesce(assessment.human_review_required, false) as human_review_required,
    assessment.dominant_blocker,
    assessment.explanation_text,
    coalesce(assessment.latest_assessment_at, assessment.created_at) as latest_assessment_at
from landintel.canonical_sites as site
left join latest_assessment as assessment
  on assessment.canonical_site_id = site.id;

create or replace view analytics.v_live_site_review_state
with (security_invoker = true) as
with latest_status as (
    select
        ranked.canonical_site_id,
        ranked.review_status,
        ranked.actor_name,
        ranked.reason_text,
        ranked.created_at
    from (
        select
            event.canonical_site_id,
            event.review_status,
            event.actor_name,
            event.reason_text,
            event.created_at,
            row_number() over (
                partition by event.canonical_site_id
                order by event.created_at desc, event.id desc
            ) as row_number
        from landintel.site_review_events as event
        where event.review_status is not null
    ) as ranked
    where ranked.row_number = 1
),
latest_note as (
    select
        ranked.canonical_site_id,
        ranked.note_text,
        ranked.actor_name,
        ranked.created_at
    from (
        select
            event.canonical_site_id,
            event.note_text,
            event.actor_name,
            event.created_at,
            row_number() over (
                partition by event.canonical_site_id
                order by event.created_at desc, event.id desc
            ) as row_number
        from landintel.site_review_events as event
        where nullif(btrim(coalesce(event.note_text, '')), '') is not null
    ) as ranked
    where ranked.row_number = 1
),
note_rollup as (
    select
        event.canonical_site_id,
        count(*) filter (where nullif(btrim(coalesce(event.note_text, '')), '') is not null)::bigint as note_count
    from landintel.site_review_events as event
    group by event.canonical_site_id
),
override_rollup as (
    select
        override.canonical_site_id,
        count(*)::bigint as override_count,
        max(override.created_at) as latest_override_at
    from landintel.site_manual_overrides as override
    group by override.canonical_site_id
),
title_rollup as (
    select
        event.canonical_site_id,
        max(
            case event.event_type
                when 'buy_title_now' then 1
                when 'title_ordered' then 2
                when 'title_reviewed' then 3
                else 0
            end
        ) as title_stage_order,
        max(event.created_at) filter (
            where event.event_type in ('buy_title_now', 'title_ordered', 'title_reviewed')
        ) as latest_title_event_at
    from landintel.site_review_events as event
    group by event.canonical_site_id
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    coalesce(latest_status.review_status, 'New candidate') as review_status,
    case
        when coalesce(latest_status.review_status, 'New candidate') in ('Under review', 'Need more evidence', 'Conditional', 'Buy title now', 'Title ordered', 'Title reviewed') then 'Needs Review'
        when coalesce(latest_status.review_status, 'New candidate') = 'Strong candidate' then 'Strong Candidates'
        when coalesce(latest_status.review_status, 'New candidate') in ('Watchlist', 'Rejected', 'Likely missed / controlled', 'Not for us', 'Agency angle only', 'Parked') then 'Watchlist / Resurfaced'
        else 'New Candidates'
    end as review_queue,
    latest_status.actor_name as latest_reviewer_name,
    latest_status.reason_text as latest_review_reason,
    latest_status.created_at as latest_review_at,
    latest_note.note_text as latest_note_text,
    latest_note.actor_name as latest_note_actor_name,
    latest_note.created_at as latest_note_at,
    coalesce(note_rollup.note_count, 0) as note_count,
    coalesce(override_rollup.override_count, 0) as override_count,
    case coalesce(title_rollup.title_stage_order, 0)
        when 3 then 'title_reviewed'
        when 2 then 'title_ordered'
        when 1 then 'buy_title_now'
        else 'not_triggered'
    end as title_workflow_state,
    greatest(
        coalesce(latest_status.created_at, site.updated_at),
        coalesce(latest_note.created_at, site.updated_at),
        coalesce(override_rollup.latest_override_at, site.updated_at),
        coalesce(title_rollup.latest_title_event_at, site.updated_at)
    ) as latest_review_activity_at
from landintel.canonical_sites as site
left join latest_status
  on latest_status.canonical_site_id = site.id
left join latest_note
  on latest_note.canonical_site_id = site.id
left join note_rollup
  on note_rollup.canonical_site_id = site.id
left join override_rollup
  on override_rollup.canonical_site_id = site.id
left join title_rollup
  on title_rollup.canonical_site_id = site.id;

create or replace view analytics.v_live_site_change_log
with (security_invoker = true) as
select
    event.id as change_event_id,
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    event.source_family,
    event.change_category,
    event.event_type,
    event.event_summary,
    event.source_record_id,
    event.alert_priority,
    event.resurfaced_flag,
    event.metadata,
    event.created_at
from landintel.site_change_events as event
join landintel.canonical_sites as site
  on site.id = event.canonical_site_id
order by event.created_at desc, site.site_name_primary;

create or replace view analytics.v_live_priority_alerts
with (security_invoker = true) as
select
    change_log.*
from analytics.v_live_site_change_log as change_log
where change_log.alert_priority in ('high', 'critical')
  and change_log.created_at >= now() - interval '30 days'
order by change_log.created_at desc, change_log.site_name;

create or replace view analytics.v_live_site_summary_base
with (security_invoker = true) as
with planning_rollup as (
    select
        planning.canonical_site_id,
        count(*)::bigint as planning_record_count,
        max(planning.updated_at) as latest_planning_update_at
    from landintel.planning_application_records as planning
    where planning.canonical_site_id is not null
    group by planning.canonical_site_id
),
hla_rollup as (
    select
        hla.canonical_site_id,
        count(*)::bigint as hla_record_count,
        max(hla.updated_at) as latest_hla_update_at
    from landintel.hla_site_records as hla
    where hla.canonical_site_id is not null
    group by hla.canonical_site_id
),
bgs_rollup as (
    select
        bgs.canonical_site_id,
        count(*)::bigint as bgs_record_count,
        max(bgs.updated_at) as latest_bgs_update_at
    from landintel.bgs_records as bgs
    where bgs.canonical_site_id is not null
    group by bgs.canonical_site_id
),
flood_rollup as (
    select
        flood.canonical_site_id,
        count(*)::bigint as constraint_record_count,
        max(flood.updated_at) as latest_flood_update_at
    from landintel.flood_records as flood
    where flood.canonical_site_id is not null
    group by flood.canonical_site_id
),
evidence_rollup as (
    select
        evidence.canonical_site_id,
        count(*)::bigint as evidence_count,
        max(evidence.created_at) as latest_evidence_at
    from landintel.evidence_references as evidence
    where evidence.canonical_site_id is not null
      and evidence.active_flag = true
    group by evidence.canonical_site_id
),
alias_rollup as (
    select
        alias.canonical_site_id,
        count(*) filter (where alias.status = 'unresolved' and alias.active_flag = true)::bigint as unresolved_alias_count,
        max(alias.updated_at) as latest_alias_update_at
    from landintel.site_reference_aliases as alias
    where alias.canonical_site_id is not null
    group by alias.canonical_site_id
),
source_presence as (
    select planning.canonical_site_id, 'planning'::text as source_family
    from landintel.planning_application_records as planning
    where planning.canonical_site_id is not null
    union all
    select hla.canonical_site_id, 'hla'::text as source_family
    from landintel.hla_site_records as hla
    where hla.canonical_site_id is not null
    union all
    select bgs.canonical_site_id, 'bgs'::text as source_family
    from landintel.bgs_records as bgs
    where bgs.canonical_site_id is not null
    union all
    select flood.canonical_site_id, 'flood'::text as source_family
    from landintel.flood_records as flood
    where flood.canonical_site_id is not null
    union all
    select ldp.canonical_site_id, 'ldp'::text as source_family
    from landintel.ldp_site_records as ldp
    where ldp.canonical_site_id is not null
    union all
    select ela.canonical_site_id, 'ela'::text as source_family
    from landintel.ela_site_records as ela
    where ela.canonical_site_id is not null
    union all
    select vdl.canonical_site_id, 'vdl'::text as source_family
    from landintel.vdl_site_records as vdl
    where vdl.canonical_site_id is not null
    union all
    select link.canonical_site_id, link.source_family
    from landintel.site_source_links as link
    where link.active_flag = true
),
source_presence_rollup as (
    select
        presence.canonical_site_id,
        array_agg(distinct presence.source_family order by presence.source_family) as source_families_present
    from source_presence as presence
    where presence.canonical_site_id is not null
    group by presence.canonical_site_id
),
link_confidence_rollup as (
    select
        link.canonical_site_id,
        avg(link.confidence) as average_link_confidence,
        count(*) filter (where link.active_flag = true)::bigint as source_link_count,
        max(link.updated_at) as latest_link_update_at
    from landintel.site_source_links as link
    where link.canonical_site_id is not null
      and link.active_flag = true
    group by link.canonical_site_id
),
site_activity as (
    select planning.canonical_site_id, planning.updated_at as event_at, planning.ingest_run_id
    from landintel.planning_application_records as planning
    where planning.canonical_site_id is not null
    union all
    select hla.canonical_site_id, hla.updated_at as event_at, hla.ingest_run_id
    from landintel.hla_site_records as hla
    where hla.canonical_site_id is not null
    union all
    select bgs.canonical_site_id, bgs.updated_at as event_at, bgs.ingest_run_id
    from landintel.bgs_records as bgs
    where bgs.canonical_site_id is not null
    union all
    select flood.canonical_site_id, flood.updated_at as event_at, flood.ingest_run_id
    from landintel.flood_records as flood
    where flood.canonical_site_id is not null
    union all
    select ldp.canonical_site_id, ldp.updated_at as event_at, ldp.ingest_run_id
    from landintel.ldp_site_records as ldp
    where ldp.canonical_site_id is not null
    union all
    select ela.canonical_site_id, ela.updated_at as event_at, ela.ingest_run_id
    from landintel.ela_site_records as ela
    where ela.canonical_site_id is not null
    union all
    select vdl.canonical_site_id, vdl.updated_at as event_at, vdl.ingest_run_id
    from landintel.vdl_site_records as vdl
    where vdl.canonical_site_id is not null
    union all
    select link.canonical_site_id, link.updated_at as event_at, link.ingest_run_id
    from landintel.site_source_links as link
    where link.canonical_site_id is not null
      and link.active_flag = true
    union all
    select alias.canonical_site_id, alias.updated_at as event_at, alias.ingest_run_id
    from landintel.site_reference_aliases as alias
    where alias.canonical_site_id is not null
      and alias.active_flag = true
    union all
    select evidence.canonical_site_id, evidence.created_at as event_at, evidence.ingest_run_id
    from landintel.evidence_references as evidence
    where evidence.canonical_site_id is not null
      and evidence.active_flag = true
    union all
    select site.id as canonical_site_id, site.updated_at as event_at, null::uuid as ingest_run_id
    from landintel.canonical_sites as site
),
latest_source_update as (
    select
        activity.canonical_site_id,
        max(activity.event_at) as latest_source_update_at
    from site_activity as activity
    group by activity.canonical_site_id
),
latest_ingest as (
    select
        ranked.canonical_site_id,
        ranked.ingest_run_id,
        ranked.status as latest_ingest_status
    from (
        select
            activity.canonical_site_id,
            activity.ingest_run_id,
            ingest.status,
            row_number() over (
                partition by activity.canonical_site_id
                order by activity.event_at desc nulls last, ingest.started_at desc nulls last, activity.ingest_run_id desc nulls last
            ) as row_number
        from site_activity as activity
        join public.ingest_runs as ingest
          on ingest.id = activity.ingest_run_id
        where activity.ingest_run_id is not null
    ) as ranked
    where ranked.row_number = 1
),
site_rollup as (
    select
        site.id as canonical_site_id,
        site.site_code,
        site.site_name_primary as site_name,
        site.authority_name,
        null::text as settlement_name,
        site.area_acres,
        site.workflow_status,
        site.surfaced_reason,
        site.primary_ros_parcel_id as primary_parcel_id,
        coalesce(planning_rollup.planning_record_count, 0) as planning_record_count,
        coalesce(hla_rollup.hla_record_count, 0) as hla_record_count,
        coalesce(bgs_rollup.bgs_record_count, 0) as bgs_record_count,
        coalesce(flood_rollup.constraint_record_count, 0) as constraint_record_count,
        coalesce(evidence_rollup.evidence_count, 0) as evidence_count,
        coalesce(source_presence_rollup.source_families_present, '{}'::text[]) as source_families_present,
        coalesce(alias_rollup.unresolved_alias_count, 0) as unresolved_alias_count,
        latest_source_update.latest_source_update_at,
        latest_ingest.latest_ingest_status,
        coalesce(link_confidence_rollup.average_link_confidence, 0) as average_link_confidence,
        coalesce(link_confidence_rollup.source_link_count, 0) as source_link_count
    from landintel.canonical_sites as site
    left join planning_rollup
      on planning_rollup.canonical_site_id = site.id
    left join hla_rollup
      on hla_rollup.canonical_site_id = site.id
    left join bgs_rollup
      on bgs_rollup.canonical_site_id = site.id
    left join flood_rollup
      on flood_rollup.canonical_site_id = site.id
    left join evidence_rollup
      on evidence_rollup.canonical_site_id = site.id
    left join alias_rollup
      on alias_rollup.canonical_site_id = site.id
    left join source_presence_rollup
      on source_presence_rollup.canonical_site_id = site.id
    left join link_confidence_rollup
      on link_confidence_rollup.canonical_site_id = site.id
    left join latest_source_update
      on latest_source_update.canonical_site_id = site.id
    left join latest_ingest
      on latest_ingest.canonical_site_id = site.id
),
site_status as (
    select
        rollup.*,
        case
            when cardinality(rollup.source_families_present) = 0 then 'raw_only'
            when rollup.authority_name is not null
             and coalesce(rollup.area_acres, 0) > 0
             and nullif(btrim(coalesce(rollup.surfaced_reason, '')), '') is not null
             and (rollup.planning_record_count > 0 or rollup.hla_record_count > 0)
             and rollup.evidence_count >= 3
             and rollup.bgs_record_count > 0 then 'linked_enriched'
            when rollup.authority_name is not null
             and coalesce(rollup.area_acres, 0) > 0
             and nullif(btrim(coalesce(rollup.surfaced_reason, '')), '') is not null
             and (rollup.planning_record_count > 0 or rollup.hla_record_count > 0)
             and rollup.evidence_count > 0 then 'linked_core'
            else 'linked_partial'
        end as data_completeness_status,
        case
            when rollup.source_link_count = 0 or cardinality(rollup.source_families_present) = 0 then 'unresolved_links'
            when rollup.unresolved_alias_count > 0 or rollup.average_link_confidence < 0.75 then 'review_needed'
            when rollup.evidence_count > 0 then 'clear'
            else 'review_needed'
        end as traceability_status,
        case
            when rollup.planning_record_count > 0 and rollup.hla_record_count > 0 and rollup.bgs_record_count > 0 then 'planning_hla_bgs_linked'
            when rollup.planning_record_count > 0 and rollup.hla_record_count > 0 then 'planning_hla_linked'
            when rollup.planning_record_count > 0 then 'planning_only'
            when rollup.hla_record_count > 0 then 'hla_only'
            when cardinality(rollup.source_families_present) > 0 then 'linked_partial'
            else null
        end as site_stage,
        array_remove(array[
            case when rollup.planning_record_count = 0 then 'no planning link' end,
            case when rollup.hla_record_count = 0 then 'no HLA link' end,
            case when rollup.bgs_record_count = 0 then 'no BGS/ground record' end,
            case when rollup.constraint_record_count = 0 then 'no constraints context' end,
            case when rollup.evidence_count = 0 then 'no evidence references' end,
            case when nullif(btrim(coalesce(rollup.surfaced_reason, '')), '') is null then 'no surfaced reason' end,
            case when coalesce(rollup.area_acres, 0) <= 0 then 'no area acres' end,
            case when cardinality(rollup.source_families_present) = 0 then 'no linked source families' end
        ], null::text) as missing_core_inputs
    from site_rollup as rollup
)
select
    canonical_site_id,
    site_code,
    site_name,
    authority_name,
    settlement_name,
    area_acres,
    workflow_status,
    surfaced_reason,
    primary_parcel_id,
    planning_record_count,
    hla_record_count,
    bgs_record_count,
    constraint_record_count,
    evidence_count,
    source_families_present,
    unresolved_alias_count,
    latest_source_update_at,
    latest_ingest_status,
    data_completeness_status,
    traceability_status,
    site_stage,
    coalesce(area_acres, 0) > 0
        and nullif(btrim(coalesce(authority_name, '')), '') is not null
        and cardinality(source_families_present) > 0
        and (planning_record_count > 0 or hla_record_count > 0)
        and nullif(btrim(coalesce(surfaced_reason, '')), '') is not null
        and evidence_count > 0
        and traceability_status <> 'unresolved_links' as review_ready_flag,
    coalesce(area_acres, 0) > 0
        and nullif(btrim(coalesce(authority_name, '')), '') is not null
        and cardinality(source_families_present) > 0
        and (planning_record_count > 0 or hla_record_count > 0)
        and nullif(btrim(coalesce(surfaced_reason, '')), '') is not null
        and evidence_count > 0
        and traceability_status <> 'unresolved_links'
        and planning_record_count > 0
        and hla_record_count > 0
        and constraint_record_count > 0
        and evidence_count >= 3
        and traceability_status = 'clear' as commercial_ready_flag,
    missing_core_inputs,
    case
        when cardinality(source_families_present) = 0 then 'No linked source families yet.'
        when coalesce(area_acres, 0) <= 0 then 'No area acres recorded on the canonical site.'
        when planning_record_count = 0 and hla_record_count = 0 then 'No planning or HLA context linked yet.'
        when nullif(btrim(coalesce(surfaced_reason, '')), '') is null then 'No surfaced reason has been recorded yet.'
        when evidence_count = 0 then 'No evidence references have been attached yet.'
        when traceability_status = 'unresolved_links' then 'Source linkage is still unresolved.'
        when coalesce(area_acres, 0) > 0
         and nullif(btrim(coalesce(authority_name, '')), '') is not null
         and cardinality(source_families_present) > 0
         and (planning_record_count > 0 or hla_record_count > 0)
         and nullif(btrim(coalesce(surfaced_reason, '')), '') is not null
         and evidence_count > 0
         and traceability_status <> 'unresolved_links'
         and constraint_record_count = 0 then 'No constraints context linked yet.'
        when coalesce(area_acres, 0) > 0
         and nullif(btrim(coalesce(authority_name, '')), '') is not null
         and cardinality(source_families_present) > 0
         and (planning_record_count > 0 or hla_record_count > 0)
         and nullif(btrim(coalesce(surfaced_reason, '')), '') is not null
         and evidence_count > 0
         and traceability_status <> 'unresolved_links'
         and evidence_count < 3 then 'Evidence depth is still below the commercial readiness threshold.'
        else null
    end as why_not_ready
from site_status
order by authority_name, site_name;

create or replace view analytics.v_live_site_summary
with (security_invoker = true) as
select
    summary.canonical_site_id,
    summary.site_code,
    summary.site_name,
    summary.authority_name,
    summary.settlement_name,
    summary.area_acres,
    summary.workflow_status,
    summary.surfaced_reason,
    summary.primary_parcel_id,
    summary.planning_record_count,
    summary.hla_record_count,
    summary.bgs_record_count,
    summary.constraint_record_count,
    summary.evidence_count,
    summary.source_families_present,
    summary.unresolved_alias_count,
    summary.latest_source_update_at,
    summary.latest_ingest_status,
    summary.data_completeness_status,
    summary.traceability_status,
    summary.site_stage,
    summary.review_ready_flag,
    summary.commercial_ready_flag,
    summary.missing_core_inputs,
    summary.why_not_ready,
    assessment.overall_tier,
    assessment.overall_rank_score,
    assessment.queue_recommendation,
    assessment.title_state,
    assessment.ownership_control_fact_label,
    assessment.size_rank,
    assessment.planning_context_rank,
    assessment.location_rank,
    assessment.constraints_rank,
    assessment.access_rank,
    assessment.geometry_rank,
    assessment.ownership_control_rank,
    constraints.constraint_severity,
    review_state.review_status,
    review_state.review_queue,
    change_log.event_summary as last_change_summary,
    change_log.resurfaced_flag,
    assessment.good_items -> 0 ->> 'headline' as good_headline,
    assessment.bad_items -> 0 ->> 'headline' as bad_headline,
    assessment.ugly_items -> 0 ->> 'headline' as ugly_headline
from analytics.v_live_site_summary_base as summary
left join analytics.v_live_site_assessment as assessment
  on assessment.canonical_site_id = summary.canonical_site_id
left join analytics.v_live_site_constraints as constraints
  on constraints.canonical_site_id = summary.canonical_site_id
left join analytics.v_live_site_review_state as review_state
  on review_state.canonical_site_id = summary.canonical_site_id
left join lateral (
    select
        change_log.event_summary,
        change_log.resurfaced_flag,
        change_log.created_at
    from analytics.v_live_site_change_log as change_log
    where change_log.canonical_site_id = summary.canonical_site_id
    order by change_log.created_at desc, change_log.change_event_id desc
    limit 1
) as change_log on true
order by summary.authority_name, summary.site_name;

create or replace view analytics.v_live_site_readiness
with (security_invoker = true) as
select
    summary.canonical_site_id,
    summary.site_code,
    summary.site_name,
    summary.authority_name,
    summary.area_acres,
    summary.source_families_present,
    summary.planning_record_count,
    summary.hla_record_count,
    summary.bgs_record_count,
    summary.constraint_record_count,
    summary.review_ready_flag,
    summary.commercial_ready_flag,
    case
        when summary.commercial_ready_flag then 'commercial_ready'
        when summary.review_ready_flag then 'review_ready'
        else 'not_ready'
    end as minimum_readiness_band,
    summary.missing_core_inputs,
    summary.why_not_ready,
    summary.latest_source_update_at,
    summary.overall_tier,
    summary.queue_recommendation,
    summary.title_state,
    summary.review_status,
    summary.last_change_summary
from analytics.v_live_site_summary as summary
order by summary.authority_name, summary.site_name;

create or replace view analytics.v_live_site_sources
with (security_invoker = true) as
with link_rollup as (
    select
        link.canonical_site_id,
        link.source_family,
        link.source_dataset,
        count(distinct link.source_record_id)::bigint as linked_source_record_count,
        array_agg(distinct link.source_record_id order by link.source_record_id) as source_record_ids,
        avg(link.confidence) as average_link_confidence,
        max(link.updated_at) as latest_source_update_at
    from landintel.site_source_links as link
    where link.active_flag = true
    group by link.canonical_site_id, link.source_family, link.source_dataset
),
alias_rollup as (
    select
        alias.canonical_site_id,
        alias.source_family,
        alias.source_dataset,
        array_agg(distinct alias.raw_reference_value order by alias.raw_reference_value) as alias_references,
        max(alias.updated_at) as latest_alias_update_at
    from landintel.site_reference_aliases as alias
    where alias.active_flag = true
    group by alias.canonical_site_id, alias.source_family, alias.source_dataset
),
dominant_link_method as (
    select
        ranked.canonical_site_id,
        ranked.source_family,
        ranked.source_dataset,
        ranked.link_method
    from (
        select
            link.canonical_site_id,
            link.source_family,
            link.source_dataset,
            link.link_method,
            row_number() over (
                partition by link.canonical_site_id, link.source_family, link.source_dataset
                order by count(*) desc, max(link.updated_at) desc nulls last
            ) as row_number
        from landintel.site_source_links as link
        where link.active_flag = true
        group by link.canonical_site_id, link.source_family, link.source_dataset, link.link_method
    ) as ranked
    where ranked.row_number = 1
),
latest_link_run as (
    select
        ranked.canonical_site_id,
        ranked.source_family,
        ranked.source_dataset,
        ranked.ingest_run_id
    from (
        select
            link.canonical_site_id,
            link.source_family,
            link.source_dataset,
            link.ingest_run_id,
            row_number() over (
                partition by link.canonical_site_id, link.source_family, link.source_dataset
                order by link.updated_at desc nulls last, link.created_at desc, link.ingest_run_id desc nulls last
            ) as row_number
        from landintel.site_source_links as link
        where link.active_flag = true
    ) as ranked
    where ranked.row_number = 1
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    link.source_family,
    link.source_dataset,
    link.linked_source_record_count,
    array(
        select distinct reference_value.reference_value
        from unnest(coalesce(alias_rollup.alias_references, '{}'::text[]) || coalesce(link.source_record_ids, '{}'::text[])) as reference_value(reference_value)
        where nullif(btrim(reference_value.reference_value), '') is not null
        order by reference_value.reference_value
    ) as key_references,
    dominant.link_method,
    round(coalesce(link.average_link_confidence, 0), 3) as average_link_confidence,
    latest_run.ingest_run_id as latest_ingest_run_id,
    greatest(link.latest_source_update_at, coalesce(alias_rollup.latest_alias_update_at, link.latest_source_update_at)) as latest_source_update_at
from link_rollup as link
join landintel.canonical_sites as site
  on site.id = link.canonical_site_id
left join alias_rollup
  on alias_rollup.canonical_site_id = link.canonical_site_id
 and alias_rollup.source_family = link.source_family
 and alias_rollup.source_dataset = link.source_dataset
left join dominant_link_method as dominant
  on dominant.canonical_site_id = link.canonical_site_id
 and dominant.source_family = link.source_family
 and dominant.source_dataset = link.source_dataset
left join latest_link_run as latest_run
  on latest_run.canonical_site_id = link.canonical_site_id
 and latest_run.source_family = link.source_family
 and latest_run.source_dataset = link.source_dataset
order by site.authority_name, site.site_name_primary, link.source_family, link.source_dataset;

create or replace view analytics.v_live_opportunity_queue
with (security_invoker = true) as
with latest_change as (
    select
        ranked.canonical_site_id,
        ranked.event_summary as last_change_summary,
        ranked.alert_priority,
        ranked.resurfaced_flag,
        ranked.created_at as latest_change_at
    from (
        select
            change_log.canonical_site_id,
            change_log.event_summary,
            change_log.alert_priority,
            change_log.resurfaced_flag,
            change_log.created_at,
            row_number() over (
                partition by change_log.canonical_site_id
                order by change_log.created_at desc, change_log.change_event_id desc
            ) as row_number
        from analytics.v_live_site_change_log as change_log
    ) as ranked
    where ranked.row_number = 1
),
queue_base as (
    select
        summary.canonical_site_id,
        summary.site_code,
        summary.site_name,
        summary.authority_name,
        summary.area_acres,
        case
            when review_state.review_status <> 'New candidate' then review_state.review_queue
            when coalesce(latest_change.resurfaced_flag, false) then 'Watchlist / Resurfaced'
            when assessment.queue_recommendation in ('New Candidates', 'Needs Review', 'Strong Candidates', 'Watchlist / Resurfaced') then assessment.queue_recommendation
            else 'New Candidates'
        end as queue_name,
        assessment.overall_tier,
        assessment.overall_rank_score,
        coalesce(assessment.why_it_surfaced, summary.surfaced_reason, 'No surfaced reason recorded yet.') as why_it_surfaced,
        coalesce(assessment.why_it_survived, 'The site remains live because it still shows at least one route worth human review.') as why_it_survived,
        assessment.size_rank,
        assessment.planning_context_rank,
        assessment.location_rank,
        assessment.constraints_rank,
        assessment.access_rank,
        assessment.geometry_rank,
        assessment.ownership_control_rank,
        assessment.location_band,
        assessment.access_strength,
        assessment.geometry_quality,
        assessment.ownership_control_state,
        assessment.constraint_severity,
        assessment.planning_context_band,
        assessment.settlement_position,
        assessment.title_state,
        assessment.ownership_control_fact_label,
        review_state.review_status,
        review_state.latest_note_text,
        coalesce(latest_change.resurfaced_flag, false) as resurfaced_flag,
        latest_change.last_change_summary,
        latest_change.latest_change_at,
        case
            when summary.planning_record_count > 0 and summary.hla_record_count > 0 then 'multi-source'
            when summary.planning_record_count > 0 then 'planning-led'
            when summary.hla_record_count > 0 then 'hla/hls-led'
            when summary.source_families_present && array['ela', 'vdl']::text[] then 'brownfield-led'
            when summary.source_families_present && array['ldp']::text[] then 'policy-led'
            else 'parcel-led'
        end as source_route,
        assessment.good_items,
        assessment.bad_items,
        assessment.ugly_items
    from analytics.v_live_site_summary as summary
    left join analytics.v_live_site_assessment as assessment
      on assessment.canonical_site_id = summary.canonical_site_id
    left join analytics.v_live_site_review_state as review_state
      on review_state.canonical_site_id = summary.canonical_site_id
    left join latest_change
      on latest_change.canonical_site_id = summary.canonical_site_id
)
select
    base.*,
    row_number() over (
        partition by base.queue_name
        order by
            coalesce(base.overall_rank_score, 0) desc,
            base.latest_change_at desc nulls last,
            base.site_name
    ) as queue_position,
    base.good_items -> 0 ->> 'headline' as good_headline,
    base.bad_items -> 0 ->> 'headline' as bad_headline,
    base.ugly_items -> 0 ->> 'headline' as ugly_headline
from queue_base as base
order by
    case base.queue_name
        when 'Strong Candidates' then 1
        when 'Needs Review' then 2
        when 'New Candidates' then 3
        when 'Watchlist / Resurfaced' then 4
        else 5
    end,
    queue_position,
    base.site_name;
