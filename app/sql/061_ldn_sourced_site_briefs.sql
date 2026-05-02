create or replace function public.is_scottish_title_number_candidate(raw_title text)
returns boolean
language sql
immutable
set search_path = pg_catalog, public
as $$
    with normalized as (
        select public.normalize_site_title_number(raw_title) as title_number
    )
    select coalesce(
        title_number ~ '^[A-Z]{2,5}[0-9]{1,10}$'
        and title_number !~ '^SCT[0-9]+$',
        false
    )
    from normalized;
$$;

create or replace function public.extract_ros_title_number_candidate(raw_attributes jsonb, ros_inspire_id text)
returns text
language sql
immutable
set search_path = pg_catalog, public
as $$
    with attribute_candidates as (
        select
            value as raw_value,
            case lower(key)
                when 'title_no' then 1
                when 'titleno' then 1
                when 'title_number' then 1
                when 'titlenumber' then 1
                when 'title_num' then 1
                when 'titlenum' then 1
                when 'title' then 2
                when 'label' then 3
                when 'cadastralunit' then 4
                when 'cadastral_unit' then 4
                when 'nationalca' then 5
                when 'nationalcadastralreference' then 5
                else 50
            end as priority_rank
        from jsonb_each_text(coalesce(raw_attributes, '{}'::jsonb))
        where lower(key) in (
            'title_no',
            'titleno',
            'title_number',
            'titlenumber',
            'title_num',
            'titlenum',
            'title',
            'label',
            'cadastralunit',
            'cadastral_unit',
            'nationalca',
            'nationalcadastralreference'
        )
    ), inspire_tail as (
        select
            regexp_replace(coalesce(ros_inspire_id, ''), '^.*\.', '') as raw_value,
            100 as priority_rank
    ), candidate_values as (
        select raw_value, priority_rank from attribute_candidates
        union all
        select raw_value, priority_rank from inspire_tail
    ), normalized as (
        select
            priority_rank,
            public.normalize_site_title_number(raw_value) as title_number
        from candidate_values
    )
    select title_number
    from normalized
    where public.is_scottish_title_number_candidate(title_number)
    order by priority_rank
    limit 1;
$$;

update public.ros_cadastral_parcels
set title_number = null,
    normalized_title_number = null,
    updated_at = now()
where coalesce(normalized_title_number, public.normalize_site_title_number(title_number)) ~ '^SCT[0-9]+$';

update public.site_title_resolution_candidates
set candidate_title_number = null,
    normalized_title_number = null,
    resolution_status = case
        when ros_parcel_id is not null
          or ros_inspire_id is not null
          or cadastral_unit_identifier is not null then 'needs_licensed_bridge'
        else 'manual_review'
    end,
    metadata = metadata || jsonb_build_object(
        'sct_identifier_interpretation', 'SCT is retained as the RoS cadastral parcel reference, not treated as a ScotLIS title number.',
        'title_bridge_method', 'ros_attribute_title_number_only'
    ),
    updated_at = now()
where coalesce(normalized_title_number, public.normalize_site_title_number(candidate_title_number)) ~ '^SCT[0-9]+$';

update public.site_title_validation
set validation_status = 'rejected',
    confidence = 0,
    metadata = metadata || jsonb_build_object(
        'sct_identifier_interpretation', 'SCT is retained as the RoS cadastral parcel reference, not treated as a ScotLIS title number.',
        'title_bridge_method', 'ros_attribute_title_number_only'
    ),
    updated_at = now()
where coalesce(normalized_title_number, public.normalize_site_title_number(title_number)) ~ '^SCT[0-9]+$';

update landintel.site_urgent_address_title_pack
set title_number = null,
    normalized_title_number = null,
    title_candidate_status = case
        when ros_parcel_id is not null
          or ros_inspire_id is not null then 'parcel_candidate_identified'
        else 'title_required'
    end,
    metadata = metadata || jsonb_build_object(
        'sct_identifier_interpretation', 'SCT is retained as the RoS cadastral parcel reference, not treated as a ScotLIS title number.',
        'title_bridge_method', 'ros_attribute_title_number_only'
    ),
    updated_at = now()
where coalesce(normalized_title_number, public.normalize_site_title_number(title_number)) ~ '^SCT[0-9]+$';

drop view if exists analytics.v_ldn_sourced_site_brief_coverage;
drop view if exists analytics.v_ldn_sourced_site_briefs;

create or replace view analytics.v_ldn_sourced_site_briefs
with (security_invoker = true) as
with base_sites as (
    select
        site.id as canonical_site_id,
        site.id::text as site_location_id,
        site.site_name_primary,
        site.authority_name,
        site.area_acres,
        site.geometry,
        ldn.candidate_status,
        ldn.register_profile,
        ldn.register_origin_site,
        ldn.register_corroboration_status,
        ldn.ownership_classification,
        ldn.owner_name_signal,
        ldn.control_blocker_type,
        ldn.control_blocker_name,
        ldn.no_housebuilder_developer_signal,
        ldn.ldn_target_private_no_builder,
        ldn.development_progress_status,
        ldn.build_started_indicator,
        ldn.stalled_indicator,
        ldn.constraint_position,
        ldn.measured_constraint_count,
        ldn.max_constraint_overlap_pct,
        ldn.constraint_character,
        ldn.planning_position,
        ldn.market_position,
        ldn.why_it_matters,
        ldn.top_positives,
        ldn.top_warnings,
        ldn.missing_critical_evidence,
        ldn.next_action as ldn_next_action,
        prove_it.claim_statement,
        prove_it.verdict,
        prove_it.review_ready_flag,
        prove_it.title_spend_recommendation,
        prove_it.review_next_action,
        prove_it.evidence_confidence,
        prove_it.prove_it_drivers,
        prove_it.proof_points,
        pack.site_name as pack_site_name,
        pack.urgency_status,
        pack.urgency_source,
        pack.urgency_reason,
        pack.title_spend_recommendation as pack_title_spend_recommendation,
        pack.primary_address_text,
        pack.primary_uprn,
        pack.address_candidate_count,
        pack.address_link_status,
        pack.address_source,
        pack.next_action as pack_next_action
    from landintel.canonical_sites as site
    left join landintel.site_ldn_candidate_screen as ldn
      on ldn.canonical_site_id = site.id
    left join landintel.site_prove_it_assessments as prove_it
      on prove_it.canonical_site_id = site.id
     and prove_it.source_key = 'prove_it_conviction_layer'
     and prove_it.assessment_version = 1
    left join landintel.site_urgent_address_title_pack as pack
      on pack.canonical_site_id = site.id
    where ldn.canonical_site_id is not null
       or prove_it.canonical_site_id is not null
       or pack.canonical_site_id is not null
), title_candidates as (
    select
        base.canonical_site_id,
        candidate.title_number,
        candidate.normalized_title_number,
        candidate.title_candidate_source,
        candidate.title_candidate_status,
        candidate.title_confidence,
        candidate.ros_parcel_id,
        candidate.ros_inspire_id,
        candidate.cadastral_unit_identifier,
        candidate.overlap_pct_of_site,
        candidate.nearest_distance_m
    from base_sites as base
    left join lateral (
        select *
        from (
            select
                1 as source_rank,
                pack.title_number,
                pack.normalized_title_number,
                pack.title_candidate_source,
                pack.title_candidate_status,
                pack.title_confidence,
                pack.ros_parcel_id,
                pack.ros_inspire_id,
                null::text as cadastral_unit_identifier,
                null::numeric as overlap_pct_of_site,
                null::numeric as nearest_distance_m
            from landintel.site_urgent_address_title_pack as pack
            where pack.canonical_site_id = base.canonical_site_id

            union all

            select
                2,
                validation.title_number,
                validation.normalized_title_number,
                coalesce(validation.title_source, 'site_title_validation'),
                case
                    when validation.validation_status <> 'rejected'
                     and public.is_scottish_title_number_candidate(validation.normalized_title_number)
                        then 'possible_title_reference_identified'
                    else 'title_required'
                end,
                validation.confidence,
                nullif(validation.metadata ->> 'ros_parcel_id', '')::uuid,
                validation.metadata ->> 'ros_inspire_id',
                validation.metadata ->> 'cadastral_unit_identifier',
                null::numeric,
                null::numeric
            from public.site_title_validation as validation
            where validation.site_id = base.canonical_site_id::text
              and validation.validation_status <> 'rejected'

            union all

            select
                3,
                candidate.candidate_title_number,
                candidate.normalized_title_number,
                candidate.candidate_source,
                case
                    when public.is_scottish_title_number_candidate(candidate.normalized_title_number)
                        then 'possible_title_reference_identified'
                    when candidate.ros_parcel_id is not null
                      or candidate.ros_inspire_id is not null
                      or candidate.cadastral_unit_identifier is not null
                        then 'parcel_candidate_identified'
                    else 'title_required'
                end,
                candidate.confidence,
                candidate.ros_parcel_id,
                candidate.ros_inspire_id,
                candidate.cadastral_unit_identifier,
                candidate.overlap_pct_of_site,
                candidate.nearest_distance_m
            from public.site_title_resolution_candidates as candidate
            where candidate.site_id = base.canonical_site_id::text

            union all

            select
                4,
                parcel.title_number,
                parcel.normalized_title_number,
                'primary_ros_cadastral_parcel',
                case
                    when public.is_scottish_title_number_candidate(parcel.normalized_title_number)
                        then 'possible_title_reference_identified'
                    when parcel.id is not null then 'parcel_candidate_identified'
                    else 'title_required'
                end,
                0.6::numeric,
                parcel.id,
                parcel.ros_inspire_id,
                public.extract_ros_cadastral_identifier(parcel.raw_attributes, parcel.ros_inspire_id),
                null::numeric,
                null::numeric
            from landintel.canonical_sites as site
            join public.ros_cadastral_parcels as parcel
              on parcel.id = site.primary_ros_parcel_id
            where site.id = base.canonical_site_id
        ) as title_rows
        where title_candidate_status <> 'title_required'
           or ros_parcel_id is not null
           or ros_inspire_id is not null
        order by
            case
                when title_candidate_status = 'possible_title_reference_identified'
                 and public.is_scottish_title_number_candidate(normalized_title_number) then 0
                when title_candidate_status = 'parcel_candidate_identified' then 1
                else 2
            end,
            source_rank,
            title_confidence desc nulls last
        limit 1
    ) as candidate on true
), site_points as (
    select
        base.canonical_site_id,
        case when base.geometry is not null then st_transform(st_pointonsurface(base.geometry), 4326) end as point_4326
    from base_sites as base
)
select
    base.canonical_site_id,
    base.site_location_id,
    coalesce(
        nullif(base.pack_site_name, ''),
        nullif(base.site_name_primary, ''),
        case
            when base.primary_address_text is not null then 'Land near ' || base.primary_address_text
            when base.authority_name is not null then 'Land parcel in ' || base.authority_name
            else 'Sourced land parcel'
        end
    ) as site_brief_title,
    case
        when base.primary_address_text is not null then 'Land parcel near ' || base.primary_address_text
        when title.ros_inspire_id is not null then 'RoS cadastral parcel ' || title.ros_inspire_id || coalesce(' in ' || base.authority_name, '')
        else 'Sourced land parcel' || coalesce(' in ' || base.authority_name, '')
    end as what_the_site_is,
    base.authority_name,
    base.area_acres,
    round(st_y(point.point_4326)::numeric, 7) as latitude,
    round(st_x(point.point_4326)::numeric, 7) as longitude,
    case
        when point.point_4326 is not null then
            'https://www.google.com/maps/search/?api=1&query='
            || round(st_y(point.point_4326)::numeric, 7)::text
            || ','
            || round(st_x(point.point_4326)::numeric, 7)::text
        else null
    end as google_maps_url,
    case
        when public.is_scottish_title_number_candidate(title.normalized_title_number)
            then coalesce(title.title_number, title.normalized_title_number)
        else null
    end as title_number,
    case
        when public.is_scottish_title_number_candidate(title.normalized_title_number)
            then title.normalized_title_number
        else null
    end as normalized_title_number,
    case
        when public.is_scottish_title_number_candidate(title.normalized_title_number)
            then 'title_number_candidate_identified'
        when title.ros_parcel_id is not null
          or title.ros_inspire_id is not null
          or title.cadastral_unit_identifier is not null
            then 'ros_parcel_candidate_title_required'
        else 'title_required'
    end as title_status,
    title.title_candidate_source,
    title.title_confidence,
    title.ros_parcel_id,
    title.ros_inspire_id,
    title.cadastral_unit_identifier,
    title.overlap_pct_of_site as title_candidate_overlap_pct_of_site,
    title.nearest_distance_m as title_candidate_nearest_distance_m,
    base.primary_address_text,
    base.primary_uprn,
    coalesce(base.address_candidate_count, 0) as address_candidate_count,
    coalesce(base.address_link_status, 'address_missing') as address_link_status,
    base.address_source,
    base.urgency_status,
    base.urgency_source,
    base.candidate_status,
    base.verdict,
    base.review_ready_flag,
    coalesce(base.pack_title_spend_recommendation, base.title_spend_recommendation) as title_spend_recommendation,
    base.evidence_confidence,
    base.register_profile,
    base.register_origin_site,
    base.register_corroboration_status,
    base.ownership_classification,
    base.owner_name_signal,
    base.control_blocker_type,
    base.control_blocker_name,
    base.no_housebuilder_developer_signal,
    base.ldn_target_private_no_builder,
    base.development_progress_status,
    base.build_started_indicator,
    base.stalled_indicator,
    base.constraint_position,
    base.measured_constraint_count,
    base.max_constraint_overlap_pct,
    base.constraint_character,
    base.planning_position,
    base.market_position,
    coalesce(base.claim_statement, base.why_it_matters, base.urgency_reason) as why_ldn_should_look,
    base.prove_it_drivers,
    base.proof_points,
    base.top_positives,
    base.top_warnings,
    base.missing_critical_evidence,
    coalesce(base.pack_next_action, base.review_next_action, base.ldn_next_action) as what_ldn_should_do_next,
    case
        when public.is_scottish_title_number_candidate(title.normalized_title_number)
            then 'Title number candidate comes from RoS/title workflow attributes. It is not ownership confirmation.'
        when title.ros_inspire_id is not null
            then 'RoS parcel identified, but no ScotLIS title number has been proven from open attributes. Manual title workflow required.'
        else 'No RoS title candidate attached yet. Manual title workflow required if the site remains worth spend.'
    end as title_bridge_explanation,
    'ownership_not_confirmed_until_title_review'::text as ownership_limitation,
    'title_number_candidate_not_ownership_confirmation'::text as title_limitation
from base_sites as base
left join title_candidates as title
  on title.canonical_site_id = base.canonical_site_id
left join site_points as point
  on point.canonical_site_id = base.canonical_site_id
order by
    case
        when base.candidate_status = 'true_ldn_candidate' then 1
        when base.urgency_status = 'order_title_urgently' then 2
        when base.candidate_status in ('review_forgotten_soul', 'review_private_candidate') then 3
        when base.review_ready_flag then 4
        else 5
    end,
    public.is_scottish_title_number_candidate(title.normalized_title_number) desc,
    base.area_acres desc nulls last,
    base.authority_name,
    site_brief_title;

create or replace view analytics.v_ldn_sourced_site_brief_coverage
with (security_invoker = true) as
select
    count(*)::bigint as sourced_site_brief_count,
    count(*) filter (where candidate_status = 'true_ldn_candidate')::bigint as true_ldn_candidate_brief_count,
    count(*) filter (where review_ready_flag = true)::bigint as review_ready_brief_count,
    count(*) filter (where title_status = 'title_number_candidate_identified')::bigint as with_title_number_candidate_count,
    count(*) filter (where title_status = 'ros_parcel_candidate_title_required')::bigint as with_ros_parcel_title_required_count,
    count(*) filter (where title_status = 'title_required')::bigint as title_required_count,
    count(*) filter (where address_link_status = 'address_linked')::bigint as with_address_count,
    count(*) filter (where google_maps_url is not null)::bigint as with_map_link_count,
    count(*) filter (where why_ldn_should_look is not null)::bigint as with_commercial_reason_count,
    count(*) filter (where what_ldn_should_do_next is not null)::bigint as with_next_action_count
from analytics.v_ldn_sourced_site_briefs;

grant select on analytics.v_ldn_sourced_site_briefs to authenticated;
grant select on analytics.v_ldn_sourced_site_brief_coverage to authenticated;

revoke all on function public.is_scottish_title_number_candidate(text) from anon, authenticated;
revoke all on function public.extract_ros_title_number_candidate(jsonb, text) from anon, authenticated;

comment on function public.is_scottish_title_number_candidate(text)
    is 'Returns true only for title-number-shaped values. SCT parcel identifiers are deliberately excluded.';

comment on function public.extract_ros_title_number_candidate(jsonb, text)
    is 'Extracts a title-number candidate from RoS parcel attributes. SCT INSPIRE identifiers are retained as parcel references and are not treated as ScotLIS title numbers.';

comment on view analytics.v_ldn_sourced_site_briefs
    is 'Readable LDN sourcing brief showing what the site is, where it is, why LDN should look, address context, RoS parcel/title candidate status and the next human action.';

comment on view analytics.v_ldn_sourced_site_brief_coverage
    is 'Coverage audit for sourced-site briefs, including title candidate, address, map-link, commercial reason and next-action completeness.';
