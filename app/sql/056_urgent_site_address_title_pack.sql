create or replace function landintel.refresh_urgent_site_address_title_pack(
    p_batch_size integer default 25,
    p_authority_name text default null
)
returns table (
    selected_urgent_site_count integer,
    address_candidate_count integer,
    pack_row_count integer,
    pack_with_address_count integer,
    pack_with_title_candidate_count integer,
    evidence_row_count integer,
    signal_row_count integer,
    change_event_count integer
)
language plpgsql
set search_path = pg_catalog, public, landintel, extensions
as $$
declare
    v_batch_size integer := greatest(coalesce(p_batch_size, 25), 1);
    v_authority_name text := nullif(btrim(coalesce(p_authority_name, '')), '');
begin
    create temporary table if not exists tmp_urgent_sites (
        canonical_site_id uuid primary key,
        site_location_id text,
        site_name text,
        authority_name text,
        area_acres numeric,
        geometry geometry(MultiPolygon, 27700),
        urgency_status text,
        urgency_source text,
        urgency_reason text,
        title_spend_recommendation text,
        previous_signature text
    ) on commit drop;

    truncate tmp_urgent_sites;

    insert into tmp_urgent_sites (
        canonical_site_id,
        site_location_id,
        site_name,
        authority_name,
        area_acres,
        geometry,
        urgency_status,
        urgency_source,
        urgency_reason,
        title_spend_recommendation,
        previous_signature
    )
    select
        site.id,
        site.id::text,
        site.site_name_primary,
        site.authority_name,
        site.area_acres,
        site.geometry,
        case
            when prove_it.title_spend_recommendation = 'order_title_urgently' then 'order_title_urgently'
            when ldn_screen.candidate_status = 'true_ldn_candidate' then 'true_ldn_candidate'
            when title_workflow.next_action ilike '%%urgent%%'
              or title_workflow.title_order_status ilike '%%urgent%%' then 'title_order_urgent'
            when ldn_screen.candidate_status in ('review_forgotten_soul', 'review_private_candidate', 'constraint_review_required')
              or (
                    prove_it.verdict = 'review'
                and prove_it.review_ready_flag = true
                and prove_it.title_spend_recommendation = 'manual_review_before_order'
              ) then 'urgent_review'
            else 'urgent_review'
        end,
        case
            when prove_it.title_spend_recommendation = 'order_title_urgently' then 'prove_it_conviction_layer'
            when ldn_screen.candidate_status = 'true_ldn_candidate' then 'ldn_candidate_screen'
            when title_workflow.next_action ilike '%%urgent%%'
              or title_workflow.title_order_status ilike '%%urgent%%' then 'title_order_workflow'
            when ldn_screen.candidate_status in ('review_forgotten_soul', 'review_private_candidate', 'constraint_review_required') then 'ldn_candidate_review_queue'
            when prove_it.verdict = 'review'
              and prove_it.review_ready_flag = true
              and prove_it.title_spend_recommendation = 'manual_review_before_order' then 'prove_it_review_queue'
            else 'site_review'
        end,
        coalesce(
            prove_it.review_next_action,
            ldn_screen.next_action,
            title_workflow.next_action,
            'Review site requires address and title-number candidate pack before spend.'
        ),
        coalesce(prove_it.title_spend_recommendation, ldn_screen.title_spend_position, title_workflow.next_action),
        existing.source_record_signature
    from landintel.canonical_sites as site
    left join landintel.site_prove_it_assessments as prove_it
      on prove_it.canonical_site_id = site.id
     and prove_it.source_key = 'prove_it_conviction_layer'
     and prove_it.assessment_version = 1
    left join landintel.site_ldn_candidate_screen as ldn_screen
      on ldn_screen.canonical_site_id = site.id
    left join landintel.title_order_workflow as title_workflow
      on title_workflow.canonical_site_id = site.id
    left join landintel.site_urgent_address_title_pack as existing
      on existing.canonical_site_id = site.id
    where site.geometry is not null
      and (v_authority_name is null or site.authority_name ilike '%%' || v_authority_name || '%%')
      and (
            prove_it.title_spend_recommendation = 'order_title_urgently'
         or (prove_it.verdict = 'pursue' and prove_it.title_spend_recommendation = 'order_title')
         or (
                prove_it.verdict = 'review'
            and prove_it.review_ready_flag = true
            and prove_it.title_spend_recommendation = 'manual_review_before_order'
         )
         or ldn_screen.candidate_status = 'true_ldn_candidate'
         or ldn_screen.candidate_status in (
                'review_forgotten_soul',
                'review_private_candidate',
                'constraint_review_required'
         )
         or title_workflow.next_action ilike '%%urgent%%'
         or title_workflow.title_order_status ilike '%%urgent%%'
      )
    order by
        existing.updated_at nulls first,
        case
            when prove_it.title_spend_recommendation = 'order_title_urgently' then 1
            when ldn_screen.candidate_status = 'true_ldn_candidate' then 2
            when ldn_screen.candidate_status = 'review_forgotten_soul' then 3
            when ldn_screen.candidate_status = 'constraint_review_required' then 4
            when ldn_screen.candidate_status = 'review_private_candidate' then 5
            when prove_it.verdict = 'review' and prove_it.review_ready_flag = true then 6
            else 7
        end,
        coalesce(site.area_acres, 0) desc,
        prove_it.updated_at desc nulls last,
        ldn_screen.updated_at desc nulls last,
        site.id
    limit v_batch_size;

    insert into landintel.site_urgent_address_candidates (
        canonical_site_id,
        site_location_id,
        address_source,
        uprn,
        address_text,
        match_method,
        match_rank,
        distance_m,
        source_record_signature,
        raw_payload,
        metadata,
        updated_at
    )
    select
        selected.canonical_site_id,
        selected.site_location_id,
        'land_object_address_link',
        address_link.uprn,
        address_link.address_text,
        'land_object_near_site',
        row_number() over (
            partition by selected.canonical_site_id
            order by st_distance(land_object.geometry, selected.geometry), address_link.updated_at desc nulls last
        )::integer,
        round(st_distance(land_object.geometry, selected.geometry)::numeric, 2),
        md5(concat_ws(
            '|',
            selected.canonical_site_id::text,
            address_link.id::text,
            coalesce(address_link.uprn, ''),
            coalesce(address_link.address_text, ''),
            round(st_distance(land_object.geometry, selected.geometry)::numeric, 2)::text
        )),
        address_link.metadata,
        jsonb_build_object(
            'source_key', 'urgent_address_title_pack',
            'address_basis', 'land_object_address_links',
            'limitation', 'address_link_is_context_not_legal_extent'
        ),
        now()
    from tmp_urgent_sites as selected
    join public.land_objects as land_object
      on land_object.geometry OPERATOR(extensions.&&) st_expand(selected.geometry, 250)
     and st_dwithin(land_object.geometry, selected.geometry, 250)
    join public.land_object_address_links as address_link
      on address_link.land_object_id = land_object.id
    where nullif(btrim(address_link.address_text), '') is not null
    on conflict (
        canonical_site_id,
        address_source,
        source_record_signature
    ) do update set
        site_location_id = excluded.site_location_id,
        address_text = excluded.address_text,
        match_method = excluded.match_method,
        match_rank = excluded.match_rank,
        distance_m = excluded.distance_m,
        raw_payload = excluded.raw_payload,
        metadata = excluded.metadata,
        updated_at = now();

    create temporary table if not exists tmp_urgent_pack_prepared (
        canonical_site_id uuid primary key,
        site_location_id text,
        site_name text,
        authority_name text,
        area_acres numeric,
        urgency_status text,
        urgency_source text,
        urgency_reason text,
        title_spend_recommendation text,
        title_number text,
        normalized_title_number text,
        title_candidate_source text,
        title_candidate_status text,
        title_confidence numeric,
        ros_parcel_id uuid,
        ros_inspire_id text,
        primary_address_text text,
        primary_uprn text,
        address_candidate_count integer,
        address_link_status text,
        address_source text,
        next_action text,
        previous_signature text,
        current_signature text
    ) on commit drop;

    truncate tmp_urgent_pack_prepared;

    insert into tmp_urgent_pack_prepared
    select
        selected.canonical_site_id,
        selected.site_location_id,
        selected.site_name,
        selected.authority_name,
        selected.area_acres,
        selected.urgency_status,
        selected.urgency_source,
        selected.urgency_reason,
        selected.title_spend_recommendation,
        title_candidate.title_number,
        title_candidate.normalized_title_number,
        title_candidate.title_candidate_source,
        case
            when title_candidate.normalized_title_number is not null then 'possible_title_reference_identified'
            when title_candidate.ros_parcel_id is not null then 'parcel_candidate_identified'
            else 'title_required'
        end,
        title_candidate.title_confidence,
        title_candidate.ros_parcel_id,
        title_candidate.ros_inspire_id,
        primary_address.address_text,
        primary_address.uprn,
        coalesce(address_counts.address_candidate_count, 0),
        case when primary_address.address_text is not null then 'address_linked' else 'address_missing' end,
        primary_address.address_source,
        case
            when primary_address.address_text is null and title_candidate.normalized_title_number is null then 'Link address and resolve RoS title candidate before title spend.'
            when primary_address.address_text is null then 'Link address before ordering title.'
            when title_candidate.normalized_title_number is null then 'Resolve RoS title candidate before title spend.'
            when selected.urgency_status = 'order_title_urgently' then 'Order title urgently with address and title candidate attached.'
            else 'Manual DD review with address and title candidate attached.'
        end,
        selected.previous_signature,
        md5(concat_ws(
            '|',
            selected.canonical_site_id::text,
            selected.urgency_status,
            coalesce(title_candidate.normalized_title_number, ''),
            coalesce(title_candidate.ros_parcel_id::text, ''),
            coalesce(primary_address.uprn, ''),
            coalesce(primary_address.address_text, ''),
            coalesce(address_counts.address_candidate_count, 0)::text
        ))
    from tmp_urgent_sites as selected
    left join lateral (
        select *
        from (
            select
                1 as source_rank,
                workflow.title_number,
                workflow.normalized_title_number,
                'title_order_workflow'::text as title_candidate_source,
                workflow.title_confidence_level as title_confidence,
                nullif(workflow.metadata ->> 'ros_parcel_id', '')::uuid as ros_parcel_id,
                workflow.metadata ->> 'ros_inspire_id' as ros_inspire_id
            from landintel.title_order_workflow as workflow
            where workflow.canonical_site_id = selected.canonical_site_id
              and (workflow.normalized_title_number is not null or workflow.metadata ? 'ros_parcel_id')

            union all

            select
                2,
                validation.title_number,
                validation.normalized_title_number,
                coalesce(validation.title_source, 'site_title_validation'),
                validation.confidence,
                nullif(validation.metadata ->> 'ros_parcel_id', '')::uuid,
                validation.metadata ->> 'ros_inspire_id'
            from public.site_title_validation as validation
            where validation.site_id = selected.canonical_site_id::text

            union all

            select
                3,
                candidate.candidate_title_number,
                candidate.normalized_title_number,
                candidate.candidate_source,
                candidate.confidence,
                candidate.ros_parcel_id,
                candidate.ros_inspire_id
            from public.site_title_resolution_candidates as candidate
            where candidate.site_id = selected.canonical_site_id::text

            union all

            select
                4,
                parcel.title_number,
                parcel.normalized_title_number,
                'primary_ros_cadastral_parcel',
                0.6::numeric,
                parcel.id,
                parcel.ros_inspire_id
            from landintel.canonical_sites as site
            join public.ros_cadastral_parcels as parcel
              on parcel.id = site.primary_ros_parcel_id
            where site.id = selected.canonical_site_id
        ) as title_candidates
        where title_number is not null
           or normalized_title_number is not null
           or ros_parcel_id is not null
        order by source_rank, title_confidence desc nulls last
        limit 1
    ) as title_candidate on true
    left join lateral (
        select candidate.*
        from landintel.site_urgent_address_candidates as candidate
        where candidate.canonical_site_id = selected.canonical_site_id
        order by candidate.match_rank nulls last, candidate.distance_m nulls last, candidate.updated_at desc
        limit 1
    ) as primary_address on true
    left join lateral (
        select count(*)::integer as address_candidate_count
        from landintel.site_urgent_address_candidates as candidate
        where candidate.canonical_site_id = selected.canonical_site_id
    ) as address_counts on true;

    insert into landintel.site_urgent_address_title_pack (
        canonical_site_id,
        site_location_id,
        site_name,
        authority_name,
        area_acres,
        urgency_status,
        urgency_source,
        urgency_reason,
        title_spend_recommendation,
        title_number,
        normalized_title_number,
        title_candidate_source,
        title_candidate_status,
        title_confidence,
        ros_parcel_id,
        ros_inspire_id,
        primary_address_text,
        primary_uprn,
        address_candidate_count,
        address_link_status,
        address_source,
        ownership_status_pre_title,
        ownership_limitation,
        next_action,
        source_record_signature,
        metadata,
        updated_at
    )
    select
        prepared.canonical_site_id,
        prepared.site_location_id,
        prepared.site_name,
        prepared.authority_name,
        prepared.area_acres,
        prepared.urgency_status,
        prepared.urgency_source,
        prepared.urgency_reason,
        prepared.title_spend_recommendation,
        prepared.title_number,
        prepared.normalized_title_number,
        prepared.title_candidate_source,
        prepared.title_candidate_status,
        prepared.title_confidence,
        prepared.ros_parcel_id,
        prepared.ros_inspire_id,
        prepared.primary_address_text,
        prepared.primary_uprn,
        prepared.address_candidate_count,
        prepared.address_link_status,
        prepared.address_source,
        'ownership_not_confirmed',
        'ownership_not_confirmed_until_title_review',
        prepared.next_action,
        prepared.current_signature,
        jsonb_build_object(
            'source_key', 'urgent_address_title_pack',
            'urgency_source', prepared.urgency_source,
            'title_basis', prepared.title_candidate_source,
            'address_basis', prepared.address_source,
            'title_limitation', 'title_number_candidate_not_ownership_confirmation',
            'ownership_limitation', 'ownership_not_confirmed_until_title_review',
            'scotlis_workflow', 'manual_title_order_and_human_review_required'
        ),
        now()
    from tmp_urgent_pack_prepared as prepared
    on conflict (canonical_site_id) do update set
        site_location_id = excluded.site_location_id,
        site_name = excluded.site_name,
        authority_name = excluded.authority_name,
        area_acres = excluded.area_acres,
        urgency_status = excluded.urgency_status,
        urgency_source = excluded.urgency_source,
        urgency_reason = excluded.urgency_reason,
        title_spend_recommendation = excluded.title_spend_recommendation,
        title_number = excluded.title_number,
        normalized_title_number = excluded.normalized_title_number,
        title_candidate_source = excluded.title_candidate_source,
        title_candidate_status = excluded.title_candidate_status,
        title_confidence = excluded.title_confidence,
        ros_parcel_id = excluded.ros_parcel_id,
        ros_inspire_id = excluded.ros_inspire_id,
        primary_address_text = excluded.primary_address_text,
        primary_uprn = excluded.primary_uprn,
        address_candidate_count = excluded.address_candidate_count,
        address_link_status = excluded.address_link_status,
        address_source = excluded.address_source,
        ownership_status_pre_title = excluded.ownership_status_pre_title,
        ownership_limitation = excluded.ownership_limitation,
        next_action = excluded.next_action,
        source_record_signature = excluded.source_record_signature,
        metadata = excluded.metadata,
        updated_at = now();

    create temporary table if not exists tmp_urgent_pack_changed (
        canonical_site_id uuid primary key,
        previous_signature text,
        current_signature text
    ) on commit drop;

    truncate tmp_urgent_pack_changed;

    insert into tmp_urgent_pack_changed
    select
        prepared.canonical_site_id,
        prepared.previous_signature,
        prepared.current_signature
    from tmp_urgent_pack_prepared as prepared
    where prepared.previous_signature is distinct from prepared.current_signature;

    delete from landintel.evidence_references as evidence
    using tmp_urgent_pack_changed as changed
    where evidence.canonical_site_id = changed.canonical_site_id
      and evidence.source_family = 'title_control'
      and evidence.metadata ->> 'source_key' = 'urgent_address_title_pack';

    insert into landintel.evidence_references (
        canonical_site_id,
        source_family,
        source_dataset,
        source_record_id,
        source_reference,
        confidence,
        metadata
    )
    select
        pack.canonical_site_id,
        'title_control',
        'urgent_address_title_pack',
        pack.canonical_site_id::text,
        coalesce(pack.title_number, pack.ros_inspire_id, pack.primary_address_text, pack.site_name, pack.canonical_site_id::text),
        case
            when pack.address_link_status = 'address_linked'
             and pack.title_candidate_status = 'possible_title_reference_identified' then 'high'
            when pack.address_link_status = 'address_linked'
              or pack.title_candidate_status <> 'title_required' then 'medium'
            else 'low'
        end,
        jsonb_build_object(
            'source_key', 'urgent_address_title_pack',
            'urgency_status', pack.urgency_status,
            'address_link_status', pack.address_link_status,
            'title_candidate_status', pack.title_candidate_status,
            'ownership_limitation', pack.ownership_limitation
        )
    from landintel.site_urgent_address_title_pack as pack
    join tmp_urgent_pack_changed as changed
      on changed.canonical_site_id = pack.canonical_site_id;

    delete from landintel.site_signals as signal
    using tmp_urgent_pack_changed as changed
    where signal.canonical_site_id = changed.canonical_site_id
      and signal.source_family = 'title_control'
      and signal.metadata ->> 'source_key' = 'urgent_address_title_pack';

    insert into landintel.site_signals (
        canonical_site_id,
        signal_family,
        signal_name,
        signal_value_text,
        signal_value_numeric,
        confidence,
        source_family,
        source_record_id,
        fact_label,
        evidence_metadata,
        metadata,
        current_flag
    )
    select
        pack.canonical_site_id,
        'title_control',
        'urgent_address_title_pack_status',
        concat_ws(' | ', pack.urgency_status, pack.address_link_status, pack.title_candidate_status),
        pack.title_confidence,
        0.8,
        'title_control',
        pack.canonical_site_id::text,
        'urgent_address_title_pack',
        jsonb_build_object(
            'title_number', pack.title_number,
            'uprn', pack.primary_uprn,
            'address', pack.primary_address_text,
            'ownership_limitation', pack.ownership_limitation
        ),
        jsonb_build_object('source_key', 'urgent_address_title_pack'),
        true
    from landintel.site_urgent_address_title_pack as pack
    join tmp_urgent_pack_changed as changed
      on changed.canonical_site_id = pack.canonical_site_id;

    insert into landintel.site_change_events (
        canonical_site_id,
        source_family,
        source_record_id,
        change_type,
        change_summary,
        previous_signature,
        current_signature,
        triggered_refresh,
        metadata
    )
    select
        changed.canonical_site_id,
        'title_control',
        'urgent_address_title_pack',
        'urgent_address_title_pack_changed',
        'Urgent site address/title evidence pack changed.',
        changed.previous_signature,
        changed.current_signature,
        true,
        jsonb_build_object('source_key', 'urgent_address_title_pack')
    from tmp_urgent_pack_changed as changed;

    return query
    select
        (select count(*)::integer from tmp_urgent_sites),
        (select count(*)::integer from landintel.site_urgent_address_candidates as candidate join tmp_urgent_sites as selected on selected.canonical_site_id = candidate.canonical_site_id),
        (select count(*)::integer from tmp_urgent_pack_prepared),
        (select count(*)::integer from tmp_urgent_pack_prepared where address_link_status = 'address_linked'),
        (select count(*)::integer from tmp_urgent_pack_prepared where title_candidate_status = 'possible_title_reference_identified'),
        (select count(*)::integer from landintel.evidence_references as evidence join tmp_urgent_pack_changed as changed on changed.canonical_site_id = evidence.canonical_site_id where evidence.metadata ->> 'source_key' = 'urgent_address_title_pack'),
        (select count(*)::integer from landintel.site_signals as signal join tmp_urgent_pack_changed as changed on changed.canonical_site_id = signal.canonical_site_id where signal.metadata ->> 'source_key' = 'urgent_address_title_pack'),
        (select count(*)::integer from tmp_urgent_pack_changed);
end;
$$;

create or replace view analytics.v_urgent_site_address_title_pack
with (security_invoker = true) as
select
    pack.canonical_site_id,
    pack.site_location_id,
    pack.site_name,
    pack.authority_name,
    pack.area_acres,
    pack.urgency_status,
    pack.urgency_source,
    pack.urgency_reason,
    pack.title_spend_recommendation,
    pack.title_number,
    pack.normalized_title_number,
    pack.title_candidate_source,
    pack.title_candidate_status,
    pack.title_confidence,
    pack.ros_parcel_id,
    pack.ros_inspire_id,
    pack.primary_address_text,
    pack.primary_uprn,
    pack.address_candidate_count,
    pack.address_link_status,
    pack.address_source,
    pack.ownership_status_pre_title,
    pack.ownership_limitation,
    pack.next_action,
    pack.updated_at
from landintel.site_urgent_address_title_pack as pack
order by
    case pack.urgency_status
        when 'order_title_urgently' then 1
        when 'true_ldn_candidate' then 2
        when 'title_order_urgent' then 3
        else 4
    end,
    pack.updated_at desc;

create or replace view analytics.v_urgent_site_address_candidates
with (security_invoker = true) as
select
    candidate.canonical_site_id,
    pack.site_name,
    pack.authority_name,
    candidate.address_source,
    candidate.uprn,
    candidate.address_text,
    candidate.match_method,
    candidate.match_rank,
    candidate.distance_m,
    candidate.classification_code,
    candidate.classification_description,
    candidate.property_status,
    candidate.updated_at
from landintel.site_urgent_address_candidates as candidate
left join landintel.site_urgent_address_title_pack as pack
  on pack.canonical_site_id = candidate.canonical_site_id
order by candidate.canonical_site_id, candidate.match_rank nulls last, candidate.distance_m nulls last;

create or replace view analytics.v_urgent_address_title_coverage
with (security_invoker = true) as
select
    count(*)::bigint as urgent_site_pack_count,
    count(*) filter (where urgency_status = 'order_title_urgently')::bigint as order_title_urgently_count,
    count(*) filter (where urgency_status = 'true_ldn_candidate')::bigint as true_ldn_candidate_count,
    count(*) filter (where address_link_status = 'address_linked')::bigint as urgent_sites_with_address_count,
    count(*) filter (where address_link_status = 'address_missing')::bigint as urgent_sites_missing_address_count,
    count(*) filter (where title_candidate_status = 'possible_title_reference_identified')::bigint as urgent_sites_with_title_number_count,
    count(*) filter (where title_candidate_status <> 'possible_title_reference_identified')::bigint as urgent_sites_missing_title_number_count,
    count(*) filter (
        where address_link_status = 'address_linked'
          and title_candidate_status = 'possible_title_reference_identified'
    )::bigint as urgent_sites_dd_pack_ready_count,
    max(updated_at) as latest_updated_at
from landintel.site_urgent_address_title_pack;

grant select on analytics.v_urgent_site_address_title_pack to authenticated;
grant select on analytics.v_urgent_site_address_candidates to authenticated;
grant select on analytics.v_urgent_address_title_coverage to authenticated;

comment on function landintel.refresh_urgent_site_address_title_pack(integer, text)
    is 'Refreshes address candidates and RoS/ScotLIS title-number candidates for urgent LDN sites. It does not confirm ownership before human title review.';
