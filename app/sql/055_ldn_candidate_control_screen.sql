alter table landintel.title_order_workflow
    add column if not exists control_fit_status text,
    add column if not exists control_blocker_type text,
    add column if not exists control_blocker_name text,
    add column if not exists ownership_inference_basis text,
    add column if not exists owner_name_signal text,
    add column if not exists ldn_target_private_no_builder boolean not null default false,
    add column if not exists register_status_summary text;

create table if not exists landintel.site_register_status_facts (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'ldn_candidate_screen',
    source_family text not null default 'site_conviction',
    register_family text not null,
    source_role text,
    evidence_role text,
    commercial_weight text,
    corroboration_required boolean not null default true,
    limitation_text text,
    source_record_id text not null,
    authority_name text,
    site_reference text,
    site_name text,
    owner_name_signal text,
    developer_name_signal text,
    source_status_text text,
    development_progress_status text not null default 'unknown',
    build_started_indicator boolean not null default false,
    stalled_indicator boolean not null default false,
    remaining_capacity integer,
    completions integer,
    brownfield_indicator boolean,
    constraint_reasons text[] not null default '{}'::text[],
    measured_constraint_count integer not null default 0,
    max_constraint_overlap_pct numeric,
    constraint_character text,
    constraint_summary text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, register_family, source_record_id)
);

create table if not exists landintel.site_ldn_candidate_screen (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'ldn_candidate_screen',
    source_family text not null default 'site_conviction',
    site_location_id text not null,
    site_name text,
    authority_name text,
    area_acres numeric,
    size_band text,
    register_profile text,
    hla_record_count integer not null default 0,
    ela_record_count integer not null default 0,
    vdl_record_count integer not null default 0,
    ldp_record_count integer not null default 0,
    inside_settlement_signal boolean not null default false,
    unregistered_opportunity_signal boolean not null default false,
    register_origin_site boolean not null default false,
    register_origin_needs_corroboration boolean not null default false,
    independent_corroboration_count integer not null default 0,
    register_corroboration_status text not null default 'not_register_origin',
    ownership_classification text not null default 'ownership_not_confirmed',
    owner_name_signal text,
    control_blocker_type text,
    control_blocker_name text,
    no_housebuilder_developer_signal boolean not null default false,
    ldn_target_private_no_builder boolean not null default false,
    development_progress_status text not null default 'unknown',
    build_started_indicator boolean not null default false,
    stalled_indicator boolean not null default false,
    constraint_position text not null default 'unknown',
    measured_constraint_count integer not null default 0,
    max_constraint_overlap_pct numeric,
    constraint_character text,
    planning_position text not null default 'unknown',
    market_position text not null default 'unknown',
    candidate_status text not null default 'not_enough_evidence',
    title_spend_position text not null default 'manual_review_before_title',
    why_it_matters text,
    top_positives text[] not null default '{}'::text[],
    top_warnings text[] not null default '{}'::text[],
    missing_critical_evidence text[] not null default '{}'::text[],
    next_action text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

alter table landintel.site_register_status_facts
    add column if not exists source_role text,
    add column if not exists evidence_role text,
    add column if not exists commercial_weight text,
    add column if not exists corroboration_required boolean not null default true,
    add column if not exists limitation_text text;

alter table landintel.site_ldn_candidate_screen
    add column if not exists register_origin_site boolean not null default false,
    add column if not exists register_origin_needs_corroboration boolean not null default false,
    add column if not exists independent_corroboration_count integer not null default 0,
    add column if not exists register_corroboration_status text not null default 'not_register_origin';

alter table landintel.site_register_status_facts
    drop constraint if exists site_register_status_facts_family_check;

alter table landintel.site_register_status_facts
    add constraint site_register_status_facts_family_check
    check (register_family = any (array['hla', 'ela', 'vdl']::text[]));

alter table landintel.site_register_status_facts
    drop constraint if exists site_register_status_facts_progress_check;

alter table landintel.site_register_status_facts
    add constraint site_register_status_facts_progress_check
    check (development_progress_status = any (array['started', 'stalled', 'not_started', 'uneconomic', 'incomplete', 'unknown']::text[]));

alter table landintel.site_ldn_candidate_screen
    drop constraint if exists site_ldn_candidate_screen_corroboration_status_check;

alter table landintel.site_ldn_candidate_screen
    add constraint site_ldn_candidate_screen_corroboration_status_check
    check (
        register_corroboration_status = any (array[
            'not_register_origin',
            'register_context_only',
            'register_needs_corroboration',
            'register_corroborated'
        ]::text[])
    );

alter table landintel.site_ldn_candidate_screen
    drop constraint if exists site_ldn_candidate_screen_status_check;

alter table landintel.site_ldn_candidate_screen
    add constraint site_ldn_candidate_screen_status_check
    check (
        candidate_status = any (array[
            'true_ldn_candidate',
            'review_private_candidate',
            'review_forgotten_soul',
            'control_profile_not_ldn',
            'build_work_started',
            'size_below_initial_screen',
            'constraint_review_required',
            'not_enough_evidence'
        ]::text[])
    );

alter table landintel.site_ldn_candidate_screen
    drop constraint if exists site_ldn_candidate_screen_title_spend_check;

alter table landintel.site_ldn_candidate_screen
    add constraint site_ldn_candidate_screen_title_spend_check
    check (
        title_spend_position = any (array[
            'do_not_order_title',
            'manual_review_before_title',
            'title_may_be_justified_after_dd'
        ]::text[])
    );

create index if not exists site_register_status_facts_site_idx
    on landintel.site_register_status_facts (canonical_site_id, register_family, development_progress_status);

create index if not exists site_register_status_facts_owner_idx
    on landintel.site_register_status_facts (owner_name_signal, developer_name_signal);

create index if not exists site_ldn_candidate_screen_status_idx
    on landintel.site_ldn_candidate_screen (candidate_status, ldn_target_private_no_builder, updated_at desc);

create index if not exists site_ldn_candidate_screen_authority_idx
    on landintel.site_ldn_candidate_screen (authority_name, candidate_status, area_acres);

create or replace function landintel.refresh_ldn_candidate_screen(
    p_batch_size integer default 250,
    p_authority_name text default null
)
returns table (
    selected_site_count integer,
    register_fact_count integer,
    candidate_screen_count integer,
    true_ldn_candidate_count integer,
    review_candidate_count integer,
    control_profile_not_ldn_count integer,
    evidence_row_count integer,
    signal_row_count integer,
    change_event_count integer
)
language plpgsql
set search_path = pg_catalog, public, landintel, extensions
as $$
declare
    v_batch_size integer := greatest(coalesce(p_batch_size, 250), 1);
    v_authority_name text := nullif(btrim(coalesce(p_authority_name, '')), '');
begin
    create temporary table if not exists tmp_ldn_selected_sites (
        canonical_site_id uuid primary key,
        site_location_id text,
        site_name text,
        authority_name text,
        area_acres numeric,
        geometry geometry(MultiPolygon, 27700)
    ) on commit drop;

    truncate tmp_ldn_selected_sites;

    insert into tmp_ldn_selected_sites (
        canonical_site_id,
        site_location_id,
        site_name,
        authority_name,
        area_acres,
        geometry
    )
    select
        site.id,
        site.id::text,
        site.site_name_primary,
        site.authority_name,
        site.area_acres,
        site.geometry
    from landintel.canonical_sites as site
    left join landintel.site_ldn_candidate_screen as existing
      on existing.canonical_site_id = site.id
    where site.geometry is not null
      and (v_authority_name is null or site.authority_name ilike '%%' || v_authority_name || '%%')
    order by existing.updated_at nulls first, site.updated_at desc nulls last, site.id
    limit v_batch_size;

    create temporary table if not exists tmp_ldn_register_rows (
        canonical_site_id uuid,
        register_family text,
        source_record_id text,
        authority_name text,
        site_reference text,
        site_name text,
        owner_name_signal text,
        developer_name_signal text,
        source_status_text text,
        development_progress_status text,
        build_started_indicator boolean,
        stalled_indicator boolean,
        remaining_capacity integer,
        completions integer,
        brownfield_indicator boolean,
        constraint_reasons text[],
        raw_payload jsonb
    ) on commit drop;

    truncate tmp_ldn_register_rows;

    insert into tmp_ldn_register_rows
    select
        hla.canonical_site_id,
        'hla',
        hla.source_record_id,
        hla.authority_name,
        hla.site_reference,
        hla.site_name,
        nullif(btrim(coalesce(
            hla.raw_payload ->> 'owner',
            hla.raw_payload ->> 'Owner',
            hla.raw_payload ->> 'ownership',
            hla.raw_payload ->> 'Ownership',
            hla.raw_payload ->> 'landowner',
            hla.raw_payload ->> 'Landowner'
        )), ''),
        nullif(btrim(hla.developer_name), ''),
        concat_ws(' ', hla.effectiveness_status, hla.programming_horizon),
        case
            when concat_ws(' ', hla.effectiveness_status, hla.programming_horizon, hla.raw_payload::text) ilike any (array['%%uneconomic%%']) then 'uneconomic'
            when coalesce(hla.completions, 0) > 0 and coalesce(hla.remaining_capacity, 0) > 0 then 'incomplete'
            when concat_ws(' ', hla.effectiveness_status, hla.programming_horizon, hla.raw_payload::text) ilike any (array['%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']) then 'incomplete'
            when concat_ws(' ', hla.effectiveness_status, hla.programming_horizon, hla.raw_payload::text) ilike any (array['%%stall%%', '%%delay%%', '%%inactive%%', '%%constrained%%', '%%ineffective%%']) then 'stalled'
            when coalesce(hla.completions, 0) > 0
              or concat_ws(' ', hla.effectiveness_status, hla.programming_horizon, hla.raw_payload::text) ilike any (array['%%under construction%%', '%%construction started%%', '%%built%%', '%%complete%%']) then 'started'
            when coalesce(hla.remaining_capacity, 0) > 0 then 'not_started'
            else 'unknown'
        end,
        (
            coalesce(hla.completions, 0) > 0
            and coalesce(hla.remaining_capacity, 0) = 0
        )
        or (
            concat_ws(' ', hla.effectiveness_status, hla.programming_horizon, hla.raw_payload::text) ilike any (array['%%under construction%%', '%%construction started%%', '%%built%%', '%%complete%%'])
            and concat_ws(' ', hla.effectiveness_status, hla.programming_horizon, hla.raw_payload::text) not ilike any (array['%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%'])
        ),
        concat_ws(' ', hla.effectiveness_status, hla.programming_horizon, hla.raw_payload::text) ilike any (array['%%stall%%', '%%delay%%', '%%inactive%%', '%%constrained%%', '%%ineffective%%', '%%uneconomic%%', '%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']),
        hla.remaining_capacity,
        hla.completions,
        hla.brownfield_indicator,
        hla.constraint_reasons,
        hla.raw_payload
    from landintel.hla_site_records as hla
    join tmp_ldn_selected_sites as selected on selected.canonical_site_id = hla.canonical_site_id
    where hla.canonical_site_id is not null

    union all

    select
        ela.canonical_site_id,
        'ela',
        ela.source_record_id,
        ela.authority_name,
        ela.site_reference,
        ela.site_name,
        nullif(btrim(coalesce(
            ela.raw_payload ->> 'owner',
            ela.raw_payload ->> 'Owner',
            ela.raw_payload ->> 'ownership',
            ela.raw_payload ->> 'Ownership',
            ela.raw_payload ->> 'landowner',
            ela.raw_payload ->> 'Landowner',
            ela.raw_payload ->> 'organisation',
            ela.raw_payload ->> 'Organisation'
        )), ''),
        nullif(btrim(coalesce(
            ela.raw_payload ->> 'developer',
            ela.raw_payload ->> 'Developer',
            ela.raw_payload ->> 'occupier',
            ela.raw_payload ->> 'Occupier'
        )), ''),
        ela.status_text,
        case
            when concat_ws(' ', ela.status_text, ela.raw_payload::text) ilike any (array['%%uneconomic%%']) then 'uneconomic'
            when concat_ws(' ', ela.status_text, ela.raw_payload::text) ilike any (array['%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']) then 'incomplete'
            when concat_ws(' ', ela.status_text, ela.raw_payload::text) ilike any (array['%%stall%%', '%%delay%%', '%%inactive%%', '%%constrained%%', '%%vacant%%', '%%derelict%%', '%%dormant%%']) then 'stalled'
            when concat_ws(' ', ela.status_text, ela.raw_payload::text) ilike any (array['%%not started%%', '%%no development%%', '%%available%%']) then 'not_started'
            when concat_ws(' ', ela.status_text, ela.raw_payload::text) ilike any (array['%%under construction%%', '%%construction started%%', '%%built%%', '%%complete%%']) then 'started'
            else 'unknown'
        end,
        concat_ws(' ', ela.status_text, ela.raw_payload::text) ilike any (array['%%under construction%%', '%%construction started%%', '%%built%%', '%%complete%%'])
            and concat_ws(' ', ela.status_text, ela.raw_payload::text) not ilike any (array['%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']),
        concat_ws(' ', ela.status_text, ela.raw_payload::text) ilike any (array['%%stall%%', '%%delay%%', '%%inactive%%', '%%constrained%%', '%%vacant%%', '%%derelict%%', '%%dormant%%', '%%uneconomic%%', '%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']),
        null::integer,
        null::integer,
        null::boolean,
        '{}'::text[],
        ela.raw_payload
    from landintel.ela_site_records as ela
    join tmp_ldn_selected_sites as selected on selected.canonical_site_id = ela.canonical_site_id
    where ela.canonical_site_id is not null

    union all

    select
        vdl.canonical_site_id,
        'vdl',
        vdl.source_record_id,
        vdl.authority_name,
        vdl.site_reference,
        vdl.site_name,
        nullif(btrim(coalesce(
            vdl.raw_payload ->> 'owner',
            vdl.raw_payload ->> 'Owner',
            vdl.raw_payload ->> 'ownership',
            vdl.raw_payload ->> 'Ownership',
            vdl.raw_payload ->> 'landowner',
            vdl.raw_payload ->> 'Landowner',
            vdl.raw_payload ->> 'organisation',
            vdl.raw_payload ->> 'Organisation'
        )), ''),
        nullif(btrim(coalesce(
            vdl.raw_payload ->> 'developer',
            vdl.raw_payload ->> 'Developer',
            vdl.raw_payload ->> 'occupier',
            vdl.raw_payload ->> 'Occupier'
        )), ''),
        vdl.status_text,
        case
            when concat_ws(' ', vdl.status_text, vdl.raw_payload::text) ilike any (array['%%uneconomic%%']) then 'uneconomic'
            when concat_ws(' ', vdl.status_text, vdl.raw_payload::text) ilike any (array['%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']) then 'incomplete'
            when concat_ws(' ', vdl.status_text, vdl.raw_payload::text) ilike any (array['%%stall%%', '%%delay%%', '%%inactive%%', '%%constrained%%', '%%vacant%%', '%%derelict%%', '%%dormant%%']) then 'stalled'
            when concat_ws(' ', vdl.status_text, vdl.raw_payload::text) ilike any (array['%%not started%%', '%%no development%%', '%%available%%']) then 'not_started'
            when concat_ws(' ', vdl.status_text, vdl.raw_payload::text) ilike any (array['%%under construction%%', '%%construction started%%', '%%built%%', '%%complete%%']) then 'started'
            else 'unknown'
        end,
        concat_ws(' ', vdl.status_text, vdl.raw_payload::text) ilike any (array['%%under construction%%', '%%construction started%%', '%%built%%', '%%complete%%'])
            and concat_ws(' ', vdl.status_text, vdl.raw_payload::text) not ilike any (array['%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']),
        concat_ws(' ', vdl.status_text, vdl.raw_payload::text) ilike any (array['%%stall%%', '%%delay%%', '%%inactive%%', '%%constrained%%', '%%vacant%%', '%%derelict%%', '%%dormant%%', '%%uneconomic%%', '%%incomplete%%', '%%part complete%%', '%%part-built%%', '%%part built%%']),
        null::integer,
        null::integer,
        true,
        '{}'::text[],
        vdl.raw_payload
    from landintel.vdl_site_records as vdl
    join tmp_ldn_selected_sites as selected on selected.canonical_site_id = vdl.canonical_site_id
    where vdl.canonical_site_id is not null;

    delete from landintel.site_register_status_facts as fact
    using tmp_ldn_selected_sites as selected
    where fact.canonical_site_id = selected.canonical_site_id
      and fact.register_family = any (array['hla', 'ela', 'vdl']::text[]);

    insert into landintel.site_register_status_facts (
        canonical_site_id,
        register_family,
        source_role,
        evidence_role,
        commercial_weight,
        corroboration_required,
        limitation_text,
        source_record_id,
        authority_name,
        site_reference,
        site_name,
        owner_name_signal,
        developer_name_signal,
        source_status_text,
        development_progress_status,
        build_started_indicator,
        stalled_indicator,
        remaining_capacity,
        completions,
        brownfield_indicator,
        constraint_reasons,
        measured_constraint_count,
        max_constraint_overlap_pct,
        constraint_character,
        constraint_summary,
        source_record_signature,
        metadata,
        updated_at
    )
    select
        register_row.canonical_site_id,
        register_row.register_family,
        case register_row.register_family
            when 'ela' then 'emerging_land_context'
            when 'hla' then 'housing_land_supply_context'
            when 'vdl' then 'vacant_derelict_land_context'
            else 'register_origin_context'
        end,
        case register_row.register_family
            when 'ela' then 'policy_or_candidate_visibility'
            when 'hla' then 'planning_supply_visibility'
            when 'vdl' then 'regeneration_or_underuse_visibility'
            else 'context_visibility'
        end,
        'low_to_medium',
        true,
        'Register/context evidence can identify a site and explain visibility, but it does not prove availability, deliverability, clean ownership, buyer depth or commercial viability. Independent corroboration is required before treating it as a strong sourcing opportunity.',
        register_row.source_record_id,
        register_row.authority_name,
        register_row.site_reference,
        register_row.site_name,
        register_row.owner_name_signal,
        register_row.developer_name_signal,
        register_row.source_status_text,
        register_row.development_progress_status,
        register_row.build_started_indicator,
        register_row.stalled_indicator,
        register_row.remaining_capacity,
        register_row.completions,
        register_row.brownfield_indicator,
        register_row.constraint_reasons,
        coalesce(constraints.measured_constraint_count, 0),
        constraints.max_constraint_overlap_pct,
        constraints.constraint_character,
        constraints.constraint_summary,
        md5(concat_ws(
            '|',
            register_row.canonical_site_id::text,
            register_row.register_family,
            register_row.source_record_id,
            coalesce(register_row.owner_name_signal, ''),
            coalesce(register_row.developer_name_signal, ''),
            coalesce(register_row.source_status_text, ''),
            register_row.development_progress_status,
            register_row.build_started_indicator::text,
            register_row.stalled_indicator::text,
            coalesce(register_row.remaining_capacity::text, ''),
            coalesce(register_row.completions::text, ''),
            coalesce(constraints.max_constraint_overlap_pct::text, ''),
            coalesce(constraints.constraint_character, '')
        )),
        jsonb_build_object(
            'source_key', 'ldn_candidate_screen',
            'register_family', register_row.register_family,
            'source_role', case register_row.register_family
                when 'ela' then 'emerging_land_context'
                when 'hla' then 'housing_land_supply_context'
                when 'vdl' then 'vacant_derelict_land_context'
                else 'register_origin_context'
            end,
            'evidence_role', case register_row.register_family
                when 'ela' then 'policy_or_candidate_visibility'
                when 'hla' then 'planning_supply_visibility'
                when 'vdl' then 'regeneration_or_underuse_visibility'
                else 'context_visibility'
            end,
            'commercial_weight', 'low_to_medium',
            'corroboration_required', true,
            'raw_payload', register_row.raw_payload,
            'ownership_limitation', 'register_owner_or_developer_signal_not_legal_title'
        ),
        now()
    from tmp_ldn_register_rows as register_row
    left join lateral (
        select
            count(*)::integer as measured_constraint_count,
            max(summary.max_overlap_pct_of_site) as max_constraint_overlap_pct,
            (array_agg(summary.constraint_character order by summary.max_overlap_pct_of_site desc nulls last))[1] as constraint_character,
            string_agg(distinct concat_ws(': ', summary.constraint_group, summary.constraint_character), '; ') as constraint_summary
        from public.site_constraint_group_summaries as summary
        where summary.site_id = register_row.canonical_site_id::text
    ) as constraints on true;

    create temporary table if not exists tmp_ldn_candidate_prepared (
        canonical_site_id uuid primary key,
        source_record_signature text,
        candidate_status text,
        ldn_target_private_no_builder boolean,
        payload jsonb
    ) on commit drop;

    truncate tmp_ldn_candidate_prepared;

    insert into tmp_ldn_candidate_prepared (
        canonical_site_id,
        source_record_signature,
        candidate_status,
        ldn_target_private_no_builder,
        payload
    )
    with base as (
        select
            selected.canonical_site_id,
            selected.site_location_id,
            selected.site_name,
            selected.authority_name,
            selected.area_acres,
            case
                when selected.area_acres is null then 'unknown'
                when selected.area_acres < 4 then 'under_4_acres'
                when selected.area_acres < 10 then '4_to_10_acres'
                when selected.area_acres <= 30 then '10_to_30_acres'
                else '30_plus_acres'
            end as size_band,
            coalesce(registers.hla_record_count, 0) as hla_record_count,
            coalesce(registers.ela_record_count, 0) as ela_record_count,
            coalesce(registers.vdl_record_count, 0) as vdl_record_count,
            coalesce(ldp.ldp_record_count, 0) as ldp_record_count,
            coalesce(settlement.inside_settlement_signal, false) as inside_settlement_signal,
            (
                coalesce(registers.hla_record_count, 0)
                + coalesce(registers.ela_record_count, 0)
                + coalesce(registers.vdl_record_count, 0)
            ) > 0 as register_origin_site,
            (
                case when coalesce(ldp.ldp_record_count, 0) > 0 then 1 else 0 end
                + case when coalesce(settlement.inside_settlement_signal, false) then 1 else 0 end
                + case when coalesce(planning.approved_count, 0) + coalesce(planning.live_count, 0) + coalesce(planning.refused_count, 0) > 0 then 1 else 0 end
                + case when coalesce(constraints.measured_constraint_count, 0) > 0 then 1 else 0 end
                + case when coalesce(market.market_confidence_tier, 'unknown') ilike any(array['%%high%%', '%%strong%%']) then 1 else 0 end
                + case when title.title_number is not null or title.normalized_title_number is not null then 1 else 0 end
            )::integer as independent_corroboration_count,
            registers.owner_name_signal,
            registers.developer_name_signal,
            coalesce(registers.build_started_indicator, false) as build_started_indicator,
            coalesce(registers.stalled_indicator, false) as stalled_indicator,
            coalesce(registers.development_progress_status, 'unknown') as development_progress_status,
            coalesce(constraints.measured_constraint_count, 0) as measured_constraint_count,
            coalesce(constraints.max_constraint_overlap_pct, 0) as max_constraint_overlap_pct,
            constraints.constraint_character,
            coalesce(planning.latest_decision_status, 'unknown') as latest_decision_status,
            coalesce(planning.approved_count, 0) as approved_count,
            coalesce(planning.refused_count, 0) as refused_count,
            coalesce(planning.withdrawn_count, 0) as withdrawn_count,
            coalesce(planning.live_count, 0) as live_count,
            coalesce(market.market_confidence_tier, 'unknown') as market_confidence_tier,
            title.title_number,
            title.normalized_title_number,
            title.title_required_flag,
            title.title_order_status,
            title.title_review_status,
            title.control_signal_summary,
            control.control_signal_count,
            control.known_control_count,
            control.builder_control_signal,
            coalesce(registers.public_owner_signal, false) as public_owner_signal,
            coalesce(registers.rsl_lha_charity_signal, false) as rsl_lha_charity_signal,
            coalesce(registers.housebuilder_developer_signal, false) or coalesce(control.builder_control_signal, false) as housebuilder_developer_signal
        from tmp_ldn_selected_sites as selected
        left join lateral (
            select
                count(*) filter (where fact.register_family = 'hla')::integer as hla_record_count,
                count(*) filter (where fact.register_family = 'ela')::integer as ela_record_count,
                count(*) filter (where fact.register_family = 'vdl')::integer as vdl_record_count,
                bool_or(fact.build_started_indicator) as build_started_indicator,
                bool_or(fact.stalled_indicator) as stalled_indicator,
                (array_agg(fact.development_progress_status order by
                    case fact.development_progress_status
                        when 'started' then 1
                        when 'stalled' then 2
                        when 'not_started' then 3
                        else 4
                    end
                ))[1] as development_progress_status,
                nullif((array_agg(coalesce(fact.owner_name_signal, fact.developer_name_signal) order by fact.updated_at desc nulls last))[1], '') as owner_name_signal,
                nullif((array_agg(fact.developer_name_signal order by fact.updated_at desc nulls last))[1], '') as developer_name_signal,
                bool_or(concat_ws(' ', fact.owner_name_signal, fact.developer_name_signal, fact.source_status_text, fact.metadata::text) ilike any (array[
                    '%%council%%',
                    '%%local authority%%',
                    '%%scottish government%%',
                    '%%government%%',
                    '%%ministers%%',
                    '%%nhs%%',
                    '%%health board%%',
                    '%%transport scotland%%',
                    '%%network rail%%',
                    '%%university%%'
                ])) as public_owner_signal,
                bool_or(concat_ws(' ', fact.owner_name_signal, fact.developer_name_signal, fact.source_status_text, fact.metadata::text) ilike any (array[
                    '%%housing association%%',
                    '%%registered social landlord%%',
                    '%% rsl %%',
                    '%%wheatley%%',
                    '%%link housing%%',
                    '%%caledonia housing%%',
                    '%%hillcrest%%',
                    '%%kingdom housing%%',
                    '%%charity%%',
                    '%%charitable%%',
                    '%%church%%'
                ])) as rsl_lha_charity_signal,
                bool_or(concat_ws(' ', fact.owner_name_signal, fact.developer_name_signal, fact.source_status_text, fact.metadata::text) ilike any (array[
                    '%%miller homes%%',
                    '%%persimmon%%',
                    '%%barratt%%',
                    '%%bellway%%',
                    '%%cala%%',
                    '%%taylor wimpey%%',
                    '%%redrow%%',
                    '%%lovell%%',
                    '%%keepmoat%%',
                    '%%mactaggart%%',
                    '%%robertson homes%%',
                    '%%stewart milne%%',
                    '%%ogilvie homes%%',
                    '%%cruden%%',
                    '%%springfield%%',
                    '%%avant%%',
                    '%%muir homes%%',
                    '%%story homes%%',
                    '%%dandara%%',
                    '%%housebuilder%%',
                    '%%house builder%%',
                    '%%promoter%%',
                    '%%developer%%'
                ])) as housebuilder_developer_signal
            from landintel.site_register_status_facts as fact
            where fact.canonical_site_id = selected.canonical_site_id
        ) as registers on true
        left join lateral (
            select count(*)::integer as ldp_record_count
            from landintel.ldp_site_records as record
            where record.canonical_site_id = selected.canonical_site_id
        ) as ldp on true
        left join lateral (
            select true as inside_settlement_signal
            from landintel.settlement_boundary_records as boundary
            where boundary.boundary_role = 'settlement'
              and boundary.geometry OPERATOR(extensions.&&) selected.geometry
              and st_intersects(boundary.geometry, selected.geometry)
            limit 1
        ) as settlement on true
        left join lateral (
            select
                count(*)::integer as measured_constraint_count,
                max(summary.max_overlap_pct_of_site) as max_constraint_overlap_pct,
                (array_agg(summary.constraint_character order by summary.max_overlap_pct_of_site desc nulls last))[1] as constraint_character
            from public.site_constraint_group_summaries as summary
            where summary.site_id = selected.canonical_site_id::text
        ) as constraints on true
        left join landintel.site_planning_decision_context as planning
          on planning.canonical_site_id = selected.canonical_site_id
        left join landintel.site_market_context as market
          on market.canonical_site_id = selected.canonical_site_id
        left join landintel.title_order_workflow as title
          on title.canonical_site_id = selected.canonical_site_id
        left join lateral (
            select
                count(*)::integer as control_signal_count,
                bool_or(
                    concat_ws(' ', signal.signal_label, signal.signal_value_text, signal.metadata::text) ilike any (array[
                        '%%miller homes%%',
                        '%%persimmon%%',
                        '%%barratt%%',
                        '%%bellway%%',
                        '%%cala%%',
                        '%%taylor wimpey%%',
                        '%%housebuilder%%',
                        '%%promoter%%',
                        '%%developer%%'
                    ])
                ) as builder_control_signal,
                (
                    select count(*)::integer
                    from landintel.known_controlled_sites as known
                    where known.canonical_site_id = selected.canonical_site_id
                ) as known_control_count
            from landintel.ownership_control_signals as signal
            where signal.canonical_site_id = selected.canonical_site_id
        ) as control on true
    ),
    classified as (
        select
            base.*,
            case
                when public_owner_signal then 'public_sector_signal'
                when rsl_lha_charity_signal then 'rsl_lha_charity_signal'
                when housebuilder_developer_signal or coalesce(known_control_count, 0) > 0 then 'housebuilder_developer_signal'
                when owner_name_signal is not null then 'likely_private_company_signal'
                else 'ownership_not_confirmed'
            end as ownership_classification,
            case
                when public_owner_signal then 'public_sector'
                when rsl_lha_charity_signal then 'rsl_lha_charity'
                when housebuilder_developer_signal or coalesce(known_control_count, 0) > 0 then 'housebuilder_developer'
                else null::text
            end as control_blocker_type,
            case
                when public_owner_signal then coalesce(owner_name_signal, developer_name_signal, control_signal_summary, 'public-sector signal in register text')
                when rsl_lha_charity_signal then coalesce(owner_name_signal, developer_name_signal, control_signal_summary, 'RSL/LHA/charity signal in register text')
                when housebuilder_developer_signal then coalesce(owner_name_signal, developer_name_signal, control_signal_summary, 'housebuilder/developer signal in register text')
                when coalesce(known_control_count, 0) > 0 then 'known_controlled_site'
                else null::text
            end as control_blocker_name,
            not (public_owner_signal or rsl_lha_charity_signal or housebuilder_developer_signal or coalesce(known_control_count, 0) > 0) as no_housebuilder_developer_signal,
            (
                owner_name_signal is not null
                and not (public_owner_signal or rsl_lha_charity_signal or housebuilder_developer_signal or coalesce(known_control_count, 0) > 0)
            ) as ldn_target_private_no_builder,
            case
                when coalesce(measured_constraint_count, 0) = 0 then 'unknown'
                when coalesce(max_constraint_overlap_pct, 0) >= 50 and constraint_character = any(array['central', 'core-based']::text[]) then 'major_review'
                when constraint_character = any(array['edge-based', 'linear', 'fragmented']::text[]) then 'priceable_design_led'
                else 'context_only'
            end as constraint_position,
            case
                when coalesce(approved_count, 0) > 0 then 'approved_precedent_present'
                when coalesce(live_count, 0) > 0 then 'live_planning_activity'
                when coalesce(refused_count, 0) > 0 then 'refusal_reasons_need_review'
                when coalesce(ldp_record_count, 0) > 0 then 'policy_allocation_present'
                when inside_settlement_signal then 'inside_settlement_unallocated_or_unproven'
                else 'no_clear_planning_route'
            end as planning_position,
            case
                when coalesce(market_confidence_tier, 'unknown') ilike any(array['%%high%%', '%%strong%%']) then 'credible'
                when coalesce(approved_count, 0) > 0 then 'context_present'
                else 'unproven'
            end as market_position,
            (
                coalesce(hla_record_count, 0) = 0
                and coalesce(ela_record_count, 0) = 0
                and coalesce(vdl_record_count, 0) = 0
                and coalesce(ldp_record_count, 0) = 0
                and inside_settlement_signal
            ) as unregistered_opportunity_signal
        from base
    ),
    actioned as (
        select
            classified.*,
            concat_ws(
                ', ',
                case when hla_record_count > 0 then 'HLA' end,
                case when ela_record_count > 0 then 'ELA' end,
                case when vdl_record_count > 0 then 'VDL' end,
                case when ldp_record_count > 0 then 'LDP' end,
                case when unregistered_opportunity_signal then 'unregistered_inside_settlement' end
            ) as register_profile,
            (
                register_origin_site
                and independent_corroboration_count = 0
            ) as register_origin_needs_corroboration,
            case
                when not register_origin_site then 'not_register_origin'
                when independent_corroboration_count = 0 then 'register_needs_corroboration'
                when not ldn_target_private_no_builder
                  or build_started_indicator
                  or coalesce(area_acres, 0) < 4
                  or development_progress_status not in ('not_started', 'stalled', 'uneconomic', 'incomplete') then 'register_context_only'
                else 'register_corroborated'
            end as register_corroboration_status,
            case
                when control_blocker_type is not null then 'control_profile_not_ldn'
                when build_started_indicator then 'build_work_started'
                when coalesce(area_acres, 0) > 0 and area_acres < 4 then 'size_below_initial_screen'
                when constraint_position = 'major_review' then 'constraint_review_required'
                when ldn_target_private_no_builder
                  and coalesce(area_acres, 0) >= 4
                  and not build_started_indicator
                  and (
                    (
                        register_origin_site
                        and development_progress_status in ('not_started', 'stalled', 'uneconomic', 'incomplete')
                        and independent_corroboration_count > 0
                    )
                    or ldp_record_count > 0
                    or approved_count > 0
                    or unregistered_opportunity_signal
                  ) then 'true_ldn_candidate'
                when unregistered_opportunity_signal
                  and no_housebuilder_developer_signal
                  and coalesce(area_acres, 0) >= 4
                  and constraint_position = any(array['priceable_design_led', 'context_only']::text[]) then 'review_forgotten_soul'
                when no_housebuilder_developer_signal
                  and coalesce(area_acres, 0) >= 4
                  and (owner_name_signal is not null or approved_count > 0 or live_count > 0 or inside_settlement_signal) then 'review_private_candidate'
                else 'not_enough_evidence'
            end as candidate_status,
            case
                when control_blocker_type is not null or build_started_indicator or (coalesce(area_acres, 0) > 0 and area_acres < 4) then 'do_not_order_title'
                when constraint_position = 'major_review' then 'manual_review_before_title'
                when ldn_target_private_no_builder
                  and planning_position <> 'no_clear_planning_route'
                  and (
                    not register_origin_site
                    or (
                        development_progress_status in ('not_started', 'stalled', 'uneconomic', 'incomplete')
                        and independent_corroboration_count > 0
                    )
                  ) then 'title_may_be_justified_after_dd'
                else 'manual_review_before_title'
            end as title_spend_position
        from classified
    ),
    completed as (
        select
            actioned.*,
            case
                when candidate_status = 'true_ldn_candidate' then 'This is an LDN target because it has a private/no-builder control signal, is 4+ acres, is not public/RSL/charity/housebuilder/developer controlled on current evidence, and has a register, planning or settlement thread with independent corroboration where register-origin evidence is present.'
                when candidate_status = 'review_forgotten_soul' then 'This is a forgotten-soul lead: inside settlement, not on the main registers, and not showing a builder/public-sector blocker.'
                when candidate_status = 'review_private_candidate' then 'This is a private/no-obvious-builder review candidate, but the evidence is not yet strong enough for title spend.'
                when candidate_status = 'control_profile_not_ldn' then 'This is not an LDN control target because the current evidence points to public, institutional, charity/RSL or housebuilder/developer control.'
                when candidate_status = 'build_work_started' then 'This is lower priority because available register evidence suggests build work may already have started.'
                when candidate_status = 'constraint_review_required' then 'This needs constraint interpretation before ownership spend.'
                else 'Current evidence does not yet justify LDN spending the next pound or hour.'
            end as why_it_matters,
            array_remove(array[
                case when ldn_target_private_no_builder then 'Private/no-builder signal present' end,
                case when inside_settlement_signal then 'Inside settlement signal present' end,
                case when unregistered_opportunity_signal then 'Unregistered inside-settlement opportunity signal' end,
                case when register_corroboration_status = 'register_corroborated' then 'Register/context source is independently corroborated' end,
                case when approved_count > 0 then 'Planning approval context present' end,
                case when stalled_indicator then 'Stalled register signal present' end,
                case when constraint_position = 'priceable_design_led' then 'Constraints appear layout or pricing led' end
            ], null) as top_positives,
            array_remove(array[
                case when control_blocker_type is not null then concat('Control blocker signal: ', control_blocker_type) end,
                case when build_started_indicator then 'Build work started signal present' end,
                case when register_origin_needs_corroboration then 'Register/context source requires independent corroboration' end,
                case when register_corroboration_status = 'register_context_only' then 'Register/context source is not enough to treat this as a strong sourcing opportunity' end,
                case when constraint_position = 'major_review' then 'Constraint review required before spend' end,
                case when ownership_classification = 'ownership_not_confirmed' then 'Ownership not confirmed until title review' end,
                case when planning_position = 'no_clear_planning_route' then 'No clear planning route evidenced yet' end
            ], null) as top_warnings,
            array_remove(array[
                case when coalesce(title_review_status, 'not_reviewed') <> 'reviewed' then 'Title not reviewed' end,
                case when measured_constraint_count = 0 then 'Constraint measurement not yet available' end,
                case when register_origin_needs_corroboration then 'Independent corroboration required beyond HLA/ELA/VDL register presence' end,
                case when market_position = 'unproven' then 'Buyer/market evidence not yet proven' end,
                case when planning_position = 'no_clear_planning_route' then 'Planning route needs evidence' end
            ], null) as missing_critical_evidence,
            case
                when candidate_status = 'true_ldn_candidate' then 'Run DD screen, then decide whether title spend is justified.'
                when candidate_status = 'review_forgotten_soul' then 'Review planning and constraint context before title spend.'
                when candidate_status = 'review_private_candidate' then 'Manual review before title spend.'
                when register_origin_needs_corroboration then 'Do not promote from register presence alone; find independent planning, settlement, constraint, title or market corroboration first.'
                when candidate_status = 'constraint_review_required' then 'Run constraint review before title spend.'
                when candidate_status = 'control_profile_not_ldn' then 'Do not spend title money unless the control signal is disproven.'
                when candidate_status = 'build_work_started' then 'Do not prioritise unless a stalled delivery angle is proven.'
                else 'Monitor until stronger evidence appears.'
            end as next_action,
            md5(concat_ws(
                '|',
                canonical_site_id::text,
                size_band,
                hla_record_count::text,
                ela_record_count::text,
                vdl_record_count::text,
                ldp_record_count::text,
                inside_settlement_signal::text,
                unregistered_opportunity_signal::text,
                register_origin_site::text,
                register_origin_needs_corroboration::text,
                independent_corroboration_count::text,
                register_corroboration_status,
                ownership_classification,
                coalesce(owner_name_signal, ''),
                coalesce(control_blocker_type, ''),
                coalesce(control_blocker_name, ''),
                no_housebuilder_developer_signal::text,
                ldn_target_private_no_builder::text,
                development_progress_status,
                build_started_indicator::text,
                stalled_indicator::text,
                constraint_position,
                measured_constraint_count::text,
                coalesce(max_constraint_overlap_pct::text, ''),
                coalesce(constraint_character, ''),
                planning_position,
                market_position,
                candidate_status,
                title_spend_position
            )) as current_signature
        from actioned
    )
    select
        canonical_site_id,
        current_signature,
        candidate_status,
        ldn_target_private_no_builder,
        to_jsonb(completed)
    from completed;

    create temporary table if not exists tmp_ldn_candidate_previous (
        canonical_site_id uuid primary key,
        source_record_signature text
    ) on commit drop;

    truncate tmp_ldn_candidate_previous;

    insert into tmp_ldn_candidate_previous (canonical_site_id, source_record_signature)
    select existing.canonical_site_id, existing.source_record_signature
    from landintel.site_ldn_candidate_screen as existing
    join tmp_ldn_candidate_prepared as prepared
      on prepared.canonical_site_id = existing.canonical_site_id;

    with upserted as (
        insert into landintel.site_ldn_candidate_screen (
            canonical_site_id,
            site_location_id,
            site_name,
            authority_name,
            area_acres,
            size_band,
            register_profile,
            hla_record_count,
            ela_record_count,
            vdl_record_count,
            ldp_record_count,
            inside_settlement_signal,
            unregistered_opportunity_signal,
            register_origin_site,
            register_origin_needs_corroboration,
            independent_corroboration_count,
            register_corroboration_status,
            ownership_classification,
            owner_name_signal,
            control_blocker_type,
            control_blocker_name,
            no_housebuilder_developer_signal,
            ldn_target_private_no_builder,
            development_progress_status,
            build_started_indicator,
            stalled_indicator,
            constraint_position,
            measured_constraint_count,
            max_constraint_overlap_pct,
            constraint_character,
            planning_position,
            market_position,
            candidate_status,
            title_spend_position,
            why_it_matters,
            top_positives,
            top_warnings,
            missing_critical_evidence,
            next_action,
            source_record_signature,
            metadata,
            updated_at
        )
        select
            prepared.canonical_site_id,
            prepared.payload ->> 'site_location_id',
            prepared.payload ->> 'site_name',
            prepared.payload ->> 'authority_name',
            nullif(prepared.payload ->> 'area_acres', '')::numeric,
            prepared.payload ->> 'size_band',
            nullif(prepared.payload ->> 'register_profile', ''),
            coalesce((prepared.payload ->> 'hla_record_count')::integer, 0),
            coalesce((prepared.payload ->> 'ela_record_count')::integer, 0),
            coalesce((prepared.payload ->> 'vdl_record_count')::integer, 0),
            coalesce((prepared.payload ->> 'ldp_record_count')::integer, 0),
            coalesce((prepared.payload ->> 'inside_settlement_signal')::boolean, false),
            coalesce((prepared.payload ->> 'unregistered_opportunity_signal')::boolean, false),
            coalesce((prepared.payload ->> 'register_origin_site')::boolean, false),
            coalesce((prepared.payload ->> 'register_origin_needs_corroboration')::boolean, false),
            coalesce((prepared.payload ->> 'independent_corroboration_count')::integer, 0),
            coalesce(prepared.payload ->> 'register_corroboration_status', 'not_register_origin'),
            prepared.payload ->> 'ownership_classification',
            nullif(prepared.payload ->> 'owner_name_signal', ''),
            nullif(prepared.payload ->> 'control_blocker_type', ''),
            nullif(prepared.payload ->> 'control_blocker_name', ''),
            coalesce((prepared.payload ->> 'no_housebuilder_developer_signal')::boolean, false),
            prepared.ldn_target_private_no_builder,
            prepared.payload ->> 'development_progress_status',
            coalesce((prepared.payload ->> 'build_started_indicator')::boolean, false),
            coalesce((prepared.payload ->> 'stalled_indicator')::boolean, false),
            prepared.payload ->> 'constraint_position',
            coalesce((prepared.payload ->> 'measured_constraint_count')::integer, 0),
            nullif(prepared.payload ->> 'max_constraint_overlap_pct', '')::numeric,
            nullif(prepared.payload ->> 'constraint_character', ''),
            prepared.payload ->> 'planning_position',
            prepared.payload ->> 'market_position',
            prepared.candidate_status,
            prepared.payload ->> 'title_spend_position',
            prepared.payload ->> 'why_it_matters',
            coalesce(array(select jsonb_array_elements_text(prepared.payload -> 'top_positives')), '{}'::text[]),
            coalesce(array(select jsonb_array_elements_text(prepared.payload -> 'top_warnings')), '{}'::text[]),
            coalesce(array(select jsonb_array_elements_text(prepared.payload -> 'missing_critical_evidence')), '{}'::text[]),
            prepared.payload ->> 'next_action',
            prepared.source_record_signature,
            jsonb_build_object(
                'source_key', 'ldn_candidate_screen',
                'ownership_limitation', 'ownership_not_confirmed_until_title_review',
                'screen_standard', 'private_no_builder_control_fit'
            ) || prepared.payload,
            now()
        from tmp_ldn_candidate_prepared as prepared
        on conflict (canonical_site_id) do update set
            site_location_id = excluded.site_location_id,
            site_name = excluded.site_name,
            authority_name = excluded.authority_name,
            area_acres = excluded.area_acres,
            size_band = excluded.size_band,
            register_profile = excluded.register_profile,
            hla_record_count = excluded.hla_record_count,
            ela_record_count = excluded.ela_record_count,
            vdl_record_count = excluded.vdl_record_count,
            ldp_record_count = excluded.ldp_record_count,
            inside_settlement_signal = excluded.inside_settlement_signal,
            unregistered_opportunity_signal = excluded.unregistered_opportunity_signal,
            register_origin_site = excluded.register_origin_site,
            register_origin_needs_corroboration = excluded.register_origin_needs_corroboration,
            independent_corroboration_count = excluded.independent_corroboration_count,
            register_corroboration_status = excluded.register_corroboration_status,
            ownership_classification = excluded.ownership_classification,
            owner_name_signal = excluded.owner_name_signal,
            control_blocker_type = excluded.control_blocker_type,
            control_blocker_name = excluded.control_blocker_name,
            no_housebuilder_developer_signal = excluded.no_housebuilder_developer_signal,
            ldn_target_private_no_builder = excluded.ldn_target_private_no_builder,
            development_progress_status = excluded.development_progress_status,
            build_started_indicator = excluded.build_started_indicator,
            stalled_indicator = excluded.stalled_indicator,
            constraint_position = excluded.constraint_position,
            measured_constraint_count = excluded.measured_constraint_count,
            max_constraint_overlap_pct = excluded.max_constraint_overlap_pct,
            constraint_character = excluded.constraint_character,
            planning_position = excluded.planning_position,
            market_position = excluded.market_position,
            candidate_status = excluded.candidate_status,
            title_spend_position = excluded.title_spend_position,
            why_it_matters = excluded.why_it_matters,
            top_positives = excluded.top_positives,
            top_warnings = excluded.top_warnings,
            missing_critical_evidence = excluded.missing_critical_evidence,
            next_action = excluded.next_action,
            source_record_signature = excluded.source_record_signature,
            metadata = excluded.metadata,
            updated_at = now()
        returning *
    ),
    changed as (
        select
            upserted.*,
            previous.source_record_signature as previous_signature
        from upserted
        left join tmp_ldn_candidate_previous as previous
          on previous.canonical_site_id = upserted.canonical_site_id
        where previous.source_record_signature is distinct from upserted.source_record_signature
           or previous.source_record_signature is null
    ),
    title_updates as (
        update landintel.title_order_workflow as workflow
        set
            control_fit_status = upserted.candidate_status,
            control_blocker_type = upserted.control_blocker_type,
            control_blocker_name = upserted.control_blocker_name,
            ownership_inference_basis = upserted.ownership_classification,
            owner_name_signal = upserted.owner_name_signal,
            ldn_target_private_no_builder = upserted.ldn_target_private_no_builder,
            register_status_summary = upserted.register_profile,
            updated_at = now()
        from upserted
        where workflow.canonical_site_id = upserted.canonical_site_id
        returning workflow.id
    ),
    deleted_evidence as (
        delete from landintel.evidence_references as evidence
        using changed
        where evidence.canonical_site_id = changed.canonical_site_id
          and evidence.source_family = 'site_conviction'
          and evidence.metadata ->> 'source_key' = 'ldn_candidate_screen'
        returning evidence.id
    ),
    inserted_evidence as (
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
            changed.canonical_site_id,
            'site_conviction',
            'LDN candidate control screen',
            changed.id::text,
            changed.candidate_status,
            case
                when changed.ldn_target_private_no_builder then 'medium'
                when changed.control_blocker_type is not null then 'medium'
                else 'low'
            end,
            jsonb_build_object(
                'source_key', 'ldn_candidate_screen',
                'candidate_status', changed.candidate_status,
                'ownership_classification', changed.ownership_classification,
                'control_blocker_type', changed.control_blocker_type,
                'ownership_limitation', 'ownership_not_confirmed_until_title_review'
            )
        from changed
        returning id
    ),
    deleted_signals as (
        delete from landintel.site_signals as signal
        using changed
        where signal.canonical_site_id = changed.canonical_site_id
          and signal.source_family = 'site_conviction'
          and signal.metadata ->> 'source_key' = 'ldn_candidate_screen'
        returning signal.id
    ),
    inserted_signals as (
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
            changed.canonical_site_id,
            'ldn_candidate_screen',
            'private_no_builder_control_fit',
            changed.candidate_status,
            case when changed.ldn_target_private_no_builder then 1 else 0 end,
            case
                when changed.candidate_status = 'true_ldn_candidate' then 0.75
                when changed.candidate_status like 'review_%%' then 0.55
                else 0.35
            end,
            'site_conviction',
            changed.id::text,
            'ldn_candidate_control_screen',
            jsonb_build_object(
                'candidate_status', changed.candidate_status,
                'control_blocker_type', changed.control_blocker_type
            ),
            jsonb_build_object('source_key', 'ldn_candidate_screen'),
            true
        from changed
        returning id
    ),
    inserted_events as (
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
            'site_conviction',
            changed.id::text,
            'ldn_candidate_screen_changed',
            'LDN candidate control screen changed.',
            changed.previous_signature,
            changed.source_record_signature,
            false,
            jsonb_build_object(
                'source_key', 'ldn_candidate_screen',
                'candidate_status', changed.candidate_status,
                'next_action', changed.next_action
            )
        from changed
        returning id
    )
    select
        (select count(*)::integer from tmp_ldn_selected_sites),
        (select count(*)::integer from landintel.site_register_status_facts as fact join tmp_ldn_selected_sites as selected on selected.canonical_site_id = fact.canonical_site_id),
        (select count(*)::integer from upserted),
        (select count(*)::integer from upserted where candidate_status = 'true_ldn_candidate'),
        (select count(*)::integer from upserted where candidate_status in ('review_private_candidate', 'review_forgotten_soul')),
        (select count(*)::integer from upserted where candidate_status = 'control_profile_not_ldn'),
        (select count(*)::integer from inserted_evidence),
        (select count(*)::integer from inserted_signals),
        (select count(*)::integer from inserted_events)
    into
        selected_site_count,
        register_fact_count,
        candidate_screen_count,
        true_ldn_candidate_count,
        review_candidate_count,
        control_profile_not_ldn_count,
        evidence_row_count,
        signal_row_count,
        change_event_count;

    return next;
end;
$$;

drop view if exists analytics.v_ldn_candidate_screen_coverage;
drop view if exists analytics.v_register_origin_overconfidence;
drop view if exists analytics.v_register_sourced_sites_needing_corroboration;
drop view if exists analytics.v_site_register_evidence_balance;
drop view if exists analytics.v_true_ldn_sites;
drop view if exists analytics.v_ldn_review_candidates;
drop view if exists analytics.v_ldn_candidate_screen;
drop view if exists analytics.v_register_site_development_status;

create or replace view analytics.v_register_site_development_status
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    facts.register_family,
    facts.source_role,
    facts.evidence_role,
    facts.commercial_weight,
    facts.corroboration_required,
    facts.limitation_text,
    facts.site_reference,
    facts.site_name as register_site_name,
    facts.owner_name_signal,
    facts.developer_name_signal,
    facts.source_status_text,
    facts.development_progress_status,
    facts.build_started_indicator,
    facts.stalled_indicator,
    facts.remaining_capacity,
    facts.completions,
    facts.brownfield_indicator,
    facts.constraint_reasons,
    facts.measured_constraint_count,
    facts.max_constraint_overlap_pct,
    facts.constraint_character,
    facts.constraint_summary,
    facts.updated_at
from landintel.site_register_status_facts as facts
join landintel.canonical_sites as site on site.id = facts.canonical_site_id;

create or replace view analytics.v_ldn_candidate_screen
with (security_invoker = true) as
select
    screen.canonical_site_id,
    screen.site_location_id,
    screen.site_name,
    screen.authority_name,
    screen.area_acres,
    screen.size_band,
    screen.register_profile,
    screen.hla_record_count,
    screen.ela_record_count,
    screen.vdl_record_count,
    screen.ldp_record_count,
    screen.inside_settlement_signal,
    screen.unregistered_opportunity_signal,
    screen.register_origin_site,
    screen.register_origin_needs_corroboration,
    screen.independent_corroboration_count,
    screen.register_corroboration_status,
    screen.ownership_classification,
    screen.owner_name_signal,
    screen.control_blocker_type,
    screen.control_blocker_name,
    screen.no_housebuilder_developer_signal,
    screen.ldn_target_private_no_builder,
    screen.development_progress_status,
    screen.build_started_indicator,
    screen.stalled_indicator,
    screen.constraint_position,
    screen.measured_constraint_count,
    screen.max_constraint_overlap_pct,
    screen.constraint_character,
    screen.planning_position,
    screen.market_position,
    screen.candidate_status,
    screen.title_spend_position,
    screen.why_it_matters,
    screen.top_positives,
    screen.top_warnings,
    screen.missing_critical_evidence,
    screen.next_action,
    screen.updated_at
from landintel.site_ldn_candidate_screen as screen;

create or replace view analytics.v_true_ldn_sites
with (security_invoker = true) as
select *
from analytics.v_ldn_candidate_screen
where candidate_status = 'true_ldn_candidate'
  and ldn_target_private_no_builder = true
  and control_blocker_type is null
  and build_started_indicator = false
  and coalesce(area_acres, 0) >= 4
  and (
    register_origin_site = false
    or register_corroboration_status = 'register_corroborated'
  );

create or replace view analytics.v_ldn_review_candidates
with (security_invoker = true) as
select *
from analytics.v_ldn_candidate_screen
where candidate_status in ('review_private_candidate', 'review_forgotten_soul', 'constraint_review_required');

create or replace view analytics.v_site_register_evidence_balance
with (security_invoker = true) as
select
    screen.canonical_site_id,
    screen.site_name,
    screen.authority_name,
    screen.area_acres,
    screen.register_profile,
    screen.hla_record_count,
    screen.ela_record_count,
    screen.vdl_record_count,
    screen.register_origin_site,
    screen.register_origin_needs_corroboration,
    screen.independent_corroboration_count,
    screen.register_corroboration_status,
    screen.ownership_classification,
    screen.owner_name_signal,
    screen.control_blocker_type,
    screen.development_progress_status,
    screen.build_started_indicator,
    screen.stalled_indicator,
    screen.constraint_position,
    screen.planning_position,
    screen.market_position,
    screen.candidate_status,
    case
        when screen.register_origin_site = false then 'not_register_origin'
        when screen.register_origin_needs_corroboration then 'register_presence_only'
        when screen.register_corroboration_status = 'register_context_only' then 'register_context_not_commercial_proof'
        else 'register_context_with_independent_corroboration'
    end as register_evidence_balance,
    'HLA, ELA and VDL are discovery/context layers. They do not prove availability, deliverability, clean ownership, buyer depth or commercial viability without corroboration.'::text as limitation_text,
    screen.updated_at
from analytics.v_ldn_candidate_screen as screen;

create or replace view analytics.v_register_origin_overconfidence
with (security_invoker = true) as
select *
from analytics.v_site_register_evidence_balance
where register_origin_site
  and candidate_status = 'true_ldn_candidate'
  and (
        register_corroboration_status <> 'register_corroborated'
        or independent_corroboration_count = 0
        or coalesce(area_acres, 0) < 4
        or control_blocker_type is not null
        or build_started_indicator
      );

create or replace view analytics.v_register_sourced_sites_needing_corroboration
with (security_invoker = true) as
select *
from analytics.v_site_register_evidence_balance
where register_origin_site
  and (
        register_origin_needs_corroboration
        or register_corroboration_status in ('register_needs_corroboration', 'register_context_only')
      );

create or replace view analytics.v_ldn_candidate_screen_coverage
with (security_invoker = true) as
select
    count(*)::bigint as screened_site_count,
    count(*) filter (where candidate_status = 'true_ldn_candidate')::bigint as true_ldn_candidate_count,
    count(*) filter (where candidate_status in ('review_private_candidate', 'review_forgotten_soul'))::bigint as review_candidate_count,
    count(*) filter (where candidate_status = 'control_profile_not_ldn')::bigint as control_profile_not_ldn_count,
    count(*) filter (where control_blocker_type = 'public_sector')::bigint as public_sector_blocker_count,
    count(*) filter (where control_blocker_type = 'housebuilder_developer')::bigint as housebuilder_developer_blocker_count,
    count(*) filter (where control_blocker_type = 'rsl_lha_charity')::bigint as rsl_lha_charity_blocker_count,
    count(*) filter (where build_started_indicator)::bigint as build_started_signal_count,
    count(*) filter (where stalled_indicator)::bigint as stalled_signal_count,
    count(*) filter (where register_origin_site)::bigint as register_origin_site_count,
    count(*) filter (where register_origin_needs_corroboration)::bigint as register_origin_needs_corroboration_count,
    count(*) filter (where register_corroboration_status = 'register_corroborated')::bigint as register_origin_corroborated_count,
    count(*) filter (where candidate_status = 'true_ldn_candidate' and register_origin_site and register_corroboration_status <> 'register_corroborated')::bigint as register_origin_overconfidence_count,
    count(*) filter (where unregistered_opportunity_signal)::bigint as unregistered_inside_settlement_count,
    count(*) filter (where measured_constraint_count > 0)::bigint as screened_sites_with_measured_constraints,
    max(updated_at) as latest_updated_at
from landintel.site_ldn_candidate_screen;

create or replace view analytics.v_landintel_source_estate_matrix
with (security_invoker = true) as
with source_rows as (
    select source_key, source_family, count(*)::bigint as row_count from landintel.planning_appeal_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.planning_decision_facts group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_planning_decision_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.title_order_workflow group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.title_review_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.ownership_control_signals group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.corporate_owner_links group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.corporate_entity_enrichments group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.corporate_charge_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.known_controlled_sites group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.power_assets group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.power_capacity_zones group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_power_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.infrastructure_friction_facts group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_ground_risk_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_terrain_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_slope_profiles group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_cut_fill_risk group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_abnormal_cost_flags group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.market_transactions group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.epc_property_attributes group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.market_area_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_market_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.amenity_assets group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_amenity_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.location_strength_facts group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.open_location_spine_features group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_open_location_spine_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.demographic_area_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_demographic_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.housing_demand_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.planning_document_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.section75_obligation_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.intelligence_event_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_assessments group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_prove_it_assessments group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_urgent_address_candidates group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_urgent_address_title_pack group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_register_status_facts group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_ldn_candidate_screen group by source_key, source_family
),
source_row_rollup as (
    select source_key, source_family, sum(row_count)::bigint as row_count
    from source_rows
    group by source_key, source_family
),
linked_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as linked_site_count
    from (
        select appeal.source_key, link.source_family, link.canonical_site_id
        from landintel.site_planning_appeal_links as link
        join landintel.planning_appeal_records as appeal on appeal.id = link.planning_appeal_record_id
        union all select source_key, source_family, canonical_site_id from landintel.planning_decision_facts where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.site_planning_decision_context
        union all select source_key, source_family, canonical_site_id from landintel.title_order_workflow
        union all select source_key, source_family, canonical_site_id from landintel.ownership_control_signals where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.corporate_owner_links where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.corporate_entity_enrichments where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.site_power_context
        union all select source_key, source_family, canonical_site_id from landintel.site_ground_risk_context
        union all select source_key, source_family, canonical_site_id from landintel.site_market_context
        union all select source_key, source_family, canonical_site_id from landintel.site_amenity_context
        union all select source_key, source_family, canonical_site_id from landintel.site_open_location_spine_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
        union all select document.source_key, link.source_family, link.canonical_site_id
        from landintel.site_planning_document_links as link
        join landintel.planning_document_records as document on document.id = link.planning_document_record_id
        union all select event.source_key, link.source_family, link.canonical_site_id
        from landintel.site_intelligence_links as link
        join landintel.intelligence_event_records as event on event.id = link.intelligence_event_record_id
        union all select source_key, source_family, canonical_site_id from landintel.site_assessments
        union all select source_key, source_family, canonical_site_id from landintel.site_prove_it_assessments
        union all select source_key, source_family, canonical_site_id from landintel.site_urgent_address_candidates
        union all select source_key, source_family, canonical_site_id from landintel.site_urgent_address_title_pack
        union all select source_key, source_family, canonical_site_id from landintel.site_register_status_facts
        union all select source_key, source_family, canonical_site_id from landintel.site_ldn_candidate_screen
    ) as links
    where canonical_site_id is not null
    group by source_key, source_family
),
measured_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as measured_site_count
    from (
        select source_key, source_family, canonical_site_id from landintel.site_planning_decision_context
        union all select source_key, source_family, canonical_site_id from landintel.site_power_context
        union all select source_key, source_family, canonical_site_id from landintel.site_ground_risk_context
        union all select source_key, source_family, canonical_site_id from landintel.site_terrain_metrics
        union all select source_key, source_family, canonical_site_id from landintel.site_amenity_context
        union all select source_key, source_family, canonical_site_id from landintel.site_open_location_spine_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
        union all select source_key, source_family, canonical_site_id
        from landintel.site_urgent_address_title_pack
        where address_link_status = 'address_linked'
           or title_candidate_status in ('possible_title_reference_identified', 'parcel_candidate_identified')
        union all select source_key, source_family, canonical_site_id from landintel.site_register_status_facts where measured_constraint_count > 0
        union all select source_key, source_family, canonical_site_id from landintel.site_ldn_candidate_screen where measured_constraint_count > 0
    ) as measurements
    group by source_key, source_family
),
assessment_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as assessment_ready_count
    from (
        select source_key, source_family, canonical_site_id
        from landintel.site_assessments
        where review_next_action is not null
        union all
        select source_key, source_family, canonical_site_id
        from landintel.site_prove_it_assessments
        where review_ready_flag = true
        union all
        select source_key, source_family, canonical_site_id
        from landintel.site_urgent_address_title_pack
        where address_link_status = 'address_linked'
          and title_candidate_status = 'possible_title_reference_identified'
        union all
        select source_key, source_family, canonical_site_id
        from landintel.site_ldn_candidate_screen
        where candidate_status in ('true_ldn_candidate', 'review_private_candidate', 'review_forgotten_soul', 'constraint_review_required')
    ) as assessments
    group by source_key, source_family
),
evidence_rollup as (
    select
        source_family,
        metadata ->> 'source_key' as source_key,
        count(*)::bigint as evidence_count
    from landintel.evidence_references
    where metadata ? 'source_key'
    group by source_family, metadata ->> 'source_key'
),
signal_rollup as (
    select
        source_family,
        metadata ->> 'source_key' as source_key,
        count(*)::bigint as signal_count
    from landintel.site_signals
    where metadata ? 'source_key'
    group by source_family, metadata ->> 'source_key'
),
freshness as (
    select distinct on (source_family, source_key)
        source_family,
        source_key,
        freshness_status,
        live_access_status,
        last_success_at,
        records_observed,
        check_summary
    from (
        select
            source_family,
            replace(replace(source_scope_key, 'phase2:', ''), 'source_expansion:', '') as source_key,
            freshness_status,
            live_access_status,
            last_success_at,
            records_observed,
            check_summary,
            last_checked_at,
            updated_at
        from landintel.source_freshness_states
        where source_scope_key like 'phase2:%%%%'
           or source_scope_key like 'source_expansion:%%%%'
    ) as freshness_rows
    order by source_family, source_key, last_checked_at desc nulls last, updated_at desc
),
event_rollup as (
    select
        source_family,
        source_key,
        max(created_at) filter (where status in ('success', 'source_registered', 'raw_data_landed', 'evidence_generated', 'signals_generated', 'assessment_ready')) as last_successful_run
    from landintel.source_expansion_events
    group by source_family, source_key
),
matrix_base as (
    select
        registry.source_key,
        registry.source_family,
        registry.source_name,
        coalesce(registry.geography, registry.source_group, 'unknown') as authority_geography,
        registry.module_key,
        registry.programme_phase,
        registry.access_status,
        registry.ingest_status,
        registry.normalisation_status,
        registry.site_link_status,
        registry.measurement_status,
        registry.evidence_status,
        registry.signal_status,
        registry.assessment_status,
        registry.trusted_for_review as registry_trusted_for_review,
        coalesce(freshness.freshness_status, 'source_registered') as freshness_status,
        coalesce(freshness.records_observed, 0)::bigint as freshness_record_count,
        event_rollup.last_successful_run,
        coalesce(source_row_rollup.row_count, 0)::bigint as row_count,
        coalesce(linked_rollup.linked_site_count, 0)::bigint as linked_site_count,
        coalesce(measured_rollup.measured_site_count, 0)::bigint as measured_site_count,
        coalesce(assessment_rollup.assessment_ready_count, 0)::bigint as assessment_ready_count,
        coalesce(evidence_rollup.evidence_count, 0)::bigint as evidence_count,
        coalesce(signal_rollup.signal_count, 0)::bigint as signal_count,
        registry.limitation_notes,
        registry.next_action
    from landintel.source_estate_registry as registry
    left join source_row_rollup
      on source_row_rollup.source_key = registry.source_key
     and source_row_rollup.source_family = registry.source_family
    left join linked_rollup on linked_rollup.source_family = registry.source_family and linked_rollup.source_key = registry.source_key
    left join measured_rollup on measured_rollup.source_family = registry.source_family and measured_rollup.source_key = registry.source_key
    left join assessment_rollup on assessment_rollup.source_family = registry.source_family and assessment_rollup.source_key = registry.source_key
    left join evidence_rollup on evidence_rollup.source_family = registry.source_family and evidence_rollup.source_key = registry.source_key
    left join signal_rollup on signal_rollup.source_family = registry.source_family and signal_rollup.source_key = registry.source_key
    left join freshness on freshness.source_family = registry.source_family and freshness.source_key = registry.source_key
    left join event_rollup on event_rollup.source_family = registry.source_family and event_rollup.source_key = registry.source_key
),
matrix_gates as (
    select
        matrix_base.*,
        (
            access_status in ('access_required', 'gated', 'failed', 'stale')
            or freshness_status in ('failed', 'stale', 'access_required', 'gated')
            or limitation_notes ilike any (array[
                '%%%%has not yet%%%%',
                '%%%%not yet%%%%',
                '%%%%requires%%%%',
                '%%%%required%%%%',
                '%%%%must be confirmed%%%%',
                '%%%%before use%%%%',
                '%%%%adapter%%%%'
            ])
        ) as critical_limitation_blocking_review
    from matrix_base
)
select
    matrix_gates.*,
    case
        when registry_trusted_for_review
         and row_count > 0
         and linked_site_count > 0
         and evidence_count > 0
         and signal_count > 0
         and assessment_ready_count > 0
         and freshness_record_count > 0
         and freshness_status not in ('failed', 'stale', 'access_required', 'gated')
         and not critical_limitation_blocking_review
            then true
        else false
    end as trusted_for_review,
    case
        when registry_trusted_for_review
         and row_count > 0
         and linked_site_count > 0
         and evidence_count > 0
         and signal_count > 0
         and assessment_ready_count > 0
         and freshness_record_count > 0
         and freshness_status not in ('failed', 'stale', 'access_required', 'gated')
         and not critical_limitation_blocking_review
            then 'trusted_for_review'
        when assessment_ready_count > 0 then 'assessment_ready'
        when signal_count > 0 then 'signals_generated'
        when evidence_count > 0 then 'evidence_generated'
        when measured_site_count > 0 then 'measured'
        when linked_site_count > 0 then 'linked_to_site'
        when row_count > 0 and normalisation_status = 'normalised' then 'normalised'
        when row_count > 0 then 'raw_data_landed'
        when access_status = 'access_confirmed' then 'access_confirmed'
        else 'source_registered'
    end as current_lifecycle_stage,
    case
        when row_count = 0 then 'no_source_rows'
        when linked_site_count = 0 then 'no_linked_sites'
        when evidence_count = 0 then 'no_evidence_rows'
        when signal_count = 0 then 'no_signal_rows'
        when freshness_record_count = 0 then 'no_freshness_state'
        when critical_limitation_blocking_review then 'critical_limitation_blocks_review'
        when assessment_ready_count = 0 then 'not_assessment_ready'
        else null
    end as trust_block_reason
from matrix_gates;

alter table landintel.site_register_status_facts enable row level security;
alter table landintel.site_ldn_candidate_screen enable row level security;

grant select on landintel.site_register_status_facts to authenticated;
grant select on landintel.site_ldn_candidate_screen to authenticated;

drop policy if exists site_register_status_facts_select_authenticated on landintel.site_register_status_facts;
create policy site_register_status_facts_select_authenticated
    on landintel.site_register_status_facts
    for select
    to authenticated
    using (true);

drop policy if exists site_ldn_candidate_screen_select_authenticated on landintel.site_ldn_candidate_screen;
create policy site_ldn_candidate_screen_select_authenticated
    on landintel.site_ldn_candidate_screen
    for select
    to authenticated
    using (true);

grant select on analytics.v_register_site_development_status to authenticated;
grant select on analytics.v_ldn_candidate_screen to authenticated;
grant select on analytics.v_true_ldn_sites to authenticated;
grant select on analytics.v_ldn_review_candidates to authenticated;
grant select on analytics.v_ldn_candidate_screen_coverage to authenticated;
grant select on analytics.v_site_register_evidence_balance to authenticated;
grant select on analytics.v_register_origin_overconfidence to authenticated;
grant select on analytics.v_register_sourced_sites_needing_corroboration to authenticated;

comment on table landintel.site_ldn_candidate_screen
    is 'LDN control-fit screen for private/no-builder targets and public, RSL/charity or housebuilder/developer blockers. HLA, ELA and VDL are interpreted as register/context discovery layers requiring corroboration, not as commercial proof. It does not confirm legal ownership before title review.';

comment on function landintel.refresh_ldn_candidate_screen(integer, text)
    is 'Batch refresh for the LDN private/no-builder candidate screen. HLA, ELA and VDL can surface sites, but true LDN candidate status requires 4+ acres, attractive private/non-builder control, no public/RSL/charity/housebuilder/developer blocker, delivery not started/stalled/uneconomic/incomplete evidence, and independent corroboration where register-origin evidence is present.';

comment on view analytics.v_register_origin_overconfidence
    is 'Diagnostic view for register-origin sites that would be over-promoted by HLA, ELA or VDL presence without 4+ acres, attractive private/no-builder control, delivery-not-started/stalled/uneconomic/incomplete state and independent corroboration.';
