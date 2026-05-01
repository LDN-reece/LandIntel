create table if not exists landintel.site_urgent_address_candidates (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'urgent_address_title_pack',
    source_family text not null default 'title_control',
    site_location_id text not null,
    address_source text not null,
    uprn text,
    address_text text not null,
    match_method text not null default 'os_places_radius',
    match_rank integer,
    distance_m numeric,
    x_coordinate numeric,
    y_coordinate numeric,
    classification_code text,
    classification_description text,
    property_status text,
    source_record_signature text not null,
    raw_payload jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (distance_m is null or distance_m >= 0)
);

create unique index if not exists site_urgent_address_candidates_uidx
    on landintel.site_urgent_address_candidates (
        canonical_site_id,
        address_source,
        source_record_signature
    );

create index if not exists site_urgent_address_candidates_site_idx
    on landintel.site_urgent_address_candidates (canonical_site_id, match_rank, distance_m);

create table if not exists landintel.site_urgent_address_title_pack (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'urgent_address_title_pack',
    source_family text not null default 'title_control',
    site_location_id text not null,
    site_name text,
    authority_name text,
    area_acres numeric,
    urgency_status text not null,
    urgency_source text not null,
    urgency_reason text,
    title_spend_recommendation text,
    title_number text,
    normalized_title_number text,
    title_candidate_source text,
    title_candidate_status text not null default 'title_required',
    title_confidence numeric,
    ros_parcel_id uuid,
    ros_inspire_id text,
    primary_address_text text,
    primary_uprn text,
    address_candidate_count integer not null default 0,
    address_link_status text not null default 'address_missing',
    address_source text,
    ownership_status_pre_title text not null default 'ownership_not_confirmed',
    ownership_limitation text not null default 'ownership_not_confirmed_until_title_review',
    next_action text not null default 'link_address_and_review_title_candidate',
    source_record_signature text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id),
    check (urgency_status in ('order_title_urgently', 'true_ldn_candidate', 'title_order_urgent', 'urgent_review')),
    check (title_candidate_status in ('possible_title_reference_identified', 'parcel_candidate_identified', 'title_required')),
    check (address_link_status in ('address_linked', 'address_missing')),
    check (ownership_status_pre_title = 'ownership_not_confirmed')
);

create index if not exists site_urgent_address_title_pack_status_idx
    on landintel.site_urgent_address_title_pack (
        urgency_status,
        title_candidate_status,
        address_link_status,
        updated_at desc
    );

create index if not exists site_urgent_address_title_pack_title_idx
    on landintel.site_urgent_address_title_pack (normalized_title_number)
    where normalized_title_number is not null;

alter table landintel.site_urgent_address_candidates enable row level security;
alter table landintel.site_urgent_address_title_pack enable row level security;

grant select on landintel.site_urgent_address_candidates to authenticated;
grant select on landintel.site_urgent_address_title_pack to authenticated;

drop policy if exists site_urgent_address_candidates_select_authenticated on landintel.site_urgent_address_candidates;
create policy site_urgent_address_candidates_select_authenticated
    on landintel.site_urgent_address_candidates
    for select
    to authenticated
    using (true);

drop policy if exists site_urgent_address_title_pack_select_authenticated on landintel.site_urgent_address_title_pack;
create policy site_urgent_address_title_pack_select_authenticated
    on landintel.site_urgent_address_title_pack
    for select
    to authenticated
    using (true);

comment on table landintel.site_urgent_address_title_pack
    is 'Urgent LDN evidence pack requiring address linkage and RoS/ScotLIS title-number candidate visibility before title spend. Ownership remains unconfirmed until human title review.';
