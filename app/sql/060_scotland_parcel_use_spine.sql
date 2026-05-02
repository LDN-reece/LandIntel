insert into landintel.source_estate_registry (
    source_key,
    source_family,
    source_name,
    source_group,
    phase_one_role,
    source_status,
    orchestration_mode,
    target_table,
    programme_phase,
    module_key,
    geography,
    access_status,
    ingest_status,
    normalisation_status,
    site_link_status,
    measurement_status,
    evidence_status,
    signal_status,
    assessment_status,
    trusted_for_review,
    limitation_notes,
    next_action,
    metadata,
    updated_at
) values (
    'scotland_parcel_use_spine',
    'address_property_base',
    'Scotland parcel use and address classification spine',
    'address_property_base',
    'context',
    'active',
    'derived_from_os_places_address_classification_and_ros_parcel_candidates',
    'landintel.site_scotland_parcel_use_context',
    'phase_two',
    'address_property_base',
    'Scotland',
    'access_confirmed',
    'source_registered',
    'normalised',
    'source_registered',
    'source_registered',
    'source_registered',
    'source_registered',
    'source_registered',
    false,
    'OS address classification is use context only. RoS parcel and title-number candidates do not confirm ownership before manual title review.',
    'Refresh Scotland parcel use context from OS Places/address classifications, RoS parcel candidates and service-anchor evidence.',
    jsonb_build_object(
        'source_role', 'scotland_location_context',
        'evidence_role', 'address_classification_and_parcel_use_context',
        'corroboration_required', true,
        'ownership_limitation', 'ownership_not_confirmed_until_title_review'
    ),
    now()
)
on conflict (source_key) do update set
    source_family = excluded.source_family,
    source_name = excluded.source_name,
    source_group = excluded.source_group,
    orchestration_mode = excluded.orchestration_mode,
    target_table = excluded.target_table,
    programme_phase = excluded.programme_phase,
    module_key = excluded.module_key,
    geography = excluded.geography,
    access_status = excluded.access_status,
    normalisation_status = excluded.normalisation_status,
    limitation_notes = excluded.limitation_notes,
    next_action = excluded.next_action,
    metadata = landintel.source_estate_registry.metadata || excluded.metadata,
    updated_at = now();

create table if not exists landintel.os_addressbase_classification_codes (
    classification_code text primary key,
    primary_code text not null,
    secondary_code text,
    tertiary_code text,
    quaternary_code text,
    classification_label text not null,
    classification_level text not null,
    scotland_use_group text not null default 'context',
    ldn_trigger_family text not null default 'context',
    evidence_role text not null default 'property_use_context',
    commercial_weight text not null default 'low',
    corroboration_required boolean not null default true,
    source_authority text not null default 'Ordnance Survey AddressBase Classification Scheme',
    limitation_text text not null default 'Address classification is property/use context. It does not prove ownership, planning support, access or title condition.',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (commercial_weight in ('context_only', 'low', 'low_to_medium', 'medium')),
    check (classification_level in ('primary', 'secondary', 'tertiary', 'quaternary'))
);

create table if not exists landintel.scotland_addressbase_arbitrage_rules (
    rule_key text primary key,
    source_key text not null default 'scotland_parcel_use_spine',
    source_family text not null default 'address_property_base',
    rule_name text not null,
    trigger_family text not null,
    trigger_codes text[] not null,
    excluded_codes text[] not null default '{}'::text[],
    source_role text not null,
    evidence_role text not null,
    commercial_weight text not null,
    corroboration_required boolean not null default true,
    limitation_text text not null,
    material_insight_template text not null,
    review_prompt text not null,
    next_action text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (commercial_weight in ('context_only', 'low', 'low_to_medium', 'medium'))
);

create table if not exists landintel.site_scotland_parcel_use_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'scotland_parcel_use_spine',
    source_family text not null default 'address_property_base',
    site_location_id text not null,
    site_name text,
    authority_name text,
    area_acres numeric,
    ros_parcel_id uuid,
    ros_inspire_id text,
    title_number text,
    normalized_title_number text,
    title_candidate_status text not null default 'title_required',
    ownership_status_pre_title text not null default 'ownership_not_confirmed',
    ownership_limitation text not null default 'ownership_not_confirmed_until_title_review',
    primary_uprn text,
    primary_address_text text,
    address_candidate_count integer not null default 0,
    classified_address_count integer not null default 0,
    primary_classification_code text,
    primary_classification_label text,
    primary_use_group text,
    trigger_families text[] not null default '{}'::text[],
    trigger_codes text[] not null default '{}'::text[],
    trigger_rule_keys text[] not null default '{}'::text[],
    land_use_position text not null default 'unclassified_context',
    settlement_service_anchor_status text not null default 'service_anchor_not_yet_proven',
    service_anchor_count_1600m integer not null default 0,
    ldn_interest_signal text not null default 'needs_address_classification',
    commercial_weight text not null default 'low',
    corroboration_required boolean not null default true,
    corroboration_points text[] not null default '{}'::text[],
    missing_corroboration text[] not null default '{}'::text[],
    material_insight text not null,
    review_next_action text not null,
    evidence_confidence text not null default 'insufficient',
    source_record_signature text not null,
    metadata jsonb not null default '{}'::jsonb,
    measured_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id),
    check (ownership_status_pre_title = 'ownership_not_confirmed'),
    check (commercial_weight in ('context_only', 'low', 'low_to_medium', 'medium')),
    check (evidence_confidence in ('high', 'medium', 'low', 'mixed', 'insufficient')),
    check (title_candidate_status in ('possible_title_reference_identified', 'parcel_candidate_identified', 'title_required')),
    check (ldn_interest_signal in ('interesting_with_corroboration', 'context_only', 'needs_address_classification', 'not_prioritised_from_use_context'))
);

create index if not exists os_addressbase_classification_codes_primary_idx
    on landintel.os_addressbase_classification_codes (primary_code, secondary_code, scotland_use_group);

create index if not exists scotland_addressbase_rules_trigger_idx
    on landintel.scotland_addressbase_arbitrage_rules using gin (trigger_codes);

create index if not exists site_scotland_parcel_use_context_signal_idx
    on landintel.site_scotland_parcel_use_context (ldn_interest_signal, commercial_weight, measured_at desc);

create index if not exists site_scotland_parcel_use_context_title_idx
    on landintel.site_scotland_parcel_use_context (normalized_title_number)
    where normalized_title_number is not null;

insert into landintel.os_addressbase_classification_codes (
    classification_code,
    primary_code,
    secondary_code,
    tertiary_code,
    classification_label,
    classification_level,
    scotland_use_group,
    ldn_trigger_family,
    evidence_role,
    commercial_weight,
    corroboration_required,
    metadata
) values
    ('C', 'C', null, null, 'Commercial', 'primary', 'commercial_context', 'commercial_property_context', 'base_use_context', 'low', true, '{}'::jsonb),
    ('R', 'R', null, null, 'Residential', 'primary', 'residential_context', 'residential_footprint_context', 'base_use_context', 'context_only', true, '{}'::jsonb),
    ('L', 'L', null, null, 'Land', 'primary', 'land_context', 'raw_land_review', 'land_use_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('X', 'X', null, null, 'Dual use', 'primary', 'mixed_use_context', 'mixed_use_review', 'base_use_context', 'low', true, '{}'::jsonb),
    ('M', 'M', null, null, 'Military', 'primary', 'restricted_context', 'public_or_restricted_context', 'base_use_context', 'context_only', true, '{}'::jsonb),
    ('Z', 'Z', null, null, 'Object of interest', 'primary', 'object_context', 'heritage_or_object_context', 'base_use_context', 'context_only', true, '{}'::jsonb),
    ('P', 'P', null, null, 'Parent shell', 'primary', 'parent_shell_context', 'address_parent_context', 'base_use_context', 'context_only', true, '{}'::jsonb),
    ('U', 'U', null, null, 'Unclassified', 'primary', 'unclassified_context', 'needs_classification', 'base_use_context', 'context_only', true, '{}'::jsonb),
    ('CA', 'C', 'CA', null, 'Commercial agricultural', 'secondary', 'agricultural_context', 'agricultural_conversion_review', 'reuse_or_conversion_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CA01', 'C', 'CA', 'CA01', 'Farm or non-residential associated building', 'tertiary', 'agricultural_context', 'agricultural_conversion_review', 'reuse_or_conversion_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CA02', 'C', 'CA', 'CA02', 'Fishery', 'tertiary', 'agricultural_context', 'rural_reuse_review', 'reuse_or_conversion_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CA03', 'C', 'CA', 'CA03', 'Horticulture or nursery', 'tertiary', 'agricultural_context', 'rural_reuse_review', 'reuse_or_conversion_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CA04', 'C', 'CA', 'CA04', 'Slaughterhouse or abattoir', 'tertiary', 'agricultural_context', 'brownfield_reuse_review', 'reuse_or_conversion_visibility', 'medium', true, '{}'::jsonb),
    ('LA', 'L', 'LA', null, 'Agricultural land', 'secondary', 'land_context', 'raw_land_review', 'land_use_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('LF', 'L', 'LF', null, 'Forestry', 'secondary', 'land_context', 'forestry_land_review', 'land_use_visibility', 'low', true, '{}'::jsonb),
    ('LL', 'L', 'LL', null, 'Allotment', 'secondary', 'land_context', 'community_land_context', 'land_use_visibility', 'context_only', true, '{}'::jsonb),
    ('LM', 'L', 'LM', null, 'Amenity land', 'secondary', 'land_context', 'amenity_land_context', 'land_use_visibility', 'context_only', true, '{}'::jsonb),
    ('LO', 'L', 'LO', null, 'Open space', 'secondary', 'land_context', 'open_space_context', 'land_use_visibility', 'context_only', true, '{}'::jsonb),
    ('LR', 'L', 'LR', null, 'Recreation or sports ground', 'secondary', 'land_context', 'recreation_land_context', 'land_use_visibility', 'context_only', true, '{}'::jsonb),
    ('LV', 'L', 'LV', null, 'Vacant land', 'secondary', 'land_context', 'vacant_land_priority_review', 'land_use_visibility', 'medium', true, '{}'::jsonb),
    ('LW', 'L', 'LW', null, 'Water', 'secondary', 'water_context', 'water_constraint_context', 'land_use_visibility', 'context_only', true, '{}'::jsonb),
    ('CI', 'C', 'CI', null, 'Commercial industrial', 'secondary', 'industrial_context', 'industrial_yard_depot_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CI01', 'C', 'CI', 'CI01', 'Factory or manufacturing', 'tertiary', 'industrial_context', 'industrial_yard_depot_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CI02', 'C', 'CI', 'CI02', 'Mineral working or quarry', 'tertiary', 'industrial_context', 'heavy_land_reuse_review', 'reuse_or_repositioning_visibility', 'medium', true, '{}'::jsonb),
    ('CI03', 'C', 'CI', 'CI03', 'Workshop or light industrial', 'tertiary', 'industrial_context', 'industrial_yard_depot_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CI04', 'C', 'CI', 'CI04', 'Depot', 'tertiary', 'industrial_context', 'industrial_yard_depot_review', 'reuse_or_repositioning_visibility', 'medium', true, '{}'::jsonb),
    ('CI05', 'C', 'CI', 'CI05', 'Wholesale distribution or warehousing', 'tertiary', 'industrial_context', 'industrial_yard_depot_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CI08', 'C', 'CI', 'CI08', 'Recycling plant', 'tertiary', 'industrial_context', 'industrial_reuse_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CR', 'C', 'CR', null, 'Commercial retail', 'secondary', 'commercial_context', 'retail_reuse_context', 'commercial_use_visibility', 'low', true, '{}'::jsonb),
    ('CR01', 'C', 'CR', 'CR01', 'Bank or financial service', 'tertiary', 'commercial_context', 'retail_reuse_context', 'commercial_use_visibility', 'low', true, '{}'::jsonb),
    ('CR04', 'C', 'CR', 'CR04', 'Market', 'tertiary', 'commercial_context', 'retail_reuse_context', 'commercial_use_visibility', 'low', true, '{}'::jsonb),
    ('CR06', 'C', 'CR', 'CR06', 'Public house, bar or nightclub', 'tertiary', 'commercial_context', 'retail_reuse_context', 'commercial_use_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CR07', 'C', 'CR', 'CR07', 'Restaurant or cafeteria', 'tertiary', 'commercial_context', 'retail_reuse_context', 'commercial_use_visibility', 'low', true, '{}'::jsonb),
    ('CR08', 'C', 'CR', 'CR08', 'Shop or showroom', 'tertiary', 'commercial_context', 'retail_reuse_context', 'commercial_use_visibility', 'low', true, '{}'::jsonb),
    ('CR11', 'C', 'CR', 'CR11', 'Supermarket or hypermarket', 'tertiary', 'commercial_context', 'service_anchor_context', 'service_anchor_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CS', 'C', 'CS', null, 'Commercial storage', 'secondary', 'storage_context', 'yard_or_storage_reuse_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CS01', 'C', 'CS', 'CS01', 'General storage land', 'tertiary', 'storage_context', 'yard_or_storage_reuse_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CS02', 'C', 'CS', 'CS02', 'Builders yard', 'tertiary', 'storage_context', 'yard_or_storage_reuse_review', 'reuse_or_repositioning_visibility', 'medium', true, '{}'::jsonb),
    ('RD', 'R', 'RD', null, 'Residential dwelling', 'secondary', 'residential_context', 'residential_footprint_context', 'residential_visibility', 'context_only', true, '{}'::jsonb),
    ('RD01', 'R', 'RD', 'RD01', 'Caravan', 'tertiary', 'residential_context', 'residential_footprint_context', 'residential_visibility', 'context_only', true, '{}'::jsonb),
    ('RD02', 'R', 'RD', 'RD02', 'Detached house', 'tertiary', 'residential_context', 'residential_footprint_context', 'residential_visibility', 'context_only', true, '{}'::jsonb),
    ('RD03', 'R', 'RD', 'RD03', 'Semi-detached house', 'tertiary', 'residential_context', 'residential_footprint_context', 'residential_visibility', 'context_only', true, '{}'::jsonb),
    ('RD04', 'R', 'RD', 'RD04', 'Terraced house', 'tertiary', 'residential_context', 'residential_footprint_context', 'residential_visibility', 'context_only', true, '{}'::jsonb),
    ('RD06', 'R', 'RD', 'RD06', 'Self-contained flat', 'tertiary', 'residential_context', 'residential_footprint_context', 'residential_visibility', 'context_only', true, '{}'::jsonb),
    ('RH', 'R', 'RH', null, 'House in multiple occupation', 'secondary', 'residential_context', 'residential_intensification_context', 'residential_visibility', 'low', true, '{}'::jsonb),
    ('RH01', 'R', 'RH', 'RH01', 'House in multiple occupation parent', 'tertiary', 'residential_context', 'residential_intensification_context', 'residential_visibility', 'low', true, '{}'::jsonb),
    ('RH02', 'R', 'RH', 'RH02', 'House in multiple occupation bedsit', 'tertiary', 'residential_context', 'residential_intensification_context', 'residential_visibility', 'low', true, '{}'::jsonb),
    ('RI', 'R', 'RI', null, 'Residential institution', 'secondary', 'institutional_context', 'institutional_reuse_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('RI01', 'R', 'RI', 'RI01', 'Care or nursing home', 'tertiary', 'institutional_context', 'institutional_reuse_review', 'reuse_or_repositioning_visibility', 'medium', true, '{}'::jsonb),
    ('RI02', 'R', 'RI', 'RI02', 'Communal residence', 'tertiary', 'institutional_context', 'institutional_reuse_review', 'reuse_or_repositioning_visibility', 'low_to_medium', true, '{}'::jsonb),
    ('CC', 'C', 'CC', null, 'Community services', 'secondary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CE', 'C', 'CE', null, 'Education', 'secondary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CE01', 'C', 'CE', 'CE01', 'College', 'tertiary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CE02', 'C', 'CE', 'CE02', 'Children nursery', 'tertiary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CE03', 'C', 'CE', 'CE03', 'Primary school', 'tertiary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CE04', 'C', 'CE', 'CE04', 'Secondary school', 'tertiary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CM', 'C', 'CM', null, 'Medical', 'secondary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CM01', 'C', 'CM', 'CM01', 'Dentist', 'tertiary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CM02', 'C', 'CM', 'CM02', 'General practice surgery', 'tertiary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CM03', 'C', 'CM', 'CM03', 'Hospital', 'tertiary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CT', 'C', 'CT', null, 'Transport', 'secondary', 'service_anchor_context', 'service_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CT01', 'C', 'CT', 'CT01', 'Airfield', 'tertiary', 'service_anchor_context', 'transport_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CT02', 'C', 'CT', 'CT02', 'Bus shelter', 'tertiary', 'service_anchor_context', 'transport_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CT03', 'C', 'CT', 'CT03', 'Car park', 'tertiary', 'service_anchor_context', 'transport_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CT05', 'C', 'CT', 'CT05', 'Marina', 'tertiary', 'service_anchor_context', 'transport_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb),
    ('CT06', 'C', 'CT', 'CT06', 'Railway asset', 'tertiary', 'service_anchor_context', 'transport_anchor_context', 'service_anchor_visibility', 'context_only', true, '{}'::jsonb)
on conflict (classification_code) do update set
    primary_code = excluded.primary_code,
    secondary_code = excluded.secondary_code,
    tertiary_code = excluded.tertiary_code,
    classification_label = excluded.classification_label,
    classification_level = excluded.classification_level,
    scotland_use_group = excluded.scotland_use_group,
    ldn_trigger_family = excluded.ldn_trigger_family,
    evidence_role = excluded.evidence_role,
    commercial_weight = excluded.commercial_weight,
    corroboration_required = excluded.corroboration_required,
    metadata = landintel.os_addressbase_classification_codes.metadata || excluded.metadata,
    updated_at = now();

insert into landintel.scotland_addressbase_arbitrage_rules (
    rule_key,
    rule_name,
    trigger_family,
    trigger_codes,
    source_role,
    evidence_role,
    commercial_weight,
    corroboration_required,
    limitation_text,
    material_insight_template,
    review_prompt,
    next_action,
    metadata
) values
    (
        'raw_land_review',
        'Raw land and unbuilt land review',
        'raw_land_review',
        array['L', 'LA', 'LV']::text[],
        'scotland_location_context',
        'land_use_visibility',
        'low_to_medium',
        true,
        'Land classification identifies use context only. It does not prove availability, ownership, services, access or planning support.',
        'The address/use evidence points to land rather than an occupied building. This can turn a generic parcel into a focused LDN review question when size, service anchors, title readiness, constraints and planning context corroborate it.',
        'Check whether this land is privately controlled, serviceable, constraint-light and worth title spend.',
        'review_private_control_and_planning_corroboration',
        jsonb_build_object('scotland_first', true)
    ),
    (
        'vacant_land_priority_review',
        'Vacant land priority review',
        'vacant_land_priority_review',
        array['LV']::text[],
        'scotland_location_context',
        'underuse_visibility',
        'medium',
        true,
        'Vacant land classification is a strong review prompt but not proof of control, services, access or planning route.',
        'Vacant land evidence can materially sharpen sourcing because it indicates a land-use state that may be overlooked. It still needs ownership, constraints, access, settlement and buyer corroboration before spend.',
        'Prioritise DD on ownership route, constraints, service context and planning journey.',
        'review_vacant_land_with_corroboration',
        jsonb_build_object('scotland_first', true)
    ),
    (
        'agricultural_conversion_review',
        'Scottish agricultural and rural reuse review',
        'agricultural_conversion_review',
        array['CA', 'CA01', 'CA02', 'CA03', 'CA04']::text[],
        'scotland_location_context',
        'rural_reuse_visibility',
        'low_to_medium',
        true,
        'Agricultural classification is reuse context. Scottish permitted-development and planning routes require separate review.',
        'Agricultural or rural building evidence can identify a reuse angle, but it is not an English-style shortcut and it does not prove conversion rights in Scotland.',
        'Review Scottish planning route, access, services, building condition and title before spend.',
        'review_scottish_rural_reuse_route',
        jsonb_build_object('scotland_first', true, 'not_english_class_q_logic', true)
    ),
    (
        'industrial_yard_depot_review',
        'Industrial yard, depot and storage review',
        'industrial_yard_depot_review',
        array['CI', 'CI01', 'CI02', 'CI03', 'CI04', 'CI05', 'CI08', 'CS', 'CS01', 'CS02']::text[],
        'scotland_location_context',
        'repositioning_visibility',
        'low_to_medium',
        true,
        'Industrial/storage classification is use context. It does not prove relocation appetite, contamination position, services or buyer demand.',
        'Industrial, depot or yard evidence may indicate a brownfield/repositioning angle, especially inside settlement or near growth. The DD question becomes whether ownership, contamination, access and market context support an LDN approach.',
        'Review ownership route, abnormal context, access and local planning precedent.',
        'review_industrial_reuse_and_control_route',
        jsonb_build_object('scotland_first', true)
    ),
    (
        'institutional_reuse_review',
        'Institutional reuse review',
        'institutional_reuse_review',
        array['RI', 'RI01', 'RI02']::text[],
        'scotland_location_context',
        'reuse_or_repositioning_visibility',
        'low_to_medium',
        true,
        'Institutional classification identifies use context only. Operator status, title, planning and building condition need separate evidence.',
        'Institutional property evidence can highlight a repositioning lead where the operator or use has changed, but the commercial case depends on ownership, planning and buyer corroboration.',
        'Review operator status, title workflow, planning history and market depth.',
        'review_institutional_repositioning_context',
        jsonb_build_object('scotland_first', true)
    ),
    (
        'service_anchor_context',
        'Service-anchor and 20-minute-neighbourhood context',
        'service_anchor_context',
        array['CC', 'CE', 'CE01', 'CE02', 'CE03', 'CE04', 'CM', 'CM01', 'CM02', 'CM03', 'CT', 'CT01', 'CT02', 'CT03', 'CT05', 'CT06', 'CR11']::text[],
        'scotland_location_context',
        'location_strength_visibility',
        'context_only',
        true,
        'Service-anchor classification supports location context. It does not prove demand, policy support, capacity or access.',
        'Service-anchor evidence helps explain why a land parcel may be locationally interesting, especially when paired with settlement and planning context.',
        'Use as corroboration for location strength, not as a standalone sourcing claim.',
        'use_as_location_corroboration',
        jsonb_build_object('scotland_first', true)
    )
on conflict (rule_key) do update set
    rule_name = excluded.rule_name,
    trigger_family = excluded.trigger_family,
    trigger_codes = excluded.trigger_codes,
    source_role = excluded.source_role,
    evidence_role = excluded.evidence_role,
    commercial_weight = excluded.commercial_weight,
    corroboration_required = excluded.corroboration_required,
    limitation_text = excluded.limitation_text,
    material_insight_template = excluded.material_insight_template,
    review_prompt = excluded.review_prompt,
    next_action = excluded.next_action,
    metadata = landintel.scotland_addressbase_arbitrage_rules.metadata || excluded.metadata,
    updated_at = now();

create or replace function landintel.refresh_scotland_parcel_use_context(
    p_batch_size integer default 250,
    p_authority_name text default null
)
returns table (
    selected_site_count integer,
    context_row_count integer,
    context_with_classification_count integer,
    context_with_land_trigger_count integer,
    context_with_title_candidate_count integer,
    evidence_row_count integer,
    signal_row_count integer
)
language plpgsql
set search_path = pg_catalog, public, landintel, extensions
as $$
declare
    v_batch_size integer := greatest(coalesce(p_batch_size, 250), 1);
    v_authority_name text := nullif(btrim(coalesce(p_authority_name, '')), '');
begin
    create temporary table if not exists tmp_scotland_parcel_use_sites (
        canonical_site_id uuid primary key,
        site_location_id text,
        site_name text,
        authority_name text,
        area_acres numeric,
        ros_parcel_id uuid,
        previous_signature text
    ) on commit drop;

    truncate tmp_scotland_parcel_use_sites;

    insert into tmp_scotland_parcel_use_sites (
        canonical_site_id,
        site_location_id,
        site_name,
        authority_name,
        area_acres,
        ros_parcel_id,
        previous_signature
    )
    select
        site.id,
        site.id::text,
        site.site_name_primary,
        site.authority_name,
        site.area_acres,
        coalesce(pack.ros_parcel_id, site.primary_ros_parcel_id),
        existing.source_record_signature
    from landintel.canonical_sites as site
    left join landintel.site_urgent_address_title_pack as pack
      on pack.canonical_site_id = site.id
    left join landintel.site_scotland_parcel_use_context as existing
      on existing.canonical_site_id = site.id
    left join lateral (
        select true as has_address_candidate
        from landintel.site_urgent_address_candidates as candidate
        where candidate.canonical_site_id = site.id
        limit 1
    ) as address_presence on true
    left join lateral (
        select true as has_ldn_screen
        from landintel.site_ldn_candidate_screen as ldn_screen
        where ldn_screen.canonical_site_id = site.id
        limit 1
    ) as screen_presence on true
    where site.geometry is not null
      and (v_authority_name is null or site.authority_name ilike '%%' || v_authority_name || '%%')
      and (
            address_presence.has_address_candidate is true
         or pack.canonical_site_id is not null
         or screen_presence.has_ldn_screen is true
         or site.primary_ros_parcel_id is not null
      )
    order by
        existing.updated_at nulls first,
        (address_presence.has_address_candidate is true) desc,
        (pack.canonical_site_id is not null) desc,
        (screen_presence.has_ldn_screen is true) desc,
        coalesce(site.area_acres, 0) desc,
        site.id
    limit v_batch_size;

    create temporary table if not exists tmp_scotland_parcel_use_prepared (
        canonical_site_id uuid primary key,
        site_location_id text,
        site_name text,
        authority_name text,
        area_acres numeric,
        ros_parcel_id uuid,
        ros_inspire_id text,
        title_number text,
        normalized_title_number text,
        title_candidate_status text,
        primary_uprn text,
        primary_address_text text,
        address_candidate_count integer,
        classified_address_count integer,
        primary_classification_code text,
        primary_classification_label text,
        primary_use_group text,
        trigger_families text[],
        trigger_codes text[],
        trigger_rule_keys text[],
        land_use_position text,
        settlement_service_anchor_status text,
        service_anchor_count_1600m integer,
        ldn_interest_signal text,
        commercial_weight text,
        corroboration_required boolean,
        corroboration_points text[],
        missing_corroboration text[],
        material_insight text,
        review_next_action text,
        evidence_confidence text,
        previous_signature text,
        current_signature text
    ) on commit drop;

    truncate tmp_scotland_parcel_use_prepared;

    insert into tmp_scotland_parcel_use_prepared
    select
        selected.canonical_site_id,
        selected.site_location_id,
        selected.site_name,
        selected.authority_name,
        selected.area_acres,
        coalesce(title_candidate.ros_parcel_id, selected.ros_parcel_id),
        title_candidate.ros_inspire_id,
        title_candidate.title_number,
        title_candidate.normalized_title_number,
        case
            when title_candidate.normalized_title_number is not null then 'possible_title_reference_identified'
            when coalesce(title_candidate.ros_parcel_id, selected.ros_parcel_id) is not null then 'parcel_candidate_identified'
            else 'title_required'
        end,
        address_summary.primary_uprn,
        address_summary.primary_address_text,
        coalesce(address_summary.address_candidate_count, 0),
        coalesce(address_summary.classified_address_count, 0),
        address_summary.primary_classification_code,
        coalesce(primary_code.classification_label, address_summary.primary_classification_label),
        coalesce(primary_code.scotland_use_group, 'unclassified_context'),
        coalesce(rule_summary.trigger_families, '{}'::text[]),
        coalesce(address_summary.classification_codes, '{}'::text[]),
        coalesce(rule_summary.trigger_rule_keys, '{}'::text[]),
        case
            when 'vacant_land_priority_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then 'vacant_land_context'
            when 'raw_land_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then 'raw_land_context'
            when 'agricultural_conversion_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then 'agricultural_building_context'
            when 'industrial_yard_depot_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then 'industrial_reuse_context'
            when 'institutional_reuse_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then 'institutional_reuse_context'
            when coalesce(address_summary.classified_address_count, 0) > 0 then coalesce(primary_code.scotland_use_group, 'classified_context')
            else 'unclassified_context'
        end,
        case
            when coalesce(service_anchor.service_anchor_count_1600m, 0) > 0 then 'service_anchor_within_1600m'
            when 'service_anchor_context' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then 'service_anchor_on_site_or_address'
            else 'service_anchor_not_yet_proven'
        end,
        coalesce(service_anchor.service_anchor_count_1600m, 0),
        case
            when coalesce(address_summary.classified_address_count, 0) = 0 then 'needs_address_classification'
            when (
                    'vacant_land_priority_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[]))
                 or 'raw_land_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[]))
                 or 'industrial_yard_depot_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[]))
                 or 'agricultural_conversion_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[]))
                 or 'institutional_reuse_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[]))
            )
            and coalesce(selected.area_acres, 0) >= 4 then 'interesting_with_corroboration'
            when 'service_anchor_context' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then 'context_only'
            else 'not_prioritised_from_use_context'
        end,
        case
            when 'medium' = any(coalesce(rule_summary.commercial_weights, '{}'::text[])) then 'medium'
            when 'low_to_medium' = any(coalesce(rule_summary.commercial_weights, '{}'::text[])) then 'low_to_medium'
            when 'low' = any(coalesce(rule_summary.commercial_weights, '{}'::text[])) then 'low'
            else 'context_only'
        end,
        true,
        array_remove(array[
            case when coalesce(address_summary.classified_address_count, 0) > 0 then 'address_classification_present' end,
            case when coalesce(title_candidate.normalized_title_number, '') <> '' then 'possible_title_reference_identified' end,
            case when coalesce(title_candidate.ros_parcel_id, selected.ros_parcel_id) is not null then 'ros_parcel_candidate_identified' end,
            case when coalesce(service_anchor.service_anchor_count_1600m, 0) > 0 then 'service_anchor_context_within_1600m' end,
            case when coalesce(selected.area_acres, 0) >= 4 then 'site_area_meets_ldn_size_floor' end
        ], null::text),
        array_remove(array[
            case when coalesce(address_summary.classified_address_count, 0) = 0 then 'os_places_or_addressbase_classification_required' end,
            case when coalesce(title_candidate.normalized_title_number, '') = '' then 'manual_title_workflow_required' end,
            case when coalesce(service_anchor.service_anchor_count_1600m, 0) = 0 then 'service_anchor_context_not_yet_proven' end,
            'ownership_not_confirmed_until_title_review'
        ], null::text),
        case
            when coalesce(address_summary.classified_address_count, 0) = 0 then
                'This Scottish site needs OS address classification before the land-use signal can be interpreted. RoS parcel evidence can support title-number targeting but does not confirm ownership.'
            when 'vacant_land_priority_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then
                'OS classification points to vacant land. That materially improves DD focus because the site can be reviewed as a land-control question, subject to title, constraints, settlement, access and buyer corroboration.'
            when 'raw_land_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then
                'OS classification points to land rather than an occupied property. This helps separate a genuine land lead from generic map geometry, but it needs corroboration before title spend.'
            when 'industrial_yard_depot_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then
                'OS classification points to industrial, depot or storage use. This can create a repositioning DD angle if ownership, abnormal context, access and market evidence support it.'
            when 'agricultural_conversion_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then
                'OS classification points to agricultural or rural use. In Scotland this is a rural reuse review prompt, not an automatic permitted-development route.'
            when 'institutional_reuse_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[])) then
                'OS classification points to institutional use. That can create a repositioning question where operator, title, planning and market evidence corroborate it.'
            else
                'OS address classification provides property-use context. It improves interpretation but does not, alone, justify ownership or planning conclusions.'
        end,
        case
            when coalesce(address_summary.classified_address_count, 0) = 0 then 'Run OS Places/address classification for this site before use-led DD.'
            when coalesce(title_candidate.normalized_title_number, '') = '' then 'Resolve RoS/ScotLIS title-number candidate before title spend.'
            when (
                    'vacant_land_priority_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[]))
                 or 'raw_land_review' = any(coalesce(rule_summary.trigger_families, '{}'::text[]))
            )
            and coalesce(service_anchor.service_anchor_count_1600m, 0) > 0 then 'Review private control, settlement position, constraints and access before title spend.'
            else 'Use this as DD context alongside planning, constraints, market and manual title workflow.'
        end,
        case
            when coalesce(address_summary.classified_address_count, 0) > 0
             and coalesce(title_candidate.normalized_title_number, '') <> '' then 'medium'
            when coalesce(address_summary.classified_address_count, 0) > 0 then 'low'
            else 'insufficient'
        end,
        selected.previous_signature,
        md5(concat_ws(
            '|',
            selected.canonical_site_id::text,
            coalesce(title_candidate.normalized_title_number, ''),
            coalesce(title_candidate.ros_parcel_id::text, selected.ros_parcel_id::text, ''),
            coalesce(address_summary.primary_uprn, ''),
            coalesce(address_summary.primary_address_text, ''),
            coalesce(array_to_string(address_summary.classification_codes, ','), ''),
            coalesce(array_to_string(rule_summary.trigger_rule_keys, ','), ''),
            coalesce(service_anchor.service_anchor_count_1600m, 0)::text
        ))
    from tmp_scotland_parcel_use_sites as selected
    left join lateral (
        select *
        from (
            select
                1 as source_rank,
                pack.title_number,
                pack.normalized_title_number,
                pack.ros_parcel_id,
                pack.ros_inspire_id,
                pack.title_confidence as confidence
            from landintel.site_urgent_address_title_pack as pack
            where pack.canonical_site_id = selected.canonical_site_id

            union all

            select
                2,
                validation.title_number,
                validation.normalized_title_number,
                nullif(validation.metadata ->> 'ros_parcel_id', '')::uuid,
                validation.metadata ->> 'ros_inspire_id',
                validation.confidence
            from public.site_title_validation as validation
            where validation.site_id = selected.canonical_site_id::text

            union all

            select
                3,
                parcel.title_number,
                parcel.normalized_title_number,
                parcel.id,
                parcel.ros_inspire_id,
                0.6::numeric
            from public.ros_cadastral_parcels as parcel
            where parcel.id = selected.ros_parcel_id
        ) as title_candidates
        where title_number is not null
           or normalized_title_number is not null
           or ros_parcel_id is not null
        order by source_rank, confidence desc nulls last
        limit 1
    ) as title_candidate on true
    left join lateral (
        select
            count(*)::integer as address_candidate_count,
            count(*) filter (where nullif(btrim(classification_code), '') is not null)::integer as classified_address_count,
            (array_agg(uprn order by match_rank nulls last, distance_m nulls last, updated_at desc))[1] as primary_uprn,
            (array_agg(address_text order by match_rank nulls last, distance_m nulls last, updated_at desc))[1] as primary_address_text,
            (array_agg(nullif(upper(btrim(classification_code)), '') order by match_rank nulls last, distance_m nulls last, updated_at desc)
                filter (where nullif(btrim(classification_code), '') is not null))[1] as primary_classification_code,
            (array_agg(classification_description order by match_rank nulls last, distance_m nulls last, updated_at desc)
                filter (where nullif(btrim(classification_code), '') is not null))[1] as primary_classification_label,
            array_remove(array_agg(distinct nullif(upper(btrim(classification_code)), '')), null::text) as classification_codes
        from landintel.site_urgent_address_candidates as candidate
        where candidate.canonical_site_id = selected.canonical_site_id
    ) as address_summary on true
    left join landintel.os_addressbase_classification_codes as primary_code
      on primary_code.classification_code = address_summary.primary_classification_code
    left join lateral (
        select
            array_remove(array_agg(distinct rule.rule_key order by rule.rule_key), null::text) as trigger_rule_keys,
            array_remove(array_agg(distinct rule.trigger_family order by rule.trigger_family), null::text) as trigger_families,
            array_remove(array_agg(distinct rule.commercial_weight order by rule.commercial_weight), null::text) as commercial_weights
        from landintel.scotland_addressbase_arbitrage_rules as rule
        where exists (
            select 1
            from unnest(coalesce(address_summary.classification_codes, '{}'::text[])) as code(classification_code)
            join unnest(rule.trigger_codes) as trigger_code(classification_code)
              on code.classification_code = trigger_code.classification_code
              or code.classification_code like trigger_code.classification_code || '%%'
        )
    ) as rule_summary on true
    left join lateral (
        select coalesce(sum(context.count_within_1600m), 0)::integer as service_anchor_count_1600m
        from landintel.site_open_location_spine_context as context
        where context.canonical_site_id = selected.canonical_site_id
          and lower(context.feature_type) in (
              'school',
              'education',
              'college',
              'nursery',
              'gp_surgery',
              'medical',
              'healthcare',
              'hospital',
              'rail_station',
              'bus_stop',
              'public_transport_stop',
              'supermarket',
              'local_centre',
              'amenity'
          )
    ) as service_anchor on true;

    insert into landintel.site_scotland_parcel_use_context (
        canonical_site_id,
        source_key,
        source_family,
        site_location_id,
        site_name,
        authority_name,
        area_acres,
        ros_parcel_id,
        ros_inspire_id,
        title_number,
        normalized_title_number,
        title_candidate_status,
        ownership_status_pre_title,
        ownership_limitation,
        primary_uprn,
        primary_address_text,
        address_candidate_count,
        classified_address_count,
        primary_classification_code,
        primary_classification_label,
        primary_use_group,
        trigger_families,
        trigger_codes,
        trigger_rule_keys,
        land_use_position,
        settlement_service_anchor_status,
        service_anchor_count_1600m,
        ldn_interest_signal,
        commercial_weight,
        corroboration_required,
        corroboration_points,
        missing_corroboration,
        material_insight,
        review_next_action,
        evidence_confidence,
        source_record_signature,
        metadata,
        measured_at,
        updated_at
    )
    select
        prepared.canonical_site_id,
        'scotland_parcel_use_spine',
        'address_property_base',
        prepared.site_location_id,
        prepared.site_name,
        prepared.authority_name,
        prepared.area_acres,
        prepared.ros_parcel_id,
        prepared.ros_inspire_id,
        prepared.title_number,
        prepared.normalized_title_number,
        prepared.title_candidate_status,
        'ownership_not_confirmed',
        'ownership_not_confirmed_until_title_review',
        prepared.primary_uprn,
        prepared.primary_address_text,
        prepared.address_candidate_count,
        prepared.classified_address_count,
        prepared.primary_classification_code,
        prepared.primary_classification_label,
        prepared.primary_use_group,
        prepared.trigger_families,
        prepared.trigger_codes,
        prepared.trigger_rule_keys,
        prepared.land_use_position,
        prepared.settlement_service_anchor_status,
        prepared.service_anchor_count_1600m,
        prepared.ldn_interest_signal,
        prepared.commercial_weight,
        prepared.corroboration_required,
        prepared.corroboration_points,
        prepared.missing_corroboration,
        prepared.material_insight,
        prepared.review_next_action,
        prepared.evidence_confidence,
        prepared.current_signature,
        jsonb_build_object(
            'source_key', 'scotland_parcel_use_spine',
            'source_role', 'scotland_location_context',
            'evidence_role', 'address_classification_and_parcel_use_context',
            'address_classification_basis', 'OS Places or AddressBase classification code',
            'ros_basis', 'RoS parcel and title-number candidate where available',
            'title_limitation', 'title_number_candidate_not_ownership_confirmation',
            'ownership_limitation', 'ownership_not_confirmed_until_title_review',
            'corroboration_required', true
        ),
        now(),
        now()
    from tmp_scotland_parcel_use_prepared as prepared
    on conflict (canonical_site_id) do update set
        site_location_id = excluded.site_location_id,
        site_name = excluded.site_name,
        authority_name = excluded.authority_name,
        area_acres = excluded.area_acres,
        ros_parcel_id = excluded.ros_parcel_id,
        ros_inspire_id = excluded.ros_inspire_id,
        title_number = excluded.title_number,
        normalized_title_number = excluded.normalized_title_number,
        title_candidate_status = excluded.title_candidate_status,
        ownership_status_pre_title = excluded.ownership_status_pre_title,
        ownership_limitation = excluded.ownership_limitation,
        primary_uprn = excluded.primary_uprn,
        primary_address_text = excluded.primary_address_text,
        address_candidate_count = excluded.address_candidate_count,
        classified_address_count = excluded.classified_address_count,
        primary_classification_code = excluded.primary_classification_code,
        primary_classification_label = excluded.primary_classification_label,
        primary_use_group = excluded.primary_use_group,
        trigger_families = excluded.trigger_families,
        trigger_codes = excluded.trigger_codes,
        trigger_rule_keys = excluded.trigger_rule_keys,
        land_use_position = excluded.land_use_position,
        settlement_service_anchor_status = excluded.settlement_service_anchor_status,
        service_anchor_count_1600m = excluded.service_anchor_count_1600m,
        ldn_interest_signal = excluded.ldn_interest_signal,
        commercial_weight = excluded.commercial_weight,
        corroboration_required = excluded.corroboration_required,
        corroboration_points = excluded.corroboration_points,
        missing_corroboration = excluded.missing_corroboration,
        material_insight = excluded.material_insight,
        review_next_action = excluded.review_next_action,
        evidence_confidence = excluded.evidence_confidence,
        source_record_signature = excluded.source_record_signature,
        metadata = excluded.metadata,
        measured_at = now(),
        updated_at = now();

    create temporary table if not exists tmp_scotland_parcel_use_changed (
        canonical_site_id uuid primary key,
        previous_signature text,
        current_signature text
    ) on commit drop;

    truncate tmp_scotland_parcel_use_changed;

    insert into tmp_scotland_parcel_use_changed
    select
        prepared.canonical_site_id,
        prepared.previous_signature,
        prepared.current_signature
    from tmp_scotland_parcel_use_prepared as prepared
    where prepared.previous_signature is distinct from prepared.current_signature;

    delete from landintel.evidence_references as evidence
    using tmp_scotland_parcel_use_changed as changed
    where evidence.canonical_site_id = changed.canonical_site_id
      and evidence.source_family = 'address_property_base'
      and evidence.metadata ->> 'source_key' = 'scotland_parcel_use_spine';

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
        context.canonical_site_id,
        'address_property_base',
        'scotland_parcel_use_spine',
        context.canonical_site_id::text,
        coalesce(context.primary_classification_code, context.normalized_title_number, context.primary_uprn, context.canonical_site_id::text),
        case
            when context.evidence_confidence in ('high', 'medium') then context.evidence_confidence
            else 'low'
        end,
        jsonb_build_object(
            'source_key', 'scotland_parcel_use_spine',
            'classification_code', context.primary_classification_code,
            'trigger_families', context.trigger_families,
            'land_use_position', context.land_use_position,
            'ownership_limitation', context.ownership_limitation,
            'title_limitation', 'title_number_candidate_not_ownership_confirmation'
        )
    from landintel.site_scotland_parcel_use_context as context
    join tmp_scotland_parcel_use_changed as changed
      on changed.canonical_site_id = context.canonical_site_id;

    delete from landintel.site_signals as signal
    using tmp_scotland_parcel_use_changed as changed
    where signal.canonical_site_id = changed.canonical_site_id
      and signal.source_family = 'address_property_base'
      and signal.metadata ->> 'source_key' = 'scotland_parcel_use_spine';

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
        context.canonical_site_id,
        'address_property_base',
        'scotland_parcel_use_context',
        context.ldn_interest_signal,
        context.service_anchor_count_1600m,
        case
            when context.evidence_confidence = 'medium' then 0.65
            when context.evidence_confidence = 'low' then 0.45
            else 0.25
        end,
        'address_property_base',
        context.canonical_site_id::text,
        'scotland_parcel_use_spine',
        jsonb_build_object(
            'classification_code', context.primary_classification_code,
            'trigger_families', context.trigger_families,
            'corroboration_points', context.corroboration_points,
            'missing_corroboration', context.missing_corroboration
        ),
        jsonb_build_object('source_key', 'scotland_parcel_use_spine'),
        true
    from landintel.site_scotland_parcel_use_context as context
    join tmp_scotland_parcel_use_changed as changed
      on changed.canonical_site_id = context.canonical_site_id;

    return query
    select
        (select count(*)::integer from tmp_scotland_parcel_use_sites),
        (select count(*)::integer from tmp_scotland_parcel_use_prepared),
        (select count(*)::integer from tmp_scotland_parcel_use_prepared where classified_address_count > 0),
        (select count(*)::integer from tmp_scotland_parcel_use_prepared where 'raw_land_review' = any(trigger_families) or 'vacant_land_priority_review' = any(trigger_families)),
        (select count(*)::integer from tmp_scotland_parcel_use_prepared where title_candidate_status = 'possible_title_reference_identified'),
        (select count(*)::integer from landintel.evidence_references as evidence join tmp_scotland_parcel_use_changed as changed on changed.canonical_site_id = evidence.canonical_site_id where evidence.metadata ->> 'source_key' = 'scotland_parcel_use_spine'),
        (select count(*)::integer from landintel.site_signals as signal join tmp_scotland_parcel_use_changed as changed on changed.canonical_site_id = signal.canonical_site_id where signal.metadata ->> 'source_key' = 'scotland_parcel_use_spine');
end;
$$;

create or replace view analytics.v_scotland_addressbase_trigger_dictionary
with (security_invoker = true) as
select
    code.classification_code,
    code.primary_code,
    code.secondary_code,
    code.tertiary_code,
    code.classification_label,
    code.classification_level,
    code.scotland_use_group,
    code.ldn_trigger_family,
    code.evidence_role,
    code.commercial_weight,
    code.corroboration_required,
    code.limitation_text
from landintel.os_addressbase_classification_codes as code
order by code.primary_code, code.secondary_code nulls first, code.classification_code;

create or replace view analytics.v_scotland_parcel_use_context
with (security_invoker = true) as
select
    context.canonical_site_id,
    context.site_location_id,
    context.site_name,
    context.authority_name,
    context.area_acres,
    context.ros_parcel_id,
    context.ros_inspire_id,
    context.title_number,
    context.normalized_title_number,
    context.title_candidate_status,
    context.ownership_status_pre_title,
    context.ownership_limitation,
    context.primary_uprn,
    context.primary_address_text,
    context.address_candidate_count,
    context.classified_address_count,
    context.primary_classification_code,
    context.primary_classification_label,
    context.primary_use_group,
    context.trigger_families,
    context.land_use_position,
    context.settlement_service_anchor_status,
    context.service_anchor_count_1600m,
    context.ldn_interest_signal,
    context.commercial_weight,
    context.corroboration_required,
    context.corroboration_points,
    context.missing_corroboration,
    context.material_insight,
    context.review_next_action,
    context.evidence_confidence,
    context.measured_at,
    context.updated_at
from landintel.site_scotland_parcel_use_context as context
order by
    case context.ldn_interest_signal
        when 'interesting_with_corroboration' then 1
        when 'context_only' then 2
        when 'needs_address_classification' then 3
        else 4
    end,
    coalesce(context.area_acres, 0) desc,
    context.updated_at desc;

create or replace view analytics.v_scotland_land_opportunity_insight
with (security_invoker = true) as
select
    context.canonical_site_id,
    context.site_name,
    context.authority_name,
    context.area_acres,
    context.primary_classification_code,
    context.primary_classification_label,
    context.land_use_position,
    context.ldn_interest_signal,
    context.commercial_weight,
    context.settlement_service_anchor_status,
    context.service_anchor_count_1600m,
    context.title_candidate_status,
    context.normalized_title_number,
    context.ownership_status_pre_title,
    context.ownership_limitation,
    context.corroboration_points,
    context.missing_corroboration,
    context.material_insight as material_impact_on_land_opportunity,
    case
        when context.ldn_interest_signal = 'interesting_with_corroboration'
            then 'This changes the DD question from map-search to control-search: can LDN secure a privately controlled, corroborated land opportunity with title, constraints, access and market support?'
        when context.ldn_interest_signal = 'needs_address_classification'
            then 'This site still lacks property-use classification. It cannot yet be interpreted as a use-led sourcing opportunity.'
        else 'This is useful context, but not yet a standalone commercial sourcing reason.'
    end as what_this_changes_for_ldn,
    context.review_next_action,
    context.evidence_confidence,
    context.measured_at
from landintel.site_scotland_parcel_use_context as context
order by
    case context.ldn_interest_signal
        when 'interesting_with_corroboration' then 1
        when 'context_only' then 2
        when 'needs_address_classification' then 3
        else 4
    end,
    coalesce(context.area_acres, 0) desc,
    context.measured_at desc;

create or replace view analytics.v_scotland_parcel_use_coverage
with (security_invoker = true) as
select
    count(*)::bigint as context_row_count,
    count(distinct canonical_site_id)::bigint as linked_site_count,
    count(*) filter (where classified_address_count > 0)::bigint as classified_site_count,
    count(*) filter (where ldn_interest_signal = 'interesting_with_corroboration')::bigint as interesting_with_corroboration_count,
    count(*) filter (where 'raw_land_review' = any(trigger_families) or 'vacant_land_priority_review' = any(trigger_families))::bigint as land_trigger_count,
    count(*) filter (where title_candidate_status = 'possible_title_reference_identified')::bigint as title_candidate_count,
    count(*) filter (where service_anchor_count_1600m > 0)::bigint as service_anchor_context_count,
    max(measured_at) as latest_measured_at
from landintel.site_scotland_parcel_use_context;

drop view if exists analytics.v_landintel_source_lifecycle_stage_counts;
drop view if exists analytics.v_landintel_source_estate_matrix_base_060;

do $$
begin
    if to_regclass('analytics.v_landintel_source_estate_matrix_base_060') is null
       and to_regclass('analytics.v_landintel_source_estate_matrix') is not null then
        execute 'alter view analytics.v_landintel_source_estate_matrix rename to v_landintel_source_estate_matrix_base_060';
    end if;
end;
$$;

drop view if exists analytics.v_landintel_source_estate_matrix;

create or replace view analytics.v_landintel_source_estate_matrix
with (security_invoker = true) as
with base as (
    select *
    from analytics.v_landintel_source_estate_matrix_base_060
    where source_key <> 'scotland_parcel_use_spine'
), registry as (
    select *
    from landintel.source_estate_registry
    where source_key = 'scotland_parcel_use_spine'
), context_counts as (
    select
        source_key,
        source_family,
        count(*)::bigint as row_count,
        count(distinct canonical_site_id)::bigint as linked_site_count,
        count(distinct canonical_site_id)::bigint as measured_site_count
    from landintel.site_scotland_parcel_use_context
    group by source_key, source_family
), evidence_counts as (
    select
        metadata ->> 'source_key' as source_key,
        source_family,
        count(*)::bigint as evidence_count
    from landintel.evidence_references
    where metadata ->> 'source_key' = 'scotland_parcel_use_spine'
    group by metadata ->> 'source_key', source_family
), signal_counts as (
    select
        metadata ->> 'source_key' as source_key,
        source_family,
        count(*)::bigint as signal_count
    from landintel.site_signals
    where metadata ->> 'source_key' = 'scotland_parcel_use_spine'
    group by metadata ->> 'source_key', source_family
), freshness as (
    select distinct on (source_family, source_key)
        source_family,
        source_key,
        freshness_status,
        records_observed,
        last_success_at
    from (
        select
            source_family,
            replace(replace(source_scope_key, 'phase2:', ''), 'source_expansion:', '') as source_key,
            freshness_status,
            records_observed,
            last_success_at,
            last_checked_at,
            updated_at
        from landintel.source_freshness_states
        where source_scope_key in ('phase2:scotland_parcel_use_spine', 'source_expansion:scotland_parcel_use_spine')
    ) as freshness_rows
    order by source_family, source_key, last_checked_at desc nulls last, updated_at desc
), event_rollup as (
    select
        source_family,
        source_key,
        max(created_at) filter (where status in ('success', 'source_registered', 'raw_data_landed', 'evidence_generated', 'signals_generated', 'assessment_ready')) as last_successful_run
    from landintel.source_expansion_events
    where source_key = 'scotland_parcel_use_spine'
    group by source_family, source_key
), scotland_matrix as (
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
        coalesce(context_counts.row_count, 0)::bigint as row_count,
        coalesce(context_counts.linked_site_count, 0)::bigint as linked_site_count,
        coalesce(context_counts.measured_site_count, 0)::bigint as measured_site_count,
        0::bigint as assessment_ready_count,
        coalesce(evidence_counts.evidence_count, 0)::bigint as evidence_count,
        coalesce(signal_counts.signal_count, 0)::bigint as signal_count,
        registry.limitation_notes,
        registry.next_action
    from registry
    left join context_counts on context_counts.source_key = registry.source_key and context_counts.source_family = registry.source_family
    left join evidence_counts on evidence_counts.source_key = registry.source_key and evidence_counts.source_family = registry.source_family
    left join signal_counts on signal_counts.source_key = registry.source_key and signal_counts.source_family = registry.source_family
    left join freshness on freshness.source_key = registry.source_key and freshness.source_family = registry.source_family
    left join event_rollup on event_rollup.source_key = registry.source_key and event_rollup.source_family = registry.source_family
), scotland_gates as (
    select
        scotland_matrix.*,
        (
            access_status in ('access_required', 'gated', 'failed', 'stale')
            or freshness_status in ('failed', 'stale', 'access_required', 'gated')
            or limitation_notes ilike any (array[
                '%%has not yet%%',
                '%%not yet%%',
                '%%requires%%',
                '%%required%%',
                '%%must be confirmed%%',
                '%%before use%%',
                '%%adapter%%'
            ])
        ) as critical_limitation_blocking_review
    from scotland_matrix
), scotland_row as (
    select
        scotland_gates.*,
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
    from scotland_gates
)
select * from base
union all
select * from scotland_row;

create or replace view analytics.v_landintel_source_lifecycle_stage_counts
with (security_invoker = true) as
with lifecycle_stage(stage_name) as (
    values
        ('source_registered'::text),
        ('access_confirmed'::text),
        ('raw_data_landed'::text),
        ('normalised'::text),
        ('linked_to_site'::text),
        ('measured'::text),
        ('evidence_generated'::text),
        ('signals_generated'::text),
        ('assessment_ready'::text),
        ('trusted_for_review'::text)
)
select
    lifecycle_stage.stage_name,
    count(matrix.source_key)::bigint as source_count
from lifecycle_stage
left join analytics.v_landintel_source_estate_matrix as matrix
  on matrix.current_lifecycle_stage = lifecycle_stage.stage_name
group by lifecycle_stage.stage_name
order by array_position(array[
    'source_registered',
    'access_confirmed',
    'raw_data_landed',
    'normalised',
    'linked_to_site',
    'measured',
    'evidence_generated',
    'signals_generated',
    'assessment_ready',
    'trusted_for_review'
]::text[], lifecycle_stage.stage_name);

alter table landintel.os_addressbase_classification_codes enable row level security;
alter table landintel.scotland_addressbase_arbitrage_rules enable row level security;
alter table landintel.site_scotland_parcel_use_context enable row level security;

drop policy if exists os_addressbase_classification_codes_select_authenticated on landintel.os_addressbase_classification_codes;
create policy os_addressbase_classification_codes_select_authenticated
    on landintel.os_addressbase_classification_codes
    for select
    to authenticated
    using (true);

drop policy if exists scotland_addressbase_arbitrage_rules_select_authenticated on landintel.scotland_addressbase_arbitrage_rules;
create policy scotland_addressbase_arbitrage_rules_select_authenticated
    on landintel.scotland_addressbase_arbitrage_rules
    for select
    to authenticated
    using (true);

drop policy if exists site_scotland_parcel_use_context_select_authenticated on landintel.site_scotland_parcel_use_context;
create policy site_scotland_parcel_use_context_select_authenticated
    on landintel.site_scotland_parcel_use_context
    for select
    to authenticated
    using (true);

grant select on landintel.os_addressbase_classification_codes to authenticated;
grant select on landintel.scotland_addressbase_arbitrage_rules to authenticated;
grant select on landintel.site_scotland_parcel_use_context to authenticated;

grant select on analytics.v_scotland_addressbase_trigger_dictionary to authenticated;
grant select on analytics.v_scotland_parcel_use_context to authenticated;
grant select on analytics.v_scotland_land_opportunity_insight to authenticated;
grant select on analytics.v_scotland_parcel_use_coverage to authenticated;
grant select on analytics.v_landintel_source_estate_matrix to authenticated;
grant select on analytics.v_landintel_source_lifecycle_stage_counts to authenticated;

comment on table landintel.os_addressbase_classification_codes
    is 'Import-ready OS address classification dictionary with the critical Scotland-first LDN trigger subset seeded. These codes describe property/use context only.';

comment on table landintel.site_scotland_parcel_use_context
    is 'Stored Scotland parcel/use context linking OS Places/address classification evidence to RoS parcel and title-number candidates. Ownership remains unconfirmed until manual title review.';

comment on function landintel.refresh_scotland_parcel_use_context(integer, text)
    is 'Refreshes Scotland parcel/use context from OS address classification candidates, RoS title candidates and open service-anchor context. Emits evidence and signals only when the stored evidence state changes.';
