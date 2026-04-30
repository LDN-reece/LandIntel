create index if not exists landintel_planning_authority_source_record_idx
    on landintel.planning_application_records (authority_name, source_record_id);

create index if not exists landintel_planning_authority_updated_source_idx
    on landintel.planning_application_records (authority_name, updated_at desc, source_record_id);

create index if not exists landintel_hla_authority_source_record_idx
    on landintel.hla_site_records (authority_name, source_record_id);

create index if not exists landintel_hla_authority_updated_source_idx
    on landintel.hla_site_records (authority_name, updated_at desc, source_record_id);

comment on index landintel.landintel_planning_authority_source_record_idx
    is 'Supports bounded source reconcile catch-up seeding without full authority scans.';

comment on index landintel.landintel_hla_authority_source_record_idx
    is 'Supports bounded HLA reconcile catch-up seeding without full authority scans.';
