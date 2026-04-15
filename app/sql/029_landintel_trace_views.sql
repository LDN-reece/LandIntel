create or replace view landintel.v_source_ingest_summary as
select 'planning_application_records'::text as source_table,
       count(*)::bigint as record_count,
       max(updated_at) as latest_updated_at
from landintel.planning_application_records
union all
select 'hla_site_records'::text as source_table,
       count(*)::bigint as record_count,
       max(updated_at) as latest_updated_at
from landintel.hla_site_records
union all
select 'bgs_records'::text as source_table,
       count(*)::bigint as record_count,
       max(updated_at) as latest_updated_at
from landintel.bgs_records
union all
select 'canonical_sites'::text as source_table,
       count(*)::bigint as record_count,
       max(updated_at) as latest_updated_at
from landintel.canonical_sites;


create or replace view landintel.v_site_traceability as
select
    cs.id as canonical_site_id,
    cs.site_code,
    cs.site_name_primary,
    cs.authority_name,
    ssl.source_family,
    ssl.source_dataset,
    ssl.source_record_id,
    ssl.link_method,
    ssl.confidence as link_confidence,
    sra.raw_reference_value,
    sra.normalized_reference_value,
    er.source_reference,
    er.confidence as evidence_confidence,
    er.source_registry_id,
    er.ingest_run_id,
    er.metadata as evidence_metadata
from landintel.canonical_sites as cs
left join landintel.site_source_links as ssl
  on ssl.canonical_site_id = cs.id
left join landintel.site_reference_aliases as sra
  on sra.canonical_site_id = cs.id
 and sra.source_family = ssl.source_family
left join landintel.evidence_references as er
  on er.canonical_site_id = cs.id
 and er.source_family = ssl.source_family
 and er.source_record_id = ssl.source_record_id;
