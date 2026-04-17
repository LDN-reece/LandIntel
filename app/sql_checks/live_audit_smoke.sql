begin;
set local transaction read only;

-- Expected live audit views must exist.
do $$
declare
    expected_view text;
begin
    foreach expected_view in array[
        'analytics.v_live_ingest_audit',
        'analytics.v_live_source_coverage',
        'analytics.v_live_site_sources',
        'analytics.v_live_site_summary',
        'analytics.v_live_site_readiness'
    ] loop
        if to_regclass(expected_view) is null then
            raise exception 'Missing expected live audit view: %', expected_view;
        end if;
    end loop;
end $$;

-- Required summary columns must exist.
do $$
declare
    required_column text;
begin
    foreach required_column in array[
        'canonical_site_id',
        'site_code',
        'site_name',
        'authority_name',
        'settlement_name',
        'area_acres',
        'workflow_status',
        'surfaced_reason',
        'primary_parcel_id',
        'planning_record_count',
        'hla_record_count',
        'bgs_record_count',
        'constraint_record_count',
        'evidence_count',
        'source_families_present',
        'unresolved_alias_count',
        'latest_source_update_at',
        'latest_ingest_status',
        'data_completeness_status',
        'traceability_status',
        'site_stage',
        'review_ready_flag',
        'commercial_ready_flag',
        'missing_core_inputs',
        'why_not_ready'
    ] loop
        if not exists (
            select 1
            from information_schema.columns
            where table_schema = 'analytics'
              and table_name = 'v_live_site_summary'
              and column_name = required_column
        ) then
            raise exception 'Missing expected column on analytics.v_live_site_summary: %', required_column;
        end if;
    end loop;
end $$;

-- Required readiness columns must exist.
do $$
declare
    required_column text;
begin
    foreach required_column in array[
        'canonical_site_id',
        'site_code',
        'site_name',
        'authority_name',
        'area_acres',
        'source_families_present',
        'planning_record_count',
        'hla_record_count',
        'bgs_record_count',
        'constraint_record_count',
        'review_ready_flag',
        'commercial_ready_flag',
        'minimum_readiness_band',
        'missing_core_inputs',
        'why_not_ready',
        'latest_source_update_at'
    ] loop
        if not exists (
            select 1
            from information_schema.columns
            where table_schema = 'analytics'
              and table_name = 'v_live_site_readiness'
              and column_name = required_column
        ) then
            raise exception 'Missing expected column on analytics.v_live_site_readiness: %', required_column;
        end if;
    end loop;
end $$;

-- Summary row count should match the canonical live source root.
do $$
declare
    summary_count bigint;
    canonical_count bigint;
begin
    select count(*) into summary_count from analytics.v_live_site_summary;
    select count(*) into canonical_count from landintel.canonical_sites;

    if summary_count <> canonical_count then
        raise exception 'analytics.v_live_site_summary row count (%) does not match landintel.canonical_sites (%)', summary_count, canonical_count;
    end if;
end $$;

-- Coverage consistency checks.
do $$
begin
    if exists (
        select 1
        from analytics.v_live_source_coverage
        where raw_record_count < linked_source_record_count
           or unlinked_raw_record_count <> raw_record_count - linked_source_record_count
    ) then
        raise exception 'analytics.v_live_source_coverage contains inconsistent linkage counts';
    end if;
end $$;

-- Every linked source family should surface in the site-sources view.
do $$
begin
    if exists (
        select distinct link.canonical_site_id, link.source_family, link.source_dataset
        from landintel.site_source_links as link
        except
        select source.canonical_site_id, source.source_family, source.source_dataset
        from analytics.v_live_site_sources as source
    ) then
        raise exception 'analytics.v_live_site_sources is missing one or more linked source families';
    end if;
end $$;

-- Readiness bands must stay within the supported values.
do $$
begin
    if exists (
        select 1
        from analytics.v_live_site_readiness
        where minimum_readiness_band not in ('not_ready', 'review_ready', 'commercial_ready')
    ) then
        raise exception 'analytics.v_live_site_readiness contains an invalid minimum_readiness_band';
    end if;
end $$;

rollback;
