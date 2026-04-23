create unique index if not exists site_reference_aliases_dedup_uidx
    on public.site_reference_aliases (
        site_id,
        reference_family,
        source_dataset,
        coalesce(source_record_id, ''),
        raw_reference_value
    );

create index if not exists site_reference_aliases_site_idx
    on public.site_reference_aliases (site_id, reference_family);

create index if not exists site_reference_aliases_normalised_idx
    on public.site_reference_aliases (normalised_reference_value, authority_name);

create unique index if not exists site_geometry_versions_hash_uidx
    on public.site_geometry_versions (site_id, geometry_hash);

create index if not exists site_geometry_versions_site_idx
    on public.site_geometry_versions (site_id, version_status, created_at desc);

create index if not exists site_reconciliation_matches_site_idx
    on public.site_reconciliation_matches (site_id, status, created_at desc);

create unique index if not exists site_reconciliation_matches_dedup_uidx
    on public.site_reconciliation_matches (
        coalesce(site_id, '00000000-0000-0000-0000-000000000000'::uuid),
        source_dataset,
        source_table,
        coalesce(source_record_id, ''),
        coalesce(normalised_reference_value, '')
    );

create index if not exists site_reconciliation_review_queue_status_idx
    on public.site_reconciliation_review_queue (status, created_at asc);

create index if not exists site_reconciliation_review_queue_reference_idx
    on public.site_reconciliation_review_queue (normalised_reference_value, authority_name);
