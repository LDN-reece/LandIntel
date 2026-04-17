comment on table landintel.canonical_sites is
    'Reconciled live-source canonical site root for the Scotland sourcing phase. This is a reconciled operational table, not a raw ingest table. Suitable for manual browsing only when paired with analytics.v_live_site_summary and related v_live_* audit views.';
comment on column landintel.canonical_sites.site_code is
    'Stable internal site code for the current live-source canonical site.';
comment on column landintel.canonical_sites.primary_ros_parcel_id is
    'Primary linked RoS parcel used as the current parcel anchor for the canonical site.';
comment on column landintel.canonical_sites.surfaced_reason is
    'Human-readable reason the site exists in the live sourcing layer.';

comment on table landintel.site_reference_aliases is
    'Reconciled alias and reference bridge table for live source records. Technical/reconciled table for lineage and matching, not the primary manual browse surface.';
comment on column landintel.site_reference_aliases.raw_reference_value is
    'Reference value exactly as observed in the source system before normalization.';
comment on column landintel.site_reference_aliases.normalized_reference_value is
    'Normalized reference used for deterministic matching across messy source systems.';
comment on column landintel.site_reference_aliases.status is
    'Alias match state. unresolved values indicate linkage still needs review.';
comment on column landintel.site_reference_aliases.confidence is
    'Numeric confidence attached to the alias match or reconciliation outcome.';

comment on table landintel.site_source_links is
    'Reconciled link table connecting canonical sites to source-family records. Technical lineage table for the live source truth, not the first analyst browse surface.';
comment on column landintel.site_source_links.link_method is
    'How the source record was attached to the canonical site, such as direct_reference or spatial_overlap.';
comment on column landintel.site_source_links.confidence is
    'Numeric confidence for the current source link.';

comment on table landintel.planning_application_records is
    'Live planning-history source table for the Scotland sourcing phase. Raw/reconciled planning records remain technical and should be browsed manually through analytics.v_live_site_summary or analytics.v_live_site_sources first.';
comment on column landintel.planning_application_records.canonical_site_id is
    'Canonical site currently linked to the planning application record, when reconciliation has succeeded.';
comment on column landintel.planning_application_records.raw_payload is
    'Raw planning payload retained for provenance and debugging.';

comment on table landintel.hla_site_records is
    'Live Housing Land Supply source table for the Scotland sourcing phase. Raw/reconciled HLA records remain technical and should be browsed manually through analytics.v_live_site_summary or analytics.v_live_site_sources first.';
comment on column landintel.hla_site_records.canonical_site_id is
    'Canonical site currently linked to the HLA record, when reconciliation has succeeded.';
comment on column landintel.hla_site_records.raw_payload is
    'Raw HLA payload retained for provenance and debugging.';

comment on table landintel.bgs_records is
    'Live BGS enrichment table for the Scotland sourcing phase. This is a technical enrichment layer used to support site understanding and traceability.';
comment on column landintel.bgs_records.canonical_site_id is
    'Canonical site currently linked to the BGS enrichment row.';
comment on column landintel.bgs_records.record_type is
    'BGS record family or collection type used to derive the enrichment row.';
comment on column landintel.bgs_records.raw_payload is
    'Raw BGS payload retained for provenance and debugging.';

comment on table landintel.evidence_references is
    'Live evidence table capturing the source-backed assertions attached to canonical sites. This is provenance-aware and suitable for drill-down, but analytics.v_live_site_summary is the preferred starting point for manual browsing.';
comment on column landintel.evidence_references.source_reference is
    'Human-relevant source reference such as a planning reference, HLA code, or sampled BGS identifier.';
comment on column landintel.evidence_references.confidence is
    'High/medium/low evidence confidence label for the attached assertion.';

comment on view landintel.v_source_ingest_summary is
    'Technical summary of record counts in the live landintel source tables. Useful for debugging, but superseded for manual source auditing by analytics.v_live_source_coverage and analytics.v_live_ingest_audit.';
comment on view landintel.v_site_traceability is
    'Deep lineage and debug view for canonical-site source linkage. This is current live-source truth for traceability, but it is not the first manual browse surface. Use only after starting with the analytics.v_live_* audit views.';

comment on view analytics.v_live_ingest_audit is
    'Analyst-facing ingest audit view for the current live-source truth. This is the supported manual browse surface for understanding what source runs executed, where they landed, and whether they succeeded.';
comment on column analytics.v_live_ingest_audit.source_family is
    'Live source family associated with the ingest run, such as planning, hla, canonical, or bgs.';
comment on column analytics.v_live_ingest_audit.destination_table is
    'Primary live destination table or table group populated by the ingest run.';
comment on column analytics.v_live_ingest_audit.target_authorities is
    'Target authorities configured for the run, when provided in ingest metadata.';
comment on column analytics.v_live_ingest_audit.latest_error_message is
    'Latest recorded error text for failed or partially failed ingest runs.';

comment on view analytics.v_live_source_coverage is
    'Primary analyst-facing coverage view for the current live-source truth. One row per authority, source family, and source dataset showing what raw data exists and how much has been linked to canonical sites.';
comment on column analytics.v_live_source_coverage.raw_record_count is
    'Total raw/reconciled source rows present for the authority and source dataset.';
comment on column analytics.v_live_source_coverage.linked_canonical_site_count is
    'Number of distinct canonical sites currently linked from the grouped source rows.';
comment on column analytics.v_live_source_coverage.linked_source_record_count is
    'Number of grouped raw source rows that already have a canonical site linkage.';
comment on column analytics.v_live_source_coverage.unlinked_raw_record_count is
    'Number of grouped raw source rows still not linked to a canonical site.';
comment on column analytics.v_live_source_coverage.last_ingest_status is
    'Latest ingest status observed for the grouped authority and source dataset.';
comment on column analytics.v_live_source_coverage.latest_source_update_at is
    'Latest update timestamp observed in the grouped source rows.';

comment on view analytics.v_live_site_sources is
    'Analyst-facing per-site source attachment view for the current live-source truth. Use this to understand what source systems are attached to a canonical site and how those links were made.';
comment on column analytics.v_live_site_sources.linked_source_record_count is
    'Number of linked source records for the site, source family, and dataset combination.';
comment on column analytics.v_live_site_sources.key_references is
    'Readable references for the linked source family, such as planning refs, HLA codes, or source record ids.';
comment on column analytics.v_live_site_sources.link_method is
    'Dominant link method used to connect the source family to the canonical site.';
comment on column analytics.v_live_site_sources.average_link_confidence is
    'Average numeric confidence across the linked source rows for this site/source combination.';

comment on view analytics.v_live_site_summary is
    'Primary analyst-facing site browse view for the current live-source truth. This is the main Supabase surface for understanding what a live sourced site is, why it exists, what sources are attached, and whether it is partial, enriched, or ready for review.';
comment on column analytics.v_live_site_summary.data_completeness_status is
    'Deterministic completeness label: raw_only, linked_partial, linked_core, or linked_enriched.';
comment on column analytics.v_live_site_summary.traceability_status is
    'Deterministic traceability label: clear, review_needed, or unresolved_links.';
comment on column analytics.v_live_site_summary.site_stage is
    'Current live site stage based on planning/HLA/BGS linkage, such as planning_only or planning_hla_bgs_linked.';
comment on column analytics.v_live_site_summary.review_ready_flag is
    'True when the site has the minimum live-source inputs needed for human review.';
comment on column analytics.v_live_site_summary.commercial_ready_flag is
    'True when the site has the minimum live-source inputs needed for commercial appraisal or underwriting.';
comment on column analytics.v_live_site_summary.missing_core_inputs is
    'Human-readable list of missing essentials preventing confidence or readiness.';
comment on column analytics.v_live_site_summary.why_not_ready is
    'Human-readable lead blocker explaining why the site is not yet review-ready or commercial-ready.';

comment on view analytics.v_live_site_readiness is
    'Analyst-facing operational readiness view derived from the live site summary. This is the fastest Supabase surface for deciding whether a canonical site is not_ready, review_ready, or commercial_ready.';
comment on column analytics.v_live_site_readiness.minimum_readiness_band is
    'Deterministic minimum readiness band: not_ready, review_ready, or commercial_ready.';
comment on column analytics.v_live_site_readiness.missing_core_inputs is
    'Human-readable list of missing essentials blocking the current readiness level.';
comment on column analytics.v_live_site_readiness.why_not_ready is
    'Human-readable explanation of the main blocker to the next readiness level.';

comment on view analytics.v_frontend_authority_summary is
    'Legacy parcel-era authority summary view. Analyst-readable for parcel coverage, but not the current live-source truth and not the supported browse surface for live site auditing.';
comment on view analytics.v_frontend_authority_size_summary is
    'Legacy parcel-era parcel size summary view. Useful for parcel storage monitoring, but not the current live-source truth or the supported browse surface for live site auditing.';
comment on view analytics.v_ros_parcels_summary_by_authority_size is
    'Legacy parcel-era parcel distribution summary view. Useful for parcel operations only, not for live-source site auditing.';
comment on view analytics.v_ingest_run_summary is
    'Legacy generic ingest run summary view across operational jobs. Useful for broad operational monitoring, but superseded for live-source auditing by analytics.v_live_ingest_audit.';
