alter table public.authority_aoi disable row level security;
alter table public.source_registry disable row level security;
alter table public.ingest_runs disable row level security;
alter table public.ros_cadastral_parcels disable row level security;
alter table public.land_objects disable row level security;
alter table public.land_object_toid_enrichment disable row level security;
alter table public.land_object_title_matches disable row level security;
alter table public.land_object_address_links disable row level security;
alter table staging.ros_cadastral_parcels_raw disable row level security;
alter table staging.ros_cadastral_parcels_clean disable row level security;

grant usage on schema analytics to anon, authenticated;
grant select on analytics.v_ingest_run_summary to authenticated;
grant select on analytics.v_ros_parcels_summary_by_authority_size to authenticated;
grant select on analytics.v_source_registry_latest to authenticated;
