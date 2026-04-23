-- This worker writes through privileged server-side connections.
-- Keep Data API roles blocked by default unless we later add explicit policies.
alter table public.authority_aoi enable row level security;
alter table public.source_registry enable row level security;
alter table public.ingest_runs enable row level security;
alter table public.ros_cadastral_parcels enable row level security;
alter table public.land_objects enable row level security;
alter table public.land_object_toid_enrichment enable row level security;
alter table public.land_object_title_matches enable row level security;
alter table public.land_object_address_links enable row level security;
alter table staging.ros_cadastral_parcels_raw enable row level security;
alter table staging.ros_cadastral_parcels_clean enable row level security;

revoke all on table public.authority_aoi from anon, authenticated;
revoke all on table public.source_registry from anon, authenticated;
revoke all on table public.ingest_runs from anon, authenticated;
revoke all on table public.ros_cadastral_parcels from anon, authenticated;
revoke all on table public.land_objects from anon, authenticated;
revoke all on table public.land_object_toid_enrichment from anon, authenticated;
revoke all on table public.land_object_title_matches from anon, authenticated;
revoke all on table public.land_object_address_links from anon, authenticated;
revoke all on table staging.ros_cadastral_parcels_raw from anon, authenticated;
revoke all on table staging.ros_cadastral_parcels_clean from anon, authenticated;
revoke all on all sequences in schema public from anon, authenticated;
revoke all on all sequences in schema staging from anon, authenticated;

revoke all on schema analytics from anon, authenticated;
revoke all on all tables in schema analytics from anon, authenticated;
