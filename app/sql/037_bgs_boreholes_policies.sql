alter table public.bgs_boreholes_raw enable row level security;
alter table public.bgs_boreholes enable row level security;

revoke all on table public.bgs_boreholes_raw from anon, authenticated;
revoke all on table public.bgs_boreholes from anon, authenticated;
revoke all on table analytics.v_bgs_borehole_ingest_summary from anon, authenticated;
