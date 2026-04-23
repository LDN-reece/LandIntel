drop trigger if exists site_infrastructure_records_touch_updated_at on public.site_infrastructure_records;
create trigger site_infrastructure_records_touch_updated_at
before update on public.site_infrastructure_records
for each row execute function public.touch_updated_at();

drop trigger if exists site_control_records_touch_updated_at on public.site_control_records;
create trigger site_control_records_touch_updated_at
before update on public.site_control_records
for each row execute function public.touch_updated_at();

drop trigger if exists site_assessment_overrides_touch_updated_at on public.site_assessment_overrides;
create trigger site_assessment_overrides_touch_updated_at
before update on public.site_assessment_overrides
for each row execute function public.touch_updated_at();

