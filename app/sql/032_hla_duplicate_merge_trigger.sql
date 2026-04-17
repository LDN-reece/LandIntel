create or replace function landintel.source_rows_array(payload jsonb)
returns jsonb
language sql
immutable
as $$
    select case
        when payload is null then '[]'::jsonb
        when jsonb_typeof(payload) = 'object' and payload ? 'source_rows' then coalesce(payload->'source_rows', '[]'::jsonb)
        else jsonb_build_array(payload)
    end
$$;

create or replace function landintel.merge_hla_duplicate_rows()
returns trigger
language plpgsql
as $$
declare
    existing_id uuid;
    existing_rows jsonb;
    incoming_rows jsonb;
begin
    select id
    into existing_id
    from landintel.hla_site_records
    where authority_name = new.authority_name
      and source_record_id = new.source_record_id
    limit 1;

    if existing_id is null then
        return new;
    end if;

    existing_rows := landintel.source_rows_array((select raw_payload from landintel.hla_site_records where id = existing_id));
    incoming_rows := landintel.source_rows_array(new.raw_payload);

    update landintel.hla_site_records as current
    set canonical_site_id = coalesce(current.canonical_site_id, new.canonical_site_id),
        site_reference = coalesce(current.site_reference, new.site_reference),
        site_name = coalesce(current.site_name, new.site_name),
        effectiveness_status = coalesce(current.effectiveness_status, new.effectiveness_status),
        programming_horizon = coalesce(current.programming_horizon, new.programming_horizon),
        constraint_reasons = (
            select coalesce(array_agg(distinct reason) filter (where reason is not null and reason <> ''), '{}'::text[])
            from unnest(coalesce(current.constraint_reasons, '{}'::text[]) || coalesce(new.constraint_reasons, '{}'::text[])) as reason
        ),
        developer_name = coalesce(current.developer_name, new.developer_name),
        remaining_capacity = coalesce(current.remaining_capacity, new.remaining_capacity),
        completions = coalesce(current.completions, new.completions),
        tenure = coalesce(current.tenure, new.tenure),
        brownfield_indicator = case
            when current.brownfield_indicator is true or new.brownfield_indicator is true then true
            when current.brownfield_indicator is false or new.brownfield_indicator is false then false
            else null
        end,
        geometry = case
            when current.geometry is null then new.geometry
            when new.geometry is null then current.geometry
            else ST_Multi(ST_UnaryUnion(ST_Collect(current.geometry, new.geometry)))
        end,
        source_registry_id = coalesce(current.source_registry_id, new.source_registry_id),
        ingest_run_id = new.ingest_run_id,
        raw_payload = jsonb_build_object(
            'source_row_count', jsonb_array_length(existing_rows) + jsonb_array_length(incoming_rows),
            'source_rows', existing_rows || incoming_rows
        ),
        updated_at = now()
    where current.id = existing_id;

    return null;
end;
$$;

drop trigger if exists landintel_merge_hla_duplicates_before_insert on landintel.hla_site_records;

create trigger landintel_merge_hla_duplicates_before_insert
before insert on landintel.hla_site_records
for each row
execute function landintel.merge_hla_duplicate_rows();
