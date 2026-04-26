create or replace function landintel.ensure_phase_one_site_signal_compatibility()
returns trigger
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
declare
    v_key_basis text;
    v_key_prefix text;
begin
    v_key_basis := concat_ws(
        '|',
        coalesce(new.canonical_site_id::text, ''),
        coalesce(new.source_family, ''),
        coalesce(new.source_record_id, ''),
        coalesce(new.signal_family, ''),
        coalesce(new.signal_name, ''),
        coalesce(new.fact_label, '')
    );

    v_key_prefix := concat_ws(
        ':',
        coalesce(nullif(btrim(new.source_family), ''), 'source'),
        coalesce(nullif(btrim(new.signal_family), ''), 'signal'),
        coalesce(nullif(btrim(new.signal_name), ''), 'fact')
    );

    if new.signal_key is null or btrim(new.signal_key) = '' then
        new.signal_key := left(v_key_prefix, 160) || ':' || md5(v_key_basis);
    end if;

    if new.signal_payload is null then
        new.signal_payload := '{}'::jsonb;
    end if;

    if new.signal_source is null or btrim(new.signal_source) = '' then
        new.signal_source := 'derived';
    end if;

    return new;
end;
$$;

drop trigger if exists site_signals_phase_one_compatibility_trigger on landintel.site_signals;

create trigger site_signals_phase_one_compatibility_trigger
before insert or update on landintel.site_signals
for each row
execute function landintel.ensure_phase_one_site_signal_compatibility();

update landintel.site_signals
set signal_key = left(
        concat_ws(
            ':',
            coalesce(nullif(btrim(source_family), ''), 'source'),
            coalesce(nullif(btrim(signal_family), ''), 'signal'),
            coalesce(nullif(btrim(signal_name), ''), 'fact')
        ),
        160
    ) || ':' || md5(
        concat_ws(
            '|',
            coalesce(canonical_site_id::text, ''),
            coalesce(source_family, ''),
            coalesce(source_record_id, ''),
            coalesce(signal_family, ''),
            coalesce(signal_name, ''),
            coalesce(fact_label, '')
        )
    )
where signal_key is null or btrim(signal_key) = '';

comment on function landintel.ensure_phase_one_site_signal_compatibility()
    is 'Keeps the legacy not-null signal_key contract compatible with Phase One source-expansion signals while preserving signal_family and signal_name as the current operating model.';
