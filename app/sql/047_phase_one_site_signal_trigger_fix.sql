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

    return new;
end;
$$;

comment on function landintel.ensure_phase_one_site_signal_compatibility()
    is 'Live-schema-safe Phase One compatibility trigger. It only sets the required legacy signal_key and deliberately avoids optional columns that are not guaranteed on the live site_signals table.';
