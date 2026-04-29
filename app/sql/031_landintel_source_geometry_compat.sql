-- Bring any earlier MultiPolygon-only source tables onto the flexible geometry
-- type expected by the current source-phase runner. This must be conditional:
-- rewriting live source tables on every migration run burns Supabase capacity.

do $$
declare
    target record;
    current_type text;
begin
    for target in
        select *
        from (
            values
                ('landintel', 'planning_application_records'),
                ('landintel', 'hla_site_records'),
                ('landintel', 'ldp_site_records'),
                ('landintel', 'settlement_boundary_records'),
                ('landintel', 'bgs_records'),
                ('landintel', 'flood_records')
        ) as source_table(schema_name, table_name)
    loop
        select lower(replace(format_type(attribute.atttypid, attribute.atttypmod), ' ', ''))
          into current_type
        from pg_catalog.pg_attribute as attribute
        join pg_catalog.pg_class as relation
          on relation.oid = attribute.attrelid
        join pg_catalog.pg_namespace as namespace
          on namespace.oid = relation.relnamespace
        where namespace.nspname = target.schema_name
          and relation.relname = target.table_name
          and attribute.attname = 'geometry'
          and not attribute.attisdropped;

        if current_type is null then
            continue;
        end if;

        if current_type in (
            'geometry(geometry,27700)',
            'extensions.geometry(geometry,27700)'
        ) then
            continue;
        end if;

        execute format(
            'alter table %I.%I alter column geometry type geometry(Geometry, 27700) using case when geometry is null then null else ST_SetSRID(geometry, 27700) end',
            target.schema_name,
            target.table_name
        );
    end loop;
end $$;
