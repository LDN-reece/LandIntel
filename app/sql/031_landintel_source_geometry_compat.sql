-- Bring any earlier MultiPolygon-only source tables onto the flexible
-- geometry type expected by the current source-phase runner.

alter table if exists landintel.planning_application_records
    alter column geometry
    type geometry(Geometry, 27700)
    using case
        when geometry is null then null
        else ST_SetSRID(geometry, 27700)
    end;

alter table if exists landintel.hla_site_records
    alter column geometry
    type geometry(Geometry, 27700)
    using case
        when geometry is null then null
        else ST_SetSRID(geometry, 27700)
    end;

alter table if exists landintel.ldp_site_records
    alter column geometry
    type geometry(Geometry, 27700)
    using case
        when geometry is null then null
        else ST_SetSRID(geometry, 27700)
    end;

alter table if exists landintel.settlement_boundary_records
    alter column geometry
    type geometry(Geometry, 27700)
    using case
        when geometry is null then null
        else ST_SetSRID(geometry, 27700)
    end;

alter table if exists landintel.bgs_records
    alter column geometry
    type geometry(Geometry, 27700)
    using case
        when geometry is null then null
        else ST_SetSRID(geometry, 27700)
    end;

alter table if exists landintel.flood_records
    alter column geometry
    type geometry(Geometry, 27700)
    using case
        when geometry is null then null
        else ST_SetSRID(geometry, 27700)
    end;
