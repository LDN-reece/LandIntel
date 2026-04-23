"""Database access for the site qualification engine."""

from __future__ import annotations

import json
from typing import Any, Iterable

from src.db import Database
from src.site_engine.types import SiteSearchFilters, SiteSnapshot


class SiteQualificationRepository:
    """Keep database I/O explicit and dataset-aware."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def fetch_site_by_code(self, site_code: str) -> dict[str, Any] | None:
        return self.database.fetch_one(
            """
            select *
            from public.sites
            where site_code = :site_code
            """,
            {"site_code": site_code},
        )

    def fetch_parcel_record(self, ros_parcel_id: str) -> dict[str, Any] | None:
        return self.database.fetch_one(
            """
            select
                rp.id::text as ros_parcel_id,
                lo.id::text as land_object_id,
                rp.ros_inspire_id,
                rp.authority_name,
                rp.area_acres,
                rp.area_ha,
                coalesce(
                    rp.raw_attributes ->> 'title_number',
                    rp.raw_attributes ->> 'TitleNumber',
                    rp.raw_attributes ->> 'TITLE_NUMBER'
                ) as title_number
            from public.ros_cadastral_parcels as rp
            left join public.land_objects as lo
                on lo.source_system = 'ros_cadastral'
               and lo.authority_name = rp.authority_name
               and lo.source_key = rp.ros_inspire_id
            where rp.id = cast(:ros_parcel_id as uuid)
            """,
            {"ros_parcel_id": ros_parcel_id},
        )

    def select_seed_candidate_parcel(
        self,
        preferred_authorities: list[str],
        min_acres: float,
        max_acres: float,
        site_code: str,
    ) -> dict[str, Any] | None:
        sql = """
            select
                rp.id::text as ros_parcel_id,
                lo.id::text as land_object_id,
                rp.ros_inspire_id,
                rp.authority_name,
                rp.area_acres,
                rp.area_ha,
                coalesce(
                    rp.raw_attributes ->> 'title_number',
                    rp.raw_attributes ->> 'TitleNumber',
                    rp.raw_attributes ->> 'TITLE_NUMBER'
                ) as title_number
            from public.ros_cadastral_parcels as rp
            left join public.land_objects as lo
                on lo.source_system = 'ros_cadastral'
               and lo.authority_name = rp.authority_name
               and lo.source_key = rp.ros_inspire_id
            where rp.area_acres between :min_acres and :max_acres
              and not exists (
                    select 1
                    from public.site_parcels as sp
                    join public.sites as site on site.id = sp.site_id
                    where sp.ros_parcel_id = rp.id
                      and site.site_code <> :site_code
              )
        """
        params: dict[str, Any] = {
            "min_acres": min_acres,
            "max_acres": max_acres,
            "site_code": site_code,
        }
        if preferred_authorities:
            authority_clause = _build_in_clause("rp.authority_name", "authority", preferred_authorities, params)
            sql += f"\n  and {authority_clause}"
        sql += "\norder by rp.area_acres desc\nlimit 1"
        return self.database.fetch_one(sql, params)

    def select_related_parcels(
        self,
        primary_ros_parcel_id: str,
        site_code: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        return self.database.fetch_all(
            """
            select
                rp.id::text as ros_parcel_id,
                lo.id::text as land_object_id,
                rp.ros_inspire_id,
                rp.authority_name,
                rp.area_acres,
                rp.area_ha,
                coalesce(
                    rp.raw_attributes ->> 'title_number',
                    rp.raw_attributes ->> 'TitleNumber',
                    rp.raw_attributes ->> 'TITLE_NUMBER'
                ) as title_number
            from public.ros_cadastral_parcels as base
            join public.ros_cadastral_parcels as rp
                on rp.id <> base.id
               and rp.authority_name = base.authority_name
               and ST_DWithin(rp.centroid, base.centroid, 600)
            left join public.land_objects as lo
                on lo.source_system = 'ros_cadastral'
               and lo.authority_name = rp.authority_name
               and lo.source_key = rp.ros_inspire_id
            where base.id = cast(:primary_ros_parcel_id as uuid)
              and not exists (
                    select 1
                    from public.site_parcels as sp
                    join public.sites as site on site.id = sp.site_id
                    where sp.ros_parcel_id = rp.id
                      and site.site_code <> :site_code
              )
            order by ST_Distance(rp.centroid, base.centroid), rp.area_acres desc
            limit :limit
            """,
            {
                "primary_ros_parcel_id": primary_ros_parcel_id,
                "site_code": site_code,
                "limit": limit,
            },
        )

    def upsert_buyer_profiles(self, profiles: list[dict[str, Any]]) -> dict[str, str]:
        profile_ids: dict[str, str] = {}
        for profile in profiles:
            profile_id = self.database.scalar(
                """
                insert into public.buyer_profiles (
                    profile_code,
                    buyer_name,
                    target_strategy,
                    min_acres,
                    max_acres,
                    preferred_authorities,
                    min_price_per_sqft_gbp,
                    notes,
                    metadata
                )
                values (
                    :profile_code,
                    :buyer_name,
                    :target_strategy,
                    :min_acres,
                    :max_acres,
                    :preferred_authorities,
                    :min_price_per_sqft_gbp,
                    :notes,
                    cast(:metadata as jsonb)
                )
                on conflict (profile_code) do update set
                    buyer_name = excluded.buyer_name,
                    target_strategy = excluded.target_strategy,
                    min_acres = excluded.min_acres,
                    max_acres = excluded.max_acres,
                    preferred_authorities = excluded.preferred_authorities,
                    min_price_per_sqft_gbp = excluded.min_price_per_sqft_gbp,
                    notes = excluded.notes,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning id
                """,
                {
                    **profile,
                    "preferred_authorities": profile.get("preferred_authorities", []),
                    "metadata": "{}",
                },
            )
            profile_ids[str(profile["profile_code"])] = str(profile_id)
        return profile_ids

    def upsert_site(
        self,
        *,
        site_code: str,
        site_name: str,
        workflow_status: str,
        source_method: str,
        primary_ros_parcel_id: str | None,
        primary_land_object_id: str | None,
        metadata_json: str,
    ) -> str:
        return str(
            self.database.scalar(
                """
                insert into public.sites (
                    site_code,
                    site_name,
                    workflow_status,
                    source_method,
                    primary_ros_parcel_id,
                    primary_land_object_id,
                    metadata
                )
                values (
                    :site_code,
                    :site_name,
                    :workflow_status,
                    :source_method,
                    cast(:primary_ros_parcel_id as uuid),
                    cast(:primary_land_object_id as uuid),
                    cast(:metadata_json as jsonb)
                )
                on conflict (site_code) do update set
                    site_name = excluded.site_name,
                    workflow_status = excluded.workflow_status,
                    source_method = excluded.source_method,
                    primary_ros_parcel_id = excluded.primary_ros_parcel_id,
                    primary_land_object_id = excluded.primary_land_object_id,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning id
                """,
                {
                    "site_code": site_code,
                    "site_name": site_name,
                    "workflow_status": workflow_status,
                    "source_method": source_method,
                    "primary_ros_parcel_id": primary_ros_parcel_id,
                    "primary_land_object_id": primary_land_object_id,
                    "metadata_json": metadata_json,
                },
            )
        )

    def clear_site_fact_rows(self, site_id: str) -> None:
        self.database.execute(
            "delete from analytics.site_search_cache where site_id = cast(:site_id as uuid)",
            {"site_id": site_id},
        )
        self.database.execute(
            "delete from public.site_reconciliation_review_queue where candidate_site_id = cast(:site_id as uuid)",
            {"site_id": site_id},
        )
        for table_name in (
            "public.site_reconciliation_matches",
            "public.site_geometry_versions",
            "public.site_reference_aliases",
            "public.site_buyer_matches",
            "public.comparable_market_records",
            "public.site_control_records",
            "public.site_infrastructure_records",
            "public.site_constraints",
            "public.planning_context_records",
            "public.planning_records",
            "public.site_geometry_components",
            "public.site_parcels",
            "public.site_locations",
        ):
            self.database.execute(f"delete from {table_name} where site_id = cast(:site_id as uuid)", {"site_id": site_id})

    def clear_site_reconciliation_state(self, site_id: str) -> None:
        self.database.execute(
            "delete from public.site_reconciliation_review_queue where candidate_site_id = cast(:site_id as uuid)",
            {"site_id": site_id},
        )
        self.database.execute(
            "delete from public.site_reconciliation_matches where site_id = cast(:site_id as uuid)",
            {"site_id": site_id},
        )

    def insert_site_reference_alias(
        self,
        *,
        site_id: str,
        reference_family: str,
        raw_reference_value: str,
        normalised_reference_value: str,
        source_dataset: str,
        authority_name: str | None,
        plan_period: str | None,
        site_name_hint: str | None,
        geometry_hash: str | None,
        source_record_id: str | None,
        source_identifier: str | None,
        source_url: str | None,
        relation_type: str,
        status: str,
        linked_confidence: float | None,
        match_notes: str | None,
        metadata_json: str,
    ) -> None:
        self.database.execute(
            """
            insert into public.site_reference_aliases (
                site_id,
                reference_family,
                raw_reference_value,
                normalised_reference_value,
                source_dataset,
                authority_name,
                plan_period,
                site_name_hint,
                geometry_hash,
                source_record_id,
                source_identifier,
                source_url,
                relation_type,
                status,
                linked_confidence,
                match_notes,
                metadata
            )
            values (
                cast(:site_id as uuid),
                :reference_family,
                :raw_reference_value,
                :normalised_reference_value,
                :source_dataset,
                :authority_name,
                :plan_period,
                :site_name_hint,
                :geometry_hash,
                :source_record_id,
                :source_identifier,
                :source_url,
                :relation_type,
                :status,
                :linked_confidence,
                :match_notes,
                cast(:metadata_json as jsonb)
            )
            on conflict do nothing
            """,
            {
                "site_id": site_id,
                "reference_family": reference_family,
                "raw_reference_value": raw_reference_value,
                "normalised_reference_value": normalised_reference_value,
                "source_dataset": source_dataset,
                "authority_name": authority_name,
                "plan_period": plan_period,
                "site_name_hint": site_name_hint,
                "geometry_hash": geometry_hash,
                "source_record_id": source_record_id,
                "source_identifier": source_identifier,
                "source_url": source_url,
                "relation_type": relation_type,
                "status": status,
                "linked_confidence": linked_confidence,
                "match_notes": match_notes,
                "metadata_json": metadata_json,
            },
        )

    def insert_site_geometry_version_from_current_location(
        self,
        *,
        site_id: str,
        version_label: str,
        source_dataset: str,
        source_table: str,
        source_record_id: str | None,
        relation_type: str,
        match_confidence: float | None,
        source_url: str | None,
        metadata_json: str,
    ) -> None:
        self.database.execute(
            """
            insert into public.site_geometry_versions (
                site_id,
                version_label,
                version_status,
                source_dataset,
                source_table,
                source_record_id,
                relation_type,
                geometry_hash,
                match_confidence,
                centroid,
                geometry,
                area_sqm,
                source_url,
                metadata
            )
            select
                cast(:site_id as uuid),
                :version_label,
                'current',
                :source_dataset,
                :source_table,
                :source_record_id,
                :relation_type,
                md5(ST_AsEWKB(location.geometry)::text),
                :match_confidence,
                location.centroid,
                location.geometry,
                ST_Area(location.geometry),
                :source_url,
                cast(:metadata_json as jsonb)
            from public.site_locations as location
            where location.site_id = cast(:site_id as uuid)
            on conflict do nothing
            """,
            {
                "site_id": site_id,
                "version_label": version_label,
                "source_dataset": source_dataset,
                "source_table": source_table,
                "source_record_id": source_record_id,
                "relation_type": relation_type,
                "match_confidence": match_confidence,
                "source_url": source_url,
                "metadata_json": metadata_json,
            },
        )

    def insert_site_reconciliation_match(
        self,
        *,
        site_id: str,
        source_dataset: str,
        source_table: str,
        source_record_id: str | None,
        raw_site_name: str | None,
        raw_reference_value: str | None,
        normalised_reference_value: str | None,
        planning_reference: str | None,
        title_number: str | None,
        uprn: str | None,
        usrn: str | None,
        toid: str | None,
        authority_name: str | None,
        settlement_name: str | None,
        relation_type: str,
        confidence_score: float | None,
        status: str,
        geometry_overlap_ratio: float | None,
        geometry_distance_m: float | None,
        match_notes: str | None,
        metadata_json: str,
    ) -> None:
        self.database.execute(
            """
            insert into public.site_reconciliation_matches (
                site_id,
                source_dataset,
                source_table,
                source_record_id,
                raw_site_name,
                raw_reference_value,
                normalised_reference_value,
                planning_reference,
                title_number,
                uprn,
                usrn,
                toid,
                authority_name,
                settlement_name,
                relation_type,
                confidence_score,
                status,
                geometry_overlap_ratio,
                geometry_distance_m,
                match_notes,
                metadata
            )
            values (
                cast(:site_id as uuid),
                :source_dataset,
                :source_table,
                :source_record_id,
                :raw_site_name,
                :raw_reference_value,
                :normalised_reference_value,
                :planning_reference,
                :title_number,
                :uprn,
                :usrn,
                :toid,
                :authority_name,
                :settlement_name,
                :relation_type,
                :confidence_score,
                :status,
                :geometry_overlap_ratio,
                :geometry_distance_m,
                :match_notes,
                cast(:metadata_json as jsonb)
            )
            on conflict do nothing
            """,
            {
                "site_id": site_id,
                "source_dataset": source_dataset,
                "source_table": source_table,
                "source_record_id": source_record_id,
                "raw_site_name": raw_site_name,
                "raw_reference_value": raw_reference_value,
                "normalised_reference_value": normalised_reference_value,
                "planning_reference": planning_reference,
                "title_number": title_number,
                "uprn": uprn,
                "usrn": usrn,
                "toid": toid,
                "authority_name": authority_name,
                "settlement_name": settlement_name,
                "relation_type": relation_type,
                "confidence_score": confidence_score,
                "status": status,
                "geometry_overlap_ratio": geometry_overlap_ratio,
                "geometry_distance_m": geometry_distance_m,
                "match_notes": match_notes,
                "metadata_json": metadata_json,
            },
        )

    def enqueue_reconciliation_review(
        self,
        *,
        candidate_site_id: str | None,
        source_dataset: str,
        source_table: str,
        source_record_id: str | None,
        raw_site_name: str | None,
        raw_reference_value: str | None,
        normalised_reference_value: str | None,
        planning_reference: str | None,
        authority_name: str | None,
        settlement_name: str | None,
        confidence_score: float | None,
        failure_reasons_json: str,
        candidate_matches_json: str,
        metadata_json: str,
    ) -> None:
        self.database.execute(
            """
            insert into public.site_reconciliation_review_queue (
                candidate_site_id,
                source_dataset,
                source_table,
                source_record_id,
                raw_site_name,
                raw_reference_value,
                normalised_reference_value,
                planning_reference,
                authority_name,
                settlement_name,
                confidence_score,
                failure_reasons,
                candidate_matches,
                metadata
            )
            values (
                cast(:candidate_site_id as uuid),
                :source_dataset,
                :source_table,
                :source_record_id,
                :raw_site_name,
                :raw_reference_value,
                :normalised_reference_value,
                :planning_reference,
                :authority_name,
                :settlement_name,
                :confidence_score,
                cast(:failure_reasons_json as jsonb),
                cast(:candidate_matches_json as jsonb),
                cast(:metadata_json as jsonb)
            )
            on conflict do nothing
            """,
            {
                "candidate_site_id": candidate_site_id,
                "source_dataset": source_dataset,
                "source_table": source_table,
                "source_record_id": source_record_id,
                "raw_site_name": raw_site_name,
                "raw_reference_value": raw_reference_value,
                "normalised_reference_value": normalised_reference_value,
                "planning_reference": planning_reference,
                "authority_name": authority_name,
                "settlement_name": settlement_name,
                "confidence_score": confidence_score,
                "failure_reasons_json": failure_reasons_json,
                "candidate_matches_json": candidate_matches_json,
                "metadata_json": metadata_json,
            },
        )

    def insert_site_parcel(
        self,
        *,
        site_id: str,
        ros_parcel_id: str,
        land_object_id: str | None,
        title_number: str | None,
        parcel_reference: str | None,
        is_primary: bool,
        source_record_id: str,
    ) -> None:
        self.database.execute(
            """
            insert into public.site_parcels (
                site_id,
                land_object_id,
                ros_parcel_id,
                title_number,
                parcel_reference,
                source_dataset,
                source_record_id,
                source_url,
                is_primary,
                metadata
            )
            values (
                cast(:site_id as uuid),
                cast(:land_object_id as uuid),
                cast(:ros_parcel_id as uuid),
                :title_number,
                :parcel_reference,
                'mvp_seed.site_parcel',
                :source_record_id,
                :source_url,
                :is_primary,
                '{}'::jsonb
            )
            """,
            {
                "site_id": site_id,
                "land_object_id": land_object_id,
                "ros_parcel_id": ros_parcel_id,
                "title_number": title_number,
                "parcel_reference": parcel_reference,
                "source_record_id": source_record_id,
                "source_url": f"internal://mvp-seed/{source_record_id}/parcel",
                "is_primary": is_primary,
            },
        )

    def insert_site_geometry_component(
        self,
        *,
        site_id: str,
        ros_parcel_id: str,
        source_identifier: str | None,
        is_primary: bool,
    ) -> None:
        self.database.execute(
            """
            insert into public.site_geometry_components (
                site_id,
                component_role,
                source_table,
                source_record_id,
                source_identifier,
                source_dataset,
                relation_type,
                is_primary,
                source_url,
                metadata
            )
            values (
                cast(:site_id as uuid),
                'parcel_boundary',
                'public.ros_cadastral_parcels',
                :source_record_id,
                :source_identifier,
                'ros_cadastral_parcels',
                'explicit_identifier',
                :is_primary,
                :source_url,
                '{}'::jsonb
            )
            """,
            {
                "site_id": site_id,
                "source_record_id": ros_parcel_id,
                "source_identifier": source_identifier,
                "is_primary": is_primary,
                "source_url": f"internal://site-components/ros-parcel/{ros_parcel_id}",
            },
        )

    def upsert_site_location_from_parcels(
        self,
        *,
        site_id: str,
        authority_name: str,
        nearest_settlement: str,
        settlement_relationship: str,
        within_settlement_boundary: bool | None,
        distance_to_settlement_boundary_m: float | None,
        source_record_id: str,
    ) -> None:
        self.database.execute(
            """
            insert into public.site_locations (
                site_id,
                authority_name,
                nearest_settlement,
                settlement_relationship,
                within_settlement_boundary,
                distance_to_settlement_boundary_m,
                source_dataset,
                source_record_id,
                source_url,
                centroid,
                geometry,
                metadata
            )
            select
                cast(:site_id as uuid),
                :authority_name,
                :nearest_settlement,
                :settlement_relationship,
                :within_settlement_boundary,
                :distance_to_settlement_boundary_m,
                'mvp_seed.site_location',
                :source_record_id,
                :source_url,
                ST_PointOnSurface(ST_UnaryUnion(ST_Collect(rp.geometry))),
                ST_UnaryUnion(ST_Collect(rp.geometry)),
                '{}'::jsonb
            from public.site_parcels as sp
            join public.ros_cadastral_parcels as rp
                on rp.id = sp.ros_parcel_id
            where sp.site_id = cast(:site_id as uuid)
            on conflict (site_id) do update set
                authority_name = excluded.authority_name,
                nearest_settlement = excluded.nearest_settlement,
                settlement_relationship = excluded.settlement_relationship,
                within_settlement_boundary = excluded.within_settlement_boundary,
                distance_to_settlement_boundary_m = excluded.distance_to_settlement_boundary_m,
                source_dataset = excluded.source_dataset,
                source_record_id = excluded.source_record_id,
                source_url = excluded.source_url,
                centroid = excluded.centroid,
                geometry = excluded.geometry,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            {
                "site_id": site_id,
                "authority_name": authority_name,
                "nearest_settlement": nearest_settlement,
                "settlement_relationship": settlement_relationship,
                "within_settlement_boundary": within_settlement_boundary,
                "distance_to_settlement_boundary_m": distance_to_settlement_boundary_m,
                "source_record_id": source_record_id,
                "source_url": f"internal://mvp-seed/{source_record_id}/site-location",
            },
        )

    def insert_planning_records(self, site_id: str, rows: Iterable[dict[str, Any]]) -> None:
        payload = [{"site_id": site_id, **row, "raw_payload": json.dumps(row.get("raw_payload", {}))} for row in rows]
        self.database.execute_many(
            """
            insert into public.planning_records (
                site_id,
                record_type,
                application_reference,
                application_outcome,
                application_status,
                decision_date,
                description,
                source_dataset,
                source_record_id,
                source_url,
                import_version,
                raw_payload
            )
            values (
                cast(:site_id as uuid),
                :record_type,
                :application_reference,
                :application_outcome,
                :application_status,
                :decision_date,
                :description,
                :source_dataset,
                :source_record_id,
                :source_url,
                :import_version,
                cast(:raw_payload as jsonb)
            )
            """,
            payload,
        )

    def insert_planning_context_records(self, site_id: str, rows: Iterable[dict[str, Any]]) -> None:
        payload = [{"site_id": site_id, **row, "raw_payload": json.dumps(row.get("raw_payload", {}))} for row in rows]
        self.database.execute_many(
            """
            insert into public.planning_context_records (
                site_id,
                context_type,
                context_status,
                context_label,
                distance_m,
                description,
                source_dataset,
                source_record_id,
                source_url,
                import_version,
                raw_payload
            )
            values (
                cast(:site_id as uuid),
                :context_type,
                :context_status,
                :context_label,
                :distance_m,
                :description,
                :source_dataset,
                :source_record_id,
                :source_url,
                :import_version,
                cast(:raw_payload as jsonb)
            )
            """,
            payload,
        )

    def insert_constraint_records(self, site_id: str, rows: Iterable[dict[str, Any]]) -> None:
        payload = [{"site_id": site_id, **row, "raw_payload": json.dumps(row.get("raw_payload", {}))} for row in rows]
        self.database.execute_many(
            """
            insert into public.site_constraints (
                site_id,
                constraint_type,
                severity,
                status,
                distance_m,
                description,
                source_dataset,
                source_record_id,
                source_url,
                import_version,
                raw_payload
            )
            values (
                cast(:site_id as uuid),
                :constraint_type,
                :severity,
                :status,
                :distance_m,
                :description,
                :source_dataset,
                :source_record_id,
                :source_url,
                :import_version,
                cast(:raw_payload as jsonb)
            )
            """,
            payload,
        )

    def insert_infrastructure_records(self, site_id: str, rows: Iterable[dict[str, Any]]) -> None:
        payload = [{"site_id": site_id, **row, "raw_payload": json.dumps(row.get("raw_payload", {}))} for row in rows]
        self.database.execute_many(
            """
            insert into public.site_infrastructure_records (
                site_id,
                infrastructure_type,
                burden_level,
                status,
                description,
                source_dataset,
                source_record_id,
                source_url,
                import_version,
                raw_payload
            )
            values (
                cast(:site_id as uuid),
                :infrastructure_type,
                :burden_level,
                :status,
                :description,
                :source_dataset,
                :source_record_id,
                :source_url,
                :import_version,
                cast(:raw_payload as jsonb)
            )
            """,
            payload,
        )

    def insert_control_records(self, site_id: str, rows: Iterable[dict[str, Any]]) -> None:
        payload = [{"site_id": site_id, **row, "raw_payload": json.dumps(row.get("raw_payload", {}))} for row in rows]
        self.database.execute_many(
            """
            insert into public.site_control_records (
                site_id,
                control_type,
                control_level,
                status,
                description,
                source_dataset,
                source_record_id,
                source_url,
                import_version,
                raw_payload
            )
            values (
                cast(:site_id as uuid),
                :control_type,
                :control_level,
                :status,
                :description,
                :source_dataset,
                :source_record_id,
                :source_url,
                :import_version,
                cast(:raw_payload as jsonb)
            )
            """,
            payload,
        )

    def insert_market_records(self, site_id: str, rows: Iterable[dict[str, Any]]) -> None:
        payload = [{"site_id": site_id, **row, "raw_payload": json.dumps(row.get("raw_payload", {}))} for row in rows]
        self.database.execute_many(
            """
            insert into public.comparable_market_records (
                site_id,
                comparable_type,
                address,
                transaction_type,
                price_gbp,
                price_per_sqft_gbp,
                sale_date,
                distance_m,
                record_strength,
                description,
                source_dataset,
                source_record_id,
                source_url,
                import_version,
                raw_payload
            )
            values (
                cast(:site_id as uuid),
                :comparable_type,
                :address,
                :transaction_type,
                :price_gbp,
                :price_per_sqft_gbp,
                :sale_date,
                :distance_m,
                :record_strength,
                :description,
                :source_dataset,
                :source_record_id,
                :source_url,
                :import_version,
                cast(:raw_payload as jsonb)
            )
            """,
            payload,
        )

    def insert_buyer_matches(
        self,
        site_id: str,
        rows: Iterable[dict[str, Any]],
        profile_ids: dict[str, str],
    ) -> None:
        payload = []
        for row in rows:
            profile_id = profile_ids[str(row["profile_code"])]
            payload.append(
                {
                    "site_id": site_id,
                    "buyer_profile_id": profile_id,
                    "fit_rating": row["fit_rating"],
                    "match_reason": row.get("match_reason"),
                    "evidence_summary": row.get("evidence_summary"),
                    "metadata": "{}",
                }
            )
        if not payload:
            return
        self.database.execute_many(
            """
            insert into public.site_buyer_matches (
                site_id,
                buyer_profile_id,
                fit_rating,
                match_reason,
                evidence_summary,
                metadata
            )
            values (
                cast(:site_id as uuid),
                cast(:buyer_profile_id as uuid),
                :fit_rating,
                :match_reason,
                :evidence_summary,
                cast(:metadata as jsonb)
            )
            """,
            payload,
        )

    def record_status_history(self, site_id: str, workflow_status: str, note: str) -> None:
        self.database.execute(
            """
            insert into public.site_review_status_history (site_id, workflow_status, note)
            values (cast(:site_id as uuid), :workflow_status, :note)
            """,
            {"site_id": site_id, "workflow_status": workflow_status, "note": note},
        )

    def enqueue_site_refresh(
        self,
        *,
        site_id: str,
        trigger_source: str,
        source_table: str,
        source_record_id: str,
        metadata_json: str,
        refresh_scope: str = "signals_and_interpretations",
    ) -> None:
        self.database.execute(
            """
            insert into public.site_refresh_queue (
                site_id,
                trigger_source,
                source_table,
                source_record_id,
                refresh_scope,
                status,
                metadata
            )
            select
                cast(:site_id as uuid),
                :trigger_source,
                :source_table,
                :source_record_id,
                :refresh_scope,
                'pending',
                cast(:metadata_json as jsonb)
            where not exists (
                select 1
                from public.site_refresh_queue
                where site_id = cast(:site_id as uuid)
                  and refresh_scope = :refresh_scope
                  and status in ('pending', 'processing')
            )
            """,
            {
                "site_id": site_id,
                "trigger_source": trigger_source,
                "source_table": source_table,
                "source_record_id": source_record_id,
                "refresh_scope": refresh_scope,
                "metadata_json": metadata_json,
            },
        )

    def fetch_pending_refresh_requests(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            select *
            from public.site_refresh_queue
            where status = 'pending'
            order by requested_at asc
            limit :limit
            """,
            {"limit": limit},
        )

    def update_refresh_request_status(
        self,
        request_id: str,
        *,
        status: str,
        error_message: str | None = None,
    ) -> None:
        self.database.execute(
            """
            update public.site_refresh_queue
            set
                status = :status,
                error_message = :error_message,
                processed_at = case when :status in ('completed', 'failed') then now() else processed_at end
            where id = cast(:request_id as uuid)
            """,
            {
                "request_id": request_id,
                "status": status,
                "error_message": error_message,
            },
        )

    def fetch_site_snapshot(self, site_id: str) -> SiteSnapshot:
        site = self.database.fetch_one(
            "select * from public.sites where id = cast(:site_id as uuid)",
            {"site_id": site_id},
        )
        if not site:
            raise ValueError(f"Unknown site id: {site_id}")
        location = self.database.fetch_one(
            "select * from public.site_locations where site_id = cast(:site_id as uuid)",
            {"site_id": site_id},
        )
        parcels = self.database.fetch_all(
            """
            select
                sp.*,
                rp.ros_inspire_id,
                rp.authority_name,
                coalesce(lo.area_acres, rp.area_acres) as area_acres,
                coalesce(lo.area_ha, rp.area_ha) as area_ha
            from public.site_parcels as sp
            left join public.ros_cadastral_parcels as rp on rp.id = sp.ros_parcel_id
            left join public.land_objects as lo on lo.id = sp.land_object_id
            where sp.site_id = cast(:site_id as uuid)
            order by sp.is_primary desc, sp.created_at asc
            """,
            {"site_id": site_id},
        )
        geometry_components = self.database.fetch_all(
            """
            select *
            from public.site_geometry_components
            where site_id = cast(:site_id as uuid)
            order by is_primary desc, created_at asc
            """,
            {"site_id": site_id},
        )
        geometry_versions = self.database.fetch_all(
            """
            select *
            from public.site_geometry_versions
            where site_id = cast(:site_id as uuid)
            order by created_at desc
            """,
            {"site_id": site_id},
        )
        reference_aliases = self.database.fetch_all(
            """
            select *
            from public.site_reference_aliases
            where site_id = cast(:site_id as uuid)
            order by reference_family, raw_reference_value
            """,
            {"site_id": site_id},
        )
        reconciliation_matches = self.database.fetch_all(
            """
            select *
            from public.site_reconciliation_matches
            where site_id = cast(:site_id as uuid)
            order by created_at desc
            """,
            {"site_id": site_id},
        )
        reconciliation_review_items = self.database.fetch_all(
            """
            select *
            from public.site_reconciliation_review_queue
            where candidate_site_id = cast(:site_id as uuid)
            order by created_at desc
            """,
            {"site_id": site_id},
        )
        planning_records = self.database.fetch_all(
            """
            select *
            from public.planning_records
            where site_id = cast(:site_id as uuid)
            order by decision_date desc nulls last, created_at desc
            """,
            {"site_id": site_id},
        )
        planning_context = self.database.fetch_all(
            """
            select *
            from public.planning_context_records
            where site_id = cast(:site_id as uuid)
            order by created_at desc
            """,
            {"site_id": site_id},
        )
        constraints = self.database.fetch_all(
            """
            select *
            from public.site_constraints
            where site_id = cast(:site_id as uuid)
            order by created_at desc
            """,
            {"site_id": site_id},
        )
        infrastructure_records = self.database.fetch_all(
            """
            select *
            from public.site_infrastructure_records
            where site_id = cast(:site_id as uuid)
            order by created_at desc
            """,
            {"site_id": site_id},
        )
        control_records = self.database.fetch_all(
            """
            select *
            from public.site_control_records
            where site_id = cast(:site_id as uuid)
            order by created_at desc
            """,
            {"site_id": site_id},
        )
        comparables = self.database.fetch_all(
            """
            select *
            from public.comparable_market_records
            where site_id = cast(:site_id as uuid)
            order by sale_date desc nulls last, created_at desc
            """,
            {"site_id": site_id},
        )
        buyer_matches = self.database.fetch_all(
            """
            select
                sbm.*,
                bp.profile_code,
                bp.buyer_name,
                bp.target_strategy
            from public.site_buyer_matches as sbm
            join public.buyer_profiles as bp on bp.id = sbm.buyer_profile_id
            where sbm.site_id = cast(:site_id as uuid)
            order by sbm.created_at desc
            """,
            {"site_id": site_id},
        )
        return SiteSnapshot(
            site=site,
            location=location,
            parcels=parcels,
            geometry_components=geometry_components,
            geometry_versions=geometry_versions,
            reference_aliases=reference_aliases,
            reconciliation_matches=reconciliation_matches,
            reconciliation_review_items=reconciliation_review_items,
            planning_records=planning_records,
            planning_context_records=planning_context,
            constraints=constraints,
            infrastructure_records=infrastructure_records,
            control_records=control_records,
            comparable_market_records=comparables,
            buyer_matches=buyer_matches,
        )

    def create_analysis_run(self, site_id: str, *, ruleset_version: str, triggered_by: str, metadata_json: str) -> str:
        return str(
            self.database.scalar(
                """
                insert into public.site_analysis_runs (
                    site_id,
                    run_type,
                    ruleset_version,
                    status,
                    triggered_by,
                    metadata
                )
                values (
                    cast(:site_id as uuid),
                    'rule_engine',
                    :ruleset_version,
                    'running',
                    :triggered_by,
                    cast(:metadata_json as jsonb)
                )
                returning id
                """,
                {
                    "site_id": site_id,
                    "ruleset_version": ruleset_version,
                    "triggered_by": triggered_by,
                    "metadata_json": metadata_json,
                },
            )
        )

    def complete_analysis_run(self, run_id: str, metadata_json: str) -> None:
        self.database.execute(
            """
            update public.site_analysis_runs
            set
                status = 'completed',
                metadata = cast(:metadata_json as jsonb),
                completed_at = now()
            where id = cast(:run_id as uuid)
            """,
            {"run_id": run_id, "metadata_json": metadata_json},
        )

    def fail_analysis_run(self, run_id: str, error_message: str) -> None:
        self.database.execute(
            """
            update public.site_analysis_runs
            set
                status = 'failed',
                metadata = jsonb_build_object('error_message', :error_message),
                completed_at = now()
            where id = cast(:run_id as uuid)
            """,
            {"run_id": run_id, "error_message": error_message},
        )

    def insert_signal(
        self,
        *,
        site_id: str,
        run_id: str,
        signal_key: str,
        signal_label: str,
        signal_group: str,
        value_type: str,
        signal_state: str,
        bool_value: bool | None,
        numeric_value: float | None,
        text_value: str | None,
        json_value: str | None,
        reasoning: str,
    ) -> str:
        return str(
            self.database.scalar(
                """
                insert into public.site_signals (
                    analysis_run_id,
                    site_id,
                    signal_key,
                    signal_label,
                    signal_group,
                    value_type,
                    signal_state,
                    bool_value,
                    numeric_value,
                    text_value,
                    json_value,
                    reasoning
                )
                values (
                    cast(:run_id as uuid),
                    cast(:site_id as uuid),
                    :signal_key,
                    :signal_label,
                    :signal_group,
                    :value_type,
                    :signal_state,
                    :bool_value,
                    :numeric_value,
                    :text_value,
                    cast(:json_value as jsonb),
                    :reasoning
                )
                returning id
                """,
                {
                    "run_id": run_id,
                    "site_id": site_id,
                    "signal_key": signal_key,
                    "signal_label": signal_label,
                    "signal_group": signal_group,
                    "value_type": value_type,
                    "signal_state": signal_state,
                    "bool_value": bool_value,
                    "numeric_value": numeric_value,
                    "text_value": text_value,
                    "json_value": json_value,
                    "reasoning": reasoning,
                },
            )
        )

    def insert_evidence_reference(self, site_id: str, row: dict[str, Any]) -> str:
        return str(
            self.database.scalar(
                """
                insert into public.evidence_references (
                    site_id,
                    source_table,
                    source_record_id,
                    dataset_name,
                    source_identifier,
                    source_url,
                    observed_at,
                    import_version,
                    confidence_label,
                    confidence_score,
                    assertion,
                    excerpt,
                    metadata
                )
                values (
                    cast(:site_id as uuid),
                    :source_table,
                    :source_record_id,
                    :dataset_name,
                    :source_identifier,
                    :source_url,
                    :observed_at,
                    :import_version,
                    :confidence_label,
                    :confidence_score,
                    :assertion,
                    :excerpt,
                    cast(:metadata as jsonb)
                )
                returning id
                """,
                {
                    "site_id": site_id,
                    "source_table": row.get("source_table"),
                    "source_record_id": row.get("source_record_id"),
                    "dataset_name": row.get("dataset_name"),
                    "source_identifier": row.get("source_identifier"),
                    "source_url": row.get("source_url"),
                    "observed_at": row.get("observed_at"),
                    "import_version": row.get("import_version"),
                    "confidence_label": row.get("confidence_label"),
                    "confidence_score": row.get("confidence_score"),
                    "assertion": row.get("assertion"),
                    "excerpt": row.get("excerpt"),
                    "metadata": row.get("metadata", "{}"),
                },
            )
        )

    def link_signal_evidence(self, signal_id: str, evidence_reference_id: str) -> None:
        self.database.execute(
            """
            insert into public.site_signal_evidence (signal_id, evidence_reference_id)
            values (cast(:signal_id as uuid), cast(:evidence_reference_id as uuid))
            on conflict do nothing
            """,
            {"signal_id": signal_id, "evidence_reference_id": evidence_reference_id},
        )

    def insert_interpretation(
        self,
        *,
        site_id: str,
        run_id: str,
        interpretation_key: str,
        category: str,
        title: str,
        summary: str,
        reasoning: str,
        rule_code: str,
        priority: int,
    ) -> str:
        return str(
            self.database.scalar(
                """
                insert into public.site_interpretations (
                    analysis_run_id,
                    site_id,
                    interpretation_key,
                    category,
                    title,
                    summary,
                    reasoning,
                    rule_code,
                    priority
                )
                values (
                    cast(:run_id as uuid),
                    cast(:site_id as uuid),
                    :interpretation_key,
                    :category,
                    :title,
                    :summary,
                    :reasoning,
                    :rule_code,
                    :priority
                )
                returning id
                """,
                {
                    "run_id": run_id,
                    "site_id": site_id,
                    "interpretation_key": interpretation_key,
                    "category": category,
                    "title": title,
                    "summary": summary,
                    "reasoning": reasoning,
                    "rule_code": rule_code,
                    "priority": priority,
                },
            )
        )

    def link_interpretation_evidence(self, interpretation_id: str, evidence_reference_id: str) -> None:
        self.database.execute(
            """
            insert into public.site_interpretation_evidence (interpretation_id, evidence_reference_id)
            values (cast(:interpretation_id as uuid), cast(:evidence_reference_id as uuid))
            on conflict do nothing
            """,
            {
                "interpretation_id": interpretation_id,
                "evidence_reference_id": evidence_reference_id,
            },
        )

    def insert_assessment(
        self,
        *,
        site_id: str,
        run_id: str,
        jurisdiction: str,
        assessment_version: str,
        bucket_code: str,
        bucket_label: str,
        likely_opportunity_type: str,
        monetisation_horizon: str,
        horizon_year_band: str,
        dominant_blocker: str,
        blocker_themes_json: str,
        primary_reason: str,
        secondary_reasons_json: str,
        buyer_profile_guess: str | None,
        likely_buyer_profiles_json: str,
        cost_to_control_band: str,
        human_review_required: bool,
        hard_fail_flags_json: str,
        review_flags_json: str,
        explanation_text: str,
        metadata_json: str,
    ) -> str:
        return str(
            self.database.scalar(
                """
                insert into public.site_assessments (
                    analysis_run_id,
                    site_id,
                    jurisdiction,
                    assessment_version,
                    bucket_code,
                    bucket_label,
                    likely_opportunity_type,
                    monetisation_horizon,
                    horizon_year_band,
                    dominant_blocker,
                    blocker_themes,
                    primary_reason,
                    secondary_reasons,
                    buyer_profile_guess,
                    likely_buyer_profiles,
                    cost_to_control_band,
                    human_review_required,
                    hard_fail_flags,
                    review_flags,
                    explanation_text,
                    metadata
                )
                values (
                    cast(:run_id as uuid),
                    cast(:site_id as uuid),
                    :jurisdiction,
                    :assessment_version,
                    :bucket_code,
                    :bucket_label,
                    :likely_opportunity_type,
                    :monetisation_horizon,
                    :horizon_year_band,
                    :dominant_blocker,
                    cast(:blocker_themes_json as jsonb),
                    :primary_reason,
                    cast(:secondary_reasons_json as jsonb),
                    :buyer_profile_guess,
                    cast(:likely_buyer_profiles_json as jsonb),
                    :cost_to_control_band,
                    :human_review_required,
                    cast(:hard_fail_flags_json as jsonb),
                    cast(:review_flags_json as jsonb),
                    :explanation_text,
                    cast(:metadata_json as jsonb)
                )
                returning id
                """,
                {
                    "run_id": run_id,
                    "site_id": site_id,
                    "jurisdiction": jurisdiction,
                    "assessment_version": assessment_version,
                    "bucket_code": bucket_code,
                    "bucket_label": bucket_label,
                    "likely_opportunity_type": likely_opportunity_type,
                    "monetisation_horizon": monetisation_horizon,
                    "horizon_year_band": horizon_year_band,
                    "dominant_blocker": dominant_blocker,
                    "blocker_themes_json": blocker_themes_json,
                    "primary_reason": primary_reason,
                    "secondary_reasons_json": secondary_reasons_json,
                    "buyer_profile_guess": buyer_profile_guess,
                    "likely_buyer_profiles_json": likely_buyer_profiles_json,
                    "cost_to_control_band": cost_to_control_band,
                    "human_review_required": human_review_required,
                    "hard_fail_flags_json": hard_fail_flags_json,
                    "review_flags_json": review_flags_json,
                    "explanation_text": explanation_text,
                    "metadata_json": metadata_json,
                },
            )
        )

    def insert_assessment_score(
        self,
        *,
        assessment_id: str,
        site_id: str,
        score_code: str,
        score_label: str,
        score_value: int,
        confidence_label: str,
        score_summary: str,
        score_reasoning: str,
        blocker_theme: str | None,
        metadata_json: str,
    ) -> str:
        return str(
            self.database.scalar(
                """
                insert into public.site_assessment_scores (
                    site_assessment_id,
                    site_id,
                    score_code,
                    score_label,
                    score_value,
                    confidence_label,
                    score_summary,
                    score_reasoning,
                    blocker_theme,
                    metadata
                )
                values (
                    cast(:assessment_id as uuid),
                    cast(:site_id as uuid),
                    :score_code,
                    :score_label,
                    :score_value,
                    :confidence_label,
                    :score_summary,
                    :score_reasoning,
                    :blocker_theme,
                    cast(:metadata_json as jsonb)
                )
                returning id
                """,
                {
                    "assessment_id": assessment_id,
                    "site_id": site_id,
                    "score_code": score_code,
                    "score_label": score_label,
                    "score_value": score_value,
                    "confidence_label": confidence_label,
                    "score_summary": score_summary,
                    "score_reasoning": score_reasoning,
                    "blocker_theme": blocker_theme,
                    "metadata_json": metadata_json,
                },
            )
        )

    def link_assessment_evidence(self, assessment_id: str, evidence_reference_id: str) -> None:
        self.database.execute(
            """
            insert into public.site_assessment_evidence (site_assessment_id, evidence_reference_id)
            values (cast(:assessment_id as uuid), cast(:evidence_reference_id as uuid))
            on conflict do nothing
            """,
            {"assessment_id": assessment_id, "evidence_reference_id": evidence_reference_id},
        )

    def link_assessment_score_evidence(self, assessment_score_id: str, evidence_reference_id: str) -> None:
        self.database.execute(
            """
            insert into public.site_assessment_score_evidence (site_assessment_score_id, evidence_reference_id)
            values (cast(:assessment_score_id as uuid), cast(:evidence_reference_id as uuid))
            on conflict do nothing
            """,
            {"assessment_score_id": assessment_score_id, "evidence_reference_id": evidence_reference_id},
        )

    def update_site_surfaced_reason(self, site_id: str, surfaced_reason: str) -> None:
        self.database.execute(
            """
            update public.sites
            set surfaced_reason = :surfaced_reason, updated_at = now()
            where id = cast(:site_id as uuid)
            """,
            {"site_id": site_id, "surfaced_reason": surfaced_reason},
        )

    def upsert_site_search_cache_row(self, site_id: str) -> None:
        self.database.execute(
            "select analytics.upsert_site_search_cache_row(cast(:site_id as uuid));",
            {"site_id": site_id},
        )

    def resolve_site_ids(self, site_ids: list[str] | None, site_codes: list[str] | None) -> list[str]:
        resolved: list[str] = []
        if site_ids:
            resolved.extend(site_ids)
        if site_codes:
            params: dict[str, Any] = {}
            site_code_clause = _build_in_clause("site_code", "site_code", site_codes, params)
            rows = self.database.fetch_all(
                f"""
                select id::text as id
                from public.sites
                where {site_code_clause}
                """,
                params,
            )
            resolved.extend(str(row["id"]) for row in rows)
        return list(dict.fromkeys(resolved))

    def fetch_reconciliation_index(self, authority_name: str | None = None) -> list[dict[str, Any]]:
        sql = "select * from analytics.v_site_reference_index where 1 = 1"
        params: dict[str, Any] = {}
        if authority_name:
            sql += " and authority_name = :authority_name"
            params["authority_name"] = authority_name
        sql += " order by site_code"
        return self.database.fetch_all(sql, params)

    def search_sites(self, filters: SiteSearchFilters) -> list[dict[str, Any]]:
        sql = "select * from analytics.v_site_search_summary where 1 = 1"
        params: dict[str, Any] = {"limit": filters.limit}

        if filters.query:
            params["query"] = f"%{filters.query.strip()}%"
            sql += """
                and (
                    site_name ilike :query
                    or site_code ilike :query
                    or coalesce(nearest_settlement, '') ilike :query
                    or coalesce(authority_name, '') ilike :query
                )
            """
        if filters.authority_name:
            sql += " and authority_name = :authority_name"
            params["authority_name"] = filters.authority_name
        if filters.workflow_status:
            sql += " and workflow_status = :workflow_status"
            params["workflow_status"] = filters.workflow_status
        if filters.min_area_acres is not None:
            sql += " and coalesce(area_acres, 0) >= :min_area_acres"
            params["min_area_acres"] = filters.min_area_acres
        if filters.max_area_acres is not None:
            sql += " and coalesce(area_acres, 0) <= :max_area_acres"
            params["max_area_acres"] = filters.max_area_acres
        if filters.bucket_code:
            sql += " and opportunity_bucket = :bucket_code"
            params["bucket_code"] = filters.bucket_code
        if filters.monetisation_horizon:
            sql += " and monetisation_horizon = :monetisation_horizon"
            params["monetisation_horizon"] = filters.monetisation_horizon
        if filters.previous_application_exists is not None:
            sql += " and coalesce(previous_application_exists, false) = :previous_application_exists"
            params["previous_application_exists"] = filters.previous_application_exists
        if filters.allocation_status:
            sql += " and coalesce(allocation_status, 'unknown') = :allocation_status"
            params["allocation_status"] = filters.allocation_status
        if filters.flood_risk:
            sql += " and coalesce(flood_risk, 'unknown') = :flood_risk"
            params["flood_risk"] = filters.flood_risk
        if filters.access_status:
            sql += " and coalesce(access_status, 'unknown') = :access_status"
            params["access_status"] = filters.access_status
        if filters.comparable_strength:
            sql += " and coalesce(new_build_comparable_strength, 'unknown') = :comparable_strength"
            params["comparable_strength"] = filters.comparable_strength
        if filters.min_buyer_fit_count is not None:
            sql += " and coalesce(buyer_fit_count, 0) >= :min_buyer_fit_count"
            params["min_buyer_fit_count"] = filters.min_buyer_fit_count
        if filters.human_review_required is not None:
            sql += " and coalesce(human_review_required, false) = :human_review_required"
            params["human_review_required"] = filters.human_review_required

        sql += """
            order by
                case coalesce(opportunity_bucket, 'Z')
                    when 'C' then 1
                    when 'B' then 2
                    when 'A' then 3
                    when 'D' then 4
                    when 'E' then 5
                    when 'F' then 6
                    else 7
                end,
                coalesce(possible_fatal_count, 0) asc,
                coalesce(positive_count, 0) desc,
                coalesce(buyer_fit_count, 0) desc,
                updated_at desc
            limit :limit
        """
        return self.database.fetch_all(sql, params)

    def fetch_filter_options(self) -> dict[str, list[str]]:
        return {
            "authorities": [
                row["authority_name"]
                for row in self.database.fetch_all(
                    """
                    select distinct authority_name
                    from analytics.v_site_search_summary
                    where authority_name is not null
                    order by authority_name
                    """
                )
            ],
            "workflow_statuses": [
                row["workflow_status"]
                for row in self.database.fetch_all(
                    """
                    select distinct workflow_status
                    from public.sites
                    order by workflow_status
                    """
                )
            ],
            "bucket_codes": [
                row["opportunity_bucket"]
                for row in self.database.fetch_all(
                    """
                    select distinct opportunity_bucket
                    from analytics.v_site_search_summary
                    where opportunity_bucket is not null
                    order by opportunity_bucket
                    """
                )
            ],
            "horizons": [
                row["monetisation_horizon"]
                for row in self.database.fetch_all(
                    """
                    select distinct monetisation_horizon
                    from analytics.v_site_search_summary
                    where monetisation_horizon is not null
                    order by monetisation_horizon
                    """
                )
            ],
        }

    def fetch_site_detail(self, site_id: str) -> dict[str, Any] | None:
        summary = self.database.fetch_one(
            "select * from analytics.v_site_fact_summary where site_id = cast(:site_id as uuid)",
            {"site_id": site_id},
        )
        if not summary:
            return None
        return {
            "summary": summary,
            "canonical_site": self.database.fetch_one(
                """
                select *
                from analytics.v_canonical_sites
                where site_id = cast(:site_id as uuid)
                """,
                {"site_id": site_id},
            ),
            "parcels": self.database.fetch_all(
                """
                select
                    sp.*,
                    rp.ros_inspire_id,
                    rp.authority_name,
                    coalesce(lo.area_acres, rp.area_acres) as area_acres,
                    coalesce(lo.area_ha, rp.area_ha) as area_ha
                from public.site_parcels as sp
                left join public.ros_cadastral_parcels as rp on rp.id = sp.ros_parcel_id
                left join public.land_objects as lo on lo.id = sp.land_object_id
                where sp.site_id = cast(:site_id as uuid)
                order by sp.is_primary desc, sp.created_at asc
                """,
                {"site_id": site_id},
            ),
            "geometry_components": self.database.fetch_all(
                """
                select *
                from public.site_geometry_components
                where site_id = cast(:site_id as uuid)
                order by is_primary desc, created_at asc
                """,
                {"site_id": site_id},
            ),
            "geometry_versions": self.database.fetch_all(
                """
                select *
                from public.site_geometry_versions
                where site_id = cast(:site_id as uuid)
                order by created_at desc
                """,
                {"site_id": site_id},
            ),
            "reference_aliases": self.database.fetch_all(
                """
                select *
                from public.site_reference_aliases
                where site_id = cast(:site_id as uuid)
                order by reference_family, raw_reference_value
                """,
                {"site_id": site_id},
            ),
            "reconciliation_matches": self.database.fetch_all(
                """
                select *
                from public.site_reconciliation_matches
                where site_id = cast(:site_id as uuid)
                order by created_at desc
                """,
                {"site_id": site_id},
            ),
            "reconciliation_review_items": self.database.fetch_all(
                """
                select *
                from public.site_reconciliation_review_queue
                where candidate_site_id = cast(:site_id as uuid)
                order by created_at desc
                """,
                {"site_id": site_id},
            ),
            "planning_records": self.database.fetch_all(
                """
                select *
                from public.planning_records
                where site_id = cast(:site_id as uuid)
                order by decision_date desc nulls last, created_at desc
                """,
                {"site_id": site_id},
            ),
            "planning_context_records": self.database.fetch_all(
                """
                select *
                from public.planning_context_records
                where site_id = cast(:site_id as uuid)
                order by created_at desc
                """,
                {"site_id": site_id},
            ),
            "constraints": self.database.fetch_all(
                """
                select *
                from public.site_constraints
                where site_id = cast(:site_id as uuid)
                order by created_at desc
                """,
                {"site_id": site_id},
            ),
            "infrastructure_records": self.database.fetch_all(
                """
                select *
                from public.site_infrastructure_records
                where site_id = cast(:site_id as uuid)
                order by created_at desc
                """,
                {"site_id": site_id},
            ),
            "control_records": self.database.fetch_all(
                """
                select *
                from public.site_control_records
                where site_id = cast(:site_id as uuid)
                order by created_at desc
                """,
                {"site_id": site_id},
            ),
            "comparable_market_records": self.database.fetch_all(
                """
                select *
                from public.comparable_market_records
                where site_id = cast(:site_id as uuid)
                order by sale_date desc nulls last, created_at desc
                """,
                {"site_id": site_id},
            ),
            "buyer_matches": self.database.fetch_all(
                """
                select
                    sbm.*,
                    bp.profile_code,
                    bp.buyer_name,
                    bp.target_strategy
                from public.site_buyer_matches as sbm
                join public.buyer_profiles as bp on bp.id = sbm.buyer_profile_id
                where sbm.site_id = cast(:site_id as uuid)
                order by sbm.created_at desc
                """,
                {"site_id": site_id},
            ),
            "signals": self.database.fetch_all(
                """
                select *
                from analytics.v_site_current_signals
                where site_id = cast(:site_id as uuid)
                order by signal_group, signal_label
                """,
                {"site_id": site_id},
            ),
            "interpretations": self.database.fetch_all(
                """
                select *
                from analytics.v_site_current_interpretations
                where site_id = cast(:site_id as uuid)
                order by priority, title
                """,
                {"site_id": site_id},
            ),
            "assessment": self.database.fetch_one(
                """
                select *
                from analytics.v_site_current_assessments
                where site_id = cast(:site_id as uuid)
                """,
                {"site_id": site_id},
            ),
            "assessment_scores": self.database.fetch_all(
                """
                select *
                from analytics.v_site_current_assessment_scores
                where site_id = cast(:site_id as uuid)
                order by
                    case score_code
                        when 'P' then 1
                        when 'G' then 2
                        when 'I' then 3
                        when 'R' then 4
                        when 'F' then 5
                        when 'K' then 6
                        when 'B' then 7
                        else 8
                    end
                """,
                {"site_id": site_id},
            ),
            "signal_evidence_rows": self.database.fetch_all(
                """
                select
                    link.signal_id::text as signal_id,
                    evidence.*
                from public.site_signal_evidence as link
                join public.evidence_references as evidence
                    on evidence.id = link.evidence_reference_id
                join analytics.v_site_current_signals as signal
                    on signal.id = link.signal_id
                where signal.site_id = cast(:site_id as uuid)
                order by evidence.created_at asc
                """,
                {"site_id": site_id},
            ),
            "interpretation_evidence_rows": self.database.fetch_all(
                """
                select
                    link.interpretation_id::text as interpretation_id,
                    evidence.*
                from public.site_interpretation_evidence as link
                join public.evidence_references as evidence
                    on evidence.id = link.evidence_reference_id
                join analytics.v_site_current_interpretations as interpretation
                    on interpretation.id = link.interpretation_id
                where interpretation.site_id = cast(:site_id as uuid)
                order by evidence.created_at asc
                """,
                {"site_id": site_id},
            ),
            "assessment_evidence_rows": self.database.fetch_all(
                """
                select
                    link.site_assessment_id::text as site_assessment_id,
                    evidence.*
                from public.site_assessment_evidence as link
                join public.evidence_references as evidence
                    on evidence.id = link.evidence_reference_id
                join analytics.v_site_current_assessments as assessment
                    on assessment.id = link.site_assessment_id
                where assessment.site_id = cast(:site_id as uuid)
                order by evidence.created_at asc
                """,
                {"site_id": site_id},
            ),
            "assessment_score_evidence_rows": self.database.fetch_all(
                """
                select
                    link.site_assessment_score_id::text as site_assessment_score_id,
                    evidence.*
                from public.site_assessment_score_evidence as link
                join public.evidence_references as evidence
                    on evidence.id = link.evidence_reference_id
                join analytics.v_site_current_assessment_scores as score
                    on score.id = link.site_assessment_score_id
                where score.site_id = cast(:site_id as uuid)
                order by evidence.created_at asc
                """,
                {"site_id": site_id},
            ),
        }


def _build_in_clause(column_name: str, prefix: str, values: list[str], params: dict[str, Any]) -> str:
    placeholders: list[str] = []
    for index, value in enumerate(values):
        key = f"{prefix}_{index}"
        placeholders.append(f":{key}")
        params[key] = value
    return f"{column_name} in ({', '.join(placeholders)})"
