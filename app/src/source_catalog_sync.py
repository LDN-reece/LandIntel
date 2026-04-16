"""Sync the locked Scotland source register into the landintel schema."""

from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path
from typing import Any

from config.settings import Settings, get_settings
from src.db import Database
from src.loaders.supabase_loader import SupabaseLoader
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
SOURCE_CATALOG_PATH = CONFIG_DIR / "scotland_source_catalog.json"
SOURCE_ENDPOINT_PATH = CONFIG_DIR / "scotland_source_endpoints.json"
ENTITY_BLUEPRINT_PATH = CONFIG_DIR / "scotland_entity_blueprint.json"


class SourceCatalogSync:
    """Persist the workbook-derived master source register into Supabase."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.logger = configure_logging(settings).getChild("source_catalog_sync")
        self.loader = SupabaseLoader(settings, Database(settings), self.logger)
        self.database = self.loader.database
        self.source_catalog = json.loads(SOURCE_CATALOG_PATH.read_text(encoding="utf-8"))
        self.source_endpoints = json.loads(SOURCE_ENDPOINT_PATH.read_text(encoding="utf-8"))
        self.entity_blueprint = json.loads(ENTITY_BLUEPRINT_PATH.read_text(encoding="utf-8"))

    def close(self) -> None:
        self.loader.close()

    def run_migrations(self) -> None:
        self.loader.run_migrations()

    def sync(self) -> dict[str, int]:
        sources = list(self.source_catalog.get("sources", []))
        service_endpoints = list(self.source_endpoints.get("service_endpoints", []))
        priority_endpoints = list(self.source_endpoints.get("priority_endpoints", []))
        entity_blueprint = list(self.entity_blueprint.get("entity_blueprint", []))
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="sync_source_catalog",
                source_name="landintel.source_catalog",
                status="running",
                metadata={
                    "register_name": self.source_catalog.get("register_name"),
                    "generated_from_workbook": self.source_catalog.get("generated_from_workbook"),
                },
            )
        )
        try:
            self.database.execute("delete from landintel.source_endpoint_catalog")
            self.database.execute("delete from landintel.entity_blueprint_catalog")
            self.database.execute("delete from landintel.source_catalog")

            source_key_lookup: dict[str, str] = {}
            for source in sources:
                self.database.execute(
                    """
                        insert into landintel.source_catalog (
                            source_key,
                            domain,
                            source_name,
                            source_role,
                            scope,
                            actionable_endpoint,
                            developer_page,
                            access_pattern,
                            auth_type,
                            primary_landintel_use,
                            why_it_matters,
                            primary_output_object,
                            primary_join_method,
                            secondary_join_method,
                            interacts_with,
                            suggested_raw_table,
                            suggested_normalized_table,
                            refresh_cadence,
                            existing_drive_asset,
                            existing_asset_note,
                            schema_minimum_fields,
                            critical_notes,
                            workflow_stage,
                            workflow_ready,
                            metadata
                        )
                        values (
                            :source_key,
                            :domain,
                            :source_name,
                            :source_role,
                            :scope,
                            :actionable_endpoint,
                            :developer_page,
                            :access_pattern,
                            :auth_type,
                            :primary_landintel_use,
                            :why_it_matters,
                            :primary_output_object,
                            :primary_join_method,
                            :secondary_join_method,
                            :interacts_with,
                            :suggested_raw_table,
                            :suggested_normalized_table,
                            :refresh_cadence,
                            :existing_drive_asset,
                            :existing_asset_note,
                            :schema_minimum_fields,
                            :critical_notes,
                            :workflow_stage,
                            :workflow_ready,
                            cast(:metadata as jsonb)
                        )
                    """,
                    {
                        "source_key": source["source_key"],
                        "domain": source.get("domain"),
                        "source_name": source.get("source_name"),
                        "source_role": source.get("source_role"),
                        "scope": source.get("scope"),
                        "actionable_endpoint": source.get("actionable_endpoint"),
                        "developer_page": source.get("developer_page"),
                        "access_pattern": source.get("access_pattern"),
                        "auth_type": source.get("auth_type"),
                        "primary_landintel_use": source.get("primary_landintel_use"),
                        "why_it_matters": source.get("why_it_matters"),
                        "primary_output_object": source.get("primary_output_object"),
                        "primary_join_method": source.get("primary_join_method"),
                        "secondary_join_method": source.get("secondary_join_method"),
                        "interacts_with": source.get("interacts_with") or [],
                        "suggested_raw_table": source.get("suggested_raw_table"),
                        "suggested_normalized_table": source.get("suggested_normalized_table"),
                        "refresh_cadence": source.get("refresh_cadence"),
                        "existing_drive_asset": source.get("existing_drive_asset"),
                        "existing_asset_note": source.get("existing_asset_note"),
                        "schema_minimum_fields": source.get("schema_minimum_fields") or [],
                        "critical_notes": source.get("critical_notes"),
                        "workflow_stage": source.get("workflow_stage"),
                        "workflow_ready": source.get("workflow_ready", False),
                        "metadata": json.dumps(
                            {
                                "register_name": self.source_catalog.get("register_name"),
                                "generated_at": self.source_catalog.get("generated_at"),
                            }
                        ),
                    },
                )
                source_key_lookup[_normalize_ref(source.get("source_name"))] = source["source_key"]

            endpoint_rows = []
            for endpoint in service_endpoints:
                endpoint_rows.append(
                    {
                        "endpoint_key": endpoint["endpoint_key"],
                        "source_name": endpoint.get("source_name"),
                        "endpoint_url": endpoint.get("endpoint_url"),
                        "endpoint_type": endpoint.get("endpoint_type"),
                        "auth_required": endpoint.get("auth_required"),
                        "purpose": endpoint.get("what_it_gives"),
                        "notes": endpoint.get("why_use_it"),
                        "endpoint_group": endpoint.get("category"),
                        "metadata": endpoint,
                    }
                )
            for endpoint in priority_endpoints:
                endpoint_rows.append(
                    {
                        "endpoint_key": endpoint["endpoint_key"],
                        "source_name": endpoint.get("source_name"),
                        "endpoint_url": endpoint.get("endpoint_url"),
                        "endpoint_type": "priority_endpoint",
                        "auth_required": None,
                        "purpose": endpoint.get("purpose"),
                        "notes": None,
                        "endpoint_group": endpoint.get("category"),
                        "metadata": endpoint,
                    }
                )

            for endpoint in endpoint_rows:
                self.database.execute(
                    """
                        insert into landintel.source_endpoint_catalog (
                            endpoint_key,
                            source_key,
                            endpoint_name,
                            endpoint_url,
                            endpoint_type,
                            auth_required,
                            purpose,
                            notes,
                            endpoint_group,
                            metadata
                        )
                        values (
                            :endpoint_key,
                            :source_key,
                            :endpoint_name,
                            :endpoint_url,
                            :endpoint_type,
                            :auth_required,
                            :purpose,
                            :notes,
                            :endpoint_group,
                            cast(:metadata as jsonb)
                        )
                    """,
                    {
                        "endpoint_key": endpoint["endpoint_key"],
                        "source_key": source_key_lookup.get(_normalize_ref(endpoint.get("source_name"))),
                        "endpoint_name": endpoint.get("source_name"),
                        "endpoint_url": endpoint.get("endpoint_url"),
                        "endpoint_type": endpoint.get("endpoint_type"),
                        "auth_required": endpoint.get("auth_required"),
                        "purpose": endpoint.get("purpose"),
                        "notes": endpoint.get("notes"),
                        "endpoint_group": endpoint.get("endpoint_group"),
                        "metadata": json.dumps(endpoint["metadata"]),
                    },
                )

            for entity in entity_blueprint:
                self.database.execute(
                    """
                        insert into landintel.entity_blueprint_catalog (
                            entity_name,
                            purpose,
                            minimum_required_fields,
                            primary_source,
                            primary_join_key,
                            secondary_join_key,
                            feeds_decision,
                            metadata
                        )
                        values (
                            :entity_name,
                            :purpose,
                            :minimum_required_fields,
                            :primary_source,
                            :primary_join_key,
                            :secondary_join_key,
                            :feeds_decision,
                            '{}'::jsonb
                        )
                    """,
                    {
                        "entity_name": entity.get("entity_name"),
                        "purpose": entity.get("purpose"),
                        "minimum_required_fields": entity.get("minimum_required_fields") or [],
                        "primary_source": entity.get("primary_source"),
                        "primary_join_key": entity.get("primary_join_key"),
                        "secondary_join_key": entity.get("secondary_join_key"),
                        "feeds_decision": entity.get("feeds_decision"),
                    },
                )

            result = {
                "source_catalog_count": len(sources),
                "source_endpoint_count": len(endpoint_rows),
                "entity_blueprint_count": len(entity_blueprint),
            }
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=len(sources) + len(endpoint_rows) + len(entity_blueprint),
                    records_loaded=len(sources) + len(endpoint_rows) + len(entity_blueprint),
                    records_retained=len(sources),
                    metadata=result,
                    finished=True,
                ),
            )
            self.logger.info("source_catalog_sync_completed", extra=result)
            return result
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise


def _normalize_ref(value: Any) -> str:
    import re

    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower()) or "unknown"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync thHШЪЩYШЫЭ[™ЫЭ\ЩH™YЪ\Э\‹€ЉB€\њЩ\‹YШ\™Э[Y[ќ
ЫЫ[X[™‹ЪЪXЩ\ПJњќ[‹[ZYЬ][ЫњИ‹њЮ[Л\ЫЭ\ЩKXШ][ЩИЉJB€™]\›€\њЩ\‚‚‚™Y€XZ[Љ
HO€[ќ‚€\њЩ\€HќZ[Ь\њЩ\Љ
B€\™ЬИH\њЩ\‹њ\њЩWШ\™ЬК
B€Щ][™ЬИHЩ]ЬЩ][™ЬК
B€Ю[Щ\€HЫЭ\ЩPШ][ЩФЮ[КЩ][™ЬКB€ћN‚€Y€\™ЬЛЫЫ[X[™OHњќ[‹[ZYЬ][ЫњИЋ‚€Ю[Щ\‹њќ[—ЫZYЬ][ЫњК
B€[Y€\™ЬЛЫЫ[X[™OHњЮ[Л\ЫЭ\ЩKXШ][ЩИЋ‚€Ю[Щ\‹њЮ[К
B€™]\›€€^Щ\^Щ\[ЫЋ‚€Ю[Щ\‹›ЩЩЩ\‹™^Щ\[ЫЉњЫЭ\ЩWШШ][ЩЧЬЮ[ЧЩZ[Y‹^O^ИЫЫ[X[™Ћ€\™ЬЛЫЫ[X[™JB€™]\›€B€љ[[N‚€Ю[Щ\‹ЫЬЩJ
B‚‚љY€ЧЫ[YWЧИOH—ЧЫXZ[—ЧИЋ‚€Z\ЩHЮ\Э[Q^]
XZ[Љ
JB