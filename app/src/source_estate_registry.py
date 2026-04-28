"""Register, probe, and audit the LandIntel Phase One source estate."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import httpx
import yaml

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


MANIFEST_PATH = Path(__file__).resolve().parents[1] / "config" / "phase_one_source_estate.yaml"


class SourceEstateRegistryRunner:
    """Keep the source estate bagged, tagged, probed, and auditable."""

    def __init__(self, settings: Any, logger: Any) -> None:
        self.settings = settings
        self.logger = logger.getChild("source_estate_registry")
        self.database = Database(settings)
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)
        self.manifest = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8")) or {}

    def close(self) -> None:
        self.client.close()
        self.database.dispose()

    def register_source_estate(self, *, probe: bool = False, source_family: str | None = None) -> dict[str, Any]:
        sources = self._sources(source_family)
        source_count = 0
        asset_count = 0
        probe_results: list[dict[str, Any]] = []
        for source in sources:
            self._upsert_source(source)
            source_count += 1
            for asset in source.get("static_assets") or []:
                self._upsert_asset(source, asset)
                asset_count += 1
            if probe:
                probe_results.append(self._probe_and_record_source(source))
            else:
                self._record_freshness_without_probe(source)

        payload = {
            "source_count": source_count,
            "asset_count": asset_count,
            "probe_count": len(probe_results),
            "probe_results": probe_results,
        }
        self.logger.info("source_estate_registered", extra=payload)
        return payload

    def probe_source_estate(self, *, source_family: str | None = None) -> dict[str, Any]:
        return self.register_source_estate(probe=True, source_family=source_family)

    def audit_source_estate(self) -> dict[str, Any]:
        rows = self.database.fetch_all(
            """
                select *
                from analytics.v_phase_one_source_estate_matrix
                order by
                    case phase_one_role
                        when 'critical' then 1
                        when 'target_live' then 2
                        else 3
                    end,
                    source_family,
                    source_key
            """
        )
        blocked = [row for row in rows if str(row.get("operational_status") or "").startswith("blocked")]
        unproven = [row for row in rows if row.get("operational_status") == "registered_unproven"]
        payload = {
            "source_count": len(rows),
            "blocked_count": len(blocked),
            "unproven_count": len(unproven),
            "blocked_sources": [row.get("source_key") for row in blocked],
            "unproven_sources": [row.get("source_key") for row in unproven],
            "matrix": rows,
        }
        self.logger.info("source_estate_audit", extra=payload)
        return payload

    def discover_authority_sources(self, source_family: str) -> dict[str, Any]:
        discovery = (self.manifest.get("discovery") or {}).get(source_family)
        if not discovery:
            raise KeyError(f"No discovery config for {source_family}")
        authorities = self.settings.load_target_councils()
        search_url = str(discovery["search_url"])
        query_base = str(discovery.get("query") or source_family)
        if "package_show" in search_url:
            payload = self._fetch_json(search_url, params={})
            package = dict(payload.get("result") or payload)
            for source in self._sources(source_family):
                self._upsert_source(source)
                self._record_freshness(
                    source,
                    freshness_status="current",
                    live_access_status="reachable",
                    summary=f"{source['source_name']} package registered from SpatialHub package_show.",
                )
            result = {
                "source_family": source_family,
                "authorities_checked": 0,
                "registry_rows_written": len(self._sources(source_family)),
                "package_results": 1 if package else 0,
                "resource_count": len(package.get("resources") or []),
            }
            self.logger.info("source_estate_discovery_completed", extra=result)
            return result
        if "WFSServer" in search_url or "service=WFS" in search_url:
            feature_count = self._wfs_hit_count(search_url, query_base)
            for source in self._sources(source_family):
                self._upsert_source(source)
                self._record_freshness(
                    source,
                    freshness_status="current",
                    live_access_status="reachable",
                    summary=f"{source['source_name']} registered from WFS capabilities.",
                    records_observed=feature_count,
                )
            result = {
                "source_family": source_family,
                "authorities_checked": 0,
                "registry_rows_written": len(self._sources(source_family)),
                "wfs_results": 1,
                "feature_count": feature_count,
            }
            self.logger.info("source_estate_discovery_completed", extra=result)
            return result
        inserted = 0
        result_count = 0
        for authority in authorities:
            payload = self._fetch_json(search_url, params={"q": f"{authority} {query_base}", "rows": 25})
            packages = list(((payload.get("result") or {}).get("results")) or [])
            if not packages:
                self._upsert_discovered_placeholder(source_family, discovery, authority, search_url)
                inserted += 1
                continue
            result_count += len(packages)
            for package in packages:
                self._upsert_discovered_package(source_family, discovery, authority, package)
                inserted += 1
        payload = {
            "source_family": source_family,
            "authorities_checked": len(authorities),
            "registry_rows_written": inserted,
            "package_results": result_count,
        }
        self.logger.info("source_estate_discovery_completed", extra=payload)
        return payload

    def _sources(self, source_family: str | None = None) -> list[dict[str, Any]]:
        rows = list(self.manifest.get("sources") or [])
        if source_family:
            rows = [row for row in rows if row.get("source_family") == source_family]
        return rows

    def _upsert_source(self, source: dict[str, Any]) -> None:
        self.database.execute(
            """
                insert into landintel.source_estate_registry (
                    source_key, source_family, source_name, source_group, phase_one_role,
                    source_status, orchestration_mode, endpoint_url, auth_env_vars, target_table,
                    reconciliation_path, evidence_path, signal_output, ranking_impact,
                    resurfacing_trigger, data_age_basis, drive_folder_url, notes,
                    ranking_eligible, review_output_eligible, metadata, last_registered_at, updated_at
                )
                values (
                    :source_key, :source_family, :source_name, :source_group, :phase_one_role,
                    :source_status, :orchestration_mode, :endpoint_url, :auth_env_vars, :target_table,
                    :reconciliation_path, :evidence_path, :signal_output, :ranking_impact,
                    :resurfacing_trigger, :data_age_basis, :drive_folder_url, :notes,
                    :ranking_eligible, :review_output_eligible, cast(:metadata as jsonb), now(), now()
                )
                on conflict (source_key) do update set
                    source_family = excluded.source_family,
                    source_name = excluded.source_name,
                    source_group = excluded.source_group,
                    phase_one_role = excluded.phase_one_role,
                    source_status = excluded.source_status,
                    orchestration_mode = excluded.orchestration_mode,
                    endpoint_url = excluded.endpoint_url,
                    auth_env_vars = excluded.auth_env_vars,
                    target_table = excluded.target_table,
                    reconciliation_path = excluded.reconciliation_path,
                    evidence_path = excluded.evidence_path,
                    signal_output = excluded.signal_output,
                    ranking_impact = excluded.ranking_impact,
                    resurfacing_trigger = excluded.resurfacing_trigger,
                    data_age_basis = excluded.data_age_basis,
                    drive_folder_url = excluded.drive_folder_url,
                    notes = excluded.notes,
                    ranking_eligible = excluded.ranking_eligible,
                    review_output_eligible = excluded.review_output_eligible,
                    metadata = excluded.metadata,
                    last_registered_at = now(),
                    updated_at = now()
            """,
            {
                "source_key": source["source_key"],
                "source_family": source["source_family"],
                "source_name": source["source_name"],
                "source_group": source.get("source_group") or "unknown",
                "phase_one_role": source.get("phase_one_role") or "context",
                "source_status": source.get("source_status") or "unknown",
                "orchestration_mode": source.get("orchestration_mode") or "unknown",
                "endpoint_url": source.get("endpoint_url"),
                "auth_env_vars": list(source.get("auth_env_vars") or []),
                "target_table": source.get("target_table"),
                "reconciliation_path": source.get("reconciliation_path"),
                "evidence_path": source.get("evidence_path"),
                "signal_output": source.get("signal_output"),
                "ranking_impact": source.get("ranking_impact"),
                "resurfacing_trigger": source.get("resurfacing_trigger"),
                "data_age_basis": source.get("data_age_basis"),
                "drive_folder_url": source.get("drive_folder_url"),
                "notes": source.get("notes"),
                "ranking_eligible": bool(source.get("ranking_eligible", source.get("phase_one_role") in {"critical", "target_live"})),
                "review_output_eligible": bool(source.get("review_output_eligible", True)),
                "metadata": json.dumps({k: v for k, v in source.items() if k not in {"static_assets"}}),
            },
        )

    def _upsert_asset(self, source: dict[str, Any], asset: dict[str, Any]) -> None:
        self.database.execute(
            """
                insert into landintel.source_corpus_assets (
                    source_key, asset_key, file_name, drive_url, local_reference,
                    layer_names, feature_count, modified_at, asset_role, metadata, updated_at
                )
                values (
                    :source_key, :asset_key, :file_name, :drive_url, :local_reference,
                    :layer_names, :feature_count, cast(:modified_at as timestamptz),
                    :asset_role, cast(:metadata as jsonb), now()
                )
                on conflict (source_key, asset_key) do update set
                    file_name = excluded.file_name,
                    drive_url = excluded.drive_url,
                    local_reference = excluded.local_reference,
                    layer_names = excluded.layer_names,
                    feature_count = excluded.feature_count,
                    modified_at = excluded.modified_at,
                    asset_role = excluded.asset_role,
                    metadata = excluded.metadata,
                    updated_at = now()
            """,
            {
                "source_key": source["source_key"],
                "asset_key": asset["asset_key"],
                "file_name": asset["file_name"],
                "drive_url": asset.get("drive_url") or source.get("drive_folder_url"),
                "local_reference": asset.get("local_reference"),
                "layer_names": list(asset.get("layer_names") or []),
                "feature_count": asset.get("feature_count"),
                "modified_at": asset.get("modified_at"),
                "asset_role": asset.get("asset_role") or "static_snapshot",
                "metadata": json.dumps(asset),
            },
        )

    def _probe_and_record_source(self, source: dict[str, Any]) -> dict[str, Any]:
        missing_secrets = [name for name in source.get("auth_env_vars") or [] if not os.getenv(str(name))]
        if missing_secrets:
            status = "missing_required_secret"
            summary = "Missing required GitHub Actions secret(s): " + ", ".join(missing_secrets)
            self._update_probe(source["source_key"], status, summary)
            self._record_freshness(source, freshness_status="unknown", live_access_status=status, summary=summary)
            return {"source_key": source["source_key"], "status": status, "summary": summary}

        endpoint_url = source.get("endpoint_url")
        if not endpoint_url:
            status = "not_probeable"
            summary = "No endpoint URL; this source is registry/static/discovery only."
            self._update_probe(source["source_key"], status, summary)
            self._record_freshness_without_probe(source)
            return {"source_key": source["source_key"], "status": status, "summary": summary}

        try:
            response = self.client.get(endpoint_url, params=self._probe_params(endpoint_url))
            status = "reachable" if response.status_code < 400 else "failed"
            summary = f"HTTP {response.status_code} from source endpoint"
        except Exception as exc:
            status = "failed"
            summary = str(exc)

        self._update_probe(source["source_key"], status, summary)
        self._record_freshness(
            source,
            freshness_status="current" if status == "reachable" else "failed",
            live_access_status=status,
            summary=summary,
        )
        return {"source_key": source["source_key"], "status": status, "summary": summary}

    def _record_freshness_without_probe(self, source: dict[str, Any]) -> None:
        source_status = str(source.get("source_status") or "unknown")
        if source_status == "live_internal_validation":
            freshness_status = "current"
            access_status = "internal_table_registered"
        elif source_status == "core_pending_adapter":
            freshness_status = "core_pending_adapter"
            access_status = "monitored"
        elif source_status == "explicitly_deferred":
            freshness_status = "explicitly_deferred"
            access_status = "monitored"
        elif source_status == "discovery_only":
            freshness_status = "discovery_only"
            access_status = "registered"
        elif source_status == "static_snapshot":
            freshness_status = "manual_snapshot"
            access_status = "static_registered"
        else:
            freshness_status = "unknown"
            access_status = "registered_unproven"
        self._record_freshness(
            source,
            freshness_status=freshness_status,
            live_access_status=access_status,
            summary="Registered from Phase One source estate manifest.",
        )

    def _record_freshness(
        self,
        source: dict[str, Any],
        *,
        freshness_status: str,
        live_access_status: str,
        summary: str,
        records_observed: int | None = None,
    ) -> None:
        self.database.execute(
            """
                insert into landintel.source_freshness_states (
                    source_scope_key, source_family, source_dataset, source_name,
                    source_access_mode, source_url, refresh_cadence, max_staleness_days,
                    source_observed_at, last_checked_at, last_success_at, next_refresh_due_at,
                    freshness_status, live_access_status, ranking_eligible, review_output_eligible,
                    stale_reason_code, check_summary, records_observed, metadata, updated_at
                )
                values (
                    :scope_key, :source_family, :source_dataset, :source_name,
                    :source_access_mode, :source_url, :refresh_cadence, :max_staleness_days,
                    now(), now(), :last_success_at, :next_refresh_due_at,
                    :freshness_status, :live_access_status, :ranking_eligible, :review_output_eligible,
                    :stale_reason_code, :check_summary, :records_observed, cast(:metadata as jsonb), now()
                )
                on conflict (source_scope_key) do update set
                    source_family = excluded.source_family,
                    source_dataset = excluded.source_dataset,
                    source_name = excluded.source_name,
                    source_access_mode = excluded.source_access_mode,
                    source_url = excluded.source_url,
                    last_checked_at = excluded.last_checked_at,
                    last_success_at = excluded.last_success_at,
                    next_refresh_due_at = excluded.next_refresh_due_at,
                    freshness_status = excluded.freshness_status,
                    live_access_status = excluded.live_access_status,
                    ranking_eligible = excluded.ranking_eligible,
                    review_output_eligible = excluded.review_output_eligible,
                    stale_reason_code = excluded.stale_reason_code,
                    check_summary = excluded.check_summary,
                    records_observed = excluded.records_observed,
                    metadata = excluded.metadata,
                    updated_at = now()
            """,
            {
                "scope_key": f"source_estate:{source['source_key']}",
                "source_family": source["source_family"],
                "source_dataset": source["source_name"],
                "source_name": source["source_name"],
                "source_access_mode": source.get("orchestration_mode") or "unknown",
                "source_url": source.get("endpoint_url") or source.get("drive_folder_url"),
                "refresh_cadence": "weekly" if source.get("phase_one_role") == "critical" else "monthly",
                "max_staleness_days": 7 if source.get("phase_one_role") == "critical" else 30,
                "last_success_at": (
                    datetime.now(timezone.utc)
                    if freshness_status
                    in {"current", "manual_snapshot", "explicitly_deferred", "core_pending_adapter", "discovery_only"}
                    else None
                ),
                "next_refresh_due_at": None,
                "freshness_status": freshness_status,
                "live_access_status": live_access_status,
                "ranking_eligible": bool(source.get("ranking_eligible", source.get("phase_one_role") in {"critical", "target_live"})),
                "review_output_eligible": bool(source.get("review_output_eligible", True)),
                "stale_reason_code": (
                    "authority_adapter_not_validated"
                    if freshness_status == "core_pending_adapter"
                    else None
                    if freshness_status in {"current", "manual_snapshot"}
                    else live_access_status
                ),
                "check_summary": summary,
                "records_observed": (
                    records_observed
                    if records_observed is not None
                    else sum(int(asset.get("feature_count") or 0) for asset in source.get("static_assets") or [])
                ),
                "metadata": json.dumps({"source_key": source["source_key"], "phase_one_role": source.get("phase_one_role")}),
            },
        )

    def _update_probe(self, source_key: str, status: str, summary: str) -> None:
        self.database.execute(
            """
                update landintel.source_estate_registry
                   set last_probe_at = now(),
                       last_probe_status = :status,
                       last_probe_summary = :summary,
                       updated_at = now()
                 where source_key = :source_key
            """,
            {"source_key": source_key, "status": status, "summary": summary[:500]},
        )

    def _upsert_discovered_placeholder(self, source_family: str, discovery: dict[str, Any], authority: str, search_url: str) -> None:
        source_key = f"{source_family}:{authority.lower().replace(' ', '_')}:placeholder"
        self._upsert_source(
            {
                "source_key": source_key,
                "source_family": source_family,
                "source_name": f"{authority} {source_family} source discovery placeholder",
                "source_group": "policy",
                "phase_one_role": "critical",
                "source_status": discovery.get("source_status") or "explicitly_deferred",
                "orchestration_mode": "authority_discovery",
                "endpoint_url": search_url,
                "target_table": "landintel.authority_source_registry",
                "reconciliation_path": "deferred until authority adapter validated",
                "evidence_path": "registry audit only until promoted",
                "signal_output": "none until promoted",
                "ranking_impact": "none while deferred",
                "resurfacing_trigger": "source discovery or promotion",
                "data_age_basis": "last discovery check",
                "notes": discovery.get("defer_reason_code"),
                "ranking_eligible": False,
                "review_output_eligible": False,
            }
        )

    def _upsert_discovered_package(self, source_family: str, discovery: dict[str, Any], authority: str, package: dict[str, Any]) -> None:
        package_id = str(package.get("id") or package.get("name") or package.get("title") or "unknown")
        source_key = f"{source_family}:{authority.lower().replace(' ', '_')}:{package_id}"
        self._upsert_source(
            {
                "source_key": source_key,
                "source_family": source_family,
                "source_name": str(package.get("title") or package_id),
                "source_group": "policy",
                "phase_one_role": "critical",
                "source_status": discovery.get("source_status") or "explicitly_deferred",
                "orchestration_mode": "authority_discovery",
                "endpoint_url": str(package.get("url") or discovery["search_url"]),
                "target_table": "landintel.authority_source_registry",
                "reconciliation_path": "deferred until authority adapter validated",
                "evidence_path": "registry audit only until promoted",
                "signal_output": "none until promoted",
                "ranking_impact": "none while deferred",
                "resurfacing_trigger": "source discovery or promotion",
                "data_age_basis": "SpatialHub package modified metadata",
                "notes": f"Authority discovery candidate: {authority}",
                "ranking_eligible": False,
                "review_output_eligible": False,
                "metadata": package,
            }
        )

    def _probe_params(self, endpoint_url: str) -> dict[str, str]:
        if "package_search" in endpoint_url:
            return {"q": "planning", "rows": "1"}
        if endpoint_url.rstrip("/").endswith("MapServer") or endpoint_url.rstrip("/").endswith("FeatureServer"):
            return {"f": "json"}
        if endpoint_url.endswith("/query"):
            return {"f": "json", "where": "1=1", "returnCountOnly": "true"}
        if endpoint_url.endswith("/wfs") or "WFSServer" in endpoint_url:
            return {"service": "WFS", "request": "GetCapabilities"}
        return {}

    def _wfs_hit_count(self, search_url: str, type_name: str) -> int:
        parsed = urlparse(search_url)
        endpoint_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.scheme else search_url.split("?", 1)[0]
        response = self.client.get(
            endpoint_url,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": type_name,
                "resultType": "hits",
            },
        )
        response.raise_for_status()
        try:
            root = ET.fromstring(response.text.encode("utf-8"))
        except ET.ParseError:
            return 0
        value = root.attrib.get("numberMatched") or root.attrib.get("numberOfFeatures")
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return 0

    def _fetch_json(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.client.get(url, params=params or None)
        response.raise_for_status()
        return response.json()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operate the LandIntel Phase One source estate registry.")
    parser.add_argument(
        "command",
        choices=(
            "register-source-estate",
            "probe-source-estate",
            "audit-source-estate",
            "discover-ldp-sources",
            "discover-settlement-sources",
        ),
    )
    parser.add_argument("--source-family")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = SourceEstateRegistryRunner(settings, logger)
    try:
        if args.command == "register-source-estate":
            runner.register_source_estate(source_family=args.source_family)
        elif args.command == "probe-source-estate":
            runner.probe_source_estate(source_family=args.source_family)
        elif args.command == "audit-source-estate":
            runner.audit_source_estate()
        elif args.command == "discover-ldp-sources":
            runner.discover_authority_sources("ldp")
        elif args.command == "discover-settlement-sources":
            runner.discover_authority_sources("settlement")
        logger.info("source_estate_registry_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception("source_estate_registry_command_failed", extra={"command": args.command})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
