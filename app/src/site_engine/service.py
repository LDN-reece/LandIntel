"""Orchestrate seed creation, queued reprocessing, and review payload assembly."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import yaml

from config.settings import Settings
from src.db import Database
from src.site_engine.review_brief import build_site_review_brief
from src.site_engine.repository import SiteQualificationRepository
from src.site_engine.rule_engine import apply_interpretation_rules, build_site_assessment
from src.site_engine.seed_data import RULESET_VERSION, SEED_SITE_SCENARIOS
from src.site_engine.signal_engine import build_site_signals
from src.site_engine.site_reference_reconciliation_engine import prepare_site_reconciliation
from src.site_engine.source_normalisers import normalise_site_evidence
from src.site_engine.types import EvidenceItem, SiteSearchFilters, SiteSnapshot


class SiteQualificationService:
    """Site-first reasoning pipeline with explicit evidence and queued updates."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger.getChild("site_engine")
        self.database = Database(settings)
        self.repository = SiteQualificationRepository(self.database)

    def close(self) -> None:
        self.database.dispose()

    def seed_demo_sites(self, limit: int = 6) -> dict[str, Any]:
        scenarios = SEED_SITE_SCENARIOS[:limit]
        buyer_profiles = self._load_buyer_profiles(self.settings.buyer_profiles_config_path)
        profile_ids = self.repository.upsert_buyer_profiles(buyer_profiles)
        seeded_site_codes: list[str] = []

        for scenario in scenarios:
            existing_site = self.repository.fetch_site_by_code(str(scenario["site_code"]))
            primary_parcel = None
            if existing_site and existing_site.get("primary_ros_parcel_id"):
                primary_parcel = self.repository.fetch_parcel_record(str(existing_site["primary_ros_parcel_id"]))
            if not primary_parcel:
                primary_parcel = self.repository.select_seed_candidate_parcel(
                    preferred_authorities=list(scenario["preferred_authorities"]),
                    min_acres=float(scenario["min_acres"]),
                    max_acres=float(scenario["max_acres"]),
                    site_code=str(scenario["site_code"]),
                )
            if not primary_parcel:
                raise RuntimeError(f"No candidate parcel found for seed scenario {scenario['site_code']}.")

            related_parcels = self.repository.select_related_parcels(
                primary_ros_parcel_id=str(primary_parcel["ros_parcel_id"]),
                site_code=str(scenario["site_code"]),
                limit=max(int(scenario.get("component_target", 1)) - 1, 0),
            )
            linked_parcels = [primary_parcel, *related_parcels]

            site_id = self.repository.upsert_site(
                site_code=str(scenario["site_code"]),
                site_name=str(scenario["site_name"]),
                workflow_status=str(scenario["workflow_status"]),
                source_method="mvp_seed",
                primary_ros_parcel_id=str(primary_parcel["ros_parcel_id"]),
                primary_land_object_id=str(primary_parcel["land_object_id"]) if primary_parcel.get("land_object_id") else None,
                metadata_json=json.dumps(
                    {
                        "seed_scenario": scenario["site_code"],
                        "component_target": scenario.get("component_target", 1),
                        "site_name_aliases": [scenario["site_name"]],
                    }
                ),
            )

            self.repository.clear_site_fact_rows(site_id)
            for index, parcel in enumerate(linked_parcels):
                source_record_id = f"{scenario['site_code']}-parcel-{index + 1}"
                self.repository.insert_site_parcel(
                    site_id=site_id,
                    ros_parcel_id=str(parcel["ros_parcel_id"]),
                    land_object_id=str(parcel["land_object_id"]) if parcel.get("land_object_id") else None,
                    title_number=parcel.get("title_number"),
                    parcel_reference=parcel.get("ros_inspire_id"),
                    is_primary=index == 0,
                    source_record_id=source_record_id,
                )
                self.repository.insert_site_geometry_component(
                    site_id=site_id,
                    ros_parcel_id=str(parcel["ros_parcel_id"]),
                    source_identifier=parcel.get("ros_inspire_id"),
                    is_primary=index == 0,
                )

            self.repository.upsert_site_location_from_parcels(
                site_id=site_id,
                authority_name=str(primary_parcel["authority_name"]),
                nearest_settlement=str(scenario["nearest_settlement"]),
                settlement_relationship=str(scenario["settlement_relationship"]),
                within_settlement_boundary=scenario.get("within_settlement_boundary"),
                distance_to_settlement_boundary_m=scenario.get("distance_to_settlement_boundary_m"),
                source_record_id=str(scenario["site_code"]),
            )
            self.repository.insert_planning_records(site_id, scenario["planning_records"])
            self.repository.insert_planning_context_records(site_id, scenario["planning_context_records"])
            self.repository.insert_constraint_records(site_id, scenario["constraints"])
            self.repository.insert_infrastructure_records(site_id, scenario.get("infrastructure_records", []))
            self.repository.insert_control_records(site_id, scenario.get("control_records", []))
            self.repository.insert_market_records(site_id, scenario["comparables"])
            self.repository.insert_buyer_matches(site_id, scenario["buyer_matches"], profile_ids)
            self.repository.record_status_history(
                site_id,
                str(scenario["workflow_status"]),
                "MVP seed scenario refreshed.",
            )
            self.repository.enqueue_site_refresh(
                site_id=site_id,
                trigger_source="mvp_seed",
                source_table="public.sites",
                source_record_id=site_id,
                metadata_json=json.dumps({"site_code": scenario["site_code"]}),
            )
            seeded_site_codes.append(str(scenario["site_code"]))

        processed = self.process_pending_refresh_requests(limit=max(limit, 1))
        processed["seeded_site_codes"] = seeded_site_codes
        return processed

    def process_pending_refresh_requests(self, limit: int = 20) -> dict[str, Any]:
        requests = self.repository.fetch_pending_refresh_requests(limit=limit)
        processed: list[str] = []
        failed: list[dict[str, str]] = []
        for request in requests:
            request_id = str(request["id"])
            site_id = str(request["site_id"])
            self.repository.update_refresh_request_status(request_id, status="processing")
            try:
                self.refresh_site(site_id, triggered_by=str(request.get("trigger_source") or "queue"))
                self.repository.update_refresh_request_status(request_id, status="completed")
                processed.append(site_id)
            except Exception as exc:  # pragma: no cover - exercised in runtime, not unit tests
                self.logger.exception("site_refresh_failed", extra={"site_id": site_id})
                self.repository.update_refresh_request_status(
                    request_id,
                    status="failed",
                    error_message=str(exc),
                )
                failed.append({"site_id": site_id, "error": str(exc)})
        return {"processed_count": len(processed), "processed_site_ids": processed, "failed": failed}

    def refresh_site(self, site_id: str, *, triggered_by: str = "manual") -> dict[str, Any]:
        snapshot = self.repository.fetch_site_snapshot(site_id)
        self._sync_site_reconciliation(snapshot)
        snapshot = self.repository.fetch_site_snapshot(site_id)
        run_id = self.repository.create_analysis_run(
            site_id,
            ruleset_version=RULESET_VERSION,
            triggered_by=triggered_by,
            metadata_json=json.dumps({"site_code": snapshot.site["site_code"]}),
        )
        try:
            site_evidence = normalise_site_evidence(snapshot)
            signals = build_site_signals(snapshot, site_evidence)
            evidence_id_cache: dict[tuple[str | None, str | None, str | None, str], str] = {}
            signal_index: dict[str, dict[str, Any]] = {}
            for signal in signals:
                signal_id = self.repository.insert_signal(
                    site_id=site_id,
                    run_id=run_id,
                    signal_key=signal.key,
                    signal_label=signal.label,
                    signal_group=signal.group,
                    value_type=signal.value_type,
                    signal_state=signal.state,
                    bool_value=signal.bool_value,
                    numeric_value=signal.numeric_value,
                    text_value=signal.text_value,
                    json_value=json.dumps(signal.json_value) if signal.json_value is not None else None,
                    reasoning=signal.reasoning,
                )
                evidence_ids: list[str] = []
                for evidence in signal.evidence:
                    evidence_id = self._persist_evidence(site_id, evidence, evidence_id_cache)
                    self.repository.link_signal_evidence(signal_id, evidence_id)
                    evidence_ids.append(evidence_id)
                signal_index[signal.key] = {"signal": signal, "signal_id": signal_id, "evidence_ids": evidence_ids}

            signal_map = {key: value["signal"] for key, value in signal_index.items()}
            assessment = build_site_assessment(
                snapshot,
                site_evidence,
            )
            assessment_id = self.repository.insert_assessment(
                site_id=site_id,
                run_id=run_id,
                jurisdiction=assessment.jurisdiction,
                assessment_version=assessment.assessment_version,
                bucket_code=assessment.bucket_code,
                bucket_label=assessment.bucket_label,
                likely_opportunity_type=assessment.likely_opportunity_type,
                monetisation_horizon=assessment.monetisation_horizon,
                horizon_year_band=assessment.horizon_year_band,
                dominant_blocker=assessment.dominant_blocker,
                blocker_themes_json=json.dumps(assessment.blocker_themes),
                primary_reason=assessment.primary_reason,
                secondary_reasons_json=json.dumps(assessment.secondary_reasons),
                buyer_profile_guess=assessment.buyer_profile_guess,
                likely_buyer_profiles_json=json.dumps(assessment.likely_buyer_profiles),
                cost_to_control_band=assessment.cost_to_control_band,
                human_review_required=assessment.human_review_required,
                hard_fail_flags_json=json.dumps([flag.__dict__ for flag in assessment.hard_fail_flags]),
                review_flags_json=json.dumps(assessment.review_flags),
                explanation_text=assessment.explanation_text,
                metadata_json=json.dumps({"site_code": snapshot.site["site_code"]}),
            )
            for evidence_key in assessment.evidence_keys:
                for evidence in site_evidence.field_evidence.get(evidence_key, []):
                    evidence_id = self._persist_evidence(site_id, evidence, evidence_id_cache)
                    self.repository.link_assessment_evidence(assessment_id, evidence_id)

            for score in assessment.scores.values():
                score_id = self.repository.insert_assessment_score(
                    assessment_id=assessment_id,
                    site_id=site_id,
                    score_code=score.score_code,
                    score_label=score.label,
                    score_value=score.value,
                    confidence_label=score.confidence_label,
                    score_summary=score.summary,
                    score_reasoning=score.reasoning,
                    blocker_theme=score.blocker_theme,
                    metadata_json=json.dumps(
                        {
                            "contributions": [
                                {
                                    "source_family": contribution.source_family,
                                    "delta": contribution.delta,
                                    "summary": contribution.summary,
                                    "reasoning": contribution.reasoning,
                                    "evidence_keys": contribution.evidence_keys,
                                }
                                for contribution in score.contributions
                            ]
                        }
                    ),
                )
                for contribution in score.contributions:
                    for evidence_key in contribution.evidence_keys:
                        for evidence in site_evidence.field_evidence.get(evidence_key, []):
                            evidence_id = self._persist_evidence(site_id, evidence, evidence_id_cache)
                            self.repository.link_assessment_score_evidence(score_id, evidence_id)

            interpretations = apply_interpretation_rules(
                assessment,
                signal_map,
            )
            surfaced_reason = f"{assessment.bucket_label}: {assessment.primary_reason.capitalize()}."
            self.repository.update_site_surfaced_reason(site_id, surfaced_reason)

            for interpretation in interpretations:
                interpretation_id = self.repository.insert_interpretation(
                    site_id=site_id,
                    run_id=run_id,
                    interpretation_key=interpretation.key,
                    category=interpretation.category,
                    title=interpretation.title,
                    summary=interpretation.summary,
                    reasoning=interpretation.reasoning,
                    rule_code=interpretation.rule_code,
                    priority=interpretation.priority,
                )
                linked_evidence_ids = _dedupe(
                    evidence_id
                    for signal_key in interpretation.signal_keys
                    for evidence_id in signal_index.get(signal_key, {}).get("evidence_ids", [])
                )
                for evidence_id in linked_evidence_ids:
                    self.repository.link_interpretation_evidence(interpretation_id, evidence_id)

            self.repository.complete_analysis_run(
                run_id,
                metadata_json=json.dumps(
                    {
                        "site_code": snapshot.site["site_code"],
                        "signal_count": len(signals),
                        "assessment_bucket": assessment.bucket_code,
                        "interpretation_count": len(interpretations),
                    }
                ),
            )
            self.repository.upsert_site_search_cache_row(site_id)
            return {
                "site_id": site_id,
                "signal_count": len(signals),
                "interpretation_count": len(interpretations),
                "assessment_bucket": assessment.bucket_code,
            }
        except Exception as exc:
            self.repository.fail_analysis_run(run_id, str(exc))
            raise

    def refresh_explicit_sites(
        self,
        *,
        site_ids: list[str] | None = None,
        site_codes: list[str] | None = None,
    ) -> dict[str, Any]:
        resolved_site_ids = self.repository.resolve_site_ids(site_ids, site_codes)
        refreshed: list[str] = []
        for site_id in resolved_site_ids:
            self.refresh_site(site_id, triggered_by="explicit_request")
            refreshed.append(site_id)
        return {"processed_count": len(refreshed), "processed_site_ids": refreshed, "failed": []}

    def search_sites(self, filters: SiteSearchFilters) -> dict[str, Any]:
        return {
            "filters": filters,
            "results": self.repository.search_sites(filters),
            "options": self.repository.fetch_filter_options(),
        }

    def get_site_review(self, site_id: str) -> dict[str, Any] | None:
        detail = self.repository.fetch_site_detail(site_id)
        if not detail:
            return None
        detail["signal_evidence"] = _group_by_key(detail.pop("signal_evidence_rows"), "signal_id")
        detail["interpretation_evidence"] = _group_by_key(
            detail.pop("interpretation_evidence_rows"),
            "interpretation_id",
        )
        detail["assessment_evidence"] = _group_by_key(
            detail.pop("assessment_evidence_rows"),
            "site_assessment_id",
        )
        detail["assessment_score_evidence"] = _group_by_key(
            detail.pop("assessment_score_evidence_rows"),
            "site_assessment_score_id",
        )
        return {
            "detail": detail,
            "brief": build_site_review_brief(detail),
        }

    @staticmethod
    def _load_buyer_profiles(path: Path) -> list[dict[str, Any]]:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return list(payload.get("buyer_profiles", []))

    @staticmethod
    def _choose_surfaced_reason(interpretations: list[Any]) -> str:
        if not interpretations:
            return "No interpretations have been generated yet."
        for category in ("positive", "risk", "possible_fatal", "unknown"):
            for interpretation in interpretations:
                if interpretation.category == category:
                    return interpretation.summary
        return interpretations[0].summary

    def _persist_evidence(
        self,
        site_id: str,
        evidence: EvidenceItem,
        evidence_id_cache: dict[tuple[str | None, str | None, str | None, str], str],
    ) -> str:
        cache_key = (
            evidence.dataset_name,
            evidence.source_table,
            evidence.source_record_id,
            evidence.assertion,
        )
        if cache_key in evidence_id_cache:
            return evidence_id_cache[cache_key]
        evidence_id = self.repository.insert_evidence_reference(
            site_id,
            {
                "dataset_name": evidence.dataset_name,
                "source_table": evidence.source_table,
                "source_record_id": evidence.source_record_id,
                "source_identifier": evidence.source_identifier,
                "source_url": evidence.source_url,
                "observed_at": evidence.observed_at,
                "import_version": evidence.import_version,
                "confidence_label": evidence.confidence_label,
                "confidence_score": evidence.confidence_score,
                "assertion": evidence.assertion,
                "excerpt": evidence.excerpt,
                "metadata": json.dumps(evidence.metadata),
            },
        )
        evidence_id_cache[cache_key] = evidence_id
        return evidence_id

    def _sync_site_reconciliation(self, snapshot: SiteSnapshot) -> None:
        bundle = prepare_site_reconciliation(snapshot)
        for alias in bundle.aliases_to_upsert:
            self.repository.insert_site_reference_alias(
                site_id=str(snapshot.site["id"]),
                reference_family=str(alias["reference_family"]),
                raw_reference_value=str(alias["raw_reference_value"]),
                normalised_reference_value=str(alias["normalised_reference_value"]),
                source_dataset=str(alias["source_dataset"]),
                authority_name=alias.get("authority_name"),
                plan_period=alias.get("plan_period"),
                site_name_hint=alias.get("site_name_hint"),
                geometry_hash=alias.get("geometry_hash"),
                source_record_id=alias.get("source_record_id"),
                source_identifier=alias.get("source_identifier"),
                source_url=alias.get("source_url"),
                relation_type=str(alias["relation_type"]),
                status=str(alias["status"]),
                linked_confidence=alias.get("linked_confidence"),
                match_notes=alias.get("match_notes"),
                metadata_json=json.dumps(alias.get("metadata_json") or {}),
            )

        self.repository.insert_site_geometry_version_from_current_location(
            site_id=str(snapshot.site["id"]),
            version_label=f"{snapshot.site['site_code']}-canonical",
            source_dataset="canonical_site",
            source_table="public.site_locations",
            source_record_id=str(snapshot.location.get("id")) if snapshot.location else None,
            relation_type="canonical_union",
            match_confidence=0.99,
            source_url=f"internal://canonical-site/{snapshot.site['site_code']}/geometry",
            metadata_json=json.dumps({"site_code": snapshot.site["site_code"]}),
        )

        self.repository.clear_site_reconciliation_state(str(snapshot.site["id"]))
        for match in bundle.matches_to_record:
            self.repository.insert_site_reconciliation_match(
                site_id=str(snapshot.site["id"]),
                source_dataset=str(match["source_dataset"]),
                source_table=str(match["source_table"]),
                source_record_id=match.get("source_record_id"),
                raw_site_name=match.get("raw_site_name"),
                raw_reference_value=match.get("raw_reference_value"),
                normalised_reference_value=match.get("normalised_reference_value"),
                planning_reference=match.get("planning_reference"),
                title_number=match.get("title_number"),
                uprn=match.get("uprn"),
                usrn=match.get("usrn"),
                toid=match.get("toid"),
                authority_name=match.get("authority_name"),
                settlement_name=match.get("settlement_name"),
                relation_type=str(match["relation_type"]),
                confidence_score=match.get("confidence_score"),
                status=str(match["status"]),
                geometry_overlap_ratio=match.get("geometry_overlap_ratio"),
                geometry_distance_m=match.get("geometry_distance_m"),
                match_notes=match.get("match_notes"),
                metadata_json=json.dumps(match.get("metadata_json") or {}),
            )

        for review_item in bundle.review_items:
            self.repository.enqueue_reconciliation_review(
                candidate_site_id=str(snapshot.site["id"]),
                source_dataset=str(review_item["source_dataset"]),
                source_table=str(review_item["source_table"]),
                source_record_id=review_item.get("source_record_id"),
                raw_site_name=review_item.get("raw_site_name"),
                raw_reference_value=review_item.get("raw_reference_value"),
                normalised_reference_value=review_item.get("normalised_reference_value"),
                planning_reference=review_item.get("planning_reference"),
                authority_name=review_item.get("authority_name"),
                settlement_name=review_item.get("settlement_name"),
                confidence_score=review_item.get("confidence_score"),
                failure_reasons_json=json.dumps(review_item.get("failure_reasons_json") or []),
                candidate_matches_json=json.dumps(review_item.get("candidate_matches_json") or []),
                metadata_json=json.dumps(review_item.get("metadata_json") or {}),
            )


def _group_by_key(rows: list[dict[str, Any]], key_name: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[key_name])].append(row)
    return dict(grouped)


def _dedupe(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))
