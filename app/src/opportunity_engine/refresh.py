"""Refresh helpers for Phase One queue processing."""

from __future__ import annotations

from src.opportunity_engine.types import OpportunityAssessment, OpportunitySignal


def assessment_to_record(assessment: OpportunityAssessment) -> dict[str, object]:
    """Flatten the typed assessment into a persistable record."""

    return {
        "bucket": None,
        "monetisation_horizon": None,
        "dominant_blocker": assessment.dominant_blocker,
        "scores": {
            "overall_tier": assessment.overall_tier,
            "overall_rank_score": assessment.overall_rank_score,
            **assessment.subrank_summary,
        },
        "score_confidence": {
            "title_state": assessment.title_state,
            "ownership_control_fact_label": assessment.ownership_control_fact_label,
        },
        "human_review_required": assessment.human_review_required,
        "explanation_text": assessment.explanation_text,
        "metadata": {
            "phase": "landintel_phase_one",
        },
        "overall_tier": assessment.overall_tier,
        "overall_rank_score": assessment.overall_rank_score,
        "queue_recommendation": assessment.queue_recommendation,
        "why_it_surfaced": assessment.why_it_surfaced,
        "why_it_survived": assessment.why_it_survived,
        "good_items": assessment.good_items,
        "bad_items": assessment.bad_items,
        "ugly_items": assessment.ugly_items,
        "subrank_summary": assessment.subrank_summary,
        "title_state": assessment.title_state,
        "ownership_control_fact_label": assessment.ownership_control_fact_label,
        "resurfaced_reason": assessment.resurfaced_reason,
    }


def signals_to_rows(signals: list[OpportunitySignal]) -> list[OpportunitySignal]:
    """Keep the refresh pipeline explicit at the call site."""

    return signals
