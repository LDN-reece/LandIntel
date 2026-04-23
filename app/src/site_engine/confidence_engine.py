"""Score-confidence logic for the Scottish reasoning layer."""

from __future__ import annotations

from collections import Counter

from src.site_engine.types import ConfidenceLabel, ScoreContribution


def infer_score_confidence(contributions: list[ScoreContribution], *, has_unknown_pressure: bool = False) -> ConfidenceLabel:
    """Infer a conservative confidence level from evidence-family coverage."""

    if not contributions:
        return "low"
    family_counts = Counter(contribution.source_family for contribution in contributions)
    strongest_family_count = max(family_counts.values())
    if len(family_counts) >= 3 and not has_unknown_pressure:
        return "high"
    if len(family_counts) >= 2 or strongest_family_count >= 2:
        return "medium" if has_unknown_pressure else "high"
    return "low" if has_unknown_pressure else "medium"

