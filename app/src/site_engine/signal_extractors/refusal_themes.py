"""Scottish planning refusal theme extraction."""

from __future__ import annotations

from typing import Any, Iterable

from src.site_engine.evidence_utils import row_payload


THEME_KEYWORDS = {
    "planning_principle": ("principle", "outwith settlement", "countryside", "green belt", "policy conflict"),
    "access": ("access", "junction", "visibility", "road safety"),
    "roads": ("roads", "transport", "junction", "road network"),
    "drainage": ("drainage", "surface water", "suDS", "sewer", "wastewater"),
    "flood": ("flood", "flooding", "fluvial", "pluvial"),
    "education": ("education", "school", "catchment"),
    "design_density": ("design", "density", "layout", "placemaking"),
    "ecology_heritage": ("ecology", "bat", "heritage", "listed", "archaeology"),
    "landscape": ("landscape", "visual impact", "character"),
    "contamination_ground": ("contamination", "ground", "made ground", "remediation"),
    "prematurity": ("premature", "prematurity"),
    "affordability_obligations": ("section 75", "affordable", "obligation", "contribution"),
}


def extract_refusal_themes(records: Iterable[dict[str, Any]]) -> list[str]:
    themes: set[str] = set()
    for record in records:
        payload = row_payload(record)
        payload_themes = payload.get("refusal_themes")
        if isinstance(payload_themes, list):
            themes.update(str(value) for value in payload_themes if value)
            continue

        text_parts = [
            str(record.get("description") or ""),
            str(payload.get("decision_reason") or ""),
            str(payload.get("notes") or ""),
        ]
        haystack = " ".join(text_parts).lower()
        for theme, keywords in THEME_KEYWORDS.items():
            if any(keyword in haystack for keyword in keywords):
                themes.add(theme)
    return sorted(themes)

