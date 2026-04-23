"""Buyer-fit and market-depth helpers."""

from __future__ import annotations

from typing import Any


def extract_buyer_signal(
    buyer_matches: list[dict[str, Any]],
    comparable_records: list[dict[str, Any]],
    nearest_settlement: str | None,
) -> dict[str, Any]:
    strong_matches = [row for row in buyer_matches if row.get("fit_rating") == "strong"]
    moderate_matches = [row for row in buyer_matches if row.get("fit_rating") == "moderate"]
    comparable_count = len([row for row in comparable_records if row.get("comparable_type") == "new_build"])

    if strong_matches or len(moderate_matches) >= 2:
        buyer_depth_estimate = "broad"
    elif moderate_matches or comparable_count >= 2:
        buyer_depth_estimate = "workable"
    elif comparable_count == 1:
        buyer_depth_estimate = "narrow"
    else:
        buyer_depth_estimate = "thin"

    settlement_name = (nearest_settlement or "").strip()
    if strong_matches or (settlement_name and comparable_count >= 3):
        settlement_strength = "strong"
    elif comparable_count >= 2:
        settlement_strength = "workable"
    elif settlement_name:
        settlement_strength = "mixed"
    else:
        settlement_strength = "weak"

    return {
        "buyer_depth_estimate": buyer_depth_estimate,
        "settlement_strength": settlement_strength,
        "strong_buyer_fit_count": len(strong_matches),
        "moderate_buyer_fit_count": len(moderate_matches),
        "buyer_profile_fit": tuple(
            str(row.get("profile_code") or row.get("buyer_name"))
            for row in [*strong_matches, *moderate_matches]
            if row.get("profile_code") or row.get("buyer_name")
        ),
    }

