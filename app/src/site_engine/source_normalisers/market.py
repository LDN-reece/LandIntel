"""Normalise buyer depth and market evidence."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence
from src.site_engine.signal_extractors import extract_buyer_signal
from src.site_engine.site_evidence_schema import MarketEvidence


def normalise_market(
    location: dict[str, Any] | None,
    comparable_records: list[dict[str, Any]],
    buyer_matches: list[dict[str, Any]],
) -> tuple[MarketEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    extracted = extract_buyer_signal(
        buyer_matches,
        comparable_records,
        location.get("nearest_settlement") if location else None,
    )
    new_build_records = [
        row for row in comparable_records if str(row.get("comparable_type") or "").lower() == "new_build"
    ]
    average_ppsf = None
    if new_build_records:
        prices = [float(row["price_per_sqft_gbp"]) for row in new_build_records if row.get("price_per_sqft_gbp") is not None]
        if prices:
            average_ppsf = round(sum(prices) / len(prices), 2)
        for row in new_build_records:
            add_evidence(
                field_evidence,
                "market.comparable_strength",
                "public.comparable_market_records",
                row,
                "New-build comparable evidence is linked to the site.",
            )

    if len(new_build_records) >= 4:
        comparable_strength = "high"
    elif len(new_build_records) >= 2:
        comparable_strength = "medium"
    elif len(new_build_records) == 1:
        comparable_strength = "low"
    else:
        comparable_strength = "unknown"

    for row in buyer_matches:
        if row.get("fit_rating") in {"strong", "moderate"}:
            add_evidence(
                field_evidence,
                "market.buyer_profile_fit",
                "public.site_buyer_matches",
                row,
                f"Buyer match '{row.get('profile_code') or row.get('buyer_name')}' supports later exit logic.",
            )
            add_evidence(
                field_evidence,
                "market.buyer_depth_estimate",
                "public.site_buyer_matches",
                row,
                "Buyer fit evidence contributes to buyer-depth estimation.",
            )

    if not field_evidence.get("market.buyer_depth_estimate"):
        for row in new_build_records:
            add_evidence(
                field_evidence,
                "market.buyer_depth_estimate",
                "public.comparable_market_records",
                row,
                "Comparable depth contributes to buyer-depth estimation where buyer matches are still thin.",
            )

    if location:
        add_evidence(
            field_evidence,
            "market.settlement_strength",
            "public.site_locations",
            location,
            f"Settlement '{location.get('nearest_settlement') or 'unknown'}' contributes to market-strength reasoning.",
        )

    return (
        MarketEvidence(
            settlement_strength=str(extracted["settlement_strength"]),
            buyer_profile_fit=tuple(extracted["buyer_profile_fit"]),
            buyer_depth_estimate=str(extracted["buyer_depth_estimate"]),
            comparable_strength=comparable_strength,
            strong_buyer_fit_count=int(extracted["strong_buyer_fit_count"]),
            moderate_buyer_fit_count=int(extracted["moderate_buyer_fit_count"]),
            average_price_per_sqft_gbp=average_ppsf,
        ),
        field_evidence,
    )
