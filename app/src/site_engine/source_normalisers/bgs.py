"""Normalise BGS and ground-investigation indicators."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, as_bool, as_int, row_payload
from src.site_engine.site_evidence_schema import BgsEvidence


def normalise_bgs(constraint_records: list[dict[str, Any]]) -> tuple[BgsEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    borehole_site = 0
    borehole_100m = 0
    borehole_250m = 0
    borehole_500m = 0
    site_investigation_overlap = False
    opencast_overlap = False
    water_well_presence = False
    aquifer_presence = False
    geophysical_logs_presence = False
    drillcore_presence = False

    for row in constraint_records:
        payload = row_payload(row)
        constraint_type = str(row.get("constraint_type") or "").lower()
        if constraint_type == "borehole":
            borehole_site += as_int(payload.get("count_site"), 0) or 1
            borehole_100m += as_int(payload.get("count_100m"), 0)
            borehole_250m += as_int(payload.get("count_250m"), 0)
            borehole_500m += as_int(payload.get("count_500m"), 0)
            add_evidence(
                field_evidence,
                "bgs.borehole_count_site",
                "public.site_constraints",
                row,
                "BGS borehole evidence indicates prior subsurface investigation nearby or on site.",
            )
        elif constraint_type == "site_investigation":
            site_investigation_overlap = True
            add_evidence(
                field_evidence,
                "bgs.site_investigation_overlap",
                "public.site_constraints",
                row,
                "Site investigation evidence overlaps the site area.",
            )
        elif constraint_type == "opencast":
            opencast_overlap = str(row.get("severity") or "").lower() in {"medium", "high"} or as_bool(payload.get("overlap"))
            add_evidence(
                field_evidence,
                "bgs.opencast_overlap",
                "public.site_constraints",
                row,
                "Opencast or mining legacy evidence is linked to the site.",
            )
        elif constraint_type == "water_well":
            water_well_presence = True
            add_evidence(
                field_evidence,
                "bgs.water_well_presence",
                "public.site_constraints",
                row,
                "Water well evidence is linked to the site area.",
            )
        elif constraint_type == "aquifer":
            aquifer_presence = True
            add_evidence(
                field_evidence,
                "bgs.aquifer_presence",
                "public.site_constraints",
                row,
                "Aquifer evidence suggests groundwater sensitivity.",
            )
        elif constraint_type == "geophysical_logs":
            geophysical_logs_presence = True
            add_evidence(
                field_evidence,
                "bgs.geophysical_logs_presence",
                "public.site_constraints",
                row,
                "Geophysical logging evidence is linked to the site.",
            )
        elif constraint_type == "drillcore":
            drillcore_presence = True
            add_evidence(
                field_evidence,
                "bgs.drillcore_presence",
                "public.site_constraints",
                row,
                "Drillcore evidence is linked to the site.",
            )

    return (
        BgsEvidence(
            borehole_count_site=borehole_site,
            borehole_count_100m=borehole_100m,
            borehole_count_250m=borehole_250m,
            borehole_count_500m=borehole_500m,
            site_investigation_overlap=site_investigation_overlap,
            opencast_overlap=opencast_overlap,
            water_well_presence=water_well_presence,
            aquifer_presence=aquifer_presence,
            geophysical_logs_presence=geophysical_logs_presence,
            drillcore_presence=drillcore_presence,
        ),
        field_evidence,
    )

