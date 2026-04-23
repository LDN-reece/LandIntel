"""Chunked inspection and normalisation helpers for BGS borehole archives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite
from pathlib import Path
from typing import Any, Iterator
from zipfile import ZipFile

import pyogrio


BGS_BOREHOLE_SOURCE_NAME = "BGS Single Onshore Borehole Index"
BGS_BOREHOLE_DATASET_KEY = "bgs_single_onshore_borehole_index"
EXPECTED_BGS_BOREHOLE_FIELDS = (
    "QS",
    "NUMB",
    "BSUFF",
    "REGNO",
    "RT",
    "GRID_REFER",
    "EASTING",
    "NORTHING",
    "X",
    "Y",
    "CONFIDENTI",
    "STRTHEIGHT",
    "NAME",
    "LENGTH",
    "BGS_ID",
    "DATE_KNOWN",
    "DATE_K_TYP",
    "DATE_ENTER",
    "AGS_LOG_UR",
)


@dataclass(frozen=True)
class BgsBoreholeSourceInfo:
    """Lightweight metadata about the authoritative BGS borehole source."""

    archive_path: Path
    vsi_path: str
    internal_member_path: str
    source_file_name: str
    source_snapshot_date: date
    feature_count: int
    crs: str
    fields: tuple[str, ...]
    bounds: tuple[float, float, float, float]


@dataclass(frozen=True)
class BgsBoreholeBatch:
    """A chunk of raw BGS rows ready for staged database insertion."""

    offset: int
    row_count: int
    rows: list[dict[str, Any]]


def inspect_bgs_borehole_archive(archive_path: Path) -> BgsBoreholeSourceInfo:
    """Inspect the BGS archive and validate the expected shapefile exists."""

    if archive_path.suffix.lower() != ".zip":
        raise ValueError(f"Expected a ZIP archive, got: {archive_path}")
    if not archive_path.exists():
        raise FileNotFoundError(f"BGS archive not found: {archive_path}")

    internal_member_path = _find_borehole_member(archive_path)
    vsi_path = f"/vsizip/{archive_path}/{internal_member_path}"
    info = pyogrio.read_info(vsi_path)

    crs = str(info.get("crs") or "")
    fields = tuple(str(field) for field in info.get("fields", ()))
    bounds = tuple(float(value) for value in info.get("total_bounds", ()))
    feature_count = int(info.get("features") or 0)
    snapshot_date = _parse_snapshot_date(info.get("layer_metadata") or {})

    if crs != "EPSG:27700":
        raise ValueError(f"Expected EPSG:27700 borehole source, got {crs or 'unknown CRS'}.")
    if fields != EXPECTED_BGS_BOREHOLE_FIELDS:
        raise ValueError(
            "Unexpected BGS borehole field layout. "
            f"Expected {EXPECTED_BGS_BOREHOLE_FIELDS!r}, got {fields!r}."
        )
    if feature_count <= 0:
        raise ValueError("The BGS borehole source did not contain any rows.")
    if len(bounds) != 4:
        raise ValueError("The BGS borehole source did not expose valid spatial bounds.")

    return BgsBoreholeSourceInfo(
        archive_path=archive_path,
        vsi_path=vsi_path,
        internal_member_path=internal_member_path,
        source_file_name=Path(internal_member_path).name,
        source_snapshot_date=snapshot_date,
        feature_count=feature_count,
        crs=crs,
        fields=fields,
        bounds=bounds,
    )


def iter_bgs_borehole_batches(
    source: BgsBoreholeSourceInfo,
    *,
    batch_size: int,
) -> Iterator[BgsBoreholeBatch]:
    """Stream the BGS archive in deterministic chunks for raw staging loads."""

    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero.")

    for offset in range(0, source.feature_count, batch_size):
        frame = pyogrio.read_dataframe(
            source.vsi_path,
            skip_features=offset,
            max_features=batch_size,
            columns=list(EXPECTED_BGS_BOREHOLE_FIELDS),
        )
        records = frame.to_dict(orient="records")
        rows = [
            _normalise_raw_borehole_row(
                record,
                source_row_number=offset + row_index + 1,
            )
            for row_index, record in enumerate(records)
        ]
        yield BgsBoreholeBatch(offset=offset, row_count=len(rows), rows=rows)


def _find_borehole_member(archive_path: Path) -> str:
    """Return the archive member path for the borehole shapefile."""

    with ZipFile(archive_path) as zip_file:
        members = [member for member in zip_file.namelist() if member.lower().endswith("/borehole.shp")]
    if not members:
        raise RuntimeError("The BGS archive did not contain borehole.shp.")
    if len(members) > 1:
        raise RuntimeError(f"Expected one borehole.shp in the archive, found {len(members)}.")
    return members[0]


def _parse_snapshot_date(layer_metadata: dict[str, Any]) -> date:
    """Read the source snapshot date from the shapefile metadata."""

    raw_value = layer_metadata.get("DBF_DATE_LAST_UPDATE")
    if raw_value:
        try:
            return date.fromisoformat(str(raw_value))
        except ValueError:
            pass
    raise ValueError("The BGS archive did not expose a valid snapshot date.")


def _normalise_raw_borehole_row(record: dict[str, Any], *, source_row_number: int) -> dict[str, Any]:
    """Convert a source record into the raw-staging payload shape."""

    return {
        "source_row_number": source_row_number,
        "qs": _clean_text(record.get("QS")),
        "numb": _safe_int(record.get("NUMB")),
        "bsuff": _clean_text(record.get("BSUFF")),
        "regno": _clean_text(record.get("REGNO")),
        "rt": _clean_text(record.get("RT")),
        "grid_refer": _clean_text(record.get("GRID_REFER")),
        "easting": _clean_text(record.get("EASTING")),
        "northing": _clean_text(record.get("NORTHING")),
        "x": _safe_float(record.get("X")),
        "y": _safe_float(record.get("Y")),
        "confidenti": _clean_text(record.get("CONFIDENTI")),
        "strtheight": _safe_float(record.get("STRTHEIGHT")),
        "name": _clean_text(record.get("NAME")),
        "length": _safe_float(record.get("LENGTH")),
        "bgs_id": _safe_int(record.get("BGS_ID")),
        "date_known": _safe_int(record.get("DATE_KNOWN")),
        "date_k_typ": _clean_text(record.get("DATE_K_TYP")),
        "date_enter": _safe_date(record.get("DATE_ENTER")),
        "ags_log_ur": _clean_text(record.get("AGS_LOG_UR")),
        "raw_record": {
            field: _json_safe(record.get(field))
            for field in EXPECTED_BGS_BOREHOLE_FIELDS
        },
        "geom": record.get("geometry"),
    }


def _clean_text(value: Any) -> str | None:
    """Return a trimmed string or None for blank-like values."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: Any) -> int | None:
    """Convert common numeric inputs into integers when safe."""

    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            return None
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _safe_float(value: Any) -> float | None:
    """Convert a source value into a float when safe."""

    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, (int, float)):
        numeric_value = float(value)
        return numeric_value if isfinite(numeric_value) else None
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric_value = float(text)
    except ValueError:
        return None
    return numeric_value if isfinite(numeric_value) else None


def _safe_date(value: Any) -> date | None:
    """Convert a shapefile date-like value into a Python date."""

    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().date()
        except Exception:
            pass
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _json_safe(value: Any) -> Any:
    """Convert source values into JSON-safe primitives."""

    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().isoformat()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value
