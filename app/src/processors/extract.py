"""Archive extraction helpers."""

from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile


def extract_archive(archive_path: Path, destination_dir: Path) -> list[Path]:
    """Extract a ZIP archive and return candidate spatial files."""

    destination_dir.mkdir(parents=True, exist_ok=True)
    if archive_path.suffix.lower() != ".zip":
        raise ValueError(f"Unsupported archive format: {archive_path.suffix}")

    with ZipFile(archive_path) as zip_file:
        zip_file.extractall(destination_dir)
    return find_spatial_candidates(destination_dir)


def find_spatial_candidates(search_dir: Path) -> list[Path]:
    """Return likely spatial datasets from an extracted archive."""

    candidates: list[Path] = []
    for pattern in ("*.shp", "*.gpkg", "*.geojson", "*.json"):
        candidates.extend(sorted(search_dir.rglob(pattern)))
    for directory in sorted(search_dir.rglob("*.gdb")):
        if directory.is_dir():
            candidates.append(directory)
    return candidates


def choose_preferred_candidate(candidates: list[Path]) -> Path:
    """Choose the best spatial candidate for ingestion."""

    if not candidates:
        raise RuntimeError("No spatial dataset candidates were found after extraction.")

    def score(path: Path) -> tuple[int, str]:
        name = path.name.lower()
        if "27700" in name or "bng" in name or "british" in name:
            return (0, name)
        if path.suffix.lower() == ".shp":
            return (1, name)
        if path.suffix.lower() == ".gpkg":
            return (2, name)
        if path.suffix.lower() in {".geojson", ".json"}:
            return (3, name)
        return (4, name)

    return sorted(candidates, key=score)[0]

