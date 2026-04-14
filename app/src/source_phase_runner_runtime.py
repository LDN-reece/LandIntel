"""Runtime wrapper that points the source phase runner at the quoted manifest."""

from __future__ import annotations

from pathlib import Path

from src import source_phase_runner as base_runner

base_runner.MANIFEST_PATH = Path(__file__).resolve().parents[1] / "config" / "scotland_core_sources_runtime.yaml"


if __name__ == "__main__":
    raise SystemExit(base_runner.main())
