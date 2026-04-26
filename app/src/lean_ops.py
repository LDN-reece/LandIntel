"""Retired lean operations entrypoint.

Phase One source orchestration is GitHub Actions + Supabase through
`Run LandIntel Sources`. The old lean parcel workflow is deliberately
inert so it cannot mutate Supabase outside the Phase One model.
"""

from __future__ import annotations

import argparse


RETIREMENT_MESSAGE = (
    "app.src.lean_ops is retired for LandIntel Phase One. "
    "Use the Run LandIntel Sources GitHub Actions workflow instead. "
    "No Supabase writes are available from this entrypoint."
)


def build_parser() -> argparse.ArgumentParser:
    """Build a compatibility parser that refuses legacy lean commands."""

    parser = argparse.ArgumentParser(description="Retired LandIntel lean runner")
    parser.add_argument("command", nargs="?", default="retired")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    return parser


def main() -> int:
    """Return a hard failure so old lean commands cannot run silently."""

    build_parser().parse_args()
    print(RETIREMENT_MESSAGE)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
