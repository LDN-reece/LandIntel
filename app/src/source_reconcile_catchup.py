"""Catch-up scan runner aligned to the live ingest_runs schema."""

from __future__ import annotations

import argparse
import traceback

from config.settings import get_settings
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.source_reconcile_incremental import IncrementalReconcileRunner


class IncrementalReconcileCatchupRunner(IncrementalReconcileRunner):
    """Run catch-up scans using the live ingest run timestamp columns."""

    def reconcile_catchup_scan(self, *, source_family: str | None = None) -> dict[str, int]:
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="reconcile_catchup_scan",
                source_name="landintel.source_reconcile_state",
                status="running",
                metadata={"source_family": source_family},
            )
        )
        try:
            result = {"planning": 0, "hla": 0}
            if source_family in (None, "planning"):
                latest_planning = self._latest_successful_ingest_run("ingest_planning_history")
                if latest_planning:
                    result["planning"] = int(
                        self.database.scalar(
                            "select landintel.queue_planning_reconcile_from_ingest(cast(:run_id as uuid))",
                            {"run_id": latest_planning["id"]},
                        )
                    )
            if source_family in (None, "hla"):
                latest_hla = self._latest_successful_ingest_run("ingest_hla")
                if latest_hla:
                    result["hla"] = int(
                        self.database.scalar(
                            "select landintel.queue_hla_reconcile_from_ingest(cast(:run_id as uuid))",
                            {"run_id": latest_hla["id"]},
                        )
                    )
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=result["planning"] + result["hla"],
                    records_loaded=result["planning"] + result["hla"],
                    records_retained=result["planning"] + result["hla"],
                    metadata=result,
                    finished=True,
                ),
            )
            self.logger.info("incremental_reconcile_catchup_completed", extra=result)
            return result
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def _latest_successful_ingest_run(self, run_type: str) -> dict[str, object] | None:
        return self.database.fetch_one(
            """
            select id
            from public.ingest_runs
            where run_type = :run_type
              and status = 'success'
            order by finished_at desc nulls last, started_at desc nulls last, id desc
            limit 1
            """,
            {"run_type": run_type},
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the LandIntel incremental reconcile catch-up scan.")
    parser.add_argument("command", choices=("reconcile-catchup-scan",))
    parser.add_argument("--source-family", choices=("planning", "hla"), default=None)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = IncrementalReconcileCatchupRunner(settings, logger)
    try:
        runner.reconcile_catchup_scan(source_family=args.source_family)
        runner.logger.info("incremental_reconcile_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        runner.logger.exception("incremental_reconcile_command_failed", extra={"command": args.command})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())
