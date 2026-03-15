from __future__ import annotations

"""Compatibility wrapper for callers still using the old semantic framework entrypoint.

This module no longer owns a separate output contract. It forwards execution to the
structured semantic area runners plus the unified semantic report layer.
"""

import argparse
from pathlib import Path

from Scripts.run_semantic_lifecycle import main as lifecycle_main
from Scripts.run_semantic_ordertype import main as ordertype_main
from Scripts.run_semantic_session import main as session_main
from Scripts.run_semantic_tradedir import main as tradedir_main
from Scripts.runtime import DEFAULT_DATA_ROOT, DEFAULT_LOG_ROOT, print_scaffold_plan
from Scripts.semantic_report import main as report_main

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compatibility wrapper for the structured 2026 semantic verification framework."
    )
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
    parser.add_argument("--dates", help="Comma-separated trade dates in YYYYMMDD or YYYY-MM-DD format.")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--latest-days", action="store_true")
    parser.add_argument("--stage-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_DQA_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_AUDITS_ROOT)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--sample-only", action="store_true")
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def build_runner_argv(args: argparse.Namespace) -> list[str]:
    argv = [
        "--year",
        str(args.year),
        "--input-root",
        str(args.stage_root),
        "--output-root",
        str(args.output_root),
        "--research-root",
        str(args.research_root),
        "--log-root",
        str(args.log_root),
    ]
    if args.dates:
        argv.extend(["--dates", args.dates])
    if args.max_days:
        argv.extend(["--max-days", str(args.max_days)])
    if args.latest_days:
        argv.append("--latest-days")
    if args.overwrite_existing:
        argv.append("--overwrite-existing")
    if args.limit_rows:
        argv.extend(["--limit-rows", str(args.limit_rows)])
    if args.sample_only:
        argv.append("--sample-only")
    return argv


def run_entrypoint(entrypoint: callable, argv: list[str]) -> int:
    import sys

    original_argv = sys.argv
    try:
        sys.argv = [original_argv[0], *argv]
        return entrypoint()
    finally:
        sys.argv = original_argv


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_semantic_framework",
            purpose="Compatibility wrapper that runs the structured semantic lifecycle/tradedir/ordertype/session probes plus summary aggregation.",
            responsibilities=[
                "Dispatch the four semantic area runners with a shared CLI shape.",
                "Preserve a single top-level entrypoint for callers that still expect run_semantic_framework.",
                "Finish by materializing the unified semantic report and admissibility bridge.",
            ],
            inputs=[
                "candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet",
                "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "dqa/semantic/year=<year>/semantic_*",
                "Research/Audits/semantic_<year>_summary.md",
            ],
        )
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")

    runner_argv = build_runner_argv(args)
    for entrypoint in (lifecycle_main, tradedir_main, ordertype_main, session_main):
        run_entrypoint(entrypoint, runner_argv)

    report_argv = [
        "--year",
        str(args.year),
        "--input-root",
        str(args.output_root),
        "--research-root",
        str(args.research_root),
    ]
    return run_entrypoint(report_main, report_argv)


if __name__ == "__main__":
    raise SystemExit(main())
