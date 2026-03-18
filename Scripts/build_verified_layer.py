from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    append_jsonl,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_VERIFIED_ROOT = DEFAULT_DATA_ROOT / "verified"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_REPORTS_ROOT = REPO_ROOT / "Research" / "Reports"
DEFAULT_POLICY_PATH = REPO_ROOT / "Research" / "Validation" / "verified_field_policy_2026-03-15.json"
PROGRESS_EMIT_SECONDS = 5.0


@dataclass(frozen=True)
class VerifiedTask:
    year: str
    table_name: str
    date: str
    input_paths: tuple[str, ...]
    output_path: str
    allowed_columns: tuple[str, ...]
    excluded_columns: tuple[str, ...]

    @property
    def task_key(self) -> str:
        return f"{self.date}:{self.table_name}"

    @property
    def verified_table_name(self) -> str:
        return f"verified_{self.table_name}"


def default_workers() -> int:
    cpu_count = os.cpu_count() or 1
    return max(1, min(8, cpu_count - 1))


def default_executor_mode() -> str:
    return "thread" if os.uname().sysname == "Darwin" else "auto"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize verified v1 tables from admit-now fields only.")
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
    parser.add_argument(
        "--table",
        choices=["all", "orders", "trades"],
        default="all",
        help="Verified logical table to build.",
    )
    parser.add_argument(
        "--stage-root",
        type=Path,
        default=DEFAULT_STAGE_ROOT,
        help="Root directory for candidate_cleaned parquet inputs.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_VERIFIED_ROOT,
        help="Root directory for verified outputs.",
    )
    parser.add_argument(
        "--research-root",
        type=Path,
        default=DEFAULT_RESEARCH_REPORTS_ROOT,
        help="Root directory for research-facing verified summaries.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for verified logs.",
    )
    parser.add_argument(
        "--policy-path",
        type=Path,
        default=DEFAULT_POLICY_PATH,
        help="Path to the verified field policy JSON.",
    )
    parser.add_argument("--dates", help="Comma-separated trade dates in YYYYMMDD or YYYY-MM-DD format.")
    parser.add_argument("--max-days", type=int, default=0, help="Optional limit on number of dates to process.")
    parser.add_argument(
        "--latest-days",
        action="store_true",
        help="When used with --max-days, select latest dates instead of earliest.",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint and skip completed tasks.")
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Overwrite existing verified parquet output for selected tasks.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=default_workers(),
        help="Number of verified table tasks to process in parallel.",
    )
    parser.add_argument(
        "--executor",
        choices=["auto", "process", "thread"],
        default=default_executor_mode(),
        help="Executor mode for parallel task dispatch.",
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def canonical_date(value: str) -> str:
    digits = value.replace("-", "").strip()
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(f"Invalid date token: {value}")
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def read_policy(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def table_policy_columns(policy: dict[str, Any], table_name: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    allowed: list[str] = []
    excluded: list[str] = []
    for column_name, spec in policy[table_name].items():
        if spec["bucket"] == "admit_now":
            allowed.append(column_name)
        else:
            excluded.append(column_name)
    return tuple(allowed), tuple(excluded)


def parse_selected_dates(args: argparse.Namespace, table_name: str) -> list[str]:
    table_root = args.stage_root / table_name
    available_dates = sorted(
        path.name.split("=", 1)[1]
        for path in table_root.glob("date=*")
        if path.name.split("=", 1)[1].startswith(f"{args.year}-")
    )
    if args.dates:
        selected_dates = [canonical_date(token) for token in args.dates.split(",") if token.strip()]
        return [value for value in selected_dates if value in available_dates]
    if args.max_days and args.latest_days:
        return available_dates[-args.max_days :]
    if args.max_days:
        return available_dates[: args.max_days]
    return available_dates


def output_path_for_task(output_root: Path, year: str, table_name: str, trade_date: str) -> Path:
    verified_table_name = f"verified_{table_name}"
    return output_root / verified_table_name / f"year={year}" / f"date={trade_date}" / "part-00000.parquet"


def discover_tasks(args: argparse.Namespace, policy: dict[str, Any]) -> list[VerifiedTask]:
    tables = ("orders", "trades") if args.table == "all" else (args.table,)
    tasks: list[VerifiedTask] = []
    for table_name in tables:
        allowed_columns, excluded_columns = table_policy_columns(policy, table_name)
        for trade_date in parse_selected_dates(args, table_name):
            input_paths = tuple(str(path) for path in sorted((args.stage_root / table_name / f"date={trade_date}").glob("*.parquet")))
            if not input_paths:
                continue
            tasks.append(
                VerifiedTask(
                    year=str(args.year),
                    table_name=table_name,
                    date=trade_date,
                    input_paths=input_paths,
                    output_path=str(output_path_for_task(args.output_root, str(args.year), table_name, trade_date)),
                    allowed_columns=allowed_columns,
                    excluded_columns=excluded_columns,
                )
            )
    return tasks


def manifest_root(output_root: Path, year: str) -> Path:
    return output_root / "manifests" / f"year={year}"


def checkpoint_path(output_root: Path, year: str) -> Path:
    return manifest_root(output_root, year) / "checkpoint.json"


def heartbeat_path(output_root: Path, year: str) -> Path:
    return manifest_root(output_root, year) / "heartbeat.json"


def partitions_jsonl_path(output_root: Path, year: str) -> Path:
    return manifest_root(output_root, year) / "verified_partitions.jsonl"


def summary_json_path(output_root: Path, year: str) -> Path:
    return manifest_root(output_root, year) / "summary.json"


def append_partition_row(path: Path, payload: dict[str, Any]) -> None:
    append_jsonl(path, payload)


def read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def build_executor(mode: str, max_workers: int, logger: Any) -> tuple[Any, str]:
    if mode == "thread":
        return ThreadPoolExecutor(max_workers=max_workers), "thread"
    if mode == "process":
        return ProcessPoolExecutor(max_workers=max_workers), "process"
    try:
        return ProcessPoolExecutor(max_workers=max_workers), "process"
    except (OSError, PermissionError) as exc:
        logger.warning("ProcessPoolExecutor unavailable (%s); falling back to ThreadPoolExecutor.", exc)
        return ThreadPoolExecutor(max_workers=max_workers), "thread"


def input_rows(paths: tuple[str, ...]) -> int:
    total_rows = 0
    for path in paths:
        total_rows += pq.ParquetFile(path).metadata.num_rows
    return int(total_rows)


def research_time_grade_for_year(year: str) -> str:
    return "fine_ok" if year == "2026" else "coarse_only"


def process_task(task: VerifiedTask) -> dict[str, Any]:
    output_path = Path(task.output_path)
    ensure_dir(output_path.parent)
    scan = pl.scan_parquet(list(task.input_paths))
    available_columns = set(scan.collect_schema().names())
    missing_columns = [column for column in task.allowed_columns if column not in available_columns]
    if missing_columns:
        raise ValueError(f"Missing admit-now columns for {task.task_key}: {missing_columns}")
    row_count = input_rows(task.input_paths)
    scan.select(list(task.allowed_columns)).sink_parquet(output_path)
    file_size = output_path.stat().st_size
    return {
        "task_key": task.task_key,
        "date": task.date,
        "year": task.year,
        "table_name": task.table_name,
        "verified_table_name": task.verified_table_name,
        "output_path": str(output_path),
        "input_paths": list(task.input_paths),
        "input_row_count": input_rows(task.input_paths),
        "output_row_count": row_count,
        "output_bytes": file_size,
        "included_columns": list(task.allowed_columns),
        "excluded_columns": list(task.excluded_columns),
        "verified_policy_version": "2026-03-15",
        "source_layer": "candidate_cleaned",
        "admission_rule": "admit_now_only",
        "contains_caveat_fields": False,
        "reference_join_applied": False,
        "research_time_grade": research_time_grade_for_year(task.year),
        "generated_at": iso_utc_now(),
    }


def build_summary(*, state: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_table: dict[str, dict[str, Any]] = {}
    for row in rows:
        table_summary = by_table.setdefault(
            row["verified_table_name"],
            {
                "partitions": 0,
                "rows": 0,
                "bytes": 0,
                "dates": [],
            },
        )
        table_summary["partitions"] += 1
        table_summary["rows"] += int(row["output_row_count"])
        table_summary["bytes"] += int(row["output_bytes"])
        table_summary["dates"].append(row["date"])
    for table_summary in by_table.values():
        table_summary["dates"] = sorted(table_summary["dates"])
    return {
        "pipeline": "build_verified_layer",
        "status": state["status"],
        "year": state["year"],
        "generated_at": iso_utc_now(),
        "completed_count": state["completed_count"],
        "failed_count": state["failed_count"],
        "pending_count": state["pending_count"],
        "workers": state["workers"],
        "executor_mode": state["executor_mode"],
        "tables": by_table,
    }


def write_checkpoint(ckpt_path: Path, hb_path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = iso_utc_now()
    write_json(ckpt_path, state)
    write_json(
        hb_path,
        {
            "status": state["status"],
            "year": state["year"],
            "updated_at": state["updated_at"],
            "completed_count": state["completed_count"],
            "failed_count": state["failed_count"],
            "pending_count": state["pending_count"],
            "active_task_key": state.get("active_task_key"),
            "active_task_keys": state.get("active_task_keys", []),
            "workers": state.get("workers"),
            "executor_mode": state.get("executor_mode"),
        },
    )


def write_markdown(path: Path, *, year: str, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    lines = [
        f"# Verified Layer {year}",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- status: {summary['status']}",
        f"- completed_count: {summary['completed_count']}",
        f"- failed_count: {summary['failed_count']}",
        "",
        "## Tables",
        "",
    ]
    for table_name, table_summary in sorted(summary["tables"].items()):
        lines.extend(
            [
                f"### {table_name}",
                f"- partitions: {table_summary['partitions']}",
                f"- rows: {table_summary['rows']}",
                f"- bytes: {table_summary['bytes']}",
                f"- first_date: {table_summary['dates'][0] if table_summary['dates'] else 'n/a'}",
                f"- last_date: {table_summary['dates'][-1] if table_summary['dates'] else 'n/a'}",
                "",
            ]
        )
    if rows:
        lines.extend(["## Sample Partition Rows", ""])
        for row in rows[:4]:
            lines.extend(
                [
                    f"### {row['verified_table_name']} {row['date']}",
                    f"- output_row_count: {row['output_row_count']}",
                    f"- included_columns: {', '.join(row['included_columns'])}",
                    f"- excluded_columns: {', '.join(row['excluded_columns'])}",
                    f"- research_time_grade: {row['research_time_grade']}",
                    "",
                ]
            )
    ensure_dir(path.parent)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def state_signature(state: dict[str, Any]) -> tuple[Any, ...]:
    return (
        state.get("status"),
        state.get("completed_count"),
        state.get("failed_count"),
        state.get("pending_count"),
        tuple(state.get("active_task_keys", [])),
    )


def reset_manifest_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="build_verified_layer",
            purpose="Materialize research-ready tables from mechanically safe and semantically verified fields.",
            responsibilities=[
                "Read candidate_cleaned inputs and verified admission policy decisions.",
                "Build verified_orders and verified_trades from admit-now fields only.",
                "Keep manifest, policy versions, and excluded-field lists in sync.",
                "Defer verified_trade_order_linkage and broker enrichment until a later phase.",
            ],
            inputs=[
                "candidate_cleaned partitions.",
                "Semantic verification matrix and DQA results.",
                "Verified admission matrix and verified field policy.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/verified",
                "Research/Reports verified-layer summaries.",
                "Verified manifest with year-level caveats and policy versions.",
            ],
        )
        return 0

    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")

    policy = read_policy(args.policy_path)
    tasks = discover_tasks(args, policy)
    if not tasks:
        raise SystemExit("No verified tasks matched the requested selection.")

    logger = configure_logger("build_verified_layer", args.log_root / f"build_verified_layer_{args.year}.log")
    ckpt_path = checkpoint_path(args.output_root, str(args.year))
    hb_path = heartbeat_path(args.output_root, str(args.year))
    parts_path = partitions_jsonl_path(args.output_root, str(args.year))
    summary_path = summary_json_path(args.output_root, str(args.year))
    report_path = args.research_root / f"verified_layer_{args.year}.md"

    existing_rows = read_jsonl_rows(parts_path)
    completed_task_keys = {row["task_key"] for row in existing_rows}

    if not args.resume and args.overwrite_existing:
        reset_manifest_files([parts_path, ckpt_path, hb_path, summary_path])
        existing_rows = []
        completed_task_keys = set()

    state = {
        "status": "running",
        "year": str(args.year),
        "started_at": iso_utc_now(),
        "updated_at": iso_utc_now(),
        "workers": args.workers,
        "executor_mode": args.executor,
        "completed_task_keys": sorted(completed_task_keys),
        "completed_count": len(completed_task_keys),
        "failed_tasks": {},
        "failed_count": 0,
        "pending_count": max(0, len(tasks) - len(completed_task_keys)),
        "active_task_key": None,
        "active_task_keys": [],
    }

    if args.resume and ckpt_path.exists():
        state = json.loads(ckpt_path.read_text(encoding="utf-8"))
        state["status"] = "running"
        state["workers"] = args.workers
        state["executor_mode"] = args.executor
        state["active_task_key"] = None
        state["active_task_keys"] = []
        completed_task_keys = set(state.get("completed_task_keys", []))

    for task in tasks:
        output_path = Path(task.output_path)
        if output_path.exists() and not args.overwrite_existing:
            completed_task_keys.add(task.task_key)

    state["completed_task_keys"] = sorted(completed_task_keys)
    state["completed_count"] = len(completed_task_keys)
    state["pending_count"] = max(0, len(tasks) - len(completed_task_keys))
    write_checkpoint(ckpt_path, hb_path, state)

    pending_tasks = [task for task in tasks if task.task_key not in completed_task_keys]
    future_to_task: dict[Any, VerifiedTask] = {}
    executor, resolved_mode = build_executor(args.executor, args.workers, logger)
    state["executor_mode"] = resolved_mode
    write_checkpoint(ckpt_path, hb_path, state)
    last_state_signature = state_signature(state)

    with executor:
        task_iter = iter(pending_tasks)

        def submit_until_capacity() -> None:
            while len(future_to_task) < args.workers:
                try:
                    task = next(task_iter)
                except StopIteration:
                    break
                logger.info("Verified %s queued", task.task_key)
                future_to_task[executor.submit(process_task, task)] = task

        submit_until_capacity()
        pending_futures = set(future_to_task)
        while pending_futures:
            done_futures, pending_futures = wait(
                pending_futures,
                timeout=PROGRESS_EMIT_SECONDS,
                return_when=FIRST_COMPLETED,
            )
            state["active_task_keys"] = sorted(future_to_task[future].task_key for future in pending_futures)
            state["active_task_key"] = state["active_task_keys"][0] if state["active_task_keys"] else None
            state["pending_count"] = max(
                0,
                len(tasks) - len(completed_task_keys) - len(state["failed_tasks"]) - len(state["active_task_keys"]),
            )
            current_signature = state_signature(state)
            if done_futures or current_signature != last_state_signature:
                write_checkpoint(ckpt_path, hb_path, state)
                last_state_signature = current_signature

            for future in done_futures:
                task = future_to_task.pop(future)
                try:
                    row = future.result()
                    append_partition_row(parts_path, row)
                    existing_rows.append(row)
                    completed_task_keys.add(task.task_key)
                    logger.info(
                        "Verified %s complete: rows=%s bytes=%s",
                        task.task_key,
                        row["output_row_count"],
                        row["output_bytes"],
                    )
                except Exception as exc:
                    state["failed_tasks"][task.task_key] = str(exc)
                    logger.error("Verified %s failed: %s", task.task_key, exc)
            if done_futures:
                submit_until_capacity()
                pending_futures = set(future_to_task)
                state["completed_task_keys"] = sorted(completed_task_keys)
                state["completed_count"] = len(completed_task_keys)
                state["failed_count"] = len(state["failed_tasks"])
                state["active_task_keys"] = sorted(future_to_task[future].task_key for future in pending_futures)
                state["active_task_key"] = state["active_task_keys"][0] if state["active_task_keys"] else None
                state["pending_count"] = max(
                    0,
                    len(tasks) - state["completed_count"] - state["failed_count"] - len(state["active_task_keys"]),
                )
                write_checkpoint(ckpt_path, hb_path, state)
                last_state_signature = state_signature(state)

    state["status"] = "completed" if not state["failed_tasks"] else "completed_with_failures"
    state["active_task_keys"] = []
    state["active_task_key"] = None
    summary = build_summary(state=state, rows=existing_rows)
    write_json(summary_path, summary)
    write_markdown(report_path, year=str(args.year), rows=existing_rows, summary=summary)
    write_checkpoint(ckpt_path, hb_path, state)
    logger.info("Verified layer build complete for %s with %s partitions", args.year, len(existing_rows))
    return 0 if not state["failed_tasks"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
