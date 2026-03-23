from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from collections import deque
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
    scratch_root: str | None = None
    input_read_mode: str = "direct_stage"

    @property
    def task_key(self) -> str:
        return f"{self.date}:{self.table_name}"

    @property
    def verified_table_name(self) -> str:
        return f"verified_{self.table_name}"


@dataclass(frozen=True)
class PrefetchResult:
    effective_input_paths: tuple[str, ...]
    scratch_input_paths: tuple[str, ...]
    prefetch_copied_files: int
    prefetch_reused_files: int
    prefetch_bytes_copied: int
    prefetch_seconds: float


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
    parser.add_argument("--start-date", help="Inclusive lower bound trade date in YYYYMMDD or YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="Inclusive upper bound trade date in YYYYMMDD or YYYY-MM-DD format.")
    parser.add_argument("--max-days", type=int, default=0, help="Optional limit on number of dates to process.")
    parser.add_argument(
        "--latest-days",
        action="store_true",
        help="When used with --max-days, select latest dates instead of earliest.",
    )
    parser.add_argument(
        "--date-batch-size",
        type=int,
        default=0,
        help="Optional number of dates per contiguous batch after filtering.",
    )
    parser.add_argument(
        "--date-batch-index",
        type=int,
        default=1,
        help="1-based batch index used with --date-batch-size.",
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
    parser.add_argument(
        "--scratch-root",
        type=Path,
        help="Optional local scratch root used to prefetch selected parquet inputs before materialization.",
    )
    parser.add_argument(
        "--scratch-table",
        choices=["orders", "trades", "all"],
        default="orders",
        help="Which logical table should use --scratch-root prefetching.",
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def canonical_date(value: str) -> str:
    digits = value.replace("-", "").strip()
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(f"Invalid date token: {value}")
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def unique_preserving_order(values: list[str]) -> list[str]:
    ordered_values: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        if value in seen_values:
            continue
        seen_values.add(value)
        ordered_values.append(value)
    return ordered_values


def explicit_dates_from_args(args: argparse.Namespace) -> list[str]:
    if not args.dates:
        return []
    return unique_preserving_order([canonical_date(token) for token in args.dates.split(",") if token.strip()])


def validate_args(args: argparse.Namespace) -> None:
    if args.start_date:
        args.start_date = canonical_date(args.start_date)
    if args.end_date:
        args.end_date = canonical_date(args.end_date)
    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise SystemExit("--start-date must be earlier than or equal to --end-date.")
    if args.max_days < 0:
        raise SystemExit("--max-days must be >= 0.")
    if args.date_batch_size < 0:
        raise SystemExit("--date-batch-size must be >= 0.")
    if args.date_batch_index < 1:
        raise SystemExit("--date-batch-index must be >= 1.")
    if args.date_batch_size == 0 and args.date_batch_index != 1:
        raise SystemExit("--date-batch-index requires --date-batch-size.")


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
    if args.start_date:
        available_dates = [value for value in available_dates if value >= args.start_date]
    if args.end_date:
        available_dates = [value for value in available_dates if value <= args.end_date]
    if args.dates:
        selected_dates = [value for value in explicit_dates_from_args(args) if value in available_dates]
    else:
        selected_dates = available_dates
    if args.max_days and args.latest_days:
        selected_dates = selected_dates[-args.max_days :]
    elif args.max_days:
        selected_dates = selected_dates[: args.max_days]
    if args.date_batch_size:
        if not selected_dates:
            return []
        total_batches = (len(selected_dates) + args.date_batch_size - 1) // args.date_batch_size
        if args.date_batch_index > total_batches:
            raise ValueError(
                f"--date-batch-index={args.date_batch_index} is out of range for "
                f"{len(selected_dates)} selected dates and --date-batch-size={args.date_batch_size}."
            )
        batch_start = (args.date_batch_index - 1) * args.date_batch_size
        selected_dates = selected_dates[batch_start : batch_start + args.date_batch_size]
    return selected_dates


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
                    scratch_root=(
                        str(args.scratch_root)
                        if args.scratch_root and (args.scratch_table == "all" or args.scratch_table == table_name)
                        else None
                    ),
                    input_read_mode=(
                        "scratch_prefetch"
                        if args.scratch_root and (args.scratch_table == "all" or args.scratch_table == table_name)
                        else "direct_stage"
                    ),
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


def is_partial_selection(args: argparse.Namespace) -> bool:
    return bool(
        args.table != "all"
        or args.dates
        or args.start_date
        or args.end_date
        or args.max_days
        or args.date_batch_size
    )


def selection_label(args: argparse.Namespace) -> str:
    if not is_partial_selection(args):
        return "full"
    parts: list[str] = []
    if args.table != "all":
        parts.append(args.table)
    explicit_dates = explicit_dates_from_args(args)
    if explicit_dates:
        if len(explicit_dates) <= 3:
            compact_dates = "_".join(value.replace("-", "") for value in explicit_dates)
            parts.append(f"dates_{compact_dates}")
        else:
            parts.append(f"dates_{len(explicit_dates)}")
            parts.append(f"first_{explicit_dates[0]}")
            parts.append(f"last_{explicit_dates[-1]}")
    if args.start_date:
        parts.append(f"from_{args.start_date}")
    if args.end_date:
        parts.append(f"to_{args.end_date}")
    if args.max_days:
        edge = "latest" if args.latest_days else "first"
        parts.append(f"{edge}_{args.max_days}d")
    if args.date_batch_size:
        parts.append(f"batch_{args.date_batch_index}_of_size_{args.date_batch_size}")
    return "__".join(parts)


def report_path_for_run(research_root: Path, args: argparse.Namespace) -> Path:
    label = selection_label(args)
    if label == "full":
        return research_root / f"verified_layer_{args.year}.md"
    return research_root / f"verified_layer_{args.year}__{label}.md"


def build_selection_metadata(args: argparse.Namespace, tasks: list[VerifiedTask]) -> dict[str, Any]:
    selected_dates = sorted({task.date for task in tasks})
    explicit_dates = explicit_dates_from_args(args)
    return {
        "label": selection_label(args),
        "is_partial": is_partial_selection(args),
        "table": args.table,
        "explicit_dates": explicit_dates,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "max_days": args.max_days or None,
        "latest_days": bool(args.latest_days),
        "date_batch_size": args.date_batch_size or None,
        "date_batch_index": args.date_batch_index if args.date_batch_size else None,
        "selected_date_count": len(selected_dates),
        "selected_task_count": len(tasks),
        "first_selected_date": selected_dates[0] if selected_dates else None,
        "last_selected_date": selected_dates[-1] if selected_dates else None,
    }


def append_partition_row(path: Path, payload: dict[str, Any]) -> None:
    append_jsonl(path, payload)


def write_jsonl_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        if path.exists():
            path.unlink()
        return
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def temp_output_path(output_path: Path) -> Path:
    return output_path.with_name(f".{output_path.name}.tmp")


def temp_prefetch_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp")


def scratch_input_path(task: VerifiedTask, source_path: Path) -> Path:
    if task.scratch_root is None:
        raise ValueError(f"Scratch root is not configured for {task.task_key}")
    return (
        Path(task.scratch_root)
        / "verified_prefetch"
        / f"year={task.year}"
        / task.table_name
        / f"date={task.date}"
        / source_path.name
    )


def prefetch_input_file(source_path: Path, scratch_path: Path) -> tuple[bool, int]:
    ensure_dir(scratch_path.parent)
    source_bytes = source_path.stat().st_size
    if scratch_path.exists() and scratch_path.stat().st_size == source_bytes:
        return True, 0

    tmp_path = temp_prefetch_path(scratch_path)
    if tmp_path.exists():
        tmp_path.unlink()
    try:
        shutil.copy2(source_path, tmp_path)
        os.replace(tmp_path, scratch_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    return False, source_bytes


def prepare_task_inputs(task: VerifiedTask) -> PrefetchResult:
    if task.scratch_root is None:
        return PrefetchResult(
            effective_input_paths=task.input_paths,
            scratch_input_paths=(),
            prefetch_copied_files=0,
            prefetch_reused_files=0,
            prefetch_bytes_copied=0,
            prefetch_seconds=0.0,
        )

    started_at = time.perf_counter()
    scratch_input_paths: list[str] = []
    copied_files = 0
    reused_files = 0
    copied_bytes = 0

    for input_path in task.input_paths:
        source_path = Path(input_path)
        scratch_path = scratch_input_path(task, source_path)
        reused_existing_copy, file_bytes_copied = prefetch_input_file(source_path, scratch_path)
        scratch_input_paths.append(str(scratch_path))
        if reused_existing_copy:
            reused_files += 1
        else:
            copied_files += 1
            copied_bytes += file_bytes_copied

    return PrefetchResult(
        effective_input_paths=tuple(scratch_input_paths),
        scratch_input_paths=tuple(scratch_input_paths),
        prefetch_copied_files=copied_files,
        prefetch_reused_files=reused_files,
        prefetch_bytes_copied=copied_bytes,
        prefetch_seconds=time.perf_counter() - started_at,
    )


def interleave_tasks_by_table(tasks: list[VerifiedTask]) -> list[VerifiedTask]:
    table_order = ("orders", "trades")
    grouped_tasks: dict[str, list[VerifiedTask]] = {table_name: [] for table_name in table_order}
    for task in tasks:
        grouped_tasks.setdefault(task.table_name, []).append(task)
    task_queues: dict[str, deque[VerifiedTask]] = {
        table_name: deque(grouped_tasks[table_name]) for table_name in grouped_tasks
    }

    ordered_tasks: list[VerifiedTask] = []
    active_tables = [table_name for table_name, queue in task_queues.items() if queue]
    while active_tables:
        next_active_tables: list[str] = []
        for table_name in active_tables:
            queue = task_queues[table_name]
            if not queue:
                continue
            ordered_tasks.append(queue.popleft())
            if queue:
                next_active_tables.append(table_name)
        active_tables = next_active_tables
    return ordered_tasks


def reconcile_partition_rows(
    tasks: list[VerifiedTask],
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[str], list[str]]:
    task_map = {task.task_key: task for task in tasks}
    seen_task_keys: set[str] = set()
    duplicate_task_keys: set[str] = set()
    stale_task_keys: list[str] = []
    filtered_rows: list[dict[str, Any]] = []
    completed_task_keys: set[str] = set()

    for row in rows:
        task_key = row.get("task_key")
        task = task_map.get(task_key)
        if task is None:
            filtered_rows.append(row)
            continue
        if task_key in seen_task_keys:
            duplicate_task_keys.add(task_key)
            continue
        seen_task_keys.add(task_key)
        if Path(task.output_path).exists():
            filtered_rows.append(row)
            completed_task_keys.add(task_key)
        else:
            stale_task_keys.append(task_key)

    if duplicate_task_keys:
        duplicates = ", ".join(sorted(duplicate_task_keys))
        raise SystemExit(
            "Found duplicate partition manifest rows for selected verified tasks: "
            f"{duplicates}. Clean the manifest or rerun with --overwrite-existing."
        )
    return filtered_rows, completed_task_keys, stale_task_keys


def remove_selected_partition_rows(tasks: list[VerifiedTask], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected_task_keys = {task.task_key for task in tasks}
    return [row for row in rows if row.get("task_key") not in selected_task_keys]


def validate_existing_outputs(tasks: list[VerifiedTask], completed_task_keys: set[str], overwrite_existing: bool) -> None:
    if overwrite_existing:
        return
    orphan_outputs: list[str] = []
    for task in tasks:
        output_path = Path(task.output_path)
        if output_path.exists() and task.task_key not in completed_task_keys:
            orphan_outputs.append(f"{task.task_key} -> {output_path}")
    if orphan_outputs:
        details = "\n".join(orphan_outputs[:10])
        raise SystemExit(
            "Found verified outputs without matching manifest rows for the selected tasks. "
            "This looks like an interrupted or partially recorded run.\n"
            "Use --overwrite-existing to rebuild them, or remove the orphan outputs first.\n"
            f"{details}"
        )


def research_time_grade_for_year(year: str) -> str:
    return "fine_ok" if year == "2026" else "coarse_only"


def process_task(task: VerifiedTask) -> dict[str, Any]:
    task_started_at = time.perf_counter()
    output_path = Path(task.output_path)
    temp_path = temp_output_path(output_path)
    ensure_dir(output_path.parent)
    if temp_path.exists():
        temp_path.unlink()
    prefetch = prepare_task_inputs(task)
    effective_input_paths = prefetch.effective_input_paths
    materialize_started_at = time.perf_counter()
    scan = pl.scan_parquet(list(effective_input_paths))
    available_columns = set(scan.collect_schema().names())
    missing_columns = [column for column in task.allowed_columns if column not in available_columns]
    if missing_columns:
        raise ValueError(f"Missing admit-now columns for {task.task_key}: {missing_columns}")
    row_count = input_rows(effective_input_paths)
    try:
        scan.select(list(task.allowed_columns)).sink_parquet(temp_path)
        os.replace(temp_path, output_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise
    materialize_seconds = time.perf_counter() - materialize_started_at
    file_size = output_path.stat().st_size
    return {
        "task_key": task.task_key,
        "date": task.date,
        "year": task.year,
        "table_name": task.table_name,
        "verified_table_name": task.verified_table_name,
        "output_path": str(output_path),
        "input_paths": list(task.input_paths),
        "effective_input_paths": list(effective_input_paths),
        "scratch_input_paths": list(prefetch.scratch_input_paths),
        "input_read_mode": task.input_read_mode,
        "input_row_count": row_count,
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
        "prefetch_copied_files": prefetch.prefetch_copied_files,
        "prefetch_reused_files": prefetch.prefetch_reused_files,
        "prefetch_bytes_copied": prefetch.prefetch_bytes_copied,
        "prefetch_seconds": prefetch.prefetch_seconds,
        "materialize_seconds": materialize_seconds,
        "total_task_seconds": time.perf_counter() - task_started_at,
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
        "selection": state.get("selection"),
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
            "selection": state.get("selection"),
        },
    )


def write_markdown(path: Path, *, year: str, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    selection = summary.get("selection") or {}
    lines = [
        f"# Verified Layer {year}",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- status: {summary['status']}",
        f"- completed_count: {summary['completed_count']}",
        f"- failed_count: {summary['failed_count']}",
        "",
    ]
    if selection:
        lines.extend(
            [
                "## Selection",
                "",
                f"- label: {selection.get('label', 'full')}",
                f"- table: {selection.get('table', 'all')}",
                f"- selected_date_count: {selection.get('selected_date_count', 0)}",
                f"- selected_task_count: {selection.get('selected_task_count', 0)}",
                f"- first_selected_date: {selection.get('first_selected_date') or 'n/a'}",
                f"- last_selected_date: {selection.get('last_selected_date') or 'n/a'}",
            ]
        )
        if selection.get("start_date") or selection.get("end_date"):
            lines.append(
                f"- date_range: {selection.get('start_date') or 'min'} -> {selection.get('end_date') or 'max'}"
            )
        if selection.get("date_batch_size"):
            lines.append(
                f"- date_batch: {selection['date_batch_index']} / size {selection['date_batch_size']}"
            )
        if selection.get("explicit_dates"):
            lines.append(f"- explicit_dates: {', '.join(selection['explicit_dates'])}")
        lines.extend(["",])
    lines.extend(
        [
        "## Tables",
        "",
        ]
    )
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
    validate_args(args)
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
    try:
        tasks = discover_tasks(args, policy)
    except ValueError as exc:
        raise SystemExit(str(exc))
    if not tasks:
        raise SystemExit("No verified tasks matched the requested selection.")

    logger = configure_logger("build_verified_layer", args.log_root / f"build_verified_layer_{args.year}.log")
    ckpt_path = checkpoint_path(args.output_root, str(args.year))
    hb_path = heartbeat_path(args.output_root, str(args.year))
    parts_path = partitions_jsonl_path(args.output_root, str(args.year))
    summary_path = summary_json_path(args.output_root, str(args.year))
    report_path = report_path_for_run(args.research_root, args)
    selection = build_selection_metadata(args, tasks)
    selected_task_keys = {task.task_key for task in tasks}

    if not args.resume and args.overwrite_existing:
        reset_manifest_files([parts_path, ckpt_path, hb_path, summary_path])
    existing_rows = read_jsonl_rows(parts_path)
    if args.resume and args.overwrite_existing and existing_rows:
        existing_rows = remove_selected_partition_rows(tasks, existing_rows)
        write_jsonl_rows(parts_path, existing_rows)

    existing_rows, completed_task_keys, stale_task_keys = reconcile_partition_rows(tasks, existing_rows)
    if stale_task_keys:
        write_jsonl_rows(parts_path, existing_rows)
        logger.warning(
            "Dropped %s stale partition manifest rows whose outputs were missing: %s",
            len(stale_task_keys),
            ", ".join(sorted(stale_task_keys)),
        )
    validate_existing_outputs(tasks, completed_task_keys, args.overwrite_existing)
    selected_rows = [row for row in existing_rows if row.get("task_key") in selected_task_keys]

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
        "selection": selection,
    }

    if args.resume and ckpt_path.exists():
        state = json.loads(ckpt_path.read_text(encoding="utf-8"))
        state["status"] = "running"
        state["workers"] = args.workers
        state["executor_mode"] = args.executor
        state["failed_tasks"] = {}
        state["failed_count"] = 0
        state["active_task_key"] = None
        state["active_task_keys"] = []

    state["completed_task_keys"] = sorted(completed_task_keys)
    state["completed_count"] = len(completed_task_keys)
    state["pending_count"] = max(0, len(tasks) - len(completed_task_keys))
    state["selection"] = selection
    write_checkpoint(ckpt_path, hb_path, state)

    pending_tasks = interleave_tasks_by_table([task for task in tasks if task.task_key not in completed_task_keys])
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
                    selected_rows.append(row)
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
    summary = build_summary(state=state, rows=selected_rows)
    write_json(summary_path, summary)
    write_markdown(report_path, year=str(args.year), rows=selected_rows, summary=summary)
    write_checkpoint(ckpt_path, hb_path, state)
    logger.info("Verified layer build complete for %s with %s partitions", args.year, len(existing_rows))
    return 0 if not state["failed_tasks"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
