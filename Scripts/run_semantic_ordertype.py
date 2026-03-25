from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, DEFAULT_LOG_ROOT, configure_logger, ensure_dir, iso_utc_now, print_scaffold_plan, write_json
from Scripts.semantic_contract import (
    BLOCKING_LEVEL_BLOCKING,
    CONFIDENCE_LOW,
    SEMANTIC_AREA_ORDERTYPE,
    STATUS_NOT_APPLICABLE,
    STATUS_UNKNOWN,
    STATUS_WEAK_PASS,
    SUMMARY_TABLE_BY_AREA,
    area_modules,
    build_daily_result,
    build_summary_result,
    map_semantic_result_to_admissibility,
    parse_selected_dates,
)

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"
PROGRESS_EMIT_SECONDS = 5.0
HEARTBEAT_WRITE_SECONDS = 30.0


@dataclass(frozen=True)
class OrderTypeTask:
    year: str
    date: str
    stage_root: str
    output_root: str
    limit_rows: int

    @property
    def task_key(self) -> str:
        return self.date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OrderType semantic probe results.")
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
    parser.add_argument("--dates")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--latest-days", action="store_true")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_DQA_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_AUDITS_ROOT)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--sample-only", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--workers", type=int, default=default_workers())
    parser.add_argument("--executor", choices=["auto", "process", "thread"], default=default_executor_mode())
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def default_workers() -> int:
    cpu_count = os.cpu_count() or 1
    return max(1, min(8, cpu_count - 1))


def default_executor_mode() -> str:
    return "thread" if os.uname().sysname == "Darwin" else "auto"


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


def safe_rate(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return numerator / denominator


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


def write_daily_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_daily_jsonl_rows(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        return
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_existing_daily_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return pl.read_parquet(path).to_dicts()


def reset_manifest_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def input_bytes(paths: list[str]) -> int:
    return sum(Path(path).stat().st_size for path in paths)


def cache_dir_for_date(output_root: Path, year: str, trade_date: str) -> Path:
    return output_root / "semantic" / f"year={year}" / "cache" / f"date={trade_date}"


def cache_paths_for_date(output_root: Path, year: str, trade_date: str) -> Path:
    return cache_dir_for_date(output_root, year, trade_date) / "ordertype_order_group.parquet"


def format_top_values(frame: pl.DataFrame, *, value_column: str, count_column: str, limit: int = 5) -> str | None:
    if frame.is_empty():
        return None
    rows = frame.head(limit).to_dicts()
    return ",".join(f"{row[value_column]}:{row[count_column]}" for row in rows)


def investigate_date(
    trade_date: str,
    *,
    stage_root: Path,
    output_root: Path,
    year: str,
    limit_rows: int,
    logger: Any | None = None,
) -> dict[str, Any]:
    order_paths = [str(path) for path in sorted((stage_root / "orders" / f"date={trade_date}").glob("*.parquet"))]
    if logger is not None:
        logger.info(
            "OrderType %s: discovered %s order files (%s bytes)",
            trade_date,
            len(order_paths),
            input_bytes(order_paths),
        )

    orders_raw = pl.scan_parquet(order_paths)
    if limit_rows > 0:
        orders_raw = orders_raw.limit(limit_rows)
    schema_names = orders_raw.collect_schema().names()
    if logger is not None:
        logger.info("OrderType %s: order columns=%s", trade_date, ",".join(schema_names))
    orders = orders_raw.select([column for column in ("OrderId", "OrderType") if column in schema_names])

    row_stats_query = orders.select(
        [
            pl.len().cast(pl.Int64).alias("tested_rows"),
            pl.col("OrderType").is_not_null().sum().cast(pl.Int64).alias("nonnull_count"),
            pl.col("OrderType").drop_nulls().n_unique().alias("distinct_values"),
        ]
    )
    top_values_query = (
        orders.filter(pl.col("OrderType").is_not_null())
        .group_by("OrderType")
        .agg(pl.len().cast(pl.Int64).alias("row_count"))
        .sort(["row_count", "OrderType"], descending=[True, False])
        .limit(5)
    )
    orderid_frame_query = (
        orders.filter(pl.col("OrderId").is_not_null() & (pl.col("OrderId") != 0))
        .group_by("OrderId")
        .agg(
            [
                pl.col("OrderType").drop_nulls().alias("ordertype_values"),
                pl.col("OrderType").drop_nulls().n_unique().alias("ordertype_count"),
            ]
        )
    )
    cache_enabled = limit_rows <= 0
    order_group_cache_path = cache_paths_for_date(output_root, year, trade_date)
    if cache_enabled and order_group_cache_path.exists():
        if logger is not None:
            logger.info("OrderType %s: loading cached grouped table", trade_date)
        row_stats_df, top_values, orderid_frame = pl.collect_all([row_stats_query, top_values_query, pl.scan_parquet(str(order_group_cache_path))])
    else:
        if logger is not None:
            logger.info("OrderType %s: materializing row stats, top values, and grouped table once", trade_date)
        row_stats_df, top_values, orderid_frame = pl.collect_all([row_stats_query, top_values_query, orderid_frame_query])
        if cache_enabled:
            ensure_dir(order_group_cache_path.parent)
            orderid_frame.write_parquet(order_group_cache_path)
            if logger is not None:
                logger.info("OrderType %s: wrote grouped table cache", trade_date)

    row_stats = row_stats_df.to_dicts()[0]
    orderid_stats = (
        orderid_frame.lazy().select(
            [
                pl.len().cast(pl.Int64).alias("distinct_orderids"),
                (pl.col("ordertype_count") > 1).cast(pl.Int64).sum().alias("multi_count"),
                (pl.col("ordertype_count") == 1).cast(pl.Int64).sum().alias("single_count"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )
    if logger is not None:
        logger.info(
            "OrderType %s: built lazy per-OrderId aggregates; collecting compact pattern summary",
            trade_date,
        )
    pattern_rows = (
        orderid_frame.lazy().with_columns(
            pl.col("ordertype_values")
            .list.eval(pl.element().cast(pl.String))
            .list.join(",")
            .alias("transition_pattern")
        )
        .group_by("transition_pattern")
        .agg(pl.len().cast(pl.Int64).alias("pattern_count"))
        .sort(["pattern_count", "transition_pattern"], descending=[True, False])
        .limit(3)
        .collect()
    )
    multi_count = int(orderid_stats["multi_count"] or 0)
    single_count = int(orderid_stats["single_count"] or 0)
    distinct_orderids = int(orderid_stats["distinct_orderids"] or 0)
    pattern_sample = format_top_values(pattern_rows, value_column="transition_pattern", count_column="pattern_count", limit=3)
    tested_rows = int(row_stats["tested_rows"] or 0)
    distinct_values = int(row_stats["distinct_values"] or 0)
    nonnull_count = int(row_stats["nonnull_count"] or 0)
    status = STATUS_NOT_APPLICABLE if tested_rows == 0 else STATUS_WEAK_PASS if distinct_values > 0 and multi_count > 0 else STATUS_UNKNOWN
    impact = map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_ORDERTYPE, status=status, blocking_level=BLOCKING_LEVEL_BLOCKING)
    if logger is not None:
        logger.info(
            "OrderType %s summary: distinct_values=%s distinct_orderids=%s multi_count=%s pattern_count=%s",
            trade_date,
            distinct_values,
            distinct_orderids,
            multi_count,
            pattern_rows.height,
        )
    return build_daily_result(
        SEMANTIC_AREA_ORDERTYPE,
        date=trade_date,
        year=year,
        semantic_area=SEMANTIC_AREA_ORDERTYPE,
        scope="orders OrderType trajectory probe",
        status=status,
        confidence=CONFIDENCE_LOW,
        blocking_level=BLOCKING_LEVEL_BLOCKING,
        tested_rows=tested_rows,
        pass_rows=single_count if status == STATUS_WEAK_PASS else 0,
        fail_rows=0,
        unknown_rows=max(tested_rows - nonnull_count, 0),
        summary=f"distinct_ordertype_values={distinct_values}, multi_ordertype_orderid_count={multi_count}",
        admissibility_impact=impact,
        evidence_path=f"dqa/semantic/year={year}/semantic_ordertype_daily.parquet",
        ordertype_nonnull_count=nonnull_count,
        ordertype_nonnull_rate=safe_rate(nonnull_count, tested_rows),
        distinct_ordertype_values=distinct_values,
        single_ordertype_orderid_count=single_count,
        multi_ordertype_orderid_count=multi_count,
        single_ordertype_orderid_rate=safe_rate(single_count, distinct_orderids),
        multi_ordertype_orderid_rate=safe_rate(multi_count, distinct_orderids),
        top_ordertype_values=format_top_values(top_values, value_column="OrderType", count_column="row_count"),
        ordertype_transition_pattern_count=pattern_rows.height,
        ordertype_transition_pattern_sample=pattern_sample,
        ordertype_status=status,
    )


def process_task(task: OrderTypeTask) -> dict[str, Any]:
    return investigate_date(
        task.date,
        stage_root=Path(task.stage_root),
        output_root=Path(task.output_root),
        year=task.year,
        limit_rows=task.limit_rows,
        logger=None,
    )


def build_yearly_summary(year: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    modules = area_modules(SEMANTIC_AREA_ORDERTYPE)
    status = STATUS_NOT_APPLICABLE if not rows else max(rows, key=lambda row: {"pass": 1, "weak_pass": 2, "unknown": 3, "fail": 4, "not_run": 5, "not_applicable": 0}[row["status"]])["status"]
    distinct_values_union = sorted({int(row["distinct_ordertype_values"]) for row in rows if row["distinct_ordertype_values"] is not None})
    return build_summary_result(
        SEMANTIC_AREA_ORDERTYPE,
        year=year,
        semantic_area=SEMANTIC_AREA_ORDERTYPE,
        status=status,
        confidence=CONFIDENCE_LOW,
        blocking_level=BLOCKING_LEVEL_BLOCKING,
        days_total=len(rows),
        days_run=len(rows),
        days_pass=sum(1 for row in rows if row["status"] == "pass"),
        days_weak_pass=sum(1 for row in rows if row["status"] == STATUS_WEAK_PASS),
        days_fail=sum(1 for row in rows if row["status"] == "fail"),
        days_unknown=sum(1 for row in rows if row["status"] in {STATUS_UNKNOWN, "not_run"}),
        tested_rows_total=sum(int(row["tested_rows"] or 0) for row in rows),
        distinct_ordertype_values_union=",".join(str(value) for value in distinct_values_union) if distinct_values_union else None,
        single_ordertype_orderid_rate_avg=safe_rate(sum(float(row["single_ordertype_orderid_rate"] or 0) for row in rows), len(rows)),
        multi_ordertype_orderid_rate_avg=safe_rate(sum(float(row["multi_ordertype_orderid_rate"] or 0) for row in rows), len(rows)),
        ordertype_transition_pattern_count_total=sum(int(row["ordertype_transition_pattern_count"] or 0) for row in rows),
        summary=f"dates={len(rows)} OrderType probes materialized",
        admissibility_impact=map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_ORDERTYPE, status=status, blocking_level=BLOCKING_LEVEL_BLOCKING),
        recommended_modules=",".join(modules["recommended"]),
        blocked_modules=",".join(modules["blocked"]),
    )


def write_markdown(path: Path, *, year: str, rows: list[dict[str, Any]], summary_row: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    lines = [f"# Semantic OrderType Probe {year}", "", f"- generated_at: {iso_utc_now()}", f"- status: {summary_row['status']}"]
    for row in rows:
        lines.extend(["", f"## {row['date']}", f"- distinct_ordertype_values: {row['distinct_ordertype_values']}", f"- multi_ordertype_orderid_rate: {row['multi_ordertype_orderid_rate']}", f"- ordertype_transition_pattern_sample: {row['ordertype_transition_pattern_sample']}", f"- status: {row['status']}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_tasks(*, year: str, stage_root: Path, output_root: Path, dates: list[str], limit_rows: int) -> list[OrderTypeTask]:
    return [
        OrderTypeTask(
            year=year,
            date=trade_date,
            stage_root=str(stage_root),
            output_root=str(output_root),
            limit_rows=limit_rows,
        )
        for trade_date in dates
    ]


def build_summary(state: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    return {
        "pipeline": "semantic_ordertype",
        "status": state["status"],
        "year": state["year"],
        "generated_at": iso_utc_now(),
        "completed_count": state["completed_count"],
        "failed_count": state["failed_count"],
        "pending_count": state["pending_count"],
        "artifacts": {
            "checkpoint": str(output_dir / "checkpoint.json"),
            "heartbeat": str(output_dir / "heartbeat.json"),
            "daily_jsonl": str(output_dir / "semantic_ordertype_daily.jsonl"),
            "summary": str(output_dir / "semantic_ordertype_summary.json"),
        },
    }


def write_checkpoint(checkpoint_path: Path, heartbeat_path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = iso_utc_now()
    write_json(checkpoint_path, state)
    write_json(
        heartbeat_path,
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


def state_signature(state: dict[str, Any]) -> tuple[Any, ...]:
    return (
        state.get("status"),
        state.get("completed_count"),
        state.get("failed_count"),
        state.get("pending_count"),
        tuple(state.get("active_task_keys", [])),
        tuple(sorted(state.get("failed_tasks", {}).keys())),
    )


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


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(name="run_semantic_ordertype", purpose="Materialize the OrderType semantic probe without asserting event-type semantics.", responsibilities=["Count distinct OrderType values.", "Profile per-OrderId OrderType trajectories.", "Emit unified semantic daily and yearly results."], inputs=["candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet"], outputs=["dqa/semantic/year=<year>/semantic_ordertype_daily.parquet", "Research/Audits/semantic_ordertype_<year>.md"])
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")
    selected_dates = parse_selected_dates(stage_root=args.input_root, year=str(args.year), dates=args.dates, max_days=args.max_days, latest_days=args.latest_days)
    if not selected_dates:
        raise SystemExit("No overlapping stage dates matched the requested selection.")
    selected_date_set = set(selected_dates)
    tasks = build_tasks(
        year=str(args.year),
        stage_root=args.input_root,
        output_root=args.output_root,
        dates=selected_dates,
        limit_rows=args.limit_rows,
    )
    output_dir = args.output_root / "semantic" / f"year={args.year}"
    daily_path = output_dir / "semantic_ordertype_daily.parquet"
    yearly_path = output_dir / SUMMARY_TABLE_BY_AREA[SEMANTIC_AREA_ORDERTYPE]
    daily_jsonl_path = output_dir / "semantic_ordertype_daily.jsonl"
    checkpoint_path = output_dir / "checkpoint.json"
    heartbeat_path = output_dir / "heartbeat.json"
    report_path = args.research_root / f"semantic_ordertype_{args.year}.md"
    logger = configure_logger("semantic_ordertype", args.log_root / f"semantic_ordertype_{args.year}.log")
    ensure_dir(output_dir)
    existing_rows = [row for row in read_existing_daily_rows(daily_path) if str(row["date"]) in selected_date_set]
    if existing_rows and (args.overwrite_existing or not daily_jsonl_path.exists()):
        write_daily_jsonl(existing_rows, daily_jsonl_path)
    if not args.resume:
        if args.overwrite_existing:
            reset_manifest_files([daily_jsonl_path, checkpoint_path, heartbeat_path])
            existing_rows = []
        completed_dates = sorted(str(row["date"]) for row in existing_rows)
        state = {
            "status": "running",
            "year": str(args.year),
            "started_at": iso_utc_now(),
            "updated_at": iso_utc_now(),
            "workers": args.workers,
            "completed_task_keys": completed_dates,
            "failed_tasks": {},
            "completed_count": len(completed_dates),
            "failed_count": 0,
            "pending_count": max(0, len(tasks) - len(completed_dates)),
            "active_task_key": None,
            "active_task_keys": [],
            "executor_mode": args.executor,
        }
    else:
        if not checkpoint_path.exists():
            raise SystemExit(f"Cannot resume because checkpoint is missing: {checkpoint_path}")
        state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        state["status"] = "running"
        state["workers"] = args.workers
        state["executor_mode"] = args.executor
        state["active_task_key"] = None
        state["active_task_keys"] = []

    if existing_rows:
        merged_completed = set(state.get("completed_task_keys", [])) | {str(row["date"]) for row in existing_rows}
        state["completed_task_keys"] = sorted(merged_completed)
        state["completed_count"] = len(merged_completed)
        state["pending_count"] = max(0, len(tasks) - state["completed_count"] - len(state.get("failed_tasks", {})))

    completed_task_keys = set(state.get("completed_task_keys", []))
    write_checkpoint(checkpoint_path, heartbeat_path, state)
    pending_tasks = [task for task in tasks if task.task_key not in completed_task_keys]
    future_to_task: dict[Any, OrderTypeTask] = {}
    executor, resolved_mode = build_executor(args.executor, args.workers, logger)
    state["executor_mode"] = resolved_mode
    write_checkpoint(checkpoint_path, heartbeat_path, state)
    last_state_write_at = time.monotonic()
    last_state_signature = state_signature(state)

    with executor:
        task_iter = iter(pending_tasks)

        def submit_until_capacity() -> None:
            while len(future_to_task) < args.workers:
                try:
                    task = next(task_iter)
                except StopIteration:
                    break
                logger.info("OrderType %s: queued for processing", task.task_key)
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
            if done_futures:
                pass
            elif (
                current_signature != last_state_signature
                or (time.monotonic() - last_state_write_at) >= HEARTBEAT_WRITE_SECONDS
            ):
                write_checkpoint(checkpoint_path, heartbeat_path, state)
                last_state_write_at = time.monotonic()
                last_state_signature = current_signature

            completed_rows: list[dict[str, Any]] = []
            for future in done_futures:
                task = future_to_task[future]
                try:
                    row = future.result()
                    completed_rows.append(row)
                    completed_task_keys.add(task.task_key)
                    state["failed_tasks"].pop(task.task_key, None)
                    logger.info(
                        "OrderType %s complete: multi_ordertype_orderid_rate=%s status=%s",
                        task.task_key,
                        row["multi_ordertype_orderid_rate"],
                        row["status"],
                    )
                except Exception as exc:
                    state["failed_tasks"][task.task_key] = str(exc)
                    logger.error("OrderType %s failed: %s", task.task_key, exc)
                finally:
                    future_to_task.pop(future, None)
            if done_futures:
                append_daily_jsonl_rows(completed_rows, daily_jsonl_path)
                submit_until_capacity()
                pending_futures = set(future_to_task)
                state["completed_task_keys"] = sorted(completed_task_keys)
                state["completed_count"] = len(completed_task_keys)
                state["failed_count"] = len(state["failed_tasks"])
                state["active_task_keys"] = sorted(future_to_task[pending].task_key for pending in pending_futures)
                state["active_task_key"] = state["active_task_keys"][0] if state["active_task_keys"] else None
                state["pending_count"] = max(
                    0,
                    len(tasks) - state["completed_count"] - state["failed_count"] - len(state["active_task_keys"]),
                )
                write_checkpoint(checkpoint_path, heartbeat_path, state)
                last_state_write_at = time.monotonic()
                last_state_signature = state_signature(state)

    rows = read_jsonl_rows(daily_jsonl_path)
    summary_row = build_yearly_summary(str(args.year), rows)
    write_parquet(rows, daily_path)
    write_parquet([summary_row], yearly_path)
    write_markdown(report_path, year=str(args.year), rows=rows, summary_row=summary_row)
    state["status"] = "completed" if not state["failed_tasks"] else "completed_with_failures"
    state["active_task_keys"] = []
    state["active_task_key"] = None
    write_checkpoint(checkpoint_path, heartbeat_path, state)
    write_json(output_dir / "semantic_ordertype_summary.json", build_summary(state, output_dir))
    logger.info("Semantic OrderType probe complete for %s with %s dates", args.year, len(rows))
    return 0 if not state["failed_tasks"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
