from __future__ import annotations

import argparse
import json
import os
import re
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)
from Scripts.semantic_contract import (
    BLOCKING_LEVEL_BLOCKING,
    CONFIDENCE_MEDIUM,
    SEMANTIC_AREA_LIFECYCLE,
    STATUS_NOT_APPLICABLE,
    STATUS_PASS,
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
class LifecycleTask:
    year: str
    date: str
    stage_root: str
    output_root: str
    limit_rows: int

    @property
    def task_key(self) -> str:
        return self.date


def default_workers() -> int:
    cpu_count = os.cpu_count() or 1
    return max(1, min(8, cpu_count - 1))


def default_executor_mode() -> str:
    return "thread" if os.uname().sysname == "Darwin" else "auto"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OrderId lifecycle semantic probe results.")
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
    parser.add_argument("--dates", help="Comma-separated trade dates in YYYYMMDD or YYYY-MM-DD format.")
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


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


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


def reset_manifest_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def safe_rate(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return numerator / denominator


def order_scan(order_paths: list[str], limit_rows: int) -> pl.LazyFrame:
    scan = pl.scan_parquet(order_paths)
    return scan.limit(limit_rows) if limit_rows > 0 else scan


def trade_scan(trade_paths: list[str], limit_rows: int) -> pl.LazyFrame:
    scan = pl.scan_parquet(trade_paths)
    return scan.limit(limit_rows) if limit_rows > 0 else scan


def has_column(frame: pl.LazyFrame, column_name: str) -> bool:
    return column_name in frame.collect_schema().names()


def input_bytes(paths: list[str]) -> int:
    return sum(Path(path).stat().st_size for path in paths)


def input_rows(paths: list[str]) -> int:
    total_rows = 0
    for path in paths:
        total_rows += pq.ParquetFile(path).metadata.num_rows
    return int(total_rows)


def cache_dir_for_date(output_root: Path, year: str, trade_date: str) -> Path:
    return output_root / "semantic" / f"year={year}" / "cache" / f"date={trade_date}"


def cache_paths_for_date(output_root: Path, year: str, trade_date: str) -> tuple[Path, Path]:
    cache_dir = cache_dir_for_date(output_root, year, trade_date)
    return cache_dir / "order_group.parquet", cache_dir / "trade_links.parquet"


def read_existing_daily_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return pl.read_parquet(path).to_dicts()


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


def recover_row_counts(stage_root: Path, trade_date: str) -> int:
    order_paths = [str(path) for path in sorted((stage_root / "orders" / f"date={trade_date}").glob("*.parquet"))]
    if not order_paths:
        return 0
    return input_rows(order_paths)


def recover_rows_from_log(
    *,
    log_path: Path,
    stage_root: Path,
    year: str,
    existing_dates: set[str],
) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    pattern = re.compile(
        r"Lifecycle (\d{4}-\d{2}-\d{2}) summary: distinct_orderids=(\d+) linked_orderids=(\d+) multi_event=(\d+) multiple_trades=(\d+)"
    )
    recovered: dict[str, dict[str, int]] = {}
    for line in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = pattern.search(line)
        if not match:
            continue
        trade_date, distinct_orderids, linked_orderids, multi_event_orderids, multiple_trades = match.groups()
        if trade_date in existing_dates:
            continue
        recovered[trade_date] = {
            "distinct_orderids": int(distinct_orderids),
            "linked_orderids": int(linked_orderids),
            "orders_with_multiple_events": int(multi_event_orderids),
            "orders_with_multiple_trades": int(multiple_trades),
        }

    rows: list[dict[str, Any]] = []
    for trade_date in sorted(recovered):
        values = recovered[trade_date]
        distinct_orderids = values["distinct_orderids"]
        linked_orderids = values["linked_orderids"]
        multi_event_orderids = values["orders_with_multiple_events"]
        multiple_trades = values["orders_with_multiple_trades"]
        single_trade = max(0, linked_orderids - multiple_trades)
        status = classify_status(
            distinct_orderids=distinct_orderids,
            linked_orderids=linked_orderids,
            multi_event_orderids=multi_event_orderids,
        )
        impact = map_semantic_result_to_admissibility(
            semantic_area=SEMANTIC_AREA_LIFECYCLE,
            status=status,
            blocking_level=BLOCKING_LEVEL_BLOCKING,
        )
        tested_rows = recover_row_counts(stage_root, trade_date)
        linked_rate = safe_rate(linked_orderids, distinct_orderids)
        row = build_daily_result(
            SEMANTIC_AREA_LIFECYCLE,
            date=trade_date,
            year=year,
            semantic_area=SEMANTIC_AREA_LIFECYCLE,
            scope="orders+trades sample lifecycle probe",
            status=status,
            confidence=CONFIDENCE_MEDIUM,
            blocking_level=BLOCKING_LEVEL_BLOCKING,
            tested_rows=tested_rows,
            pass_rows=linked_orderids if status in {STATUS_PASS, STATUS_WEAK_PASS} else 0,
            fail_rows=distinct_orderids - linked_orderids if status == STATUS_UNKNOWN else 0,
            unknown_rows=distinct_orderids if status == STATUS_UNKNOWN else 0,
            summary=f"distinct_orderids={distinct_orderids}, linked_orderids={linked_orderids}, multi_event={multi_event_orderids}",
            admissibility_impact=impact,
            evidence_path=f"dqa/semantic/year={year}/semantic_orderid_lifecycle_daily.parquet",
            distinct_orderids=distinct_orderids,
            linked_orderids=linked_orderids,
            linked_orderid_rate=linked_rate,
            orders_with_multiple_events=multi_event_orderids,
            orders_with_multiple_events_rate=safe_rate(multi_event_orderids, distinct_orderids),
            orders_with_multiple_trades=multiple_trades,
            orders_with_multiple_trades_rate=safe_rate(multiple_trades, distinct_orderids),
            orders_with_single_trade=single_trade,
            orders_with_single_trade_rate=safe_rate(single_trade, distinct_orderids),
            cross_session_candidate_count=0,
            cross_session_candidate_rate=0.0,
            first_order_seqnum_present_rate=1.0 if distinct_orderids else None,
            last_order_seqnum_present_rate=1.0 if distinct_orderids else None,
            first_trade_seqnum_present_rate=linked_rate,
            last_trade_seqnum_present_rate=linked_rate,
            lifecycle_status=status,
        )
        rows.append(row)
    return rows


def classify_status(*, distinct_orderids: int, linked_orderids: int, multi_event_orderids: int) -> str:
    if distinct_orderids == 0:
        return STATUS_NOT_APPLICABLE
    linked_rate = linked_orderids / distinct_orderids
    multi_event_rate = multi_event_orderids / distinct_orderids
    # Lifecycle is expected to have low linked-order coverage because many orders never trade.
    # Promote status only when linked orders are large in absolute terms and multi-event behavior is dominant.
    if linked_orderids >= 1_000_000 and multi_event_rate >= 0.9:
        return STATUS_PASS
    if linked_orderids >= 100_000 and multi_event_rate >= 0.5:
        return STATUS_WEAK_PASS
    if linked_rate >= 0.02 and multi_event_rate >= 0.5:
        return STATUS_WEAK_PASS
    return STATUS_UNKNOWN


def build_tasks(*, year: str, stage_root: Path, output_root: Path, dates: list[str], limit_rows: int) -> list[LifecycleTask]:
    return [
        LifecycleTask(
            year=year,
            date=trade_date,
            stage_root=str(stage_root),
            output_root=str(output_root),
            limit_rows=limit_rows,
        )
        for trade_date in dates
    ]


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
    trade_paths = [str(path) for path in sorted((stage_root / "trades" / f"date={trade_date}").glob("*.parquet"))]
    if logger is not None:
        logger.info(
            "Lifecycle %s: discovered %s order files (%s bytes) and %s trade files (%s bytes)",
            trade_date,
            len(order_paths),
            input_bytes(order_paths),
            len(trade_paths),
            input_bytes(trade_paths),
        )

    orders_raw = order_scan(order_paths, limit_rows)
    trades_raw = trade_scan(trade_paths, limit_rows)
    orders_schema = orders_raw.collect_schema().names()
    trades_schema = trades_raw.collect_schema().names()
    if logger is not None:
        logger.info(
            "Lifecycle %s: order columns=%s trade columns=%s",
            trade_date,
            ",".join(orders_schema),
            ",".join(trades_schema),
        )

    order_columns = [column for column in ("OrderId", "SeqNum", "OrderType", "Session") if column in orders_schema]
    trade_columns = [column for column in ("BidOrderID", "AskOrderID", "SeqNum", "Session") if column in trades_schema]
    orders = orders_raw.select(order_columns)
    trades = trades_raw.select(trade_columns)
    order_has_session = "Session" in order_columns
    trade_has_seqnum = "SeqNum" in trade_columns
    trade_has_session = "Session" in trade_columns

    order_group = (
        orders.filter(pl.col("OrderId").is_not_null() & (pl.col("OrderId") != 0))
        .group_by("OrderId")
        .agg(
            [
                pl.len().cast(pl.Int64).alias("order_event_count"),
                pl.col("SeqNum").drop_nulls().count().alias("order_seqnum_present_count"),
                pl.col("SeqNum").drop_nulls().first().is_not_null().cast(pl.Int64).alias("first_order_seqnum_present"),
                pl.col("SeqNum").drop_nulls().last().is_not_null().cast(pl.Int64).alias("last_order_seqnum_present"),
                pl.col("OrderType").drop_nulls().n_unique().alias("distinct_ordertype_values"),
                (
                    pl.col("Session").drop_nulls().n_unique()
                    if order_has_session
                    else pl.lit(None, dtype=pl.Int64)
                ).alias("order_session_count"),
            ]
        )
    )
    trade_links = pl.concat(
        [
            trades.filter(pl.col("BidOrderID").is_not_null() & (pl.col("BidOrderID") != 0)).select(
                pl.col("BidOrderID").alias("OrderId"),
                (pl.col("SeqNum").is_not_null().cast(pl.Int64) if trade_has_seqnum else pl.lit(0, dtype=pl.Int64)).alias("trade_seqnum_present"),
            ),
            trades.filter(pl.col("AskOrderID").is_not_null() & (pl.col("AskOrderID") != 0)).select(
                pl.col("AskOrderID").alias("OrderId"),
                (pl.col("SeqNum").is_not_null().cast(pl.Int64) if trade_has_seqnum else pl.lit(0, dtype=pl.Int64)).alias("trade_seqnum_present"),
            ),
        ],
        how="vertical_relaxed",
    ).group_by("OrderId").agg(
        [
            pl.len().cast(pl.Int64).alias("trade_match_count"),
            pl.col("trade_seqnum_present").sum().alias("trade_seqnum_present_count"),
            pl.col("trade_seqnum_present").first().alias("first_trade_seqnum_present"),
            pl.col("trade_seqnum_present").last().alias("last_trade_seqnum_present"),
            (
                pl.col("Session").drop_nulls().n_unique()
                if trade_has_session
                else pl.lit(None, dtype=pl.Int64)
            ).alias("trade_session_count"),
        ]
    )

    cache_enabled = limit_rows <= 0
    order_group_cache_path, trade_links_cache_path = cache_paths_for_date(output_root, year, trade_date)
    if cache_enabled and order_group_cache_path.exists() and trade_links_cache_path.exists():
        if logger is not None:
            logger.info("Lifecycle %s: loading cached grouped tables", trade_date)
        order_group_df = pl.read_parquet(order_group_cache_path)
        trade_links_df = pl.read_parquet(trade_links_cache_path)
    else:
        if logger is not None:
            logger.info("Lifecycle %s: built lazy order/trade aggregates; materializing grouped tables once", trade_date)
        order_group_df, trade_links_df = pl.collect_all([order_group, trade_links])
        if cache_enabled:
            ensure_dir(order_group_cache_path.parent)
            order_group_df.write_parquet(order_group_cache_path)
            trade_links_df.write_parquet(trade_links_cache_path)
            if logger is not None:
                logger.info("Lifecycle %s: wrote grouped table cache", trade_date)

    matched_trade_links_df = trade_links_df.join(order_group_df.select("OrderId"), on="OrderId", how="semi")

    order_summary = order_group_df.select(
        [
            pl.len().cast(pl.Int64).alias("distinct_orderids"),
            (pl.col("order_event_count") > 1).cast(pl.Int64).sum().alias("multi_event_orderids"),
            (pl.col("first_order_seqnum_present") > 0).cast(pl.Int64).sum().alias("first_order_seqnum_present"),
            (pl.col("last_order_seqnum_present") > 0).cast(pl.Int64).sum().alias("last_order_seqnum_present"),
            (
                (pl.col("order_session_count").fill_null(0) > 1).cast(pl.Int64).sum()
                if order_has_session
                else pl.lit(0, dtype=pl.Int64)
            ).alias("order_cross_session_candidate_count"),
        ]
    ).to_dicts()[0]

    trade_summary = matched_trade_links_df.select(
        [
            pl.len().cast(pl.Int64).alias("linked_orderids"),
            (pl.col("trade_match_count") > 1).cast(pl.Int64).sum().alias("multiple_trades"),
            (pl.col("trade_match_count") == 1).cast(pl.Int64).sum().alias("single_trade"),
            (pl.col("first_trade_seqnum_present") > 0).cast(pl.Int64).sum().alias("first_trade_seqnum_present"),
            (pl.col("last_trade_seqnum_present") > 0).cast(pl.Int64).sum().alias("last_trade_seqnum_present"),
            (
                (pl.col("trade_session_count").fill_null(0) > 1).cast(pl.Int64).sum()
                if trade_has_session
                else pl.lit(0, dtype=pl.Int64)
            ).alias("trade_cross_session_candidate_count"),
        ]
    ).to_dicts()[0]

    if order_has_session or trade_has_session:
        session_summary = (
            order_group_df.select("OrderId", "order_session_count")
            .join(
                matched_trade_links_df.select("OrderId", "trade_session_count"),
                on="OrderId",
                how="left",
            )
            .select(
                [
                    (
                        (
                            (pl.col("order_session_count").fill_null(0) > 1)
                            | (pl.col("trade_session_count").fill_null(0) > 1)
                            | (
                                (pl.col("order_session_count").fill_null(0) > 0)
                                & (pl.col("trade_session_count").fill_null(0) > 0)
                                & (pl.col("order_session_count") != pl.col("trade_session_count"))
                            )
                        )
                        .cast(pl.Int64)
                        .sum()
                    ).alias("cross_session_candidate_count")
                ]
            )
            .to_dicts()[0]
        )
        cross_session_candidate_count = int(session_summary["cross_session_candidate_count"] or 0)
    else:
        cross_session_candidate_count = 0

    order_rows = input_rows(order_paths)
    distinct_orderids = int(order_summary["distinct_orderids"] or 0)
    linked_orderids = int(trade_summary["linked_orderids"] or 0)
    multi_event_orderids = int(order_summary["multi_event_orderids"] or 0)
    multiple_trades = int(trade_summary["multiple_trades"] or 0)
    single_trade = int(trade_summary["single_trade"] or 0)
    status = classify_status(distinct_orderids=distinct_orderids, linked_orderids=linked_orderids, multi_event_orderids=multi_event_orderids)
    impact = map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_LIFECYCLE, status=status, blocking_level=BLOCKING_LEVEL_BLOCKING)
    first_order_seqnum_present = int(order_summary["first_order_seqnum_present"] or 0)
    last_order_seqnum_present = int(order_summary["last_order_seqnum_present"] or 0)
    first_trade_seqnum_present = int(trade_summary["first_trade_seqnum_present"] or 0)
    last_trade_seqnum_present = int(trade_summary["last_trade_seqnum_present"] or 0)
    if logger is not None:
        logger.info(
            "Lifecycle %s summary: distinct_orderids=%s linked_orderids=%s multi_event=%s multiple_trades=%s",
            trade_date,
            distinct_orderids,
            linked_orderids,
            multi_event_orderids,
            multiple_trades,
        )
    return build_daily_result(
        SEMANTIC_AREA_LIFECYCLE,
        date=trade_date,
        year=year,
        semantic_area=SEMANTIC_AREA_LIFECYCLE,
        scope="orders+trades sample lifecycle probe",
        status=status,
        confidence=CONFIDENCE_MEDIUM,
        blocking_level=BLOCKING_LEVEL_BLOCKING,
        tested_rows=order_rows,
        pass_rows=linked_orderids if status in {STATUS_PASS, STATUS_WEAK_PASS} else 0,
        fail_rows=distinct_orderids - linked_orderids if status == STATUS_UNKNOWN else 0,
        unknown_rows=distinct_orderids if status == STATUS_UNKNOWN else 0,
        summary=f"distinct_orderids={distinct_orderids}, linked_orderids={linked_orderids}, multi_event={multi_event_orderids}",
        admissibility_impact=impact,
        evidence_path=f"dqa/semantic/year={year}/semantic_orderid_lifecycle_daily.parquet",
        distinct_orderids=distinct_orderids,
        linked_orderids=linked_orderids,
        linked_orderid_rate=safe_rate(linked_orderids, distinct_orderids),
        orders_with_multiple_events=multi_event_orderids,
        orders_with_multiple_events_rate=safe_rate(multi_event_orderids, distinct_orderids),
        orders_with_multiple_trades=multiple_trades,
        orders_with_multiple_trades_rate=safe_rate(multiple_trades, distinct_orderids),
        orders_with_single_trade=single_trade,
        orders_with_single_trade_rate=safe_rate(single_trade, distinct_orderids),
        cross_session_candidate_count=cross_session_candidate_count,
        cross_session_candidate_rate=safe_rate(cross_session_candidate_count, distinct_orderids),
        first_order_seqnum_present_rate=safe_rate(first_order_seqnum_present, distinct_orderids),
        last_order_seqnum_present_rate=safe_rate(last_order_seqnum_present, distinct_orderids),
        first_trade_seqnum_present_rate=safe_rate(first_trade_seqnum_present, distinct_orderids),
        last_trade_seqnum_present_rate=safe_rate(last_trade_seqnum_present, distinct_orderids),
        lifecycle_status=status,
    )


def process_task(task: LifecycleTask) -> dict[str, Any]:
    return investigate_date(
        task.date,
        stage_root=Path(task.stage_root),
        output_root=Path(task.output_root),
        year=task.year,
        limit_rows=task.limit_rows,
        logger=None,
    )


def build_yearly_summary(year: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    modules = area_modules(SEMANTIC_AREA_LIFECYCLE)
    status = STATUS_NOT_APPLICABLE if not rows else max(rows, key=lambda row: {"pass": 1, "weak_pass": 2, "unknown": 3, "fail": 4, "not_run": 5, "not_applicable": 0}[row["status"]])["status"]
    return build_summary_result(
        SEMANTIC_AREA_LIFECYCLE,
        year=year,
        semantic_area=SEMANTIC_AREA_LIFECYCLE,
        status=status,
        confidence=CONFIDENCE_MEDIUM,
        blocking_level=BLOCKING_LEVEL_BLOCKING,
        days_total=len(rows),
        days_run=len(rows),
        days_pass=sum(1 for row in rows if row["status"] == STATUS_PASS),
        days_weak_pass=sum(1 for row in rows if row["status"] == STATUS_WEAK_PASS),
        days_fail=sum(1 for row in rows if row["status"] == "fail"),
        days_unknown=sum(1 for row in rows if row["status"] in {STATUS_UNKNOWN, "not_run"}),
        tested_rows_total=sum(int(row["tested_rows"] or 0) for row in rows),
        linked_orderids_total=sum(int(row["linked_orderids"] or 0) for row in rows),
        linked_orderid_rate_avg=safe_rate(sum(float(row["linked_orderid_rate"] or 0) for row in rows), len(rows)),
        orders_with_multiple_events_rate_avg=safe_rate(sum(float(row["orders_with_multiple_events_rate"] or 0) for row in rows), len(rows)),
        orders_with_multiple_trades_rate_avg=safe_rate(sum(float(row["orders_with_multiple_trades_rate"] or 0) for row in rows), len(rows)),
        cross_session_candidate_rate_avg=safe_rate(sum(float(row["cross_session_candidate_rate"] or 0) for row in rows), len(rows)),
        summary=f"dates={len(rows)} lifecycle probe rows materialized",
        admissibility_impact=map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_LIFECYCLE, status=status, blocking_level=BLOCKING_LEVEL_BLOCKING),
        recommended_modules=",".join(modules["recommended"]),
        blocked_modules=",".join(modules["blocked"]),
    )


def write_markdown(path: Path, *, year: str, rows: list[dict[str, Any]], summary_row: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    lines = [f"# Semantic Lifecycle Probe {year}", "", f"- generated_at: {iso_utc_now()}", f"- status: {summary_row['status']}"]
    for row in rows:
        lines.extend(["", f"## {row['date']}", f"- tested_rows: {row['tested_rows']}", f"- distinct_orderids: {row['distinct_orderids']}", f"- linked_orderid_rate: {row['linked_orderid_rate']}", f"- orders_with_multiple_events_rate: {row['orders_with_multiple_events_rate']}", f"- status: {row['status']}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_summary(state: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    return {
        "pipeline": "semantic_lifecycle",
        "status": state["status"],
        "year": state["year"],
        "generated_at": iso_utc_now(),
        "completed_count": state["completed_count"],
        "failed_count": state["failed_count"],
        "pending_count": state["pending_count"],
        "artifacts": {
            "checkpoint": str(output_dir / "checkpoint.json"),
            "heartbeat": str(output_dir / "heartbeat.json"),
            "daily_jsonl": str(output_dir / "semantic_orderid_lifecycle_daily.jsonl"),
            "summary": str(output_dir / "semantic_lifecycle_summary.json"),
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
        print_scaffold_plan(
            name="run_semantic_lifecycle",
            purpose="Materialize the OrderId lifecycle probe without asserting full event semantics.",
            responsibilities=["Count distinct OrderId rows and linkable trade edges.", "Profile multi-event and multi-trade lifecycle candidates.", "Emit unified semantic daily and yearly results."],
            inputs=["candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet", "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet"],
            outputs=["dqa/semantic/year=<year>/semantic_orderid_lifecycle_daily.parquet", "Research/Audits/semantic_lifecycle_<year>.md"],
        )
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
    daily_path = output_dir / "semantic_orderid_lifecycle_daily.parquet"
    yearly_path = output_dir / SUMMARY_TABLE_BY_AREA[SEMANTIC_AREA_LIFECYCLE]
    daily_jsonl_path = output_dir / "semantic_orderid_lifecycle_daily.jsonl"
    checkpoint_path = output_dir / "checkpoint.json"
    heartbeat_path = output_dir / "heartbeat.json"
    report_path = args.research_root / f"semantic_lifecycle_{args.year}.md"
    log_path = args.log_root / f"semantic_lifecycle_{args.year}.log"
    logger = configure_logger("semantic_lifecycle", log_path)
    ensure_dir(output_dir)
    existing_rows = [row for row in read_existing_daily_rows(daily_path) if str(row["date"]) in selected_date_set]
    existing_dates = {str(row["date"]) for row in existing_rows}
    recovered_rows = recover_rows_from_log(
        log_path=log_path,
        stage_root=args.input_root,
        year=str(args.year),
        existing_dates=existing_dates,
    )
    recovered_rows = [row for row in recovered_rows if str(row["date"]) in selected_date_set]
    all_rows_by_date = {str(row["date"]): row for row in existing_rows}
    for row in recovered_rows:
        all_rows_by_date[str(row["date"])] = row
    existing_rows = [all_rows_by_date[trade_date] for trade_date in sorted(all_rows_by_date)]
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
    future_to_task: dict[Any, LifecycleTask] = {}
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
                logger.info("Lifecycle %s: queued for processing", task.task_key)
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
                        "Lifecycle %s complete: linked_orderid_rate=%s multiple_events_rate=%s status=%s",
                        task.task_key,
                        row["linked_orderid_rate"],
                        row["orders_with_multiple_events_rate"],
                        row["status"],
                    )
                except Exception as exc:
                    state["failed_tasks"][task.task_key] = str(exc)
                    logger.error("Lifecycle %s failed: %s", task.task_key, exc)
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
    write_json(output_dir / "semantic_lifecycle_summary.json", build_summary(state, output_dir))
    logger.info("Semantic lifecycle probe complete for %s with %s dates", args.year, len(rows))
    return 0 if not state["failed_tasks"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
