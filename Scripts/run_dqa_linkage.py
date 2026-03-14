from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

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
DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"


@dataclass(frozen=True)
class LinkageTask:
    year: str
    date: str
    order_paths: tuple[str, ...]
    trade_paths: tuple[str, ...]

    @property
    def task_key(self) -> str:
        return self.date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build first-pass trade-order linkage feasibility audit from stage parquet."
    )
    parser.add_argument("--year", required=True, help="Year such as 2025 or 2026.")
    parser.add_argument(
        "--stage-root",
        type=Path,
        default=DEFAULT_STAGE_ROOT,
        help="Root directory for candidate_cleaned stage parquet.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_DQA_ROOT,
        help="Root directory for DQA parquet outputs.",
    )
    parser.add_argument(
        "--research-root",
        type=Path,
        default=DEFAULT_RESEARCH_AUDITS_ROOT,
        help="Root directory for research-facing audit summaries.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for DQA logs.",
    )
    parser.add_argument(
        "--dates",
        help="Comma-separated trade dates in YYYYMMDD or YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=0,
        help="Optional limit on the number of dates to process.",
    )
    parser.add_argument(
        "--latest-days",
        action="store_true",
        help="When used with --max-days, select latest dates instead of earliest.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint and skip completed dates.",
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def canonical_date(value: str) -> str:
    digits = value.replace("-", "").strip()
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(f"Invalid date token: {value}")
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.DataFrame(rows).write_parquet(path)
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


def parse_selected_dates(args: argparse.Namespace) -> list[str]:
    order_root = args.stage_root / "orders"
    trade_root = args.stage_root / "trades"
    order_dates = {
        path.name.split("=", 1)[1]
        for path in order_root.glob("date=*")
        if path.name.split("=", 1)[1].startswith(f"{args.year}-")
    }
    trade_dates = {
        path.name.split("=", 1)[1]
        for path in trade_root.glob("date=*")
        if path.name.split("=", 1)[1].startswith(f"{args.year}-")
    }
    available_dates = sorted(order_dates & trade_dates)
    if args.dates:
        selected_dates = [canonical_date(token) for token in args.dates.split(",") if token.strip()]
    else:
        selected_dates = available_dates[-args.max_days :] if args.max_days and args.latest_days else available_dates
        if args.max_days and not args.latest_days:
            selected_dates = selected_dates[: args.max_days]
    return [value for value in selected_dates if value in available_dates]


def discover_tasks(args: argparse.Namespace) -> list[LinkageTask]:
    tasks: list[LinkageTask] = []
    for trade_date in parse_selected_dates(args):
        order_dir = args.stage_root / "orders" / f"date={trade_date}"
        trade_dir = args.stage_root / "trades" / f"date={trade_date}"
        order_paths = tuple(str(path) for path in sorted(order_dir.glob("*.parquet")))
        trade_paths = tuple(str(path) for path in sorted(trade_dir.glob("*.parquet")))
        if order_paths and trade_paths:
            tasks.append(
                LinkageTask(
                    year=str(args.year),
                    date=trade_date,
                    order_paths=order_paths,
                    trade_paths=trade_paths,
                )
            )
    return tasks


def present_id_expr(column_name: str) -> pl.Expr:
    return pl.col(column_name).is_not_null() & (pl.col(column_name) != 0)


def order_lookup_lazy(task: LinkageTask) -> pl.LazyFrame:
    return (
        pl.scan_parquet(list(task.order_paths))
        .filter(present_id_expr("OrderId"))
        .group_by("OrderId")
        .agg(pl.col("SendTime").min().alias("order_first_sendtime"))
    )


def compute_edge_metrics(task: LinkageTask) -> dict[str, Any]:
    trades_scan = pl.scan_parquet(list(task.trade_paths))
    order_lookup = order_lookup_lazy(task).select(
        [
            pl.col("OrderId").alias("trade_order_id"),
            pl.col("order_first_sendtime"),
        ]
    )
    edges = pl.concat(
        [
            trades_scan.filter(present_id_expr("BidOrderID")).select(
                [
                    pl.lit("bid").alias("side"),
                    pl.col("BidOrderID").alias("trade_order_id"),
                    pl.col("SendTime").alias("trade_sendtime"),
                ]
            ),
            trades_scan.filter(present_id_expr("AskOrderID")).select(
                [
                    pl.lit("ask").alias("side"),
                    pl.col("AskOrderID").alias("trade_order_id"),
                    pl.col("SendTime").alias("trade_sendtime"),
                ]
            ),
        ],
        how="vertical_relaxed",
    )
    return (
        edges.join(order_lookup, on="trade_order_id", how="left")
        .select(
            [
                (pl.col("side") == "bid").cast(pl.Int64).sum().alias("bid_present_count"),
                (
                    (pl.col("side") == "bid") & pl.col("order_first_sendtime").is_not_null()
                )
                .cast(pl.Int64)
                .sum()
                .alias("bid_matched_count"),
                (pl.col("side") == "ask").cast(pl.Int64).sum().alias("ask_present_count"),
                (
                    (pl.col("side") == "ask") & pl.col("order_first_sendtime").is_not_null()
                )
                .cast(pl.Int64)
                .sum()
                .alias("ask_matched_count"),
                (
                    pl.col("order_first_sendtime").is_not_null() & pl.col("trade_sendtime").is_not_null()
                )
                .cast(pl.Int64)
                .sum()
                .alias("matched_with_time_count"),
                (
                    pl.col("order_first_sendtime").is_not_null()
                    & pl.col("trade_sendtime").is_not_null()
                    & (pl.col("trade_sendtime") < pl.col("order_first_sendtime"))
                )
                .cast(pl.Int64)
                .sum()
                .alias("negative_time_lag_count"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )


def compute_both_sides_metrics(task: LinkageTask) -> dict[str, Any]:
    trades_scan = pl.scan_parquet(list(task.trade_paths)).filter(
        present_id_expr("BidOrderID") & present_id_expr("AskOrderID")
    )
    order_ids = order_lookup_lazy(task)
    return (
        trades_scan.join(
            order_ids.select(
                [
                    pl.col("OrderId").alias("bid_lookup_order_id"),
                    pl.col("order_first_sendtime").alias("bid_lookup_sendtime"),
                ]
            ),
            left_on="BidOrderID",
            right_on="bid_lookup_order_id",
            how="left",
        )
        .join(
            order_ids.select(
                [
                    pl.col("OrderId").alias("ask_lookup_order_id"),
                    pl.col("order_first_sendtime").alias("ask_lookup_sendtime"),
                ]
            ),
            left_on="AskOrderID",
            right_on="ask_lookup_order_id",
            how="left",
        )
        .select(
            [
                pl.len().alias("both_sides_present_count"),
                (
                    pl.col("bid_lookup_sendtime").is_not_null() & pl.col("ask_lookup_sendtime").is_not_null()
                )
                .cast(pl.Int64)
                .sum()
                .alias("both_sides_matched_count"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )


def linkage_status(
    *,
    combined_present_count: int,
    combined_match_rate: float | None,
    negative_time_lag_rate: float | None,
) -> str:
    if combined_present_count == 0:
        return "not_applicable"
    if combined_match_rate is None or combined_match_rate == 0:
        return "fail"
    if negative_time_lag_rate is not None and negative_time_lag_rate > 0.25:
        return "fail"
    if combined_match_rate < 0.50:
        return "warn"
    if negative_time_lag_rate is not None and negative_time_lag_rate > 0.05:
        return "warn"
    return "pass"


def process_task(task: LinkageTask) -> dict[str, Any]:
    edge_metrics = compute_edge_metrics(task)
    both_metrics = compute_both_sides_metrics(task)

    bid_present_count = int(edge_metrics["bid_present_count"] or 0)
    bid_matched_count = int(edge_metrics["bid_matched_count"] or 0)
    ask_present_count = int(edge_metrics["ask_present_count"] or 0)
    ask_matched_count = int(edge_metrics["ask_matched_count"] or 0)
    both_sides_present_count = int(both_metrics["both_sides_present_count"] or 0)
    both_sides_matched_count = int(both_metrics["both_sides_matched_count"] or 0)
    matched_with_time_count = int(edge_metrics["matched_with_time_count"] or 0)
    negative_time_lag_count = int(edge_metrics["negative_time_lag_count"] or 0)
    combined_present_count = bid_present_count + ask_present_count
    combined_matched_count = bid_matched_count + ask_matched_count

    return {
        "year": task.year,
        "date": task.date,
        "bid_orderid_present_count": bid_present_count,
        "bid_orderid_matched_count": bid_matched_count,
        "bid_match_rate": (bid_matched_count / bid_present_count) if bid_present_count else None,
        "ask_orderid_present_count": ask_present_count,
        "ask_orderid_matched_count": ask_matched_count,
        "ask_match_rate": (ask_matched_count / ask_present_count) if ask_present_count else None,
        "both_sides_present_count": both_sides_present_count,
        "both_sides_matched_count": both_sides_matched_count,
        "both_sides_match_rate": (
            both_sides_matched_count / both_sides_present_count
            if both_sides_present_count
            else None
        ),
        "matched_link_count": combined_matched_count,
        "negative_time_lag_count": negative_time_lag_count,
        "negative_time_lag_rate": (
            negative_time_lag_count / matched_with_time_count if matched_with_time_count else None
        ),
        "cross_session_match_rate": None,
        "cross_session_supported": False,
        "status": linkage_status(
            combined_present_count=combined_present_count,
            combined_match_rate=(
                combined_matched_count / combined_present_count if combined_present_count else None
            ),
            negative_time_lag_rate=(
                negative_time_lag_count / matched_with_time_count if matched_with_time_count else None
            ),
        ),
    }


def build_summary(state: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    return {
        "pipeline": "dqa_linkage",
        "status": state["status"],
        "year": state["year"],
        "generated_at": iso_utc_now(),
        "completed_count": state["completed_count"],
        "failed_count": state["failed_count"],
        "pending_count": state["pending_count"],
        "artifacts": {
            "checkpoint": str(output_dir / "checkpoint.json"),
            "heartbeat": str(output_dir / "heartbeat.json"),
            "linkage_jsonl": str(output_dir / "audit_linkage_feasibility_daily.jsonl"),
            "summary": str(output_dir / "summary.json"),
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
        },
    )


def report_markdown(path: Path, *, year: str, rows: list[dict[str, Any]], state: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    top_lines = [
        f"# DQA Linkage {year}",
        "",
        f"- generated_at: {iso_utc_now()}",
        f"- completed_count: {state['completed_count']}",
        f"- failed_count: {state['failed_count']}",
        f"- pending_count: {state['pending_count']}",
        "",
    ]
    if rows:
        passes = sum(1 for row in rows if row["status"] == "pass")
        warns = sum(1 for row in rows if row["status"] == "warn")
        fails = sum(1 for row in rows if row["status"] == "fail")
        top_lines.extend(
            [
                f"- pass_days: {passes}",
                f"- warn_days: {warns}",
                f"- fail_days: {fails}",
                "",
                "First-pass linkage feasibility focuses only on same-day OrderId matchability and lag sanity.",
            ]
        )
    else:
        top_lines.append("No linkage rows were materialized.")
    path.write_text("\n".join(top_lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_dqa_linkage",
            purpose="Measure same-day trade-order linkage feasibility before semantic claims are made.",
            responsibilities=[
                "Estimate BidOrderID and AskOrderID matchability into same-day Orders.OrderId.",
                "Measure first-pass negative lag rate using trade SendTime versus earliest matched order SendTime.",
                "Persist a daily linkage feasibility table plus resumable task state and summary report.",
            ],
            inputs=[
                "/Volumes/Data/港股Tick数据/candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet",
                "/Volumes/Data/港股Tick数据/candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/dqa/linkage/year=<year>/audit_linkage_feasibility_daily.parquet",
                "Research/Audits/dqa_linkage_<year>.md",
            ],
        )
        return 0

    tasks = discover_tasks(args)
    output_dir = args.output_root / "linkage" / f"year={args.year}"
    research_report_path = args.research_root / f"dqa_linkage_{args.year}.md"
    log_path = args.log_root / f"dqa_linkage_{args.year}.log"
    logger = configure_logger("dqa_linkage", log_path)

    if not tasks:
        logger.error("No stage parquet orders/trades date intersections matched the requested selection.")
        return 1

    ensure_dir(output_dir)
    checkpoint_path = output_dir / "checkpoint.json"
    heartbeat_path = output_dir / "heartbeat.json"
    linkage_jsonl_path = output_dir / "audit_linkage_feasibility_daily.jsonl"
    summary_path = output_dir / "summary.json"

    if not args.resume:
        reset_manifest_files([checkpoint_path, heartbeat_path, linkage_jsonl_path, summary_path])
        state = {
            "status": "running",
            "year": args.year,
            "started_at": iso_utc_now(),
            "updated_at": iso_utc_now(),
            "completed_task_keys": [],
            "failed_tasks": {},
            "completed_count": 0,
            "failed_count": 0,
            "pending_count": len(tasks),
            "active_task_key": None,
        }
    else:
        if not checkpoint_path.exists():
            logger.error("Cannot resume because checkpoint is missing: %s", checkpoint_path)
            return 1
        state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        state["status"] = "running"
        state["active_task_key"] = None

    completed_task_keys = set(state.get("completed_task_keys", []))
    write_checkpoint(checkpoint_path, heartbeat_path, state)

    for task in tasks:
        if task.task_key in completed_task_keys:
            continue

        state["active_task_key"] = task.task_key
        state["pending_count"] = max(0, len(tasks) - len(completed_task_keys) - state["failed_count"])
        write_checkpoint(checkpoint_path, heartbeat_path, state)

        try:
            row = process_task(task)
            append_jsonl(linkage_jsonl_path, row)
            completed_task_keys.add(task.task_key)
            logger.info(
                "DQA linkage task %s complete: bid_match_rate=%s ask_match_rate=%s status=%s",
                task.task_key,
                row["bid_match_rate"],
                row["ask_match_rate"],
                row["status"],
            )
        except Exception as exc:
            state["failed_tasks"][task.task_key] = str(exc)
            logger.error("DQA linkage task %s failed: %s", task.task_key, exc)

        state["completed_task_keys"] = sorted(completed_task_keys)
        state["completed_count"] = len(completed_task_keys)
        state["failed_count"] = len(state["failed_tasks"])
        state["pending_count"] = max(0, len(tasks) - state["completed_count"] - state["failed_count"])
        state["active_task_key"] = None
        write_checkpoint(checkpoint_path, heartbeat_path, state)

    linkage_rows = read_jsonl_rows(linkage_jsonl_path)
    write_parquet(linkage_rows, output_dir / "audit_linkage_feasibility_daily.parquet")
    report_markdown(research_report_path, year=str(args.year), rows=linkage_rows, state=state)

    state["status"] = "completed" if not state["failed_tasks"] else "completed_with_failures"
    write_checkpoint(checkpoint_path, heartbeat_path, state)
    write_json(summary_path, build_summary(state, output_dir))
    logger.info(
        "DQA linkage %s for %s: completed=%s failed=%s output=%s",
        state["status"],
        args.year,
        state["completed_count"],
        state["failed_count"],
        output_dir,
    )
    return 0 if not state["failed_tasks"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
