from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Investigate OrderId vs BidOrderID/AskOrderID ID-space relationships."
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
        help="Root directory for semantic investigation outputs.",
    )
    parser.add_argument(
        "--research-root",
        type=Path,
        default=DEFAULT_RESEARCH_AUDITS_ROOT,
        help="Root directory for research-facing semantic notes.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for semantic investigation logs.",
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
        pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


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


def present_id_expr(column_name: str) -> pl.Expr:
    return pl.col(column_name).is_not_null() & (pl.col(column_name) != 0)


def order_lookup_lazy(order_paths: list[str]) -> pl.LazyFrame:
    return (
        pl.scan_parquet(order_paths)
        .filter(present_id_expr("OrderId"))
        .group_by("OrderId")
        .agg(
            [
                pl.len().alias("order_event_count"),
                pl.col("SendTime").is_not_null().any().alias("order_has_sendtime"),
                pl.col("SendTime").min().alias("order_first_sendtime"),
            ]
        )
        .select(
            [
                pl.col("OrderId").alias("trade_order_id"),
                pl.col("order_event_count"),
                pl.col("order_has_sendtime"),
                pl.col("order_first_sendtime"),
            ]
        )
    )


def build_edge_scan(trade_paths: list[str]) -> pl.LazyFrame:
    trades = pl.scan_parquet(trade_paths)
    return pl.concat(
        [
            trades.filter(present_id_expr("BidOrderID")).select(
                [
                    pl.lit("bid").alias("side"),
                    pl.col("BidOrderID").alias("trade_order_id"),
                    pl.col("SendTime").alias("trade_sendtime"),
                ]
            ),
            trades.filter(present_id_expr("AskOrderID")).select(
                [
                    pl.lit("ask").alias("side"),
                    pl.col("AskOrderID").alias("trade_order_id"),
                    pl.col("SendTime").alias("trade_sendtime"),
                ]
            ),
        ],
        how="vertical_relaxed",
    )


def collect_id_stats(frame: pl.LazyFrame, column_name: str) -> dict[str, Any]:
    return (
        frame.select(
            [
                pl.len().alias("rows"),
                pl.col(column_name).filter(present_id_expr(column_name)).count().alias("present_count"),
                pl.col(column_name).filter(present_id_expr(column_name)).n_unique().alias("distinct_count"),
                pl.col(column_name).filter(present_id_expr(column_name)).min().alias("min_id"),
                pl.col(column_name).filter(present_id_expr(column_name)).max().alias("max_id"),
                pl.col(column_name).filter(present_id_expr(column_name)).quantile(0.5).alias("p50_id"),
                pl.col(column_name).filter(present_id_expr(column_name)).quantile(0.99).alias("p99_id"),
                pl.col(column_name)
                .filter(present_id_expr(column_name))
                .abs()
                .cast(pl.String)
                .str.len_chars()
                .mode()
                .first()
                .alias("length_mode"),
                pl.col(column_name)
                .filter(present_id_expr(column_name))
                .mod(10)
                .mode()
                .first()
                .alias("mod10_mode"),
                pl.col(column_name)
                .filter(present_id_expr(column_name))
                .mod(2)
                .mean()
                .alias("odd_share"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )


def witness_overlap_rows(
    *,
    order_paths: list[str],
    trade_paths: list[str],
) -> list[dict[str, Any]]:
    orders = pl.scan_parquet(order_paths)
    trades = pl.scan_parquet(trade_paths)
    candidate_ids = (
        orders.select(pl.col("OrderId").max().alias("witness_id"))
        .collect()
        .to_series()
        .to_list()
    )
    rows: list[dict[str, Any]] = []
    for witness_id in candidate_ids:
        if witness_id is None:
            continue
        order_hit = (
            orders.filter(pl.col("OrderId") == witness_id)
            .select(
                [
                    pl.lit("orders").alias("source"),
                    pl.col("OrderId").alias("order_id"),
                    pl.col("SendTime").cast(pl.String).alias("sendtime"),
                    pl.col("SendTimeRaw").alias("sendtime_raw"),
                    pl.col("Time").alias("time"),
                ]
            )
            .limit(1)
            .collect()
            .to_dicts()
        )
        bid_hit = (
            trades.filter(pl.col("BidOrderID") == witness_id)
            .select(
                [
                    pl.lit("trades_bid").alias("source"),
                    pl.col("BidOrderID").alias("order_id"),
                    pl.col("SendTime").cast(pl.String).alias("sendtime"),
                    pl.col("SendTimeRaw").alias("sendtime_raw"),
                    pl.col("Time").alias("time"),
                    pl.col("TickID").alias("tick_id"),
                ]
            )
            .limit(1)
            .collect()
            .to_dicts()
        )
        ask_hit = (
            trades.filter(pl.col("AskOrderID") == witness_id)
            .select(
                [
                    pl.lit("trades_ask").alias("source"),
                    pl.col("AskOrderID").alias("order_id"),
                    pl.col("SendTime").cast(pl.String).alias("sendtime"),
                    pl.col("SendTimeRaw").alias("sendtime_raw"),
                    pl.col("Time").alias("time"),
                    pl.col("TickID").alias("tick_id"),
                ]
            )
            .limit(1)
            .collect()
            .to_dicts()
        )
        combined = order_hit + bid_hit + ask_hit
        if len(combined) >= 2:
            rows.extend(combined)
    return rows


def investigate_date(trade_date: str, *, stage_root: Path, year: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    order_paths = [str(path) for path in sorted((stage_root / "orders" / f"date={trade_date}").glob("*.parquet"))]
    trade_paths = [str(path) for path in sorted((stage_root / "trades" / f"date={trade_date}").glob("*.parquet"))]

    orders = pl.scan_parquet(order_paths)
    trades = pl.scan_parquet(trade_paths)
    lookup = order_lookup_lazy(order_paths)
    edges = build_edge_scan(trade_paths)

    order_stats = collect_id_stats(orders, "OrderId")
    bid_stats = collect_id_stats(trades, "BidOrderID")
    ask_stats = collect_id_stats(trades, "AskOrderID")

    sendtime_stats = orders.select(
        [
            pl.len().alias("order_rows"),
            pl.col("SendTime").is_null().sum().alias("order_sendtime_null_count"),
            pl.col("SendTime").is_not_null().sum().alias("order_sendtime_present_count"),
            pl.col("Time").is_null().sum().alias("order_time_null_count"),
        ]
    ).collect().to_dicts()[0]

    edge_stats = (
        edges.join(lookup, on="trade_order_id", how="left")
        .select(
            [
                (((pl.col("side") == "bid") & pl.col("order_event_count").is_not_null()).cast(pl.Int64).sum()).alias("bid_id_equal_count"),
                (((pl.col("side") == "ask") & pl.col("order_event_count").is_not_null()).cast(pl.Int64).sum()).alias("ask_id_equal_count"),
                (((pl.col("side") == "bid") & pl.col("order_has_sendtime").fill_null(False)).cast(pl.Int64).sum()).alias("bid_with_order_sendtime_count"),
                (((pl.col("side") == "ask") & pl.col("order_has_sendtime").fill_null(False)).cast(pl.Int64).sum()).alias("ask_with_order_sendtime_count"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    summary = {
        "year": year,
        "date": trade_date,
        "order_present_count": int(order_stats["present_count"] or 0),
        "order_distinct_count": int(order_stats["distinct_count"] or 0),
        "order_min_id": order_stats["min_id"],
        "order_max_id": order_stats["max_id"],
        "order_p50_id": order_stats["p50_id"],
        "order_p99_id": order_stats["p99_id"],
        "order_length_mode": order_stats["length_mode"],
        "order_mod10_mode": order_stats["mod10_mode"],
        "order_odd_share": order_stats["odd_share"],
        "bid_present_count": int(bid_stats["present_count"] or 0),
        "bid_distinct_count": int(bid_stats["distinct_count"] or 0),
        "bid_min_id": bid_stats["min_id"],
        "bid_max_id": bid_stats["max_id"],
        "bid_p50_id": bid_stats["p50_id"],
        "bid_p99_id": bid_stats["p99_id"],
        "bid_length_mode": bid_stats["length_mode"],
        "bid_mod10_mode": bid_stats["mod10_mode"],
        "bid_odd_share": bid_stats["odd_share"],
        "ask_present_count": int(ask_stats["present_count"] or 0),
        "ask_distinct_count": int(ask_stats["distinct_count"] or 0),
        "ask_min_id": ask_stats["min_id"],
        "ask_max_id": ask_stats["max_id"],
        "ask_p50_id": ask_stats["p50_id"],
        "ask_p99_id": ask_stats["p99_id"],
        "ask_length_mode": ask_stats["length_mode"],
        "ask_mod10_mode": ask_stats["mod10_mode"],
        "ask_odd_share": ask_stats["odd_share"],
        "order_sendtime_null_count": int(sendtime_stats["order_sendtime_null_count"] or 0),
        "order_sendtime_present_count": int(sendtime_stats["order_sendtime_present_count"] or 0),
        "order_time_null_count": int(sendtime_stats["order_time_null_count"] or 0),
        "bid_id_equal_count": int(edge_stats["bid_id_equal_count"] or 0),
        "ask_id_equal_count": int(edge_stats["ask_id_equal_count"] or 0),
        "bid_id_equal_rate": (
            edge_stats["bid_id_equal_count"] / bid_stats["present_count"]
            if bid_stats["present_count"]
            else None
        ),
        "ask_id_equal_rate": (
            edge_stats["ask_id_equal_count"] / ask_stats["present_count"]
            if ask_stats["present_count"]
            else None
        ),
        "bid_with_order_sendtime_count": int(edge_stats["bid_with_order_sendtime_count"] or 0),
        "ask_with_order_sendtime_count": int(edge_stats["ask_with_order_sendtime_count"] or 0),
        "same_max_bid_order": bid_stats["max_id"] == order_stats["max_id"] if bid_stats["max_id"] is not None else None,
        "same_max_ask_order": ask_stats["max_id"] == order_stats["max_id"] if ask_stats["max_id"] is not None else None,
    }
    witness_rows = witness_overlap_rows(order_paths=order_paths, trade_paths=trade_paths)
    return summary, [
        {
            "year": year,
            "date": trade_date,
            **row,
        }
        for row in witness_rows
    ]


def write_report_markdown(path: Path, *, year: str, summary_rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    lines = [
        f"# Semantic ID Space Probe {year}",
        "",
        f"- generated_at: {iso_utc_now()}",
        f"- dates: {len(summary_rows)}",
        "",
        "This probe separates direct id equality from matches that also have usable order-side SendTime.",
    ]
    for row in summary_rows:
        lines.extend(
            [
                "",
                f"## {row['date']}",
                f"- bid_id_equal_rate: {row['bid_id_equal_rate']}",
                f"- ask_id_equal_rate: {row['ask_id_equal_rate']}",
                f"- bid_with_order_sendtime_count: {row['bid_with_order_sendtime_count']}",
                f"- ask_with_order_sendtime_count: {row['ask_with_order_sendtime_count']}",
                f"- order_sendtime_present_count: {row['order_sendtime_present_count']}",
                f"- same_max_bid_order: {row['same_max_bid_order']}",
                f"- same_max_ask_order: {row['same_max_ask_order']}",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_summary(year: str, output_dir: Path, summary_rows: list[dict[str, Any]], report_path: Path) -> dict[str, Any]:
    return {
        "pipeline": "semantic_idspace",
        "status": "completed",
        "year": year,
        "generated_at": iso_utc_now(),
        "date_count": len(summary_rows),
        "artifacts": {
            "summary_rows": str(output_dir / "semantic_idspace_daily.parquet"),
            "witness_rows": str(output_dir / "semantic_idspace_witness_rows.parquet"),
            "summary": str(output_dir / "summary.json"),
            "report": str(report_path),
        },
    }


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_semantic_idspace",
            purpose="Probe direct ID equality and time-backed linkage separately before semantic claims are made.",
            responsibilities=[
                "Measure how often BidOrderID/AskOrderID directly equal Orders.OrderId.",
                "Separate direct equality from edges that also have usable order-side SendTime.",
                "Leave date-level summaries plus a few witness rows for investigation notes.",
            ],
            inputs=[
                "candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet",
                "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "dqa/semantic_idspace/year=<year>/*.parquet",
                "Research/Audits/semantic_idspace_<year>.md",
            ],
        )
        return 0

    selected_dates = parse_selected_dates(args)
    if not selected_dates:
        raise SystemExit("No overlapping stage dates matched the requested selection.")

    output_dir = args.output_root / "semantic_idspace" / f"year={args.year}"
    report_path = args.research_root / f"semantic_idspace_{args.year}.md"
    log_path = args.log_root / f"semantic_idspace_{args.year}.log"
    logger = configure_logger("semantic_idspace", log_path)

    ensure_dir(output_dir)
    summary_rows: list[dict[str, Any]] = []
    witness_rows: list[dict[str, Any]] = []

    for trade_date in selected_dates:
        summary_row, date_witness_rows = investigate_date(
            trade_date,
            stage_root=args.stage_root,
            year=str(args.year),
        )
        summary_rows.append(summary_row)
        witness_rows.extend(date_witness_rows)
        logger.info(
            "Semantic id-space %s: bid_id_equal_rate=%s ask_id_equal_rate=%s order_sendtime_present=%s",
            trade_date,
            summary_row["bid_id_equal_rate"],
            summary_row["ask_id_equal_rate"],
            summary_row["order_sendtime_present_count"],
        )

    write_parquet(summary_rows, output_dir / "semantic_idspace_daily.parquet")
    write_parquet(witness_rows, output_dir / "semantic_idspace_witness_rows.parquet")
    write_report_markdown(report_path, year=str(args.year), summary_rows=summary_rows)
    write_json(output_dir / "summary.json", build_summary(str(args.year), output_dir, summary_rows, report_path))
    logger.info("Semantic id-space probe complete for %s: dates=%s output=%s", args.year, len(summary_rows), output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
