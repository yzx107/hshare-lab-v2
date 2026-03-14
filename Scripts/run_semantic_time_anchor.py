from __future__ import annotations

import argparse
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
        description="Probe whether Time can act as a coarse order-side temporal anchor."
    )
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
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
        help="Root directory for semantic time-anchor outputs.",
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
        help="Root directory for semantic time-anchor logs.",
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


def normalized_time_text_expr(column_name: str) -> pl.Expr:
    stripped = pl.col(column_name).cast(pl.String, strict=False).str.strip_chars()
    digits_only = stripped.str.replace_all(r"[^0-9]", "")
    return (
        pl.when(digits_only.is_null() | (digits_only == ""))
        .then(None)
        .when(digits_only.str.len_chars() <= 6)
        .then(digits_only.str.zfill(6))
        .otherwise(None)
    )


def time_seconds_expr(column_name: str) -> pl.Expr:
    text = normalized_time_text_expr(column_name)
    hours = text.str.slice(0, 2).cast(pl.Int32, strict=False)
    minutes = text.str.slice(2, 2).cast(pl.Int32, strict=False)
    seconds = text.str.slice(4, 2).cast(pl.Int32, strict=False)
    valid = (
        text.is_not_null()
        & hours.is_between(0, 23)
        & minutes.is_between(0, 59)
        & seconds.is_between(0, 59)
    )
    return (
        pl.when(valid)
        .then(hours * 3600 + minutes * 60 + seconds)
        .otherwise(None)
    )


def coarse_time_anchor_status(
    *,
    matched_edge_count: int,
    matched_both_time_nonnull_rate: float | None,
    matched_order_time_le_trade_time_rate: float | None,
    matched_same_time_rate: float | None,
) -> str:
    if matched_edge_count == 0:
        return "not_applicable"
    if matched_both_time_nonnull_rate is None or matched_both_time_nonnull_rate == 0:
        return "unavailable"
    if (
        matched_order_time_le_trade_time_rate is not None
        and matched_order_time_le_trade_time_rate >= 0.95
        and matched_same_time_rate is not None
        and matched_same_time_rate >= 0.50
    ):
        return "weak_pass"
    if matched_order_time_le_trade_time_rate is not None and matched_order_time_le_trade_time_rate >= 0.80:
        return "warn"
    return "fail"


def research_time_grade(
    *,
    orders_sendtime_nonnull_rate: float | None,
    coarse_status: str,
) -> str:
    if orders_sendtime_nonnull_rate is not None and orders_sendtime_nonnull_rate >= 0.99:
        return "fine_ok"
    if coarse_status in {"pass", "weak_pass"}:
        return "coarse_only"
    return "blocked"


def density_metrics(frame: pl.LazyFrame, *, time_column: str, prefix: str) -> dict[str, Any]:
    second_counts = (
        frame.with_columns(time_seconds_expr(time_column).alias("time_seconds"))
        .filter(pl.col("time_seconds").is_not_null())
        .group_by("time_seconds")
        .agg(pl.len().cast(pl.Int64).alias("row_count"))
    )
    summary = (
        second_counts.select(
            [
                pl.len().cast(pl.Int64).alias(f"{prefix}_distinct_second_count"),
                pl.col("row_count").median().alias(f"{prefix}_median_rows_per_second"),
                pl.col("row_count").quantile(0.99).alias(f"{prefix}_p99_rows_per_second"),
                pl.col("row_count").max().alias(f"{prefix}_max_rows_per_second"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )
    if summary[f"{prefix}_distinct_second_count"] is None:
        summary[f"{prefix}_distinct_second_count"] = 0
    return summary


def investigate_date(trade_date: str, *, stage_root: Path, year: str) -> dict[str, Any]:
    order_paths = [str(path) for path in sorted((stage_root / "orders" / f"date={trade_date}").glob("*.parquet"))]
    trade_paths = [str(path) for path in sorted((stage_root / "trades" / f"date={trade_date}").glob("*.parquet"))]

    orders = pl.scan_parquet(order_paths)
    trades = pl.scan_parquet(trade_paths)

    order_summary = (
        orders.select(
            [
                pl.len().cast(pl.Int64).alias("order_rows"),
                pl.col("Time").is_not_null().sum().cast(pl.Int64).alias("orders_time_nonnull_count"),
                pl.col("SendTime").is_not_null().sum().cast(pl.Int64).alias("orders_sendtime_nonnull_count"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )
    trade_summary = (
        trades.select(
            [
                pl.len().cast(pl.Int64).alias("trade_rows"),
                pl.col("Time").is_not_null().sum().cast(pl.Int64).alias("trades_time_nonnull_count"),
                pl.col("SendTime").is_not_null().sum().cast(pl.Int64).alias("trades_sendtime_nonnull_count"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    order_density = density_metrics(orders, time_column="Time", prefix="orders")
    trade_density = density_metrics(trades, time_column="Time", prefix="trades")

    order_lookup = (
        orders.filter(present_id_expr("OrderId"))
        .with_columns(
            [
                normalized_time_text_expr("Time").alias("order_time_text"),
                time_seconds_expr("Time").alias("order_time_seconds"),
            ]
        )
        .group_by("OrderId")
        .agg(
            [
                pl.col("order_time_text").drop_nulls().first().alias("order_first_time_text"),
                pl.col("order_time_seconds").drop_nulls().min().alias("order_first_time_seconds"),
                pl.col("order_time_seconds").drop_nulls().max().alias("order_last_time_seconds"),
                pl.col("order_time_seconds").is_not_null().any().alias("order_has_time"),
            ]
        )
        .select(
            [
                pl.col("OrderId").alias("trade_order_id"),
                pl.col("order_first_time_text"),
                pl.col("order_first_time_seconds"),
                pl.col("order_last_time_seconds"),
                pl.col("order_has_time"),
            ]
        )
    )

    edges = pl.concat(
        [
            trades.filter(present_id_expr("BidOrderID")).select(
                [
                    pl.lit("bid").alias("side"),
                    pl.col("BidOrderID").alias("trade_order_id"),
                    normalized_time_text_expr("Time").alias("trade_time_text"),
                    time_seconds_expr("Time").alias("trade_time_seconds"),
                ]
            ),
            trades.filter(present_id_expr("AskOrderID")).select(
                [
                    pl.lit("ask").alias("side"),
                    pl.col("AskOrderID").alias("trade_order_id"),
                    normalized_time_text_expr("Time").alias("trade_time_text"),
                    time_seconds_expr("Time").alias("trade_time_seconds"),
                ]
            ),
        ],
        how="vertical_relaxed",
    )

    edge_summary = (
        edges.join(order_lookup, on="trade_order_id", how="left")
        .with_columns(
            [
                (pl.col("order_has_time").fill_null(False) & pl.col("trade_time_seconds").is_not_null()).alias(
                    "both_times_present"
                ),
                (pl.col("trade_time_text") == pl.col("order_first_time_text")).alias("same_time_text"),
                (pl.col("trade_time_seconds") - pl.col("order_first_time_seconds")).alias("delta_seconds"),
            ]
        )
        .select(
            [
                pl.len().cast(pl.Int64).alias("matched_edge_count"),
                pl.col("order_has_time").fill_null(False).cast(pl.Int64).sum().alias("matched_order_time_nonnull_count"),
                pl.col("trade_time_seconds").is_not_null().cast(pl.Int64).sum().alias("matched_trade_time_nonnull_count"),
                pl.col("both_times_present").cast(pl.Int64).sum().alias("matched_both_time_nonnull_count"),
                (
                    pl.col("both_times_present") & pl.col("same_time_text").fill_null(False)
                )
                .cast(pl.Int64)
                .sum()
                .alias("matched_same_time_count"),
                (
                    pl.col("both_times_present") & (pl.col("delta_seconds") >= 0)
                )
                .cast(pl.Int64)
                .sum()
                .alias("matched_order_time_le_trade_time_count"),
                (
                    pl.col("both_times_present") & (pl.col("delta_seconds") < 0)
                )
                .cast(pl.Int64)
                .sum()
                .alias("matched_negative_second_delta_count"),
                (
                    pl.col("both_times_present") & (pl.col("delta_seconds") == 0)
                )
                .cast(pl.Int64)
                .sum()
                .alias("matched_zero_second_delta_count"),
                (
                    pl.col("both_times_present") & (pl.col("delta_seconds") > 0)
                )
                .cast(pl.Int64)
                .sum()
                .alias("matched_positive_second_delta_count"),
                pl.col("delta_seconds").filter(pl.col("both_times_present")).median().alias("matched_delta_p50_seconds"),
                pl.col("delta_seconds").filter(pl.col("both_times_present")).quantile(0.99).alias(
                    "matched_delta_p99_seconds"
                ),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    order_rows = int(order_summary["order_rows"] or 0)
    trade_rows = int(trade_summary["trade_rows"] or 0)
    matched_edge_count = int(edge_summary["matched_edge_count"] or 0)
    matched_both_time_nonnull_count = int(edge_summary["matched_both_time_nonnull_count"] or 0)
    matched_same_time_count = int(edge_summary["matched_same_time_count"] or 0)
    matched_order_time_le_trade_time_count = int(edge_summary["matched_order_time_le_trade_time_count"] or 0)

    matched_both_time_nonnull_rate = (
        matched_both_time_nonnull_count / matched_edge_count if matched_edge_count else None
    )
    matched_same_time_rate = matched_same_time_count / matched_edge_count if matched_edge_count else None
    matched_order_time_le_trade_time_rate = (
        matched_order_time_le_trade_time_count / matched_edge_count if matched_edge_count else None
    )
    coarse_status = coarse_time_anchor_status(
        matched_edge_count=matched_edge_count,
        matched_both_time_nonnull_rate=matched_both_time_nonnull_rate,
        matched_order_time_le_trade_time_rate=matched_order_time_le_trade_time_rate,
        matched_same_time_rate=matched_same_time_rate,
    )
    orders_sendtime_nonnull_rate = (
        int(order_summary["orders_sendtime_nonnull_count"] or 0) / order_rows if order_rows else None
    )

    return {
        "year": year,
        "date": trade_date,
        "orders_time_nonnull_count": int(order_summary["orders_time_nonnull_count"] or 0),
        "orders_time_nonnull_rate": (
            int(order_summary["orders_time_nonnull_count"] or 0) / order_rows if order_rows else None
        ),
        "orders_sendtime_nonnull_count": int(order_summary["orders_sendtime_nonnull_count"] or 0),
        "orders_sendtime_nonnull_rate": orders_sendtime_nonnull_rate,
        "trades_time_nonnull_count": int(trade_summary["trades_time_nonnull_count"] or 0),
        "trades_time_nonnull_rate": (
            int(trade_summary["trades_time_nonnull_count"] or 0) / trade_rows if trade_rows else None
        ),
        "trades_sendtime_nonnull_count": int(trade_summary["trades_sendtime_nonnull_count"] or 0),
        "trades_sendtime_nonnull_rate": (
            int(trade_summary["trades_sendtime_nonnull_count"] or 0) / trade_rows if trade_rows else None
        ),
        **order_density,
        **trade_density,
        "matched_edge_count": matched_edge_count,
        "matched_order_time_nonnull_count": int(edge_summary["matched_order_time_nonnull_count"] or 0),
        "matched_trade_time_nonnull_count": int(edge_summary["matched_trade_time_nonnull_count"] or 0),
        "matched_both_time_nonnull_count": matched_both_time_nonnull_count,
        "matched_both_time_nonnull_rate": matched_both_time_nonnull_rate,
        "matched_same_time_count": matched_same_time_count,
        "matched_same_time_rate": matched_same_time_rate,
        "matched_order_time_le_trade_time_count": matched_order_time_le_trade_time_count,
        "matched_order_time_le_trade_time_rate": matched_order_time_le_trade_time_rate,
        "matched_negative_second_delta_count": int(edge_summary["matched_negative_second_delta_count"] or 0),
        "matched_negative_second_delta_rate": (
            int(edge_summary["matched_negative_second_delta_count"] or 0) / matched_edge_count
            if matched_edge_count
            else None
        ),
        "matched_zero_second_delta_count": int(edge_summary["matched_zero_second_delta_count"] or 0),
        "matched_positive_second_delta_count": int(edge_summary["matched_positive_second_delta_count"] or 0),
        "matched_delta_p50_seconds": edge_summary["matched_delta_p50_seconds"],
        "matched_delta_p99_seconds": edge_summary["matched_delta_p99_seconds"],
        "coarse_time_anchor_status": coarse_status,
        "research_time_grade": research_time_grade(
            orders_sendtime_nonnull_rate=orders_sendtime_nonnull_rate,
            coarse_status=coarse_status,
        ),
    }


def write_report_markdown(path: Path, *, year: str, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    lines = [
        f"# Semantic Time Anchor Probe {year}",
        "",
        f"- generated_at: {iso_utc_now()}",
        f"- dates: {len(rows)}",
        "",
        "This probe checks whether `Time` can support coarse temporal validation when order-side `SendTime` is unavailable.",
    ]
    for row in rows:
        lines.extend(
            [
                "",
                f"## {row['date']}",
                f"- orders_time_nonnull_rate: {row['orders_time_nonnull_rate']}",
                f"- trades_time_nonnull_rate: {row['trades_time_nonnull_rate']}",
                f"- matched_both_time_nonnull_rate: {row['matched_both_time_nonnull_rate']}",
                f"- matched_same_time_rate: {row['matched_same_time_rate']}",
                f"- matched_order_time_le_trade_time_rate: {row['matched_order_time_le_trade_time_rate']}",
                f"- matched_negative_second_delta_rate: {row['matched_negative_second_delta_rate']}",
                f"- coarse_time_anchor_status: {row['coarse_time_anchor_status']}",
                f"- research_time_grade: {row['research_time_grade']}",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_summary(year: str, output_dir: Path, rows: list[dict[str, Any]], report_path: Path) -> dict[str, Any]:
    return {
        "pipeline": "semantic_time_anchor",
        "status": "completed",
        "year": year,
        "generated_at": iso_utc_now(),
        "date_count": len(rows),
        "artifacts": {
            "daily_rows": str(output_dir / "semantic_time_anchor_daily.parquet"),
            "summary": str(output_dir / "summary.json"),
            "report": str(report_path),
        },
    }


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_semantic_time_anchor",
            purpose="Probe whether Time can act as a coarse temporal anchor when SendTime is missing.",
            responsibilities=[
                "Measure non-null coverage of Orders.Time and Trades.Time.",
                "Measure matched-edge Time consistency after direct OrderId equality.",
                "Classify whether coarse time-aware validation looks usable, weakly usable, or unavailable.",
            ],
            inputs=[
                "candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet",
                "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "dqa/semantic_time_anchor/year=<year>/*.parquet",
                "Research/Audits/semantic_time_anchor_<year>.md",
            ],
        )
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")

    selected_dates = parse_selected_dates(args)
    if not selected_dates:
        raise SystemExit("No overlapping stage dates matched the requested selection.")

    output_dir = args.output_root / "semantic_time_anchor" / f"year={args.year}"
    report_path = args.research_root / f"semantic_time_anchor_{args.year}.md"
    log_path = args.log_root / f"semantic_time_anchor_{args.year}.log"
    logger = configure_logger("semantic_time_anchor", log_path)

    ensure_dir(output_dir)
    rows: list[dict[str, Any]] = []
    for trade_date in selected_dates:
        row = investigate_date(trade_date, stage_root=args.stage_root, year=str(args.year))
        rows.append(row)
        logger.info(
            "Semantic time-anchor %s: both_time_rate=%s order_le_trade_rate=%s status=%s",
            trade_date,
            row["matched_both_time_nonnull_rate"],
            row["matched_order_time_le_trade_time_rate"],
            row["coarse_time_anchor_status"],
        )

    write_parquet(rows, output_dir / "semantic_time_anchor_daily.parquet")
    write_report_markdown(report_path, year=str(args.year), rows=rows)
    write_json(output_dir / "summary.json", build_summary(str(args.year), output_dir, rows, report_path))
    logger.info("Semantic time-anchor probe complete for %s: dates=%s output=%s", args.year, len(rows), output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
