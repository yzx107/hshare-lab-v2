from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, DEFAULT_LOG_ROOT, configure_logger, ensure_dir, iso_utc_now, print_scaffold_plan, write_json
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
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


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


def order_scan(order_paths: list[str], limit_rows: int) -> pl.LazyFrame:
    scan = pl.scan_parquet(order_paths)
    return scan.limit(limit_rows) if limit_rows > 0 else scan


def trade_scan(trade_paths: list[str], limit_rows: int) -> pl.LazyFrame:
    scan = pl.scan_parquet(trade_paths)
    return scan.limit(limit_rows) if limit_rows > 0 else scan


def has_column(frame: pl.LazyFrame, column_name: str) -> bool:
    return column_name in frame.collect_schema().names()


def classify_status(*, distinct_orderids: int, linked_orderids: int, multi_event_orderids: int) -> str:
    if distinct_orderids == 0:
        return STATUS_NOT_APPLICABLE
    linked_rate = linked_orderids / distinct_orderids
    if linked_rate >= 0.9 and multi_event_orderids > 0:
        return STATUS_PASS
    if linked_rate >= 0.2:
        return STATUS_WEAK_PASS
    return STATUS_UNKNOWN


def investigate_date(trade_date: str, *, stage_root: Path, year: str, limit_rows: int) -> dict[str, Any]:
    order_paths = [str(path) for path in sorted((stage_root / "orders" / f"date={trade_date}").glob("*.parquet"))]
    trade_paths = [str(path) for path in sorted((stage_root / "trades" / f"date={trade_date}").glob("*.parquet"))]
    orders = order_scan(order_paths, limit_rows)
    trades = trade_scan(trade_paths, limit_rows)
    trade_has_seqnum = has_column(trades, "SeqNum")

    order_group = (
        orders.filter(pl.col("OrderId").is_not_null() & (pl.col("OrderId") != 0))
        .group_by("OrderId")
        .agg(
            [
                pl.len().cast(pl.Int64).alias("order_event_count"),
                pl.col("SeqNum").drop_nulls().count().alias("seqnum_present_count"),
                pl.col("OrderType").drop_nulls().n_unique().alias("distinct_ordertype_values"),
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
        ]
    )

    lifecycle = (
        order_group.join(trade_links, on="OrderId", how="left")
        .with_columns(
            [
                pl.col("trade_match_count").fill_null(0),
                pl.col("trade_seqnum_present_count").fill_null(0),
            ]
        )
        .collect()
    )
    order_rows = int(orders.select(pl.len().cast(pl.Int64).alias("rows")).collect().to_dicts()[0]["rows"] or 0)
    distinct_orderids = lifecycle.height
    linked_orderids = lifecycle.filter(pl.col("trade_match_count") > 0).height
    multi_event_orderids = lifecycle.filter(pl.col("order_event_count") > 1).height
    multiple_trades = lifecycle.filter(pl.col("trade_match_count") > 1).height
    single_trade = lifecycle.filter(pl.col("trade_match_count") == 1).height
    status = classify_status(distinct_orderids=distinct_orderids, linked_orderids=linked_orderids, multi_event_orderids=multi_event_orderids)
    impact = map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_LIFECYCLE, status=status, blocking_level=BLOCKING_LEVEL_BLOCKING)
    seqnum_order_present = lifecycle.filter(pl.col("seqnum_present_count") > 0).height
    seqnum_trade_present = lifecycle.filter(pl.col("trade_seqnum_present_count") > 0).height
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
        cross_session_candidate_count=0,
        cross_session_candidate_rate=safe_rate(0, distinct_orderids),
        first_order_seqnum_present_rate=safe_rate(seqnum_order_present, distinct_orderids),
        last_order_seqnum_present_rate=safe_rate(seqnum_order_present, distinct_orderids),
        first_trade_seqnum_present_rate=safe_rate(seqnum_trade_present, distinct_orderids),
        last_trade_seqnum_present_rate=safe_rate(seqnum_trade_present, distinct_orderids),
        lifecycle_status=status,
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
    output_dir = args.output_root / "semantic" / f"year={args.year}"
    daily_path = output_dir / "semantic_orderid_lifecycle_daily.parquet"
    yearly_path = output_dir / SUMMARY_TABLE_BY_AREA[SEMANTIC_AREA_LIFECYCLE]
    report_path = args.research_root / f"semantic_lifecycle_{args.year}.md"
    logger = configure_logger("semantic_lifecycle", args.log_root / f"semantic_lifecycle_{args.year}.log")
    ensure_dir(output_dir)
    rows = [investigate_date(date, stage_root=args.input_root, year=str(args.year), limit_rows=args.limit_rows) for date in selected_dates]
    summary_row = build_yearly_summary(str(args.year), rows)
    write_parquet(rows, daily_path)
    write_parquet([summary_row], yearly_path)
    write_markdown(report_path, year=str(args.year), rows=rows, summary_row=summary_row)
    write_json(output_dir / "semantic_lifecycle_summary.json", {"pipeline": "semantic_lifecycle", "year": str(args.year), "artifacts": {"daily": str(daily_path), "yearly": str(yearly_path), "report": str(report_path)}})
    logger.info("Semantic lifecycle probe complete for %s with %s dates", args.year, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
