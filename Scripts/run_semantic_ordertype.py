from __future__ import annotations

import argparse
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


def input_bytes(paths: list[str]) -> int:
    return sum(Path(path).stat().st_size for path in paths)


def has_column(frame: pl.LazyFrame, column_name: str) -> bool:
    return column_name in frame.collect_schema().names()


def format_top_values(frame: pl.DataFrame, *, value_column: str, count_column: str, limit: int = 5) -> str | None:
    if frame.is_empty():
        return None
    rows = frame.head(limit).to_dicts()
    return ",".join(f"{row[value_column]}:{row[count_column]}" for row in rows)


def serialize_pattern_values(values: Any) -> str:
    if values is None:
        return ""
    if hasattr(values, "to_list"):
        normalized = values.to_list()
    else:
        normalized = list(values)
    return ",".join(str(value) for value in normalized) if normalized else ""


def investigate_date(
    trade_date: str,
    *,
    stage_root: Path,
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

    row_stats = (
        orders.select(
            [
                pl.len().cast(pl.Int64).alias("tested_rows"),
                pl.col("OrderType").is_not_null().sum().cast(pl.Int64).alias("nonnull_count"),
                pl.col("OrderType").drop_nulls().n_unique().alias("distinct_values"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )
    top_values = (
        orders.filter(pl.col("OrderType").is_not_null())
        .group_by("OrderType")
        .agg(pl.len().cast(pl.Int64).alias("row_count"))
        .sort(["row_count", "OrderType"], descending=[True, False])
        .limit(5)
        .collect()
    )
    orderid_frame = (
        orders.filter(pl.col("OrderId").is_not_null() & (pl.col("OrderId") != 0))
        .group_by("OrderId")
        .agg(
            [
                pl.col("OrderType").drop_nulls().alias("ordertype_values"),
                pl.col("OrderType").drop_nulls().n_unique().alias("ordertype_count"),
            ]
        )
    )
    orderid_stats = (
        orderid_frame.select(
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
        orderid_frame.with_columns(
            pl.col("ordertype_values").map_elements(
                serialize_pattern_values,
                return_dtype=pl.String,
            ).alias("transition_pattern")
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
    output_dir = args.output_root / "semantic" / f"year={args.year}"
    daily_path = output_dir / "semantic_ordertype_daily.parquet"
    yearly_path = output_dir / SUMMARY_TABLE_BY_AREA[SEMANTIC_AREA_ORDERTYPE]
    report_path = args.research_root / f"semantic_ordertype_{args.year}.md"
    logger = configure_logger("semantic_ordertype", args.log_root / f"semantic_ordertype_{args.year}.log")
    ensure_dir(output_dir)
    rows = [
        investigate_date(
            date,
            stage_root=args.input_root,
            year=str(args.year),
            limit_rows=args.limit_rows,
            logger=logger,
        )
        for date in selected_dates
    ]
    summary_row = build_yearly_summary(str(args.year), rows)
    write_parquet(rows, daily_path)
    write_parquet([summary_row], yearly_path)
    write_markdown(report_path, year=str(args.year), rows=rows, summary_row=summary_row)
    write_json(output_dir / "semantic_ordertype_summary.json", {"pipeline": "semantic_ordertype", "year": str(args.year), "artifacts": {"daily": str(daily_path), "yearly": str(yearly_path), "report": str(report_path)}})
    logger.info("Semantic OrderType probe complete for %s with %s dates", args.year, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
