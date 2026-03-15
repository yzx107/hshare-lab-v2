from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, DEFAULT_LOG_ROOT, configure_logger, ensure_dir, iso_utc_now, print_scaffold_plan, write_json
from Scripts.semantic_contract import (
    BLOCKING_LEVEL_CONTEXT_ONLY,
    CONFIDENCE_LOW,
    SEMANTIC_AREA_SESSION,
    STATUS_NOT_APPLICABLE,
    STATUS_NOT_RUN,
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
    parser = argparse.ArgumentParser(description="Build Session semantic probe results.")
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


def column_names(paths: list[str]) -> set[str]:
    if not paths:
        return set()
    return set(pl.scan_parquet(paths).collect_schema().names())


def investigate_date(trade_date: str, *, stage_root: Path, year: str, limit_rows: int) -> dict[str, Any]:
    order_paths = [str(path) for path in sorted((stage_root / "orders" / f"date={trade_date}").glob("*.parquet"))]
    trade_paths = [str(path) for path in sorted((stage_root / "trades" / f"date={trade_date}").glob("*.parquet"))]
    order_columns = column_names(order_paths)
    trade_columns = column_names(trade_paths)
    session_present = "Session" in order_columns or "Session" in trade_columns
    if not session_present:
        status = STATUS_NOT_RUN
        impact = map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_SESSION, status=status, blocking_level=BLOCKING_LEVEL_CONTEXT_ONLY)
        return build_daily_result(
            SEMANTIC_AREA_SESSION,
            date=trade_date,
            year=year,
            semantic_area=SEMANTIC_AREA_SESSION,
            scope="session split probe",
            status=status,
            confidence=CONFIDENCE_LOW,
            blocking_level=BLOCKING_LEVEL_CONTEXT_ONLY,
            tested_rows=0,
            pass_rows=0,
            fail_rows=0,
            unknown_rows=0,
            summary="Session column is absent in current stage inputs; session split remains scaffold-only.",
            admissibility_impact=impact,
            evidence_path=f"dqa/semantic/year={year}/semantic_session_daily.parquet",
            distinct_session_values=None,
            session_value_count=None,
            cross_session_linkage_count=None,
            cross_session_linkage_rate=None,
            session_time_window_consistent_flag=None,
            session_split_required_flag=True,
            orders_session_nonnull_rate=None,
            trades_session_nonnull_rate=None,
            linked_edges_with_session_rate=None,
            session_status=status,
        )

    orders = pl.scan_parquet(order_paths)
    trades = pl.scan_parquet(trade_paths)
    if limit_rows > 0:
        orders = orders.limit(limit_rows)
        trades = trades.limit(limit_rows)
    order_stats = orders.select([pl.len().cast(pl.Int64).alias("rows"), pl.col("Session").is_not_null().sum().cast(pl.Int64).alias("nonnull")]).collect().to_dicts()[0]
    trade_stats = trades.select([pl.len().cast(pl.Int64).alias("rows"), pl.col("Session").is_not_null().sum().cast(pl.Int64).alias("nonnull")]).collect().to_dicts()[0]
    session_stats = orders.filter(pl.col("Session").is_not_null()).select([pl.col("Session").drop_nulls().n_unique().alias("distinct_values")]).collect().to_dicts()[0]
    linkage = (
        orders.filter(pl.col("OrderId").is_not_null() & pl.col("Session").is_not_null())
        .group_by("OrderId")
        .agg(pl.col("Session").drop_nulls().n_unique().alias("session_count"))
        .select((pl.col("session_count") > 1).cast(pl.Int64).sum().alias("cross_session_linkage_count"))
        .collect()
        .to_dicts()[0]
    )
    tested_rows = int(order_stats["rows"] or 0)
    cross_count = int(linkage["cross_session_linkage_count"] or 0)
    distinct_values = int(session_stats["distinct_values"] or 0)
    status = STATUS_NOT_APPLICABLE if tested_rows == 0 else STATUS_WEAK_PASS if distinct_values > 0 else STATUS_UNKNOWN
    impact = map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_SESSION, status=status, blocking_level=BLOCKING_LEVEL_CONTEXT_ONLY)
    return build_daily_result(
        SEMANTIC_AREA_SESSION,
        date=trade_date,
        year=year,
        semantic_area=SEMANTIC_AREA_SESSION,
        scope="session split probe",
        status=status,
        confidence=CONFIDENCE_LOW,
        blocking_level=BLOCKING_LEVEL_CONTEXT_ONLY,
        tested_rows=tested_rows,
        pass_rows=tested_rows if status == STATUS_WEAK_PASS else 0,
        fail_rows=0,
        unknown_rows=tested_rows if status == STATUS_UNKNOWN else 0,
        summary=f"distinct_session_values={distinct_values}, cross_session_linkage_count={cross_count}",
        admissibility_impact=impact,
        evidence_path=f"dqa/semantic/year={year}/semantic_session_daily.parquet",
        distinct_session_values=distinct_values,
        session_value_count=distinct_values,
        cross_session_linkage_count=cross_count,
        cross_session_linkage_rate=safe_rate(cross_count, tested_rows),
        session_time_window_consistent_flag=(cross_count == 0),
        session_split_required_flag=True,
        orders_session_nonnull_rate=safe_rate(int(order_stats["nonnull"] or 0), int(order_stats["rows"] or 0)),
        trades_session_nonnull_rate=safe_rate(int(trade_stats["nonnull"] or 0), int(trade_stats["rows"] or 0)),
        linked_edges_with_session_rate=None,
        session_status=status,
    )


def build_yearly_summary(year: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    modules = area_modules(SEMANTIC_AREA_SESSION)
    status = STATUS_NOT_APPLICABLE if not rows else max(rows, key=lambda row: {"pass": 1, "weak_pass": 2, "unknown": 3, "fail": 4, "not_run": 5, "not_applicable": 0}[row["status"]])["status"]
    distinct_union = sorted({int(row["distinct_session_values"]) for row in rows if row["distinct_session_values"] is not None})
    consistent_day_rate = safe_rate(sum(1 for row in rows if row["session_time_window_consistent_flag"] is True), len(rows))
    return build_summary_result(
        SEMANTIC_AREA_SESSION,
        year=year,
        semantic_area=SEMANTIC_AREA_SESSION,
        status=status,
        confidence=CONFIDENCE_LOW,
        blocking_level=BLOCKING_LEVEL_CONTEXT_ONLY,
        days_total=len(rows),
        days_run=len(rows),
        days_pass=sum(1 for row in rows if row["status"] == "pass"),
        days_weak_pass=sum(1 for row in rows if row["status"] == STATUS_WEAK_PASS),
        days_fail=sum(1 for row in rows if row["status"] == "fail"),
        days_unknown=sum(1 for row in rows if row["status"] in {STATUS_UNKNOWN, STATUS_NOT_RUN}),
        tested_rows_total=sum(int(row["tested_rows"] or 0) for row in rows),
        distinct_session_values_union=",".join(str(value) for value in distinct_union) if distinct_union else None,
        cross_session_linkage_rate_avg=safe_rate(sum(float(row["cross_session_linkage_rate"] or 0) for row in rows), len(rows)),
        session_time_window_consistent_day_rate=consistent_day_rate,
        session_split_required_flag=True,
        summary=f"dates={len(rows)} session probes materialized",
        admissibility_impact=map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_SESSION, status=status, blocking_level=BLOCKING_LEVEL_CONTEXT_ONLY),
        recommended_modules=",".join(modules["recommended"]),
        blocked_modules=",".join(modules["blocked"]),
    )


def write_markdown(path: Path, *, year: str, rows: list[dict[str, Any]], summary_row: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    lines = [f"# Semantic Session Probe {year}", "", f"- generated_at: {iso_utc_now()}", f"- status: {summary_row['status']}"]
    for row in rows:
        lines.extend(["", f"## {row['date']}", f"- distinct_session_values: {row['distinct_session_values']}", f"- cross_session_linkage_rate: {row['cross_session_linkage_rate']}", f"- session_split_required_flag: {row['session_split_required_flag']}", f"- status: {row['status']}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(name="run_semantic_session", purpose="Materialize the Session probe and its research-splitting signal.", responsibilities=["Detect whether a Session column exists in current stage inputs.", "Profile distinct Session values and cross-session linkage when available.", "Emit unified semantic daily and yearly results."], inputs=["candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet", "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet"], outputs=["dqa/semantic/year=<year>/semantic_session_daily.parquet", "Research/Audits/semantic_session_<year>.md"])
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")
    selected_dates = parse_selected_dates(stage_root=args.input_root, year=str(args.year), dates=args.dates, max_days=args.max_days, latest_days=args.latest_days)
    if not selected_dates:
        raise SystemExit("No overlapping stage dates matched the requested selection.")
    output_dir = args.output_root / "semantic" / f"year={args.year}"
    daily_path = output_dir / "semantic_session_daily.parquet"
    yearly_path = output_dir / SUMMARY_TABLE_BY_AREA[SEMANTIC_AREA_SESSION]
    report_path = args.research_root / f"semantic_session_{args.year}.md"
    logger = configure_logger("semantic_session", args.log_root / f"semantic_session_{args.year}.log")
    ensure_dir(output_dir)
    rows = [investigate_date(date, stage_root=args.input_root, year=str(args.year), limit_rows=args.limit_rows) for date in selected_dates]
    summary_row = build_yearly_summary(str(args.year), rows)
    write_parquet(rows, daily_path)
    write_parquet([summary_row], yearly_path)
    write_markdown(report_path, year=str(args.year), rows=rows, summary_row=summary_row)
    write_json(output_dir / "semantic_session_summary.json", {"pipeline": "semantic_session", "year": str(args.year), "artifacts": {"daily": str(daily_path), "yearly": str(yearly_path), "report": str(report_path)}})
    logger.info("Semantic Session probe complete for %s with %s dates", args.year, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
