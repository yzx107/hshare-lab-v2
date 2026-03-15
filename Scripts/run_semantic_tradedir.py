from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, DEFAULT_LOG_ROOT, configure_logger, ensure_dir, iso_utc_now, print_scaffold_plan, write_json
from Scripts.semantic_contract import (
    BLOCKING_LEVEL_BLOCKING,
    CONFIDENCE_LOW,
    SEMANTIC_AREA_TRADEDIR,
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
    parser = argparse.ArgumentParser(description="Build TradeDir semantic probe results.")
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


def has_column(frame: pl.LazyFrame, column_name: str) -> bool:
    return column_name in frame.collect_schema().names()


def investigate_date(trade_date: str, *, stage_root: Path, year: str, limit_rows: int) -> dict[str, Any]:
    trade_paths = [str(path) for path in sorted((stage_root / "trades" / f"date={trade_date}").glob("*.parquet"))]
    trades = pl.scan_parquet(trade_paths)
    if limit_rows > 0:
        trades = trades.limit(limit_rows)
    has_bid = has_column(trades, "BidOrderID")
    has_ask = has_column(trades, "AskOrderID")
    linked_edge_expr = (
        (
            (pl.col("BidOrderID").is_not_null() & (pl.col("BidOrderID") != 0))
            if has_bid
            else pl.lit(False)
        )
        | (
            (pl.col("AskOrderID").is_not_null() & (pl.col("AskOrderID") != 0))
            if has_ask
            else pl.lit(False)
        )
    )
    stats = trades.select(
        [
            pl.len().cast(pl.Int64).alias("tested_rows"),
            pl.col("Dir").is_not_null().sum().cast(pl.Int64).alias("nonnull_count"),
            (pl.col("Dir") == 0).sum().cast(pl.Int64).alias("zero_count"),
            (pl.col("Dir") > 0).sum().cast(pl.Int64).alias("pos_count"),
            (pl.col("Dir") < 0).sum().cast(pl.Int64).alias("neg_count"),
            (~pl.col("Dir").is_in([-1, 0, 1]) & pl.col("Dir").is_not_null()).sum().cast(pl.Int64).alias("other_count"),
            pl.col("Dir").drop_nulls().n_unique().alias("distinct_values"),
            linked_edge_expr.sum().cast(pl.Int64).alias("linked_edge_count"),
        ]
    ).collect().to_dicts()[0]
    tested_rows = int(stats["tested_rows"] or 0)
    nonnull_count = int(stats["nonnull_count"] or 0)
    distinct_values = int(stats["distinct_values"] or 0)
    status = STATUS_NOT_APPLICABLE if tested_rows == 0 else STATUS_WEAK_PASS if nonnull_count == tested_rows and 1 <= distinct_values <= 3 else STATUS_UNKNOWN
    impact = map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_TRADEDIR, status=status, blocking_level=BLOCKING_LEVEL_BLOCKING)
    linked_edge_count = int(stats["linked_edge_count"] or 0)
    linked_consistency = (
        trades.with_columns(
            [
                linked_edge_expr.alias("linked_edge"),
                (
                    ((pl.col("Dir") > 0) & (pl.col("AskOrderID").is_not_null()) & (pl.col("AskOrderID") != 0))
                    | ((pl.col("Dir") < 0) & (pl.col("BidOrderID").is_not_null()) & (pl.col("BidOrderID") != 0))
                ).alias("candidate_consistent")
                if has_bid and has_ask
                else pl.lit(None, dtype=pl.Boolean).alias("candidate_consistent"),
            ]
        )
        .filter(pl.col("linked_edge"))
        .select(
            [
                pl.len().cast(pl.Int64).alias("tested"),
                pl.col("candidate_consistent").fill_null(False).cast(pl.Int64).sum().alias("passed"),
            ]
        )
        .collect()
        .to_dicts()[0]
        if linked_edge_count > 0 and has_bid and has_ask
        else {"tested": None, "passed": None}
    )
    linked_side_consistency_tested = linked_consistency["tested"]
    linked_side_consistency_pass = linked_consistency["passed"]
    linked_side_consistency_fail = (
        int(linked_side_consistency_tested) - int(linked_side_consistency_pass)
        if linked_side_consistency_tested is not None and linked_side_consistency_pass is not None
        else None
    )
    return build_daily_result(
        SEMANTIC_AREA_TRADEDIR,
        date=trade_date,
        year=year,
        semantic_area=SEMANTIC_AREA_TRADEDIR,
        scope="trades Dir distribution probe",
        status=status,
        confidence=CONFIDENCE_LOW,
        blocking_level=BLOCKING_LEVEL_BLOCKING,
        tested_rows=tested_rows,
        pass_rows=nonnull_count if status == STATUS_WEAK_PASS else 0,
        fail_rows=0,
        unknown_rows=max(tested_rows - nonnull_count, 0),
        summary=f"nonnull_rate={safe_rate(nonnull_count, tested_rows)}, distinct_values={distinct_values}",
        admissibility_impact=impact,
        evidence_path=f"dqa/semantic/year={year}/semantic_tradedir_daily.parquet",
        tradedir_nonnull_count=nonnull_count,
        tradedir_nonnull_rate=safe_rate(nonnull_count, tested_rows),
        distinct_tradedir_values=distinct_values,
        tradedir_zero_count=int(stats["zero_count"] or 0),
        tradedir_pos_count=int(stats["pos_count"] or 0),
        tradedir_neg_count=int(stats["neg_count"] or 0),
        tradedir_other_count=int(stats["other_count"] or 0),
        tradedir_zero_rate=safe_rate(int(stats["zero_count"] or 0), tested_rows),
        tradedir_pos_rate=safe_rate(int(stats["pos_count"] or 0), tested_rows),
        tradedir_neg_rate=safe_rate(int(stats["neg_count"] or 0), tested_rows),
        linked_edge_count=linked_edge_count,
        linked_edge_rate=safe_rate(linked_edge_count, tested_rows),
        linked_side_consistency_tested=linked_side_consistency_tested,
        linked_side_consistency_pass=linked_side_consistency_pass,
        linked_side_consistency_fail=linked_side_consistency_fail,
        linked_side_consistency_rate=safe_rate(linked_side_consistency_pass, linked_side_consistency_tested),
        tradedir_status=status,
    )


def build_yearly_summary(year: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    modules = area_modules(SEMANTIC_AREA_TRADEDIR)
    status = STATUS_NOT_APPLICABLE if not rows else max(rows, key=lambda row: {"pass": 1, "weak_pass": 2, "unknown": 3, "fail": 4, "not_run": 5, "not_applicable": 0}[row["status"]])["status"]
    return build_summary_result(
        SEMANTIC_AREA_TRADEDIR,
        year=year,
        semantic_area=SEMANTIC_AREA_TRADEDIR,
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
        tradedir_nonnull_rate_avg=safe_rate(sum(float(row["tradedir_nonnull_rate"] or 0) for row in rows), len(rows)),
        tradedir_zero_rate_avg=safe_rate(sum(float(row["tradedir_zero_rate"] or 0) for row in rows), len(rows)),
        tradedir_pos_rate_avg=safe_rate(sum(float(row["tradedir_pos_rate"] or 0) for row in rows), len(rows)),
        tradedir_neg_rate_avg=safe_rate(sum(float(row["tradedir_neg_rate"] or 0) for row in rows), len(rows)),
        linked_side_consistency_rate_avg=safe_rate(sum(float(row["linked_side_consistency_rate"] or 0) for row in rows if row["linked_side_consistency_rate"] is not None), sum(1 for row in rows if row["linked_side_consistency_rate"] is not None)),
        summary=f"dates={len(rows)} TradeDir probes materialized",
        admissibility_impact=map_semantic_result_to_admissibility(semantic_area=SEMANTIC_AREA_TRADEDIR, status=status, blocking_level=BLOCKING_LEVEL_BLOCKING),
        recommended_modules=",".join(modules["recommended"]),
        blocked_modules=",".join(modules["blocked"]),
    )


def write_markdown(path: Path, *, year: str, rows: list[dict[str, Any]], summary_row: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    lines = [f"# Semantic TradeDir Probe {year}", "", f"- generated_at: {iso_utc_now()}", f"- status: {summary_row['status']}"]
    for row in rows:
        lines.extend(["", f"## {row['date']}", f"- tradedir_nonnull_rate: {row['tradedir_nonnull_rate']}", f"- distinct_tradedir_values: {row['distinct_tradedir_values']}", f"- linked_edge_rate: {row['linked_edge_rate']}", f"- status: {row['status']}"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(name="run_semantic_tradedir", purpose="Materialize the TradeDir semantic probe without asserting signed-flow semantics.", responsibilities=["Profile Dir non-null coverage and distinct values.", "Leave linked-side consistency columns explicitly present, even when unfilled.", "Emit unified semantic daily and yearly results."], inputs=["candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet"], outputs=["dqa/semantic/year=<year>/semantic_tradedir_daily.parquet", "Research/Audits/semantic_tradedir_<year>.md"])
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")
    selected_dates = parse_selected_dates(stage_root=args.input_root, year=str(args.year), dates=args.dates, max_days=args.max_days, latest_days=args.latest_days)
    if not selected_dates:
        raise SystemExit("No overlapping stage dates matched the requested selection.")
    output_dir = args.output_root / "semantic" / f"year={args.year}"
    daily_path = output_dir / "semantic_tradedir_daily.parquet"
    yearly_path = output_dir / SUMMARY_TABLE_BY_AREA[SEMANTIC_AREA_TRADEDIR]
    report_path = args.research_root / f"semantic_tradedir_{args.year}.md"
    logger = configure_logger("semantic_tradedir", args.log_root / f"semantic_tradedir_{args.year}.log")
    ensure_dir(output_dir)
    rows = [investigate_date(date, stage_root=args.input_root, year=str(args.year), limit_rows=args.limit_rows) for date in selected_dates]
    summary_row = build_yearly_summary(str(args.year), rows)
    write_parquet(rows, daily_path)
    write_parquet([summary_row], yearly_path)
    write_markdown(report_path, year=str(args.year), rows=rows, summary_row=summary_row)
    write_json(output_dir / "semantic_tradedir_summary.json", {"pipeline": "semantic_tradedir", "year": str(args.year), "artifacts": {"daily": str(daily_path), "yearly": str(yearly_path), "report": str(report_path)}})
    logger.info("Semantic TradeDir probe complete for %s with %s dates", args.year, len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
