from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, DEFAULT_LOG_ROOT, configure_logger, ensure_dir, iso_utc_now, print_scaffold_plan, write_json
from Scripts.semantic_contract import ADMISSIBILITY_REQUIRES_MANUAL_REVIEW, parse_selected_dates

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"

STATUS_CANDIDATE_DIRECTIONAL_SIGNAL = "candidate_directional_signal"
STATUS_NO_STABLE_CONTRAST = "no_stable_contrast"
STATUS_NOT_APPLICABLE = "not_applicable"
UPTICK_GAP_THRESHOLD = 0.02

DAILY_COLUMNS = (
    "date",
    "year",
    "dir_value",
    "row_count",
    "row_share",
    "bid_present_rate",
    "ask_present_rate",
    "both_present_rate",
    "bid_only_rate",
    "ask_only_rate",
    "neither_side_rate",
    "move_tested_count",
    "uptick_rate",
    "downtick_rate",
    "sameprice_rate",
    "volume_median",
    "volume_p90",
    "turnover_proxy_median",
    "turnover_proxy_p90",
    "session_mix",
    "summary",
)

SUMMARY_COLUMNS = (
    "year",
    "days_run",
    "observed_dir_values",
    "dir1_vs_dir2_uptick_gap_avg",
    "dir1_vs_dir2_downtick_gap_avg",
    "dir1_vs_dir2_sameprice_gap_avg",
    "dir1_vs_dir2_linkage_gap_avg",
    "dir1_vs_dir2_uptick_gap_sign_consistent_flag",
    "dir1_vs_dir2_bucket_uptick_consistent_day_count",
    "dir0_specialness_score",
    "status",
    "summary",
    "admissibility_impact",
)


@dataclass
class ContrastDateResult:
    daily_rows: list[dict[str, Any]]
    contrast_summary: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TradeDir contrast probe results.")
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
    parser.add_argument("--dates")
    parser.add_argument("--max-days", type=int, default=0)
    parser.add_argument("--latest-days", action="store_true")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_DQA_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_AUDITS_ROOT)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def write_parquet(rows: list[dict[str, Any]], path: Path, columns: tuple[str, ...]) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, schema={column: None for column in columns}, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame(schema={column: pl.Null for column in columns}).write_parquet(path)


def input_bytes(paths: list[str]) -> int:
    return sum(Path(path).stat().st_size for path in paths)


def safe_mean(values: list[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def safe_abs_sign(value: float | None) -> int | None:
    if value is None or abs(value) < 1e-12:
        return None
    return 1 if value > 0 else -1


def choose_sort_anchor(schema_names: set[str]) -> str | None:
    for column in ("SeqNum", "TickID", "Time"):
        if column in schema_names:
            return column
    return None


def time_bucket_expr() -> pl.Expr:
    hhmm = pl.col("Time").fill_null("").str.slice(0, 4)
    return (
        pl.when(hhmm == "")
        .then(pl.lit("unknown_time"))
        .when(hhmm < pl.lit("0930"))
        .then(pl.lit("0900_0929"))
        .when(hhmm < pl.lit("1200"))
        .then(pl.lit("0930_1159"))
        .when(hhmm < pl.lit("1300"))
        .then(pl.lit("1200_1259"))
        .when(hhmm < pl.lit("1600"))
        .then(pl.lit("1300_1559"))
        .otherwise(pl.lit("1600_plus"))
        .alias("time_bucket")
    )


def format_float(value: float | None, digits: int = 4) -> str:
    if value is None:
        return "na"
    text = f"{value:.{digits}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def format_bucket_mix(shares: dict[str, float]) -> str | None:
    if not shares:
        return None
    return ",".join(f"{bucket}:{format_float(share, digits=3)}" for bucket, share in sorted(shares.items()))


def observed_dir_values(rows: list[dict[str, Any]]) -> str | None:
    values = sorted({int(row["dir_value"]) for row in rows if row.get("dir_value") is not None})
    return ",".join(str(value) for value in values) if values else None


def structure_gap(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    metrics = (
        "bid_present_rate",
        "ask_present_rate",
        "both_present_rate",
        "bid_only_rate",
        "ask_only_rate",
        "neither_side_rate",
    )
    diffs = [abs(float(left[name]) - float(right[name])) for name in metrics if left.get(name) is not None and right.get(name) is not None]
    return safe_mean(diffs)


def price_move_gap(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    metrics = ("uptick_rate", "downtick_rate", "sameprice_rate")
    diffs = [abs(float(left[name]) - float(right[name])) for name in metrics if left.get(name) is not None and right.get(name) is not None]
    return safe_mean(diffs)


def total_variation_distance(left: dict[str, float], right: dict[str, float]) -> float | None:
    keys = sorted(set(left) | set(right))
    if not keys:
        return None
    return 0.5 * sum(abs(left.get(key, 0.0) - right.get(key, 0.0)) for key in keys)


def pooled_bucket_shares(
    bucket_counts: dict[int, dict[str, int]],
    row_lookup: dict[int, dict[str, Any]],
    dirs: tuple[int, ...],
) -> dict[str, float]:
    totals: dict[str, int] = {}
    combined_rows = 0
    for dir_value in dirs:
        dir_rows = row_lookup.get(dir_value, {})
        row_count = int(dir_rows.get("row_count") or 0)
        combined_rows += row_count
        for bucket, count in bucket_counts.get(dir_value, {}).items():
            totals[bucket] = totals.get(bucket, 0) + count
    if combined_rows == 0:
        return {}
    return {bucket: count / combined_rows for bucket, count in totals.items()}


def build_row_summary(row: dict[str, Any]) -> str:
    return (
        f"row_share={format_float(row['row_share'])}, "
        f"both_present_rate={format_float(row['both_present_rate'])}, "
        f"uptick_rate={format_float(row['uptick_rate'])}, "
        f"downtick_rate={format_float(row['downtick_rate'])}, "
        f"sameprice_rate={format_float(row['sameprice_rate'])}, "
        f"session_mix={row['session_mix'] or 'none'}"
    )


def investigate_date(
    trade_date: str,
    *,
    stage_root: Path,
    year: str,
    limit_rows: int,
    logger: Any | None = None,
) -> ContrastDateResult:
    trade_paths = [str(path) for path in sorted((stage_root / "trades" / f"date={trade_date}").glob("*.parquet"))]
    if logger is not None:
        logger.info(
            "TradeDir contrast %s: discovered %s trade files (%s bytes)",
            trade_date,
            len(trade_paths),
            input_bytes(trade_paths),
        )

    trades = pl.scan_parquet(trade_paths)
    if limit_rows > 0:
        trades = trades.limit(limit_rows)
    schema_names = set(trades.collect_schema().names())
    if logger is not None:
        logger.info("TradeDir contrast %s: trade columns=%s", trade_date, ",".join(sorted(schema_names)))
    if "Dir" not in schema_names:
        raise SystemExit("Dir column is required for TradeDir contrast probe.")

    selected_columns = [column for column in ("SeqNum", "TickID", "Time", "Price", "Volume", "Dir", "BidOrderID", "AskOrderID") if column in schema_names]
    enriched = trades.select(selected_columns)
    sort_anchor = choose_sort_anchor(schema_names)
    if sort_anchor is not None:
        enriched = enriched.sort(sort_anchor)
    enriched = enriched.with_columns(
        [
            ((pl.col("BidOrderID").is_not_null()) & (pl.col("BidOrderID") != 0)).alias("bid_present")
            if "BidOrderID" in schema_names
            else pl.lit(False).alias("bid_present"),
            ((pl.col("AskOrderID").is_not_null()) & (pl.col("AskOrderID") != 0)).alias("ask_present")
            if "AskOrderID" in schema_names
            else pl.lit(False).alias("ask_present"),
            time_bucket_expr() if "Time" in schema_names else pl.lit("unknown_time").alias("time_bucket"),
            (pl.col("Price") * pl.col("Volume").cast(pl.Float64)).alias("turnover_proxy")
            if {"Price", "Volume"} <= schema_names
            else pl.lit(None, dtype=pl.Float64).alias("turnover_proxy"),
            (pl.col("Price") - pl.col("Price").shift(1)).alias("price_diff")
            if "Price" in schema_names
            else pl.lit(None, dtype=pl.Float64).alias("price_diff"),
        ]
    ).with_columns(
        [
            (pl.col("bid_present") & pl.col("ask_present")).alias("both_present"),
            (pl.col("bid_present") & ~pl.col("ask_present")).alias("bid_only"),
            (~pl.col("bid_present") & pl.col("ask_present")).alias("ask_only"),
            (~pl.col("bid_present") & ~pl.col("ask_present")).alias("neither_side"),
            (pl.col("price_diff") > 0).alias("uptick"),
            (pl.col("price_diff") < 0).alias("downtick"),
            (pl.col("price_diff") == 0).alias("sameprice"),
            pl.col("price_diff").is_not_null().alias("move_tested"),
        ]
    )

    total_rows = int(enriched.select(pl.len().alias("rows")).collect().to_dicts()[0]["rows"] or 0)
    dir_stats = (
        enriched.group_by("Dir")
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("bid_present").mean().alias("bid_present_rate"),
                pl.col("ask_present").mean().alias("ask_present_rate"),
                pl.col("both_present").mean().alias("both_present_rate"),
                pl.col("bid_only").mean().alias("bid_only_rate"),
                pl.col("ask_only").mean().alias("ask_only_rate"),
                pl.col("neither_side").mean().alias("neither_side_rate"),
                pl.col("move_tested").cast(pl.Int64).sum().alias("move_tested_count"),
                pl.col("uptick").filter(pl.col("move_tested")).mean().alias("uptick_rate"),
                pl.col("downtick").filter(pl.col("move_tested")).mean().alias("downtick_rate"),
                pl.col("sameprice").filter(pl.col("move_tested")).mean().alias("sameprice_rate"),
                pl.col("Volume").median().alias("volume_median")
                if "Volume" in schema_names
                else pl.lit(None, dtype=pl.Float64).alias("volume_median"),
                pl.col("Volume").quantile(0.9).alias("volume_p90")
                if "Volume" in schema_names
                else pl.lit(None, dtype=pl.Float64).alias("volume_p90"),
                pl.col("turnover_proxy").median().alias("turnover_proxy_median"),
                pl.col("turnover_proxy").quantile(0.9).alias("turnover_proxy_p90"),
            ]
        )
        .sort("Dir")
        .collect()
    )
    bucket_counts_frame = (
        enriched.group_by(["Dir", "time_bucket"])
        .agg(pl.len().alias("bucket_rows"))
        .sort(["Dir", "time_bucket"])
        .collect()
    )
    bucket_gap_frame = (
        enriched.filter(pl.col("Dir").is_in([1, 2]))
        .group_by(["Dir", "time_bucket"])
        .agg(
            [
                pl.len().alias("row_count"),
                pl.col("uptick").filter(pl.col("move_tested")).mean().alias("uptick_rate"),
            ]
        )
        .sort(["time_bucket", "Dir"])
        .collect()
    )

    bucket_counts: dict[int, dict[str, int]] = {}
    for row in bucket_counts_frame.to_dicts():
        dir_value = int(row["Dir"])
        bucket_counts.setdefault(dir_value, {})[str(row["time_bucket"])] = int(row["bucket_rows"])

    daily_rows: list[dict[str, Any]] = []
    row_lookup: dict[int, dict[str, Any]] = {}
    for raw_row in dir_stats.to_dicts():
        dir_value = int(raw_row["Dir"])
        row_count = int(raw_row["row_count"] or 0)
        session_mix = format_bucket_mix(
            {
                bucket: count / row_count
                for bucket, count in bucket_counts.get(dir_value, {}).items()
            }
        )
        row = {
            "date": trade_date,
            "year": year,
            "dir_value": dir_value,
            "row_count": row_count,
            "row_share": (row_count / total_rows) if total_rows else None,
            "bid_present_rate": raw_row["bid_present_rate"],
            "ask_present_rate": raw_row["ask_present_rate"],
            "both_present_rate": raw_row["both_present_rate"],
            "bid_only_rate": raw_row["bid_only_rate"],
            "ask_only_rate": raw_row["ask_only_rate"],
            "neither_side_rate": raw_row["neither_side_rate"],
            "move_tested_count": int(raw_row["move_tested_count"] or 0),
            "uptick_rate": raw_row["uptick_rate"],
            "downtick_rate": raw_row["downtick_rate"],
            "sameprice_rate": raw_row["sameprice_rate"],
            "volume_median": raw_row["volume_median"],
            "volume_p90": raw_row["volume_p90"],
            "turnover_proxy_median": raw_row["turnover_proxy_median"],
            "turnover_proxy_p90": raw_row["turnover_proxy_p90"],
            "session_mix": session_mix,
            "summary": None,
        }
        row["summary"] = build_row_summary(row)
        daily_rows.append(row)
        row_lookup[dir_value] = row

    bucket_uptick: dict[int, dict[str, float | None]] = {}
    for row in bucket_gap_frame.to_dicts():
        dir_value = int(row["Dir"])
        bucket_uptick.setdefault(dir_value, {})[str(row["time_bucket"])] = row["uptick_rate"]

    dir1 = row_lookup.get(1)
    dir2 = row_lookup.get(2)
    dir0 = row_lookup.get(0)
    dir1_vs_dir2_uptick_gap = (float(dir1["uptick_rate"]) - float(dir2["uptick_rate"])) if dir1 and dir2 and dir1["uptick_rate"] is not None and dir2["uptick_rate"] is not None else None
    dir1_vs_dir2_downtick_gap = (float(dir1["downtick_rate"]) - float(dir2["downtick_rate"])) if dir1 and dir2 and dir1["downtick_rate"] is not None and dir2["downtick_rate"] is not None else None
    dir1_vs_dir2_sameprice_gap = (float(dir1["sameprice_rate"]) - float(dir2["sameprice_rate"])) if dir1 and dir2 and dir1["sameprice_rate"] is not None and dir2["sameprice_rate"] is not None else None
    dir1_vs_dir2_linkage_gap = structure_gap(dir1, dir2) if dir1 and dir2 else None

    common_buckets = sorted(set(bucket_uptick.get(1, {})) & set(bucket_uptick.get(2, {})))
    bucket_gap_signs = []
    bucket_gap_parts = []
    for bucket in common_buckets:
        left_rate = bucket_uptick[1].get(bucket)
        right_rate = bucket_uptick[2].get(bucket)
        if left_rate is None or right_rate is None:
            continue
        gap = float(left_rate) - float(right_rate)
        bucket_gap_parts.append(f"{bucket}:{format_float(gap)}")
        sign = safe_abs_sign(gap)
        if sign is not None:
            bucket_gap_signs.append(sign)
    bucket_consistent = bool(bucket_gap_signs) and len(set(bucket_gap_signs)) == 1
    bucket_gap_summary = ",".join(bucket_gap_parts) if bucket_gap_parts else None

    dir0_specialness = None
    if dir0 and dir1 and dir2:
        pooled12 = {
            metric: safe_mean([dir1.get(metric), dir2.get(metric)])
            for metric in (
                "bid_present_rate",
                "ask_present_rate",
                "both_present_rate",
                "bid_only_rate",
                "ask_only_rate",
                "neither_side_rate",
                "uptick_rate",
                "downtick_rate",
                "sameprice_rate",
            )
        }
        pooled12_buckets = pooled_bucket_shares(bucket_counts, row_lookup, (1, 2))
        dir0_bucket_shares = {
            bucket: count / int(dir0["row_count"])
            for bucket, count in bucket_counts.get(0, {}).items()
        } if int(dir0["row_count"] or 0) > 0 else {}
        dir0_specialness = safe_mean(
            [
                structure_gap(dir0, pooled12),
                price_move_gap(dir0, pooled12),
                total_variation_distance(dir0_bucket_shares, pooled12_buckets),
            ]
        )

    contrast_summary = {
        "date": trade_date,
        "observed_dir_values": observed_dir_values(daily_rows),
        "dir1_vs_dir2_uptick_gap": dir1_vs_dir2_uptick_gap,
        "dir1_vs_dir2_downtick_gap": dir1_vs_dir2_downtick_gap,
        "dir1_vs_dir2_sameprice_gap": dir1_vs_dir2_sameprice_gap,
        "dir1_vs_dir2_linkage_gap": dir1_vs_dir2_linkage_gap,
        "dir1_vs_dir2_bucket_uptick_gap_summary": bucket_gap_summary,
        "dir1_vs_dir2_bucket_uptick_consistent_flag": bucket_consistent,
        "dir0_specialness": dir0_specialness,
    }
    if logger is not None:
        logger.info(
            "TradeDir contrast %s summary: observed_dir_values=%s uptick_gap=%s linkage_gap=%s bucket_gaps=%s dir0_specialness=%s",
            trade_date,
            contrast_summary["observed_dir_values"],
            format_float(dir1_vs_dir2_uptick_gap),
            format_float(dir1_vs_dir2_linkage_gap),
            bucket_gap_summary or "none",
            format_float(dir0_specialness),
        )
    return ContrastDateResult(daily_rows=daily_rows, contrast_summary=contrast_summary)


def build_yearly_summary(year: str, daily_rows: list[dict[str, Any]], contrast_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    uptick_gaps = [row["dir1_vs_dir2_uptick_gap"] for row in contrast_summaries]
    downtick_gaps = [row["dir1_vs_dir2_downtick_gap"] for row in contrast_summaries]
    sameprice_gaps = [row["dir1_vs_dir2_sameprice_gap"] for row in contrast_summaries]
    linkage_gaps = [row["dir1_vs_dir2_linkage_gap"] for row in contrast_summaries]
    dir0_scores = [row["dir0_specialness"] for row in contrast_summaries]
    uptick_signs = [safe_abs_sign(value) for value in uptick_gaps if value is not None and abs(value) >= UPTICK_GAP_THRESHOLD]
    uptick_sign_consistent = bool(uptick_signs) and len(uptick_signs) == len(contrast_summaries) and len(set(uptick_signs)) == 1
    bucket_consistent_day_count = sum(1 for row in contrast_summaries if row["dir1_vs_dir2_bucket_uptick_consistent_flag"] is True)
    if contrast_summaries and uptick_sign_consistent and bucket_consistent_day_count == len(contrast_summaries):
        status = STATUS_CANDIDATE_DIRECTIONAL_SIGNAL
        summary = (
            "Dir=1 vs 2 shows stable uptick contrast across representative days and Time-derived buckets; "
            "treat as candidate directional signal only."
        )
    elif contrast_summaries:
        status = STATUS_NO_STABLE_CONTRAST
        summary = (
            "Dir=1 vs 2 did not show stable contrast across representative days; "
            "keep TradeDir semantics under manual review."
        )
    else:
        status = STATUS_NOT_APPLICABLE
        summary = "No TradeDir contrast rows materialized."
    return {
        "year": year,
        "days_run": len(contrast_summaries),
        "observed_dir_values": observed_dir_values(daily_rows),
        "dir1_vs_dir2_uptick_gap_avg": safe_mean(uptick_gaps),
        "dir1_vs_dir2_downtick_gap_avg": safe_mean(downtick_gaps),
        "dir1_vs_dir2_sameprice_gap_avg": safe_mean(sameprice_gaps),
        "dir1_vs_dir2_linkage_gap_avg": safe_mean(linkage_gaps),
        "dir1_vs_dir2_uptick_gap_sign_consistent_flag": uptick_sign_consistent,
        "dir1_vs_dir2_bucket_uptick_consistent_day_count": bucket_consistent_day_count,
        "dir0_specialness_score": safe_mean(dir0_scores),
        "status": status,
        "summary": summary,
        "admissibility_impact": ADMISSIBILITY_REQUIRES_MANUAL_REVIEW,
    }


def write_markdown(
    path: Path,
    *,
    year: str,
    daily_rows: list[dict[str, Any]],
    contrast_summaries: list[dict[str, Any]],
    summary_row: dict[str, Any],
) -> None:
    ensure_dir(path.parent)
    rows_by_date: dict[str, list[dict[str, Any]]] = {}
    for row in daily_rows:
        rows_by_date.setdefault(str(row["date"]), []).append(row)

    lines = [
        f"# Semantic TradeDir Contrast {year}",
        "",
        f"- generated_at: {iso_utc_now()}",
        "- session_basis: Time-derived buckets because current stage inputs do not carry a Session column",
        "- turnover_basis: turnover_proxy = Price * Volume",
        f"- status: {summary_row['status']}",
        f"- admissibility_impact: {summary_row['admissibility_impact']}",
        f"- observed_dir_values: {summary_row['observed_dir_values']}",
        f"- dir1_vs_dir2_uptick_gap_avg: {summary_row['dir1_vs_dir2_uptick_gap_avg']}",
        f"- dir1_vs_dir2_linkage_gap_avg: {summary_row['dir1_vs_dir2_linkage_gap_avg']}",
        f"- dir0_specialness_score: {summary_row['dir0_specialness_score']}",
        f"- summary: {summary_row['summary']}",
    ]
    contrast_lookup = {row["date"]: row for row in contrast_summaries}
    for trade_date in sorted(rows_by_date):
        lines.extend(["", f"## {trade_date}"])
        contrast = contrast_lookup[trade_date]
        lines.extend(
            [
                f"- dir1_vs_dir2_uptick_gap: {contrast['dir1_vs_dir2_uptick_gap']}",
                f"- dir1_vs_dir2_bucket_uptick_gap_summary: {contrast['dir1_vs_dir2_bucket_uptick_gap_summary']}",
                f"- dir0_specialness: {contrast['dir0_specialness']}",
            ]
        )
        for row in sorted(rows_by_date[trade_date], key=lambda item: int(item["dir_value"])):
            lines.extend(
                [
                    f"- Dir {row['dir_value']}: row_share={row['row_share']}, bid_present_rate={row['bid_present_rate']}, ask_present_rate={row['ask_present_rate']}, both_present_rate={row['both_present_rate']}, uptick_rate={row['uptick_rate']}, downtick_rate={row['downtick_rate']}, sameprice_rate={row['sameprice_rate']}, session_mix={row['session_mix']}",
                ]
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_semantic_tradedir_contrast",
            purpose="Materialize a lightweight Dir=1 vs 2 contrast probe without asserting signed-flow semantics.",
            responsibilities=[
                "Profile Dir-specific linkage-side structure.",
                "Measure tick-rule-style price-move contrast for Dir=1 vs 2.",
                "Expose Time-derived bucket mixes because Session is absent in current stage inputs.",
            ],
            inputs=["candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet"],
            outputs=[
                "dqa/semantic/year=<year>/semantic_tradedir_contrast_daily.parquet",
                "dqa/semantic/year=<year>/semantic_tradedir_contrast_summary.parquet",
                "Research/Audits/semantic_tradedir_contrast_<year>.md",
            ],
        )
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")

    selected_dates = parse_selected_dates(
        stage_root=args.input_root,
        year=str(args.year),
        dates=args.dates,
        max_days=args.max_days,
        latest_days=args.latest_days,
    )
    if not selected_dates:
        raise SystemExit("No overlapping stage dates matched the requested selection.")

    output_dir = args.output_root / "semantic" / f"year={args.year}"
    daily_path = output_dir / "semantic_tradedir_contrast_daily.parquet"
    yearly_path = output_dir / "semantic_tradedir_contrast_summary.parquet"
    report_path = args.research_root / f"semantic_tradedir_contrast_{args.year}.md"
    logger = configure_logger("semantic_tradedir_contrast", args.log_root / f"semantic_tradedir_contrast_{args.year}.log")
    ensure_dir(output_dir)

    date_results = [
        investigate_date(
            trade_date,
            stage_root=args.input_root,
            year=str(args.year),
            limit_rows=args.limit_rows,
            logger=logger,
        )
        for trade_date in selected_dates
    ]
    daily_rows = [row for result in date_results for row in result.daily_rows]
    contrast_summaries = [result.contrast_summary for result in date_results]
    summary_row = build_yearly_summary(str(args.year), daily_rows, contrast_summaries)
    write_parquet(daily_rows, daily_path, DAILY_COLUMNS)
    write_parquet([summary_row], yearly_path, SUMMARY_COLUMNS)
    write_markdown(
        report_path,
        year=str(args.year),
        daily_rows=daily_rows,
        contrast_summaries=contrast_summaries,
        summary_row=summary_row,
    )
    write_json(
        output_dir / "semantic_tradedir_contrast_summary.json",
        {
            "pipeline": "semantic_tradedir_contrast",
            "year": str(args.year),
            "artifacts": {
                "daily": str(daily_path),
                "yearly": str(yearly_path),
                "report": str(report_path),
            },
        },
    )
    logger.info("Semantic TradeDir contrast probe complete for %s with %s dates", args.year, len(contrast_summaries))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
