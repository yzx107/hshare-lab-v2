from __future__ import annotations

import argparse
import zipfile
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, DEFAULT_LOG_ROOT, configure_logger, ensure_dir, iso_utc_now, print_scaffold_plan, write_json

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = DEFAULT_DATA_ROOT
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "reference" / "instrument_profile"
DEFAULT_RESEARCH_REFERENCES_ROOT = REPO_ROOT / "Research" / "References"
DEFAULT_SEED_PATH = DEFAULT_RESEARCH_REFERENCES_ROOT / "normalized" / "instrument_profile_seed.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a sidecar instrument_profile reference table from observed raw universe plus optional seed enrichment."
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=DEFAULT_RAW_ROOT,
        help="Root directory for raw year-level zip archives.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory for instrument_profile sidecar outputs.",
    )
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=DEFAULT_SEED_PATH,
        help="Optional seed CSV with instrument-level enrichment such as listing_date or southbound_eligible.",
    )
    parser.add_argument(
        "--years",
        default="2025,2026",
        help="Comma-separated years to scan from raw zip archives.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for run logs.",
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def selected_years(args: argparse.Namespace) -> list[str]:
    values = [token.strip() for token in args.years.split(",") if token.strip()]
    if not values:
        raise SystemExit("At least one year is required.")
    for value in values:
        if len(value) != 4 or not value.isdigit():
            raise SystemExit(f"Invalid year token: {value}")
    return values


def raw_zip_paths(raw_root: Path, years: list[str]) -> list[Path]:
    paths: list[Path] = []
    for year in years:
        paths.extend(sorted((raw_root / year).glob("*.zip")))
    return paths


def aggregate_raw_table(raw_root: Path, table_name: str, years: list[str]) -> pl.DataFrame:
    zip_paths = raw_zip_paths(raw_root, years)
    if not zip_paths:
        return pl.DataFrame(
            schema={
                "instrument_key": pl.Utf8,
                f"{table_name}_first_seen_date": pl.Date,
                f"{table_name}_last_seen_date": pl.Date,
                f"observed_{table_name}_days": pl.Int64,
            }
        )
    member_prefix = "order/" if table_name == "orders" else "trade/"
    daily_rows: list[dict[str, Any]] = []
    for zip_path in zip_paths:
        trade_date = zip_path.stem
        if len(trade_date) != 8 or not trade_date.isdigit():
            continue
        with zipfile.ZipFile(zip_path) as archive:
            instrument_keys = sorted(
                {
                    Path(member_name).stem
                    for member_name in archive.namelist()
                    if member_name.startswith(member_prefix) and member_name.endswith(".csv")
                }
            )
        daily_rows.extend(
            {
                "instrument_key": instrument_key,
                "date": f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}",
            }
            for instrument_key in instrument_keys
        )
    if not daily_rows:
        return pl.DataFrame(
            schema={
                "instrument_key": pl.Utf8,
                f"{table_name}_first_seen_date": pl.Date,
                f"{table_name}_last_seen_date": pl.Date,
                f"observed_{table_name}_days": pl.Int64,
            }
        )
    return (
        pl.DataFrame(daily_rows)
        .with_columns(pl.col("date").str.strptime(pl.Date, strict=False))
        .group_by("instrument_key")
        .agg(
            pl.col("date").min().alias(f"{table_name}_first_seen_date"),
            pl.col("date").max().alias(f"{table_name}_last_seen_date"),
            pl.col("date").n_unique().alias(f"observed_{table_name}_days"),
        )
        .sort("instrument_key")
    )


def parse_bool_expr(column_name: str) -> pl.Expr:
    lowered = pl.col(column_name).cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    return (
        pl.when(lowered.is_in(["1", "true", "t", "yes", "y"]))
        .then(pl.lit(True))
        .when(lowered.is_in(["0", "false", "f", "no", "n"]))
        .then(pl.lit(False))
        .otherwise(pl.lit(None, dtype=pl.Boolean))
        .alias(column_name)
    )


def load_seed(seed_path: Path) -> pl.DataFrame:
    schema = {
        "instrument_key": pl.Utf8,
        "listing_date": pl.Date,
        "float_mktcap_hkd": pl.Float64,
        "southbound_eligible": pl.Boolean,
        "as_of_date": pl.Date,
        "source_label": pl.Utf8,
    }
    if not seed_path.exists() or seed_path.stat().st_size == 0:
        return pl.DataFrame(schema=schema)

    seed = pl.read_csv(seed_path, null_values=["", "NA", "N/A", "NULL", "null"])
    if "instrument_key" not in seed.columns:
        raise SystemExit(f"Seed file is missing required column instrument_key: {seed_path}")
    seed = seed.with_columns(
        pl.col("instrument_key").cast(pl.Utf8).str.strip_chars().str.zfill(5).alias("instrument_key")
    )
    if "listing_date" in seed.columns:
        seed = seed.with_columns(pl.col("listing_date").cast(pl.Utf8).str.strptime(pl.Date, strict=False))
    else:
        seed = seed.with_columns(pl.lit(None, dtype=pl.Date).alias("listing_date"))
    if "as_of_date" in seed.columns:
        seed = seed.with_columns(pl.col("as_of_date").cast(pl.Utf8).str.strptime(pl.Date, strict=False))
    else:
        seed = seed.with_columns(pl.lit(None, dtype=pl.Date).alias("as_of_date"))
    if "float_mktcap_hkd" in seed.columns:
        seed = seed.with_columns(pl.col("float_mktcap_hkd").cast(pl.Float64, strict=False))
    else:
        seed = seed.with_columns(pl.lit(None, dtype=pl.Float64).alias("float_mktcap_hkd"))
    if "southbound_eligible" in seed.columns:
        seed = seed.with_columns(parse_bool_expr("southbound_eligible"))
    else:
        seed = seed.with_columns(pl.lit(None, dtype=pl.Boolean).alias("southbound_eligible"))
    if "source_label" not in seed.columns:
        seed = seed.with_columns(pl.lit("instrument_profile_seed").alias("source_label"))
    seed = seed.select(
        "instrument_key",
        "listing_date",
        "float_mktcap_hkd",
        "southbound_eligible",
        "as_of_date",
        "source_label",
    )
    return seed.unique(subset=["instrument_key"], keep="last").sort("instrument_key")


def build_profile(raw_root: Path, years: list[str], seed_path: Path) -> tuple[pl.DataFrame, dict[str, Any]]:
    orders = aggregate_raw_table(raw_root, "orders", years)
    trades = aggregate_raw_table(raw_root, "trades", years)
    key_frames = [frame.select("instrument_key") for frame in (orders, trades) if frame.height > 0]
    if not key_frames:
        raise SystemExit("No raw zip member universe matched the requested years.")
    universe = pl.concat(key_frames).unique().sort("instrument_key")
    seed = load_seed(seed_path)
    reference_join_applied = seed.height > 0
    profile = (
        universe.join(orders, on="instrument_key", how="left")
        .join(trades, on="instrument_key", how="left")
        .join(seed, on="instrument_key", how="left")
        .with_columns(
            pl.col("observed_orders_days").fill_null(0),
            pl.col("observed_trades_days").fill_null(0),
        )
        .with_columns(
            pl.min_horizontal("orders_first_seen_date", "trades_first_seen_date").alias("observed_first_date"),
            pl.max_horizontal("orders_last_seen_date", "trades_last_seen_date").alias("observed_last_date"),
            pl.when(
                pl.any_horizontal(
                    pl.col("listing_date").is_not_null(),
                    pl.col("float_mktcap_hkd").is_not_null(),
                    pl.col("southbound_eligible").is_not_null(),
                )
            )
            .then(pl.lit("seed_enriched"))
            .otherwise(pl.lit("universe_only"))
            .alias("profile_status"),
        )
        .with_columns(
        pl.lit("raw_zip_member_universe").alias("source_layer"),
            pl.lit(reference_join_applied).alias("reference_join_applied"),
            pl.lit("instrument_profile_sidecar").alias("profile_layer"),
        )
        .select(
            "instrument_key",
            "observed_first_date",
            "observed_last_date",
            "observed_orders_days",
            "observed_trades_days",
            "listing_date",
            "float_mktcap_hkd",
            "southbound_eligible",
            "as_of_date",
            "source_label",
            "profile_status",
            "source_layer",
            "reference_join_applied",
            "profile_layer",
        )
        .sort("instrument_key")
    )
    summary = {
        "generated_at": iso_utc_now(),
        "pipeline": "build_instrument_profile",
        "years": years,
        "instrument_count": int(profile.height),
        "seed_path": str(seed_path),
        "seed_row_count": int(seed.height),
        "listing_date_non_null_count": int(profile.filter(pl.col("listing_date").is_not_null()).height),
        "float_mktcap_non_null_count": int(profile.filter(pl.col("float_mktcap_hkd").is_not_null()).height),
        "southbound_non_null_count": int(profile.filter(pl.col("southbound_eligible").is_not_null()).height),
        "profile_status_counts": profile.group_by("profile_status").len().to_dicts(),
    }
    return profile, summary


def output_paths(output_root: Path) -> tuple[Path, Path]:
    latest_root = output_root / "latest"
    return latest_root / "instrument_profile.parquet", latest_root / "summary.json"


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="build_instrument_profile",
            purpose="Build a sidecar instrument_profile reference table from the observed raw zip member universe and optional seed enrichment.",
            responsibilities=[
                "Derive stable instrument_key values from raw zip member names.",
                "Aggregate observed first/last seen dates and observed day counts per instrument.",
                "Optionally join a user-maintained seed file for listing_date, southbound_eligible, or float_mktcap_hkd.",
            ],
            inputs=[
                "/Volumes/Data/港股Tick数据/<year>/YYYYMMDD.zip",
                "Research/References/normalized/instrument_profile_seed.csv",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/reference/instrument_profile/latest/instrument_profile.parquet",
                "/Volumes/Data/港股Tick数据/reference/instrument_profile/latest/summary.json",
            ],
        )
        return 0

    years = selected_years(args)
    logger = configure_logger("build_instrument_profile", args.log_root / "build_instrument_profile.log")
    profile, summary = build_profile(args.raw_root, years, args.seed_path)
    parquet_path, summary_path = output_paths(args.output_root)
    ensure_dir(parquet_path.parent)
    profile.write_parquet(parquet_path)
    write_json(summary_path, summary)
    logger.info(
        "Instrument profile complete: instruments=%s seed_rows=%s output=%s",
        summary["instrument_count"],
        summary["seed_row_count"],
        parquet_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
