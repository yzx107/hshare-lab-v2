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


def parse_utf8_expr(column_name: str) -> pl.Expr:
    return pl.col(column_name).cast(pl.Utf8, strict=False).str.strip_chars().alias(column_name)


def official_range_instrument_family_expr() -> pl.Expr:
    code = pl.col("instrument_key").cast(pl.Int64, strict=False)
    return (
        pl.when(
            code.is_between(2800, 2849, closed="both")
            | code.is_between(3000, 3199, closed="both")
            | code.is_between(3400, 3499, closed="both")
        )
        .then(pl.lit("exchange_traded_fund"))
        .when(
            code.is_between(7200, 7399, closed="both")
            | code.is_between(7500, 7599, closed="both")
            | code.is_between(7700, 7799, closed="both")
            | code.is_between(87200, 87399, closed="both")
            | code.is_between(87500, 87599, closed="both")
            | code.is_between(87700, 87799, closed="both")
        )
        .then(pl.lit("leveraged_and_inverse_product"))
        .when(code.is_between(2900, 2999, closed="both"))
        .then(pl.lit("temporary_counter"))
        .when(code.is_between(4000, 4199, closed="both"))
        .then(pl.lit("exchange_fund_note"))
        .when(code.is_between(4200, 4299, closed="both"))
        .then(pl.lit("government_bond"))
        .when(
            code.is_between(4300, 4329, closed="both")
            | code.is_between(4400, 4599, closed="both")
            | code.is_between(5000, 6029, closed="both")
        )
        .then(pl.lit("debt_security_professional_only"))
        .when(code.is_between(4700, 4799, closed="both"))
        .then(pl.lit("debt_security_public"))
        .when(code.is_between(4800, 4999, closed="both"))
        .then(pl.lit("spac_warrant"))
        .when(code.is_between(6200, 6299, closed="both"))
        .then(pl.lit("hdr"))
        .when(code.is_between(6300, 6399, closed="both"))
        .then(pl.lit("restricted_security_or_hdr"))
        .when(code.is_between(87000, 87099, closed="both"))
        .then(pl.lit("reit_or_unit_trust_non_etf"))
        .when(code.is_between(89000, 89099, closed="both"))
        .then(pl.lit("prc_mof_bond"))
        .when(code.is_between(89200, 89599, closed="both") | code.is_between(10000, 29999, closed="both"))
        .then(pl.lit("derivative_warrant"))
        .when(code.is_between(47000, 48999, closed="both"))
        .then(pl.lit("inline_warrant"))
        .when(code.is_between(49500, 69999, closed="both"))
        .then(pl.lit("cbbc"))
        .when(code.is_between(90000, 99999, closed="both"))
        .then(pl.lit("stock_connect_security"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("official_range_instrument_family")
    )


def official_range_instrument_family_note_expr() -> pl.Expr:
    code = pl.col("instrument_key").cast(pl.Int64, strict=False)
    return (
        pl.when(
            code.is_between(2800, 2849, closed="both")
            | code.is_between(3000, 3199, closed="both")
            | code.is_between(3400, 3499, closed="both")
        )
        .then(pl.lit("HKEX Stock Code Allocation Plan ETF range: 02800-02849 / 03000-03199 / 03400-03499."))
        .when(
            code.is_between(7200, 7399, closed="both")
            | code.is_between(7500, 7599, closed="both")
            | code.is_between(7700, 7799, closed="both")
            | code.is_between(87200, 87399, closed="both")
            | code.is_between(87500, 87599, closed="both")
            | code.is_between(87700, 87799, closed="both")
        )
        .then(pl.lit("HKEX Stock Code Allocation Plan leveraged/inverse product range."))
        .when(code.is_between(2900, 2999, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan temporary counter range."))
        .when(code.is_between(4000, 4199, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan Exchange Fund Note range."))
        .when(code.is_between(4200, 4299, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan HKSAR Government Bond range."))
        .when(
            code.is_between(4300, 4329, closed="both")
            | code.is_between(4400, 4599, closed="both")
            | code.is_between(5000, 6029, closed="both")
        )
        .then(pl.lit("HKEX Stock Code Allocation Plan debt securities for professional investors only range."))
        .when(code.is_between(4700, 4799, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan debt securities for the public range."))
        .when(code.is_between(4800, 4999, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan SPAC warrant range."))
        .when(code.is_between(6200, 6299, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan Hong Kong Depositary Receipt range."))
        .when(code.is_between(6300, 6399, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan restricted security / HDR range."))
        .when(code.is_between(87000, 87099, closed="both"))
        .then(pl.lit("HKEX REIT stock code allocation range: 87000-87099."))
        .when(code.is_between(89000, 89099, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan PRC Ministry of Finance bond range."))
        .when(code.is_between(89200, 89599, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan RMB derivative warrant range: 89200-89599."))
        .when(code.is_between(10000, 29999, closed="both"))
        .then(pl.lit("HKEX structured products stock code allocation range for derivative warrants: 10000-29999."))
        .when(code.is_between(47000, 48999, closed="both"))
        .then(pl.lit("HKEX structured products stock code allocation range for inline warrants: 47000-48999."))
        .when(code.is_between(49500, 69999, closed="both"))
        .then(pl.lit("HKEX structured products stock code allocation range for CBBCs: 49500-69999."))
        .when(code.is_between(90000, 99999, closed="both"))
        .then(pl.lit("HKEX Stock Code Allocation Plan Stock Connect range: 90000-99999."))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("official_range_instrument_family_note")
    )


def stock_research_candidate_expr() -> pl.Expr:
    code = pl.col("instrument_key").cast(pl.Int64, strict=False)
    return (
        pl.when(
            (pl.col("instrument_family") == "listed_security_unclassified")
            & code.is_not_null()
            & (code < 10000)
        )
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("stock_research_candidate")
    )


def load_seed(seed_path: Path) -> pl.DataFrame:
    schema = {
        "instrument_key": pl.Utf8,
        "listing_date": pl.Date,
        "float_mktcap_hkd": pl.Float64,
        "southbound_eligible": pl.Boolean,
        "instrument_family": pl.Utf8,
        "instrument_family_source": pl.Utf8,
        "instrument_family_note": pl.Utf8,
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
    if "instrument_family" in seed.columns:
        seed = seed.with_columns(parse_utf8_expr("instrument_family"))
    else:
        seed = seed.with_columns(pl.lit(None, dtype=pl.Utf8).alias("instrument_family"))
    if "instrument_family_source" in seed.columns:
        seed = seed.with_columns(parse_utf8_expr("instrument_family_source"))
    else:
        seed = seed.with_columns(pl.lit(None, dtype=pl.Utf8).alias("instrument_family_source"))
    if "instrument_family_note" in seed.columns:
        seed = seed.with_columns(parse_utf8_expr("instrument_family_note"))
    else:
        seed = seed.with_columns(pl.lit(None, dtype=pl.Utf8).alias("instrument_family_note"))
    if "source_label" not in seed.columns:
        seed = seed.with_columns(pl.lit("instrument_profile_seed").alias("source_label"))
    seed = seed.select(
        "instrument_key",
        "listing_date",
        "float_mktcap_hkd",
        "southbound_eligible",
        "instrument_family",
        "instrument_family_source",
        "instrument_family_note",
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
            official_range_instrument_family_expr(),
            official_range_instrument_family_note_expr(),
        )
        .with_columns(
            pl.col("observed_orders_days").fill_null(0),
            pl.col("observed_trades_days").fill_null(0),
        )
        .with_columns(
            pl.min_horizontal("orders_first_seen_date", "trades_first_seen_date").alias("observed_first_date"),
            pl.max_horizontal("orders_last_seen_date", "trades_last_seen_date").alias("observed_last_date"),
            pl.coalesce("instrument_family", "official_range_instrument_family", pl.lit("listed_security_unclassified")).alias(
                "instrument_family"
            ),
            pl.when(pl.col("instrument_family_source").is_not_null())
            .then(pl.col("instrument_family_source"))
            .when(pl.col("official_range_instrument_family").is_not_null())
            .then(pl.lit("hkex_stock_code_allocation_plan"))
            .otherwise(pl.lit("raw_universe_only_unclassified"))
            .alias("instrument_family_source"),
            pl.when(pl.col("instrument_family").is_not_null())
            .then(pl.col("instrument_family_note"))
            .when(pl.col("official_range_instrument_family").is_not_null())
            .then(pl.col("official_range_instrument_family_note"))
            .otherwise(
                pl.lit(
                    "Current upstream can prove this instrument exists in the raw universe, but cannot yet safely separate it as ordinary equity vs fund/REIT/other listed security."
                )
            )
            .alias("instrument_family_note"),
            pl.when(pl.col("instrument_family").is_not_null())
            .then(pl.lit("seed_classified"))
            .when(pl.col("official_range_instrument_family").is_not_null())
            .then(pl.lit("official_range_classified"))
            .otherwise(pl.lit("listed_security_unclassified"))
            .alias("instrument_family_status"),
            pl.when(
                pl.any_horizontal(
                    pl.col("listing_date").is_not_null(),
                    pl.col("float_mktcap_hkd").is_not_null(),
                    pl.col("southbound_eligible").is_not_null(),
                    pl.col("instrument_family").is_not_null(),
                )
            )
            .then(pl.lit("seed_enriched"))
            .when(pl.col("official_range_instrument_family").is_not_null())
            .then(pl.lit("official_range_classified"))
            .otherwise(pl.lit("universe_only"))
            .alias("profile_status"),
        )
        .with_columns(stock_research_candidate_expr())
        .with_columns(
            pl.when(pl.col("stock_research_candidate"))
            .then(pl.lit("candidate_from_low_code_listed_security_unclassified"))
            .otherwise(pl.lit("not_in_stock_research_candidate_lane"))
            .alias("stock_research_candidate_status"),
            pl.when(pl.col("stock_research_candidate"))
            .then(
                pl.lit(
                    "Conservative stock research candidate lane: low-code listed security not yet classified as ETF/REIT/structured product/debt by current upstream references. This is not pure common-equity proof."
                )
            )
            .otherwise(
                pl.lit(
                    "Either already classified as non-stock/special product, or not in the current low-code candidate lane."
                )
            )
            .alias("stock_research_candidate_note"),
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
            "instrument_family",
            "instrument_family_status",
            "instrument_family_source",
            "instrument_family_note",
            "stock_research_candidate",
            "stock_research_candidate_status",
            "stock_research_candidate_note",
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
        "instrument_family_classified_count": int(profile.filter(pl.col("instrument_family_status") != "listed_security_unclassified").height),
        "stock_research_candidate_count": int(profile.filter(pl.col("stock_research_candidate")).height),
        "instrument_family_counts": profile.group_by("instrument_family").len().sort("instrument_family").to_dicts(),
        "instrument_family_status_counts": profile.group_by("instrument_family_status").len().sort("instrument_family_status").to_dicts(),
        "stock_research_candidate_status_counts": profile.group_by("stock_research_candidate_status").len().sort("stock_research_candidate_status").to_dicts(),
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
                "Optionally join a user-maintained seed file for listing_date, southbound_eligible, float_mktcap_hkd, and instrument_family.",
                "Apply conservative HKEX stock-code-allocation classification for obvious non-equity / special product ranges.",
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
