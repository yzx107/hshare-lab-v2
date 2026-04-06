from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.reference_sources import (
    DEFAULT_LOCAL_SOURCE_CONFIG_PATH,
    DEFAULT_SOURCE_REGISTRY_PATH,
    enabled_source_ids,
    get_registered_source,
    load_local_source_config,
    load_source_registry,
    resolve_source_secret,
)
from Scripts.runtime import DEFAULT_LOG_ROOT, configure_logger, iso_utc_now, print_scaffold_plan

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEED_PATH = REPO_ROOT / "Research" / "References" / "normalized" / "instrument_profile_seed.csv"


SEED_SCHEMA: dict[str, pl.DataType] = {
    "instrument_key": pl.Utf8,
    "listing_date": pl.Utf8,
    "float_mktcap_hkd": pl.Utf8,
    "southbound_eligible": pl.Utf8,
    "instrument_family": pl.Utf8,
    "instrument_family_source": pl.Utf8,
    "instrument_family_note": pl.Utf8,
    "as_of_date": pl.Utf8,
    "source_label": pl.Utf8,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync instrument_profile seed from registered reference sources such as Tushare or curated HKEX seed CSVs."
    )
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=DEFAULT_SEED_PATH,
        help="Target instrument profile seed CSV path.",
    )
    parser.add_argument(
        "--registry-path",
        type=Path,
        default=DEFAULT_SOURCE_REGISTRY_PATH,
        help="Reference source registry config path.",
    )
    parser.add_argument(
        "--local-config-path",
        type=Path,
        default=DEFAULT_LOCAL_SOURCE_CONFIG_PATH,
        help="Local private source config path.",
    )
    parser.add_argument(
        "--sources",
        default="enabled",
        help="Comma-separated source ids to run, or 'enabled' for all enabled sources in the registry.",
    )
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="As-of date recorded for source rows when the upstream source does not provide one.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for run logs.",
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def normalize_seed_frame(frame: pl.DataFrame | None) -> pl.DataFrame:
    if frame is None or frame.height == 0:
        return pl.DataFrame(schema=SEED_SCHEMA)
    normalized = frame
    for column_name, dtype in SEED_SCHEMA.items():
        if column_name not in normalized.columns:
            normalized = normalized.with_columns(pl.lit(None, dtype=dtype).alias(column_name))
        else:
            normalized = normalized.with_columns(pl.col(column_name).cast(dtype, strict=False).alias(column_name))
    normalized = normalized.select(list(SEED_SCHEMA))
    if "instrument_key" in normalized.columns:
        normalized = normalized.with_columns(
            pl.col("instrument_key").cast(pl.Utf8).str.strip_chars().str.zfill(5).alias("instrument_key")
        )
    return normalized


def load_existing_seed(seed_path: Path) -> pl.DataFrame:
    if not seed_path.exists() or seed_path.stat().st_size == 0:
        return normalize_seed_frame(None)
    return normalize_seed_frame(pl.read_csv(seed_path, null_values=["", "NA", "N/A", "NULL", "null"]))


def merge_seed(base: pl.DataFrame, incoming: pl.DataFrame) -> pl.DataFrame:
    if incoming.height == 0:
        return base
    left = base.rename({column: f"base__{column}" for column in base.columns if column != "instrument_key"})
    right = incoming.rename({column: f"incoming__{column}" for column in incoming.columns if column != "instrument_key"})
    joined = left.join(right, on="instrument_key", how="full", coalesce=True)
    expressions = [pl.col("instrument_key")]
    for column_name in SEED_SCHEMA:
        if column_name == "instrument_key":
            continue
        expressions.append(pl.coalesce(f"base__{column_name}", f"incoming__{column_name}").alias(column_name))
    return normalize_seed_frame(joined.select(expressions)).unique(subset=["instrument_key"], keep="first").sort("instrument_key")


def fetch_tushare_hk_basic(token: str, as_of_date: str) -> pl.DataFrame:
    try:
        import pandas as pd
        import tushare as ts
    except ImportError as exc:
        raise SystemExit("Tushare dependencies are missing. Install the optional reference dependencies first.") from exc

    pro = ts.pro_api(token)
    pandas_frame = pro.hk_basic(
        fields="ts_code,name,fullname,market,list_status,list_date,trade_unit,isin,curr_type"
    )
    if pandas_frame is None or len(pandas_frame) == 0:
        return normalize_seed_frame(None)
    frame = pl.from_pandas(pd.DataFrame(pandas_frame))
    return normalize_seed_frame(
        frame.with_columns(
            pl.col("ts_code").cast(pl.Utf8).str.replace(r"\.HK$", "").str.zfill(5).alias("instrument_key"),
            pl.col("list_date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m-%d")
            .alias("listing_date"),
            pl.lit(as_of_date).alias("as_of_date"),
            pl.lit("tushare_hk_basic").alias("source_label"),
        ).select(
            "instrument_key",
            "listing_date",
            pl.lit(None, dtype=pl.Utf8).alias("float_mktcap_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("southbound_eligible"),
            pl.lit(None, dtype=pl.Utf8).alias("instrument_family"),
            pl.lit(None, dtype=pl.Utf8).alias("instrument_family_source"),
            pl.lit(None, dtype=pl.Utf8).alias("instrument_family_note"),
            "as_of_date",
            "source_label",
        )
    )


def load_curated_seed(path: Path) -> pl.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return normalize_seed_frame(None)
    return normalize_seed_frame(pl.read_csv(path, null_values=["", "NA", "N/A", "NULL", "null"]))


def selected_source_ids(args: argparse.Namespace, registry: dict[str, Any]) -> list[str]:
    if args.sources == "enabled":
        return enabled_source_ids(registry)
    values = [token.strip() for token in args.sources.split(",") if token.strip()]
    if not values:
        raise SystemExit("At least one source id is required.")
    return values


def run_sources(
    *,
    source_ids: list[str],
    registry: dict[str, Any],
    local_config: dict[str, Any],
    as_of_date: str,
    logger,
) -> tuple[list[dict[str, Any]], list[pl.DataFrame]]:
    summaries: list[dict[str, Any]] = []
    frames: list[pl.DataFrame] = []
    for source_id in source_ids:
        source = get_registered_source(source_id, registry)
        if source_id == "tushare_hk_basic":
            token = resolve_source_secret(source_id, registry=registry, local_config=local_config)
            if not token:
                raise SystemExit(
                    "Missing Tushare token. Set TUSHARE_TOKEN or populate config/reference_sources.local.json."
                )
            frame = fetch_tushare_hk_basic(token, as_of_date)
        elif source.get("kind") == "curated_csv":
            source_path = REPO_ROOT / Path(source["path"])
            frame = load_curated_seed(source_path)
        elif source_id == "opend_security_snapshot":
            logger.info("OpenD source is registered but not yet materialized into seed ingestion. Skipping.")
            frame = normalize_seed_frame(None)
        else:
            raise SystemExit(f"Unsupported source id for seed sync: {source_id}")
        logger.info("Source %s rows=%s", source_id, frame.height)
        summaries.append({"source_id": source_id, "row_count": int(frame.height)})
        frames.append(frame)
    return summaries, frames


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="sync_instrument_profile_seed",
            purpose="Sync instrument_profile seed from registered reference sources such as Tushare and curated HKEX seed CSVs.",
            responsibilities=[
                "Load the registered reference source registry and local private config.",
                "Fetch or load source rows from enabled sources.",
                "Merge source rows into Research/References/normalized/instrument_profile_seed.csv without overwriting existing non-null values by default.",
                "Keep source roles explicit for listing_date, southbound, and instrument-family enrichment.",
            ],
            inputs=[
                "config/reference_sources.example.json",
                "config/reference_sources.local.json",
                "Research/References/normalized/instrument_profile_seed.csv",
                "Research/References/normalized/hkex_reit_seed.csv",
                "Research/References/normalized/hkex_southbound_seed.csv",
            ],
            outputs=[
                "Research/References/normalized/instrument_profile_seed.csv",
            ],
        )
        return 0

    logger = configure_logger("sync_instrument_profile_seed", args.log_root / "sync_instrument_profile_seed.log")
    registry = load_source_registry(args.registry_path)
    local_config = load_local_source_config(args.local_config_path)
    source_ids = selected_source_ids(args, registry)
    base_seed = load_existing_seed(args.seed_path)
    source_summaries, source_frames = run_sources(
        source_ids=source_ids,
        registry=registry,
        local_config=local_config,
        as_of_date=args.as_of_date,
        logger=logger,
    )
    merged = base_seed
    for frame in source_frames:
        merged = merge_seed(merged, frame)
    merged.write_csv(args.seed_path)
    logger.info(
        "Instrument profile seed sync complete: rows=%s generated_at=%s sources=%s",
        merged.height,
        iso_utc_now(),
        ",".join(source_id for source_id in source_ids),
    )
    for summary in source_summaries:
        logger.info("Source summary: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
