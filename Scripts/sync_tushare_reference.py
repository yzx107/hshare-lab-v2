from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.reference_sources import (
    DEFAULT_LOCAL_SOURCE_CONFIG_PATH,
    DEFAULT_SOURCE_REGISTRY_PATH,
    load_local_source_config,
    load_source_registry,
    resolve_source_secret,
)
from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    configure_logger,
    iso_utc_now,
    write_json,
)

DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "reference" / "tushare"

HK_BASIC_FIELDS = (
    "ts_code,name,fullname,enname,market,list_status,list_date,delist_date,trade_unit,"
    "isin,curr_type"
)
HK_DAILY_FIELDS = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
HK_ADJFACTOR_FIELDS = "ts_code,trade_date,cum_adjfactor,close_price"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Land Tushare HK reference snapshots as parquet plus manifest sidecars."
    )
    parser.add_argument(
        "--endpoint",
        choices=["hk_basic", "hk_daily", "hk_tradecal", "hk_adjfactor"],
    )
    parser.add_argument(
        "--trade-date",
        help="YYYYMMDD or YYYY-MM-DD. Required for hk_daily and hk_adjfactor.",
    )
    parser.add_argument("--start-date", help="YYYYMMDD or YYYY-MM-DD. Required for hk_tradecal.")
    parser.add_argument("--end-date", help="YYYYMMDD or YYYY-MM-DD. Required for hk_tradecal.")
    parser.add_argument(
        "--list-status",
        default="L",
        help="hk_basic list_status filter. Default: L.",
    )
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--registry-path", type=Path, default=DEFAULT_SOURCE_REGISTRY_PATH)
    parser.add_argument("--local-config-path", type=Path, default=DEFAULT_LOCAL_SOURCE_CONFIG_PATH)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def date_token_to_iso(value: str) -> str:
    token = value.strip()
    if len(token) == 8 and token.isdigit():
        iso = f"{token[:4]}-{token[4:6]}-{token[6:]}"
    elif len(token) == 10 and token[4] == "-" and token[7] == "-":
        iso = token
    else:
        raise ValueError(f"Invalid date token: {value}")
    date.fromisoformat(iso)
    return iso


def date_token_to_compact(value: str) -> str:
    return date_token_to_iso(value).replace("-", "")


def import_tushare():
    try:
        import pandas as pd
        import tushare as ts
    except ImportError as exc:
        raise SystemExit(
            "Tushare dependencies are missing. Install the reference extras first."
        ) from exc
    return pd, ts


def pandas_to_polars(pandas_frame: Any) -> pl.DataFrame:
    import pandas as pd

    if pandas_frame is None:
        return pl.DataFrame()
    frame = pd.DataFrame(pandas_frame)
    if len(frame) == 0:
        return pl.DataFrame({column: [] for column in frame.columns})
    return pl.DataFrame(frame.to_dict(orient="records"))


def normalize_ts_code(frame: pl.DataFrame) -> pl.DataFrame:
    if "ts_code" not in frame.columns:
        return frame
    return frame.with_columns(
        pl.col("ts_code")
        .cast(pl.Utf8)
        .str.replace(r"\.HK$", "")
        .str.zfill(5)
        .alias("instrument_key")
    )


def normalize_hk_basic_frame(frame: pl.DataFrame, *, as_of_date: str) -> pl.DataFrame:
    if frame.height == 0 and not frame.columns:
        return frame
    normalized = normalize_ts_code(frame)
    return normalized.with_columns(
        pl.col("list_date")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        .dt.strftime("%Y-%m-%d")
        .alias("listing_date"),
        pl.col("delist_date")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        .dt.strftime("%Y-%m-%d")
        .alias("delisting_date"),
        pl.lit(as_of_date).alias("as_of_date"),
        pl.lit("tushare_hk_basic").alias("source_label"),
        pl.lit(iso_utc_now()).alias("fetched_at"),
    )


def normalize_hk_daily_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0 and not frame.columns:
        return frame
    normalized = normalize_ts_code(frame)
    return normalized.with_columns(
        pl.col("trade_date")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        .dt.strftime("%Y-%m-%d")
        .alias("date"),
        pl.lit("tushare_hk_daily").alias("source_label"),
        pl.lit(iso_utc_now()).alias("fetched_at"),
    )


def normalize_hk_adjfactor_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0 and not frame.columns:
        return frame
    normalized = normalize_ts_code(frame)
    return normalized.with_columns(
        pl.col("trade_date")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        .dt.strftime("%Y-%m-%d")
        .alias("date"),
        pl.lit("tushare_hk_adjfactor").alias("source_label"),
        pl.lit(iso_utc_now()).alias("fetched_at"),
    )


def normalize_hk_tradecal_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0 and not frame.columns:
        return frame
    return frame.with_columns(
        pl.col("cal_date")
        .cast(pl.Utf8)
        .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        .dt.strftime("%Y-%m-%d")
        .alias("date"),
        pl.lit("tushare_hk_tradecal").alias("source_label"),
        pl.lit(iso_utc_now()).alias("fetched_at"),
    )


def output_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    if args.endpoint == "hk_basic":
        partition = (
            args.output_root
            / "hk_basic"
            / f"list_status={args.list_status}"
            / f"as_of_date={args.as_of_date}"
        )
        return partition / "hk_basic.parquet", partition / "manifest.json"
    if not args.trade_date:
        if args.endpoint == "hk_tradecal":
            if not args.start_date or not args.end_date:
                raise SystemExit("--start-date and --end-date are required for hk_tradecal.")
            start_date = date_token_to_iso(args.start_date)
            end_date = date_token_to_iso(args.end_date)
            year = start_date[:4] if start_date[:4] == end_date[:4] else f"{start_date}_{end_date}"
            partition = args.output_root / "hk_tradecal" / f"year={year}"
            return partition / "hk_tradecal.parquet", partition / "manifest.json"
        raise SystemExit(f"--trade-date is required for {args.endpoint}.")
    trade_date = date_token_to_iso(args.trade_date)
    partition = args.output_root / args.endpoint / f"trade_date={trade_date}"
    return partition / f"{args.endpoint}.parquet", partition / "manifest.json"


def fetch_frame(args: argparse.Namespace, token: str) -> pl.DataFrame:
    _, ts = import_tushare()
    pro = ts.pro_api(token)
    if args.endpoint == "hk_basic":
        frame = pandas_to_polars(pro.hk_basic(list_status=args.list_status, fields=HK_BASIC_FIELDS))
        return normalize_hk_basic_frame(frame, as_of_date=args.as_of_date)
    if args.endpoint == "hk_tradecal":
        if not args.start_date or not args.end_date:
            raise SystemExit("--start-date and --end-date are required for hk_tradecal.")
        frame = pandas_to_polars(
            pro.hk_tradecal(
                start_date=date_token_to_compact(args.start_date),
                end_date=date_token_to_compact(args.end_date),
            )
        )
        return normalize_hk_tradecal_frame(frame)
    if not args.trade_date:
        raise SystemExit("--trade-date is required for hk_daily.")
    if args.endpoint == "hk_daily":
        frame = pandas_to_polars(
            pro.hk_daily(trade_date=date_token_to_compact(args.trade_date), fields=HK_DAILY_FIELDS)
        )
        return normalize_hk_daily_frame(frame)
    frame = pandas_to_polars(
        pro.hk_adjfactor(
            trade_date=date_token_to_compact(args.trade_date), fields=HK_ADJFACTOR_FIELDS
        )
    )
    return normalize_hk_adjfactor_frame(frame)


def manifest_payload(
    args: argparse.Namespace, frame: pl.DataFrame, parquet_path: Path
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pipeline": "sync_tushare_reference",
        "endpoint": args.endpoint,
        "generated_at": iso_utc_now(),
        "row_count": frame.height,
        "columns": frame.columns,
        "output_file": str(parquet_path),
        "output_bytes": parquet_path.stat().st_size if parquet_path.exists() else 0,
        "source_role": "reference_landing",
    }
    if args.endpoint == "hk_basic":
        payload.update({"as_of_date": args.as_of_date, "list_status": args.list_status})
    elif args.endpoint == "hk_tradecal":
        payload.update(
            {
                "start_date": date_token_to_iso(args.start_date),
                "end_date": date_token_to_iso(args.end_date),
            }
        )
    else:
        payload.update({"trade_date": date_token_to_iso(args.trade_date)})
    return payload


def print_plan() -> None:
    print("sync_tushare_reference: land Tushare HK reference snapshots.")
    print("Responsibilities:")
    print("- Fetch only registered Tushare HK reference endpoints.")
    print("- Write parquet plus manifest under the reference layer, not raw/stage/verified.")
    print("- Keep date/as_of provenance explicit for downstream joins and audits.")
    print("Inputs:")
    print("- config/reference_sources.example.json")
    print("- config/reference_sources.local.json or TUSHARE_TOKEN")
    print("Outputs:")
    print("- /Volumes/Data/港股Tick数据/reference/tushare/<endpoint>/<partition>/")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_plan()
        return 0
    if not args.endpoint:
        raise SystemExit("--endpoint is required unless --print-plan is used.")

    registry = load_source_registry(args.registry_path)
    local_config = load_local_source_config(args.local_config_path)
    source_id = f"tushare_{args.endpoint}"
    token = resolve_source_secret(source_id, registry=registry, local_config=local_config)
    if not token:
        raise SystemExit(
            "Missing Tushare token. Set TUSHARE_TOKEN or config/reference_sources.local.json."
        )

    logger = configure_logger(
        "sync_tushare_reference", args.log_root / "sync_tushare_reference.log"
    )
    parquet_path, manifest_path = output_paths(args)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    frame = fetch_frame(args, token)
    frame.write_parquet(parquet_path)
    manifest = manifest_payload(args, frame, parquet_path)
    write_json(manifest_path, manifest)
    logger.info("Tushare %s landed rows=%s output=%s", args.endpoint, frame.height, parquet_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
