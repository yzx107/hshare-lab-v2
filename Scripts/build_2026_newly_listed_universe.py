from __future__ import annotations

import argparse
import csv
import json
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, ensure_dir, iso_utc_now, write_json

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE_PATH = (
    DEFAULT_DATA_ROOT / "reference" / "instrument_profile" / "latest" / "instrument_profile.parquet"
)
DEFAULT_SEED_PATH = (
    REPO_ROOT / "Research" / "References" / "normalized" / "instrument_profile_seed.csv"
)
DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "reference" / "newly_listed_hk"
DEFAULT_RESEARCH_ROOT = REPO_ROOT / "Research"
DEFAULT_WATCH_SYMBOLS = ("HK.01609", "HK.01879")

OUTPUT_COLUMNS = [
    "symbol",
    "instrument_key",
    "listing_date",
    "name",
    "source_label",
    "stock_research_candidate",
    "candidate_cleaned_trade_dates",
    "candidate_cleaned_order_dates",
    "first_trade_date",
    "last_trade_date",
    "coverage_days",
    "universe_status",
    "caveat",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a fail-closed 2026 newly listed HK universe for OpenD handoff."
    )
    parser.add_argument("--listing-year", type=int, default=2026)
    parser.add_argument("--profile-path", type=Path, default=DEFAULT_PROFILE_PATH)
    parser.add_argument("--seed-path", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--manual-override-csv", type=Path)
    parser.add_argument("--stage-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument(
        "--dates", help="Comma-separated dates used for candidate_cleaned coverage."
    )
    parser.add_argument("--date-from", help="Inclusive start date for coverage scan.")
    parser.add_argument("--date-to", help="Inclusive end date for coverage scan.")
    parser.add_argument("--symbols", help="Optional comma-separated symbol filter.")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--heartbeat", action="store_true")
    parser.add_argument("--write-research-report", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_root / f"year={args.listing_year}"
    parquet_path = output_dir / f"newly_listed_hk_{args.listing_year}.parquet"
    csv_path = output_dir / f"newly_listed_hk_{args.listing_year}.csv"
    manifest_path = output_dir / f"newly_listed_hk_{args.listing_year}_manifest.json"
    heartbeat_path = output_dir / "heartbeat.json"

    if (
        args.resume
        and not args.overwrite_existing
        and parquet_path.exists()
        and manifest_path.exists()
    ):
        print(manifest_path.read_text(encoding="utf-8"))
        return 0

    selected_dates = select_dates(args.stage_root, args.dates, args.date_from, args.date_to)
    if args.heartbeat:
        write_json(
            heartbeat_path,
            {
                "generated_at": iso_utc_now(),
                "status": "scanning_coverage",
                "selected_dates": selected_dates,
            },
        )

    filter_symbols = set(parse_symbols(args.symbols))
    watch_symbols = {normalize_symbol(value) for value in DEFAULT_WATCH_SYMBOLS} | filter_symbols
    references = load_reference_rows(
        profile_path=args.profile_path,
        seed_path=args.seed_path,
        manual_override_csv=args.manual_override_csv,
    )
    target_keys = candidate_reference_keys(
        references, args.listing_year, filter_symbols, watch_symbols
    )
    coverage = scan_candidate_cleaned_coverage(
        stage_root=args.stage_root,
        raw_root=args.raw_root,
        dates=selected_dates,
        instrument_keys=target_keys,
        limit_rows=args.limit_rows,
        heartbeat_path=heartbeat_path if args.heartbeat else None,
    )
    rows = build_rows(references, args.listing_year, filter_symbols, watch_symbols, coverage)
    frame = pl.DataFrame(rows, infer_schema_length=None).select(OUTPUT_COLUMNS)
    ensure_dir(output_dir)
    frame.write_parquet(parquet_path)
    csv_sidecar_frame(frame).write_csv(csv_path)

    manifest = build_manifest(args, frame, selected_dates, parquet_path, csv_path, coverage)
    write_json(manifest_path, manifest)
    if args.heartbeat:
        write_json(
            heartbeat_path,
            {
                "generated_at": iso_utc_now(),
                "status": "completed",
                "row_count": manifest["row_count"],
                "included_count": manifest["status_counts"].get("included", 0),
                "manifest": str(manifest_path),
            },
        )
    if args.write_research_report:
        write_universe_report(args.research_root, args.listing_year, manifest, frame)
        write_json_summary(args.research_root, args.listing_year, manifest)
    print(json.dumps(compact_summary(manifest), ensure_ascii=False, indent=2))
    return 0


def parse_symbols(value: str | None) -> list[str]:
    if not value:
        return []
    return [normalize_symbol(token) for token in value.split(",") if token.strip()]


def normalize_symbol(value: str) -> str:
    text = value.strip().upper()
    if "." in text:
        text = text.split(".", 1)[1]
    return text.zfill(5)


def select_dates(
    stage_root: Path,
    dates_arg: str | None,
    date_from: str | None,
    date_to: str | None,
) -> list[str]:
    if dates_arg:
        return sorted({token.strip() for token in dates_arg.split(",") if token.strip()})
    dates = sorted(
        {
            path.name.split("=", 1)[1]
            for table in ("orders", "trades")
            for path in (stage_root / table).glob("date=*")
            if path.is_dir()
        }
    )
    if date_from:
        dates = [value for value in dates if value >= date_from]
    if date_to:
        dates = [value for value in dates if value <= date_to]
    return dates


def load_reference_rows(
    *,
    profile_path: Path,
    seed_path: Path,
    manual_override_csv: Path | None,
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in load_profile_rows(profile_path):
        key = normalize_symbol(row["instrument_key"])
        rows[key] = {**row, "instrument_key": key, "source_priority": "instrument_profile"}
    for row in load_seed_rows(seed_path):
        key = normalize_symbol(row["instrument_key"])
        base = rows.get(key, {"instrument_key": key})
        rows[key] = merge_reference_row(base, row, source_priority="instrument_profile_seed")
    if manual_override_csv:
        for row in load_manual_override_rows(manual_override_csv):
            key = normalize_symbol(row["instrument_key"])
            base = rows.get(key, {"instrument_key": key})
            rows[key] = merge_reference_row(base, row, source_priority="manual_override")
    for key, row in rows.items():
        row["instrument_key"] = key
        row["symbol"] = f"HK.{key}"
        row["instrument_family"] = row.get("instrument_family") or official_family_for_key(key)
        row["stock_research_candidate"] = bool_value(
            row.get("stock_research_candidate"),
            default=is_stock_research_candidate(key, row.get("instrument_family")),
        )
    return rows


def load_profile_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    columns = [
        "instrument_key",
        "listing_date",
        "stock_research_candidate",
        "instrument_family",
        "instrument_family_status",
        "source_label",
    ]
    frame = pl.read_parquet(
        path, columns=[column for column in columns if column in pl.read_parquet_schema(path)]
    )
    return normalize_reference_frame(frame).to_dicts()


def load_seed_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    frame = pl.read_csv(
        path, null_values=["", "NA", "N/A", "NULL", "null"], infer_schema_length=None
    )
    return normalize_reference_frame(frame).to_dicts()


def load_manual_override_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for raw in reader:
            key = raw.get("instrument_key") or raw.get("symbol")
            if not key:
                continue
            raw["instrument_key"] = key
            rows.append(raw)
        return normalize_reference_frame(pl.DataFrame(rows, infer_schema_length=None)).to_dicts()


def normalize_reference_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height == 0:
        return frame
    normalized = frame
    if "symbol" in normalized.columns and "instrument_key" not in normalized.columns:
        normalized = normalized.rename({"symbol": "instrument_key"})
    if "instrument_key" not in normalized.columns:
        raise SystemExit("reference rows require instrument_key or symbol")
    for column in (
        "listing_date",
        "name",
        "source_label",
        "instrument_family",
        "instrument_family_status",
        "caveat",
    ):
        if column not in normalized.columns:
            normalized = normalized.with_columns(pl.lit(None, dtype=pl.Utf8).alias(column))
    if "stock_research_candidate" not in normalized.columns:
        normalized = normalized.with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("stock_research_candidate")
        )
    return normalized.select(
        pl.col("instrument_key")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(r"^HK\.", "")
        .str.zfill(5),
        pl.col("listing_date").cast(pl.Utf8, strict=False).str.strip_chars(),
        pl.col("name").cast(pl.Utf8, strict=False),
        pl.col("source_label").cast(pl.Utf8, strict=False),
        pl.col("instrument_family").cast(pl.Utf8, strict=False),
        pl.col("instrument_family_status").cast(pl.Utf8, strict=False),
        pl.col("stock_research_candidate").cast(pl.Boolean, strict=False),
        pl.col("caveat").cast(pl.Utf8, strict=False),
    ).unique(subset=["instrument_key"], keep="last")


def merge_reference_row(
    base: dict[str, Any],
    incoming: dict[str, Any],
    *,
    source_priority: str,
) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if value not in (None, "") and merged.get(key) in (None, ""):
            merged[key] = value
    if incoming.get("source_label") and base.get("source_label") in (None, ""):
        merged["source_label"] = incoming["source_label"]
    merged["source_priority"] = source_priority
    return merged


def bool_value(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def official_family_for_key(key: str) -> str:
    code = int(key)
    if 2800 <= code <= 2849 or 3000 <= code <= 3199 or 3400 <= code <= 3499:
        return "exchange_traded_fund"
    if (
        7200 <= code <= 7399
        or 7500 <= code <= 7599
        or 7700 <= code <= 7799
        or 87200 <= code <= 87399
        or 87500 <= code <= 87599
        or 87700 <= code <= 87799
    ):
        return "leveraged_and_inverse_product"
    if 2900 <= code <= 2999:
        return "temporary_counter"
    if 4000 <= code <= 4799 or 5000 <= code <= 6029 or 89000 <= code <= 89099:
        return "debt_or_note"
    if 4800 <= code <= 4999:
        return "spac_warrant"
    if 87000 <= code <= 87099:
        return "reit_or_unit_trust_non_etf"
    if 89200 <= code <= 89599 or 10000 <= code <= 29999:
        return "derivative_warrant"
    if 47000 <= code <= 48999:
        return "inline_warrant"
    if 49500 <= code <= 69999:
        return "cbbc"
    if 90000 <= code <= 99999:
        return "stock_connect_security"
    return "listed_security_unclassified"


def is_stock_research_candidate(key: str, family: Any) -> bool:
    return int(key) < 10000 and (family in (None, "", "listed_security_unclassified"))


def candidate_reference_keys(
    references: dict[str, dict[str, Any]],
    listing_year: int,
    filter_symbols: set[str],
    watch_symbols: set[str],
) -> set[str]:
    keys = {
        key
        for key, row in references.items()
        if listing_year_from_value(row.get("listing_date")) == listing_year
    }
    keys |= watch_symbols
    if filter_symbols:
        keys &= filter_symbols | watch_symbols
    return keys


def listing_year_from_value(value: Any) -> int | None:
    if value in (None, ""):
        return None
    text = str(value)[:10]
    try:
        return date.fromisoformat(text).year
    except ValueError:
        return None


def scan_candidate_cleaned_coverage(
    *,
    stage_root: Path,
    raw_root: Path,
    dates: list[str],
    instrument_keys: set[str],
    limit_rows: int,
    heartbeat_path: Path | None,
) -> dict[str, dict[str, list[str]]]:
    coverage: dict[str, dict[str, list[str]]] = {
        key: {"orders": [], "trades": []} for key in instrument_keys
    }
    total = len(dates) * 2
    completed = 0
    for trade_date in dates:
        for table, raw_group in (("orders", "order"), ("trades", "trade")):
            symbols = symbols_for_partition(
                stage_root, raw_root, trade_date, table, raw_group, limit_rows
            )
            for key in instrument_keys & symbols:
                coverage[key][table].append(trade_date)
            completed += 1
            if heartbeat_path:
                write_json(
                    heartbeat_path,
                    {
                        "generated_at": iso_utc_now(),
                        "status": "scanning_coverage",
                        "completed_tasks": completed,
                        "total_tasks": total,
                        "current_date": trade_date,
                        "current_table": table,
                    },
                )
    return coverage


def symbols_for_partition(
    stage_root: Path,
    raw_root: Path,
    trade_date: str,
    table: str,
    raw_group: str,
    limit_rows: int,
) -> set[str]:
    parquet_path = (
        stage_root / table / f"date={trade_date}" / f"{trade_date.replace('-', '')}_{table}.parquet"
    )
    if not parquet_path.exists():
        return set()
    zip_path = raw_root / trade_date[:4] / f"{trade_date.replace('-', '')}.zip"
    if zip_path.exists():
        with zipfile.ZipFile(zip_path) as archive:
            return {
                Path(name).stem.zfill(5)
                for name in archive.namelist()
                if name.startswith(f"{raw_group}/") and name.endswith(".csv")
            }
    frame = pl.scan_parquet(str(parquet_path)).select("source_file")
    if limit_rows > 0:
        frame = frame.limit(limit_rows)
    return {
        normalize_symbol(value)
        for value in frame.unique().collect().get_column("source_file").to_list()
    }


def build_rows(
    references: dict[str, dict[str, Any]],
    listing_year: int,
    filter_symbols: set[str],
    watch_symbols: set[str],
    coverage: dict[str, dict[str, list[str]]],
) -> list[dict[str, Any]]:
    rows = []
    keys = set(coverage)
    keys |= {
        key
        for key, row in references.items()
        if listing_year_from_value(row.get("listing_date")) == listing_year
    }
    keys |= watch_symbols
    if filter_symbols:
        keys &= filter_symbols | watch_symbols
    for key in sorted(keys):
        row = references.get(key, {"instrument_key": key, "symbol": f"HK.{key}"})
        order_dates = sorted(set(coverage.get(key, {}).get("orders", [])))
        trade_dates = sorted(set(coverage.get(key, {}).get("trades", [])))
        all_dates = sorted(set(order_dates) | set(trade_dates))
        status, caveat = classify_status(row, listing_year, all_dates)
        rows.append(
            {
                "symbol": f"HK.{key}",
                "instrument_key": key,
                "listing_date": normalize_date(row.get("listing_date")),
                "name": row.get("name"),
                "source_label": row.get("source_label"),
                "stock_research_candidate": bool(row.get("stock_research_candidate", False)),
                "candidate_cleaned_trade_dates": trade_dates,
                "candidate_cleaned_order_dates": order_dates,
                "first_trade_date": all_dates[0] if all_dates else None,
                "last_trade_date": all_dates[-1] if all_dates else None,
                "coverage_days": len(all_dates),
                "universe_status": status,
                "caveat": merge_caveat(row.get("caveat"), caveat),
            }
        )
    return rows


def classify_status(
    row: dict[str, Any], listing_year: int, all_dates: list[str]
) -> tuple[str, str]:
    row_year = listing_year_from_value(row.get("listing_date"))
    if row_year is None:
        return "missing_listing_date", "No reliable listing_date in current reference inputs."
    if row_year != listing_year:
        return (
            "missing_listing_date",
            f"Reference listing_date is not in {listing_year}; kept only as watched/unresolved.",
        )
    if not bool(row.get("stock_research_candidate", False)):
        family = row.get("instrument_family") or "unknown"
        return (
            "ambiguous_instrument_type",
            f"Not admitted as ordinary stock candidate; instrument_family={family}.",
        )
    if not all_dates:
        return (
            "excluded_no_tick_data",
            "Reference listing is in target year, but no local candidate_cleaned "
            "coverage was found.",
        )
    return (
        "included",
        "Included only as Hshare-native newly listed stock research candidate "
        "with local tick coverage.",
    )


def normalize_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value)[:10]
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def merge_caveat(existing: Any, caveat: str) -> str:
    if existing not in (None, ""):
        return f"{existing}; {caveat}"
    return caveat


def build_manifest(
    args: argparse.Namespace,
    frame: pl.DataFrame,
    selected_dates: list[str],
    parquet_path: Path,
    csv_path: Path,
    coverage: dict[str, dict[str, list[str]]],
) -> dict[str, Any]:
    status_counts = {
        row["universe_status"]: int(row["len"])
        for row in frame.group_by("universe_status").len().to_dicts()
    }
    included = frame.filter(pl.col("universe_status") == "included")
    listing_dates = included.filter(pl.col("listing_date").is_not_null()).get_column("listing_date")
    target_status = {}
    for key in DEFAULT_WATCH_SYMBOLS:
        code = normalize_symbol(key)
        rows = frame.filter(pl.col("instrument_key") == code).to_dicts()
        target_status[f"HK.{code}"] = (
            rows[0] if rows else {"universe_status": "missing_listing_date"}
        )
    return {
        "generated_at": iso_utc_now(),
        "pipeline": "build_2026_newly_listed_universe",
        "listing_year": args.listing_year,
        "source_inputs": {
            "profile_path": str(args.profile_path),
            "seed_path": str(args.seed_path),
            "manual_override_csv": str(args.manual_override_csv)
            if args.manual_override_csv
            else None,
            "stage_root": str(args.stage_root),
            "raw_root": str(args.raw_root),
            "coverage_dates": selected_dates,
            "coverage_basis": "raw_zip_members_with_candidate_cleaned_partition_present",
            "limit_rows": args.limit_rows,
        },
        "outputs": {"parquet": str(parquet_path), "csv": str(csv_path)},
        "row_count": int(frame.height),
        "status_counts": status_counts,
        "included_count": status_counts.get("included", 0),
        "listing_date_min": listing_dates.min() if len(listing_dates) else None,
        "listing_date_max": listing_dates.max() if len(listing_dates) else None,
        "symbols_with_candidate_cleaned_trades": sorted(
            f"HK.{key}" for key, item in coverage.items() if item["trades"]
        ),
        "symbols_with_candidate_cleaned_orders": sorted(
            f"HK.{key}" for key, item in coverage.items() if item["orders"]
        ),
        "unresolved_or_blocked": frame.filter(pl.col("universe_status") != "included").to_dicts(),
        "target_symbol_status": target_status,
    }


def compact_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "listing_year": manifest["listing_year"],
        "row_count": manifest["row_count"],
        "included_count": manifest["included_count"],
        "listing_date_min": manifest["listing_date_min"],
        "listing_date_max": manifest["listing_date_max"],
        "outputs": manifest["outputs"],
        "target_symbol_status": {
            key: value.get("universe_status")
            for key, value in manifest["target_symbol_status"].items()
        },
    }


def csv_sidecar_frame(frame: pl.DataFrame) -> pl.DataFrame:
    rows = frame.to_dicts()
    for row in rows:
        row["candidate_cleaned_trade_dates"] = ",".join(
            row.get("candidate_cleaned_trade_dates") or []
        )
        row["candidate_cleaned_order_dates"] = ",".join(
            row.get("candidate_cleaned_order_dates") or []
        )
    return pl.DataFrame(rows, infer_schema_length=None).select(OUTPUT_COLUMNS)


def write_universe_report(
    research_root: Path,
    listing_year: int,
    manifest: dict[str, Any],
    frame: pl.DataFrame,
) -> Path:
    path = (
        research_root
        / "Reports"
        / f"newly_listed_hk_{listing_year}_universe_{datetime.now():%Y%m%d}.md"
    )
    ensure_dir(path.parent)
    included = frame.filter(pl.col("universe_status") == "included")
    top = included.sort("coverage_days", descending=True).head(20).to_dicts()
    lines = [
        f"# Newly Listed HK {listing_year} Universe",
        "",
        f"- generated_at: {manifest['generated_at']}",
        f"- row_count: {manifest['row_count']}",
        f"- included_count: {manifest['included_count']}",
        f"- listing_date_range: {manifest['listing_date_min']} to {manifest['listing_date_max']}",
        f"- missing_listing_date_count: {manifest['status_counts'].get('missing_listing_date', 0)}",
        "- ambiguous_instrument_count: "
        f"{manifest['status_counts'].get('ambiguous_instrument_type', 0)}",
        "- excluded_no_tick_data_count: "
        f"{manifest['status_counts'].get('excluded_no_tick_data', 0)}",
        "",
        "## Target Symbols",
        "",
    ]
    for symbol, row in manifest["target_symbol_status"].items():
        lines.append(
            f"- {symbol}: {row.get('universe_status')} "
            f"(listing_date={row.get('listing_date')}, coverage_days={row.get('coverage_days')})"
        )
    lines += [
        "",
        "## Top Symbols by Coverage",
        "",
        "| symbol | listing_date | coverage_days | first_trade_date | "
        "last_trade_date | source_label |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for row in top:
        lines.append(
            f"| {row['symbol']} | {row['listing_date']} | {row['coverage_days']} | "
            f"{row['first_trade_date']} | {row['last_trade_date']} | {row['source_label']} |"
        )
    lines += [
        "",
        "## Boundary",
        "",
        "- `universe_status=included` is the only downstream-admissible universe membership flag.",
        "- Non-stock ranges, missing listing dates, and listings without local "
        "tick coverage are fail-closed.",
        "- This output is a reference/caveat handoff, not a verified semantic layer.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_json_summary(research_root: Path, listing_year: int, manifest: dict[str, Any]) -> Path:
    path = research_root / "Reports" / f"newly_listed_universe_summary_{datetime.now():%Y%m%d}.json"
    source_inputs = dict(manifest["source_inputs"])
    coverage_dates = source_inputs.pop("coverage_dates", [])
    source_inputs["coverage_date_count"] = len(coverage_dates)
    source_inputs["coverage_date_min"] = min(coverage_dates) if coverage_dates else None
    source_inputs["coverage_date_max"] = max(coverage_dates) if coverage_dates else None
    write_json(
        path,
        {
            "generated_at": manifest["generated_at"],
            "pipeline": manifest["pipeline"],
            "listing_year": listing_year,
            "row_count": manifest["row_count"],
            "included_count": manifest["included_count"],
            "status_counts": manifest["status_counts"],
            "listing_date_min": manifest["listing_date_min"],
            "listing_date_max": manifest["listing_date_max"],
            "outputs": manifest["outputs"],
            "source_inputs": source_inputs,
            "target_symbol_status": manifest["target_symbol_status"],
            "symbols_with_candidate_cleaned_trades_count": len(
                manifest["symbols_with_candidate_cleaned_trades"]
            ),
            "symbols_with_candidate_cleaned_orders_count": len(
                manifest["symbols_with_candidate_cleaned_orders"]
            ),
            "full_manifest_path": str(
                Path(manifest["outputs"]["parquet"]).with_name(
                    f"newly_listed_hk_{listing_year}_manifest.json"
                )
            ),
        },
    )
    return path


if __name__ == "__main__":
    raise SystemExit(main())
