from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, ensure_dir, iso_utc_now, write_json

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TUSHARE_ROOT = DEFAULT_DATA_ROOT / "reference" / "tushare"
DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_PROFILE_PATH = (
    DEFAULT_DATA_ROOT / "reference" / "instrument_profile" / "latest" / "instrument_profile.parquet"
)
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "dqa" / "reference" / "tushare"
DEFAULT_RESEARCH_ROOT = REPO_ROOT / "Research" / "Audits"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile Tushare daily universe with stage universe."
    )
    parser.add_argument("--year", required=True)
    parser.add_argument("--tushare-root", type=Path, default=DEFAULT_TUSHARE_ROOT)
    parser.add_argument("--stage-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--profile-path", type=Path, default=DEFAULT_PROFILE_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def json_list(values: set[str], limit: int = 20) -> str:
    return json.dumps(sorted(values)[:limit], ensure_ascii=False)


def latest_hk_basic_l_path(tushare_root: Path) -> Path | None:
    paths = sorted(
        (tushare_root / "hk_basic" / "list_status=L").glob("as_of_date=*/hk_basic.parquet")
    )
    return paths[-1] if paths else None


def instrument_set_from_parquet(path: Path, column: str = "instrument_key") -> set[str]:
    if not path.exists():
        return set()
    frame = (
        pl.scan_parquet(path)
        .select(pl.col(column).cast(pl.Utf8).str.zfill(5).alias("instrument_key"))
        .unique()
        .collect()
    )
    return set(frame["instrument_key"].drop_nulls().to_list())


def tushare_daily_partitions(tushare_root: Path, year: str) -> list[tuple[str, Path]]:
    rows: list[tuple[str, Path]] = []
    for manifest_path in sorted(
        (tushare_root / "hk_daily").glob(f"trade_date={year}-*/manifest.json")
    ):
        manifest = read_json(manifest_path)
        rows.append((manifest["trade_date"], Path(manifest["output_file"])))
    return rows


def stage_partition_path(stage_root: Path, table_name: str, trade_date: str) -> Path:
    compact = trade_date.replace("-", "")
    return stage_root / table_name / f"date={trade_date}" / f"{compact}_{table_name}.parquet"


def stage_universe(stage_root: Path, table_name: str, trade_date: str) -> set[str]:
    path = stage_partition_path(stage_root, table_name, trade_date)
    if not path.exists():
        return set()
    frame = (
        pl.scan_parquet(path)
        .select(
            pl.col("source_file")
            .cast(pl.Utf8)
            .str.extract(r"([0-9]{5})\.csv$", 1)
            .alias("instrument_key")
        )
        .unique()
        .collect()
    )
    return set(frame["instrument_key"].drop_nulls().to_list())


def row_for_date(
    *,
    trade_date: str,
    daily_path: Path,
    stage_root: Path,
    profile_universe: set[str],
    listed_universe: set[str],
) -> dict[str, Any]:
    daily_universe = instrument_set_from_parquet(daily_path)
    trade_universe = stage_universe(stage_root, "trades", trade_date)
    order_universe = stage_universe(stage_root, "orders", trade_date)
    stage_union = trade_universe | order_universe
    daily_missing_stage = daily_universe - stage_union
    stage_missing_daily = stage_union - daily_universe
    daily_missing_profile = daily_universe - profile_universe
    daily_missing_listed = daily_universe - listed_universe
    return {
        "date": trade_date,
        "tushare_daily_count": len(daily_universe),
        "stage_trades_count": len(trade_universe),
        "stage_orders_count": len(order_universe),
        "stage_union_count": len(stage_union),
        "profile_universe_count": len(profile_universe),
        "hk_basic_listed_count": len(listed_universe),
        "daily_stage_intersection_count": len(daily_universe & stage_union),
        "daily_missing_stage_count": len(daily_missing_stage),
        "stage_missing_daily_count": len(stage_missing_daily),
        "daily_missing_profile_count": len(daily_missing_profile),
        "daily_missing_listed_count": len(daily_missing_listed),
        "daily_missing_stage_sample": json_list(daily_missing_stage),
        "stage_missing_daily_sample": json_list(stage_missing_daily),
        "daily_missing_profile_sample": json_list(daily_missing_profile),
        "daily_missing_listed_sample": json_list(daily_missing_listed),
    }


def write_report(
    path: Path, year: str, summary: dict[str, Any], rows: list[dict[str, Any]]
) -> None:
    ensure_dir(path.parent)
    worst = sorted(rows, key=lambda row: row["stage_missing_daily_count"], reverse=True)[:5]
    lines = [
        f"# Tushare Daily Universe Reconciliation {year}",
        "",
        "## Summary",
        "",
        f"- generated_at: `{summary['generated_at']}`",
        f"- date_count: `{summary['date_count']}`",
        f"- total_daily_missing_stage: `{summary['total_daily_missing_stage']}`",
        f"- total_stage_missing_daily: `{summary['total_stage_missing_daily']}`",
        f"- stage_zero_date_count: `{summary['stage_zero_date_count']}`",
        f"- max_daily_missing_profile: `{summary['max_daily_missing_profile']}`",
        f"- max_stage_missing_daily: `{summary['max_stage_missing_daily']}`",
        f"- output_parquet: `{summary['output_parquet']}`",
        "",
        "## Stage Zero Dates",
        "",
        summary["stage_zero_dates_json"],
        "",
        "## Worst Stage Missing Daily Dates",
        "",
    ]
    for row in worst:
        lines.append(
            "- "
            f"{row['date']}: stage_missing_daily={row['stage_missing_daily_count']}, "
            f"daily_missing_stage={row['daily_missing_stage_count']}"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "This report is a reference reconciliation surface only. It does not promote Tushare "
            "or stage fields into verified semantics.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    daily_partitions = tushare_daily_partitions(args.tushare_root, args.year)
    if not daily_partitions:
        raise SystemExit(f"No Tushare hk_daily partitions found for year={args.year}.")
    profile_universe = instrument_set_from_parquet(args.profile_path)
    listed_path = latest_hk_basic_l_path(args.tushare_root)
    listed_universe = instrument_set_from_parquet(listed_path) if listed_path else set()
    rows: list[dict[str, Any]] = []
    output_dir = args.output_root / f"year={args.year}"
    heartbeat_path = output_dir / "heartbeat.json"
    ensure_dir(output_dir)
    for index, (trade_date, daily_path) in enumerate(daily_partitions, 1):
        row = row_for_date(
            trade_date=trade_date,
            daily_path=daily_path,
            stage_root=args.stage_root,
            profile_universe=profile_universe,
            listed_universe=listed_universe,
        )
        rows.append(row)
        write_json(
            heartbeat_path,
            {
                "generated_at": iso_utc_now(),
                "status": "running",
                "completed": index,
                "total": len(daily_partitions),
                "last_date": trade_date,
            },
        )
        print(f"[{index:03d}/{len(daily_partitions)}] {trade_date} {row}", flush=True)
    output_parquet = output_dir / "tushare_daily_universe_reconciliation.parquet"
    pl.DataFrame(rows).write_parquet(output_parquet)
    summary = {
        "pipeline": "run_tushare_daily_universe_reconciliation",
        "generated_at": iso_utc_now(),
        "year": args.year,
        "date_count": len(rows),
        "first_date": rows[0]["date"],
        "last_date": rows[-1]["date"],
        "total_daily_missing_stage": sum(row["daily_missing_stage_count"] for row in rows),
        "total_stage_missing_daily": sum(row["stage_missing_daily_count"] for row in rows),
        "stage_zero_date_count": sum(1 for row in rows if row["stage_union_count"] == 0),
        "stage_zero_dates_json": json.dumps(
            [row["date"] for row in rows if row["stage_union_count"] == 0],
            ensure_ascii=False,
        ),
        "max_daily_missing_stage": max(row["daily_missing_stage_count"] for row in rows),
        "max_stage_missing_daily": max(row["stage_missing_daily_count"] for row in rows),
        "max_daily_missing_profile": max(row["daily_missing_profile_count"] for row in rows),
        "output_parquet": str(output_parquet),
    }
    write_json(output_dir / "summary.json", summary)
    write_json(heartbeat_path, {**summary, "status": "completed"})
    write_report(
        args.research_root / f"tushare_daily_universe_{args.year}.md",
        args.year,
        summary,
        rows,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
