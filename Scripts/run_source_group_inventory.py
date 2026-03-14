from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.build_stage_parquet import (
    canonical_trade_date,
    normalize_text_expr,
    normalize_zip_member_name,
    parse_selected_dates,
    raw_group_for_member,
    read_csv_member_as_strings,
)
from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    append_jsonl,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)
from Scripts.stage_contract import CONTRACTS

DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inventory a raw source group such as HKDarkPool across one year."
    )
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
    parser.add_argument(
        "--group",
        default="HKDarkPool",
        help="Raw source group name to inventory. Defaults to HKDarkPool.",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="Root directory that contains year-level raw zip archives.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_DQA_ROOT,
        help="Root directory for source inventory outputs.",
    )
    parser.add_argument(
        "--research-root",
        type=Path,
        default=DEFAULT_RESEARCH_AUDITS_ROOT,
        help="Root directory for research-facing audit notes.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for source inventory logs.",
    )
    parser.add_argument(
        "--dates",
        help="Comma-separated trade dates in YYYYMMDD or YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=0,
        help="Optional limit on the number of trading dates to process.",
    )
    parser.add_argument(
        "--latest-days",
        action="store_true",
        help="When used with --max-days, select the latest dates instead of the earliest.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint and skip completed dates.",
    )
    parser.add_argument(
        "--print-plan",
        action="store_true",
        help="Print the intended inventory plan and exit.",
    )
    return parser.parse_args()


def json_compact(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


def reset_paths(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def group_slug(value: str) -> str:
    slug = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value.strip())
    return slug or "group"


def build_schema_hash(columns: list[str]) -> str:
    return hashlib.sha1("|".join(columns).encode("utf-8")).hexdigest()[:16]


def normalized_time_bounds(frame: pl.DataFrame) -> tuple[str | None, str | None]:
    time_column = next((column for column in frame.columns if column.lower() == "time"), None)
    if time_column is None:
        return None, None

    normalized = frame.select(
        pl.when(normalize_text_expr(time_column).is_null())
        .then(None)
        .when(normalize_text_expr(time_column).str.contains(r"^\d{1,6}$"))
        .then(normalize_text_expr(time_column).str.zfill(6))
        .otherwise(normalize_text_expr(time_column))
        .alias("Time")
    ).get_column("Time")
    values = normalized.drop_nulls()
    if len(values) == 0:
        return None, None
    return str(values.min()), str(values.max())


def infer_table_hint(columns: list[str]) -> tuple[str, int, int]:
    column_set = {column.lower() for column in columns}
    orders_columns = {column.name.lower() for column in CONTRACTS["orders"].business_columns}
    trades_columns = {column.name.lower() for column in CONTRACTS["trades"].business_columns}
    orders_overlap = len(column_set & orders_columns)
    trades_overlap = len(column_set & trades_columns)

    if orders_overlap == 0 and trades_overlap == 0:
        return "unknown", orders_overlap, trades_overlap
    if orders_overlap > trades_overlap:
        return "orders", orders_overlap, trades_overlap
    if trades_overlap > orders_overlap:
        return "trades", orders_overlap, trades_overlap
    return "mixed", orders_overlap, trades_overlap


def build_member_row(
    *,
    year: str,
    trade_date: str,
    group: str,
    zip_path: Path,
    source_file: str,
    frame: pl.DataFrame,
) -> dict[str, Any]:
    raw_columns = [column for column in frame.columns if column != "row_num_in_file"]
    table_hint, orders_overlap, trades_overlap = infer_table_hint(raw_columns)
    min_time_raw, max_time_raw = normalized_time_bounds(frame)
    row_count = frame.height

    return {
        "year": year,
        "date": trade_date,
        "source_group": group,
        "zip_path": str(zip_path),
        "source_file": source_file,
        "row_count": row_count,
        "column_count": len(raw_columns),
        "columns_json": json_compact(raw_columns),
        "schema_hash": build_schema_hash(raw_columns),
        "table_hint": table_hint,
        "orders_overlap": orders_overlap,
        "trades_overlap": trades_overlap,
        "has_time_column": any(column.lower() == "time" for column in raw_columns),
        "has_sendtime_column": any(column.lower() == "sendtime" for column in raw_columns),
        "min_time_raw": min_time_raw,
        "max_time_raw": max_time_raw,
    }


def scan_group_for_date(
    *,
    year: str,
    trade_date: str,
    zip_path: Path,
    group: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    member_rows: list[dict[str, Any]] = []

    with zipfile.ZipFile(zip_path) as zf:
        for original_name in sorted(zf.namelist()):
            normalized_name = normalize_zip_member_name(original_name)
            if not normalized_name.lower().endswith(".csv"):
                continue
            raw_group = raw_group_for_member(year, normalized_name)
            if raw_group != group:
                continue

            with zf.open(original_name) as member_stream:
                frame = read_csv_member_as_strings(member_stream)
            member_rows.append(
                build_member_row(
                    year=year,
                    trade_date=trade_date,
                    group=group,
                    zip_path=zip_path,
                    source_file=normalized_name,
                    frame=frame,
                )
            )

    hint_counts = Counter(row["table_hint"] for row in member_rows)
    non_null_min_times = [row["min_time_raw"] for row in member_rows if row["min_time_raw"] is not None]
    non_null_max_times = [row["max_time_raw"] for row in member_rows if row["max_time_raw"] is not None]
    daily_row = {
        "year": year,
        "date": trade_date,
        "source_group": group,
        "zip_path": str(zip_path),
        "matched_member_count": len(member_rows),
        "matched_row_count": sum(int(row["row_count"]) for row in member_rows),
        "schema_variant_count": len({row["schema_hash"] for row in member_rows}),
        "table_hint_counts": json_compact(dict(sorted(hint_counts.items()))),
        "member_examples": json_compact([row["source_file"] for row in member_rows[:10]]),
        "min_time_raw": min(non_null_min_times) if non_null_min_times else None,
        "max_time_raw": max(non_null_max_times) if non_null_max_times else None,
        "status": "present" if member_rows else "absent",
    }
    return member_rows, daily_row


def build_schema_fingerprint_rows(member_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregates: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in member_rows:
        key = (row["year"], row["date"], row["source_group"], row["schema_hash"])
        current = aggregates.setdefault(
            key,
            {
                "year": row["year"],
                "date": row["date"],
                "source_group": row["source_group"],
                "schema_hash": row["schema_hash"],
                "member_count": 0,
                "row_count": 0,
                "column_count": row["column_count"],
                "columns_json": row["columns_json"],
                "table_hint": row["table_hint"],
                "orders_overlap": row["orders_overlap"],
                "trades_overlap": row["trades_overlap"],
                "min_time_raw": row["min_time_raw"],
                "max_time_raw": row["max_time_raw"],
                "example_source_file": row["source_file"],
            },
        )
        current["member_count"] += 1
        current["row_count"] += int(row["row_count"])
        if row["min_time_raw"] is not None:
            current["min_time_raw"] = (
                row["min_time_raw"]
                if current["min_time_raw"] is None
                else min(current["min_time_raw"], row["min_time_raw"])
            )
        if row["max_time_raw"] is not None:
            current["max_time_raw"] = (
                row["max_time_raw"]
                if current["max_time_raw"] is None
                else max(current["max_time_raw"], row["max_time_raw"])
            )

    return [aggregates[key] for key in sorted(aggregates)]


def write_report_markdown(
    path: Path,
    *,
    year: str,
    group: str,
    daily_rows: list[dict[str, Any]],
    member_rows: list[dict[str, Any]],
    schema_rows: list[dict[str, Any]],
) -> None:
    ensure_dir(path.parent)
    matched_dates = [row["date"] for row in daily_rows if row["matched_member_count"] > 0]
    matched_row_total = sum(int(row["matched_row_count"]) for row in daily_rows)
    unique_schema_hashes = len({row["schema_hash"] for row in schema_rows})
    lines = [
        f"# Source Inventory {group} {year}",
        "",
        f"- generated_at: {iso_utc_now()}",
        f"- scanned_dates: {len(daily_rows)}",
        f"- matched_dates: {len(matched_dates)}",
        f"- matched_members: {len(member_rows)}",
        f"- matched_rows: {matched_row_total}",
        f"- schema_variants: {unique_schema_hashes}",
        f"- first_matched_date: {matched_dates[0] if matched_dates else 'n/a'}",
        f"- last_matched_date: {matched_dates[-1] if matched_dates else 'n/a'}",
        "",
        "This inventory isolates the source group before any contract expansion or semantic interpretation.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def initial_state(args: argparse.Namespace, selected_dates: list[str]) -> dict[str, Any]:
    return {
        "status": "running",
        "year": str(args.year),
        "group": args.group,
        "selected_dates": selected_dates,
        "started_at": iso_utc_now(),
        "updated_at": iso_utc_now(),
        "completed_dates": [],
        "matching_dates": [],
        "completed_count": 0,
        "pending_count": len(selected_dates),
        "matched_member_count": 0,
        "matched_row_count": 0,
        "last_date": None,
    }


def load_state(checkpoint_path: Path, args: argparse.Namespace, selected_dates: list[str]) -> dict[str, Any]:
    if not checkpoint_path.exists():
        return initial_state(args, selected_dates)

    state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if state.get("year") != str(args.year):
        raise ValueError(f"Checkpoint year mismatch: expected {args.year}, found {state.get('year')}")
    if state.get("group") != args.group:
        raise ValueError(f"Checkpoint group mismatch: expected {args.group}, found {state.get('group')}")
    if state.get("selected_dates") != selected_dates:
        raise ValueError("Checkpoint selected dates mismatch; rerun without --resume or use same date set.")
    state["status"] = "running"
    state["updated_at"] = iso_utc_now()
    return state


def write_checkpoint(checkpoint_path: Path, heartbeat_path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = iso_utc_now()
    write_json(checkpoint_path, state)
    heartbeat = {
        "status": state["status"],
        "updated_at": state["updated_at"],
        "completed_count": state["completed_count"],
        "pending_count": state["pending_count"],
        "matched_member_count": state["matched_member_count"],
        "matched_row_count": state["matched_row_count"],
        "matching_dates": state["matching_dates"],
        "last_date": state["last_date"],
    }
    write_json(heartbeat_path, heartbeat)


def build_summary(
    *,
    state: dict[str, Any],
    output_dir: Path,
    daily_rows: list[dict[str, Any]],
    member_rows: list[dict[str, Any]],
    schema_rows: list[dict[str, Any]],
    report_path: Path,
) -> dict[str, Any]:
    matched_dates = [row["date"] for row in daily_rows if row["matched_member_count"] > 0]
    unique_schema_hashes = len({row["schema_hash"] for row in schema_rows})
    return {
        "pipeline": "source_group_inventory",
        "status": state["status"],
        "year": state["year"],
        "group": state["group"],
        "generated_at": iso_utc_now(),
        "selected_dates": state["selected_dates"],
        "scanned_dates": len(daily_rows),
        "matching_dates_count": len(matched_dates),
        "matching_dates": matched_dates,
        "matched_member_count": len(member_rows),
        "matched_row_count": sum(int(row["row_count"]) for row in member_rows),
        "schema_variant_count": unique_schema_hashes,
        "schema_fingerprint_rows": len(schema_rows),
        "artifacts": {
            "checkpoint": str(output_dir / "checkpoint.json"),
            "heartbeat": str(output_dir / "heartbeat.json"),
            "member_inventory_jsonl": str(output_dir / "member_inventory.jsonl"),
            "daily_inventory_jsonl": str(output_dir / "daily_inventory.jsonl"),
            "member_inventory_parquet": str(output_dir / "audit_source_member_inventory.parquet"),
            "daily_summary_parquet": str(output_dir / "audit_source_daily_summary.parquet"),
            "schema_fingerprints_parquet": str(output_dir / "audit_source_schema_fingerprints.parquet"),
            "research_report": str(report_path),
        },
    }


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_source_group_inventory",
            purpose="Inventory a raw source group such as HKDarkPool before deciding contract scope.",
            responsibilities=[
                "Scan year-level raw zip archives and detect dates where the target source group appears.",
                "Materialize per-member counts, schema fingerprints, time windows, and table-shape hints.",
                "Leave visible checkpoint, heartbeat, summary, and a short research-facing note.",
            ],
            inputs=[
                "/Volumes/Data/港股Tick数据/<year>/*.zip",
                "Target raw source group name, such as HKDarkPool.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/dqa/source_inventory/year=<year>/group=<group>/*.parquet",
                "Research/Audits/source_inventory_<group>_<year>.md",
            ],
        )
        return 0

    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")

    year_dir = args.raw_root / str(args.year)
    output_dir = args.output_root / "source_inventory" / f"year={args.year}" / f"group={group_slug(args.group)}"
    report_path = args.research_root / f"source_inventory_{group_slug(args.group)}_{args.year}.md"
    log_path = args.log_root / f"source_inventory_{group_slug(args.group)}_{args.year}.log"
    logger = configure_logger("source_group_inventory", log_path)

    if not year_dir.exists():
        logger.error("Raw year directory does not exist: %s", year_dir)
        return 1

    selected_date_paths = parse_selected_dates(args, year_dir)
    if not selected_date_paths:
        logger.error("No zip archives matched the requested date selection.")
        return 1

    ensure_dir(output_dir)
    checkpoint_path = output_dir / "checkpoint.json"
    heartbeat_path = output_dir / "heartbeat.json"
    summary_path = output_dir / "summary.json"
    member_inventory_jsonl = output_dir / "member_inventory.jsonl"
    daily_inventory_jsonl = output_dir / "daily_inventory.jsonl"

    if args.resume and daily_inventory_jsonl.exists() and not checkpoint_path.exists():
        logger.error("Refusing to resume without checkpoint: daily inventory exists but checkpoint is missing.")
        return 1

    if not args.resume:
        reset_paths(
            [
                checkpoint_path,
                heartbeat_path,
                summary_path,
                member_inventory_jsonl,
                daily_inventory_jsonl,
                output_dir / "audit_source_member_inventory.parquet",
                output_dir / "audit_source_daily_summary.parquet",
                output_dir / "audit_source_schema_fingerprints.parquet",
            ]
        )

    selected_dates = [canonical_trade_date(date_key) for date_key, _ in selected_date_paths]
    state = load_state(checkpoint_path, args, selected_dates) if args.resume else initial_state(args, selected_dates)
    completed_dates = set(state.get("completed_dates", []))
    pending = [(date_key, zip_path) for date_key, zip_path in selected_date_paths if canonical_trade_date(date_key) not in completed_dates]

    write_checkpoint(checkpoint_path, heartbeat_path, state)

    for date_key, zip_path in pending:
        trade_date = canonical_trade_date(date_key)
        member_rows, daily_row = scan_group_for_date(
            year=str(args.year),
            trade_date=trade_date,
            zip_path=zip_path,
            group=args.group,
        )

        for row in member_rows:
            append_jsonl(member_inventory_jsonl, row)
        append_jsonl(daily_inventory_jsonl, daily_row)

        completed_dates.add(trade_date)
        state["completed_dates"] = sorted(completed_dates)
        state["matching_dates"] = sorted(
            set(state.get("matching_dates", []))
            | ({trade_date} if daily_row["matched_member_count"] > 0 else set())
        )
        state["matched_member_count"] += daily_row["matched_member_count"]
        state["matched_row_count"] += daily_row["matched_row_count"]
        state["completed_count"] = len(completed_dates)
        state["pending_count"] = max(0, len(selected_dates) - state["completed_count"])
        state["last_date"] = trade_date
        write_checkpoint(checkpoint_path, heartbeat_path, state)
        logger.info(
            "Scanned %s for %s: members=%s rows=%s status=%s",
            trade_date,
            args.group,
            daily_row["matched_member_count"],
            daily_row["matched_row_count"],
            daily_row["status"],
        )

    member_rows = read_jsonl_rows(member_inventory_jsonl)
    daily_rows = read_jsonl_rows(daily_inventory_jsonl)
    schema_rows = build_schema_fingerprint_rows(member_rows)

    write_parquet(member_rows, output_dir / "audit_source_member_inventory.parquet")
    write_parquet(daily_rows, output_dir / "audit_source_daily_summary.parquet")
    write_parquet(schema_rows, output_dir / "audit_source_schema_fingerprints.parquet")
    write_report_markdown(
        report_path,
        year=str(args.year),
        group=args.group,
        daily_rows=daily_rows,
        member_rows=member_rows,
        schema_rows=schema_rows,
    )

    state["status"] = "completed"
    write_checkpoint(checkpoint_path, heartbeat_path, state)
    summary = build_summary(
        state=state,
        output_dir=output_dir,
        daily_rows=daily_rows,
        member_rows=member_rows,
        schema_rows=schema_rows,
        report_path=report_path,
    )
    write_json(summary_path, summary)

    logger.info(
        "Source group inventory complete for %s %s: scanned_dates=%s matching_dates=%s matched_members=%s output=%s",
        args.year,
        args.group,
        len(daily_rows),
        len(summary["matching_dates"]),
        len(member_rows),
        output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
