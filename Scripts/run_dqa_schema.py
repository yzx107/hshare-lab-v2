from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

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

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"


@dataclass(frozen=True)
class SchemaTask:
    year: str
    date: str
    table_name: str
    partition_dir: str
    parquet_paths: tuple[str, ...]

    @property
    def task_key(self) -> str:
        return f"{self.date}:{self.table_name}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build DQA schema/value/time audit tables from stage parquet partitions."
    )
    parser.add_argument("--year", required=True, help="Year such as 2025 or 2026.")
    parser.add_argument(
        "--table",
        choices=["all", "orders", "trades"],
        default="all",
        help="Logical table selection.",
    )
    parser.add_argument(
        "--stage-root",
        type=Path,
        default=DEFAULT_STAGE_ROOT,
        help="Root directory for candidate_cleaned stage parquet.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_DQA_ROOT,
        help="Root directory for DQA parquet outputs.",
    )
    parser.add_argument(
        "--research-root",
        type=Path,
        default=DEFAULT_RESEARCH_AUDITS_ROOT,
        help="Root directory for research-facing audit summaries.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for DQA logs.",
    )
    parser.add_argument(
        "--dates",
        help="Comma-separated trade dates in YYYYMMDD or YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=0,
        help="Optional limit on the number of dates to process.",
    )
    parser.add_argument(
        "--latest-days",
        action="store_true",
        help="When used with --max-days, select latest dates instead of earliest.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint and skip completed date-table tasks.",
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def canonical_date(value: str) -> str:
    digits = value.replace("-", "").strip()
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(f"Invalid date token: {value}")
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


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
        pl.DataFrame(rows).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


def reset_manifest_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def parse_selected_dates(args: argparse.Namespace, stage_root: Path, selected_tables: list[str]) -> list[str]:
    available_dates: set[str] = set()
    for table_name in selected_tables:
        table_root = stage_root / table_name
        if not table_root.exists():
            continue
        for date_dir in sorted(table_root.glob("date=*")):
            trade_date = date_dir.name.split("=", 1)[1]
            if trade_date.startswith(f"{args.year}-"):
                available_dates.add(trade_date)

    if args.dates:
        selected_dates = [canonical_date(token) for token in args.dates.split(",") if token.strip()]
    else:
        selected_dates = sorted(available_dates)
        if args.max_days:
            selected_dates = selected_dates[-args.max_days :] if args.latest_days else selected_dates[: args.max_days]
    return [value for value in selected_dates if value in available_dates]


def discover_tasks(args: argparse.Namespace) -> list[SchemaTask]:
    selected_tables = ["orders", "trades"] if args.table == "all" else [args.table]
    selected_dates = parse_selected_dates(args, args.stage_root, selected_tables)
    tasks: list[SchemaTask] = []

    for table_name in selected_tables:
        for trade_date in selected_dates:
            partition_dir = args.stage_root / table_name / f"date={trade_date}"
            parquet_paths = tuple(str(path) for path in sorted(partition_dir.glob("*.parquet")))
            if parquet_paths:
                tasks.append(
                    SchemaTask(
                        year=str(args.year),
                        date=trade_date,
                        table_name=table_name,
                        partition_dir=str(partition_dir),
                        parquet_paths=parquet_paths,
                    )
                )
    return tasks


def business_field_names(table_name: str) -> list[str]:
    return [
        column.name
        for column in CONTRACTS[table_name].business_columns
        if column.name != "SendTimeRaw"
    ]


def schema_signature(task: SchemaTask) -> tuple[list[str], dict[str, str], list[dict[str, Any]]]:
    file_signatures: list[dict[str, Any]] = []
    first_columns: list[str] = []
    first_types: dict[str, str] = {}
    for index, parquet_path in enumerate(task.parquet_paths):
        schema = pq.ParquetFile(parquet_path).schema_arrow
        columns = [field.name for field in schema]
        types = {field.name: str(field.type) for field in schema}
        nullable = {field.name: field.nullable for field in schema}
        file_signatures.append(
            {
                "parquet_path": parquet_path,
                "columns": columns,
                "types": types,
                "nullable": nullable,
            }
        )
        if index == 0:
            first_columns = columns
            first_types = types
    return first_columns, first_types, file_signatures


def build_schema_fingerprint_row(task: SchemaTask) -> dict[str, Any]:
    contract = CONTRACTS[task.table_name]
    columns, types, file_signatures = schema_signature(task)
    expected_types = {field.name: str(field.type) for field in contract.arrow_schema}
    expected_columns = [field.name for field in contract.arrow_schema]
    missing_columns = [column for column in expected_columns if column not in columns]
    extra_columns = [column for column in columns if column not in expected_columns]
    type_mismatches = [
        {"column": column, "expected": expected_types[column], "actual": types.get(column)}
        for column in expected_columns
        if column in types and expected_types[column] != types[column]
    ]
    file_schema_hashes = {
        hashlib.sha256(
            json.dumps(
                {
                    "columns": signature["columns"],
                    "types": signature["types"],
                    "nullable": signature["nullable"],
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:16]
        for signature in file_signatures
    }
    schema_hash = hashlib.sha256(
        json.dumps({"columns": columns, "types": types}, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]

    if missing_columns or extra_columns or type_mismatches or len(file_schema_hashes) > 1:
        schema_status = "fail"
    else:
        schema_status = "pass"

    return {
        "year": task.year,
        "date": task.date,
        "table_name": task.table_name,
        "partition_dir": task.partition_dir,
        "input_file_count": len(task.parquet_paths),
        "column_names": json.dumps(columns, ensure_ascii=False),
        "column_types": json.dumps(types, ensure_ascii=False, sort_keys=True),
        "nullable_signature": json.dumps(file_signatures[0]["nullable"], ensure_ascii=False, sort_keys=True),
        "schema_hash": schema_hash,
        "intra_partition_schema_variant_count": len(file_schema_hashes),
        "missing_columns": json.dumps(missing_columns, ensure_ascii=False),
        "extra_columns": json.dumps(extra_columns, ensure_ascii=False),
        "type_mismatches": json.dumps(type_mismatches, ensure_ascii=False),
        "schema_status": schema_status,
    }


def collect_field_null_rows(task: SchemaTask) -> list[dict[str, Any]]:
    fields = business_field_names(task.table_name)
    scan = pl.scan_parquet(list(task.parquet_paths))
    expressions: list[pl.Expr] = [pl.len().alias("__row_count")]
    for field in fields:
        expressions.extend(
            [
                pl.col(field).is_null().sum().alias(f"{field}__null_count"),
                (
                    pl.col(field)
                    .cast(pl.String)
                    .str.strip_chars()
                    .eq("")
                    .fill_null(False)
                    .sum()
                ).alias(f"{field}__blank_count"),
                pl.col(field).min().cast(pl.String).alias(f"{field}__min_value"),
                pl.col(field).max().cast(pl.String).alias(f"{field}__max_value"),
            ]
        )

    metrics = scan.select(expressions).collect().to_dicts()[0]
    row_count = int(metrics["__row_count"])
    rows: list[dict[str, Any]] = []
    for field in fields:
        null_count = int(metrics[f"{field}__null_count"] or 0)
        blank_count = int(metrics[f"{field}__blank_count"] or 0)
        null_rate = (null_count / row_count) if row_count else None
        blank_rate = (blank_count / row_count) if row_count else None
        if row_count == 0:
            profile_status = "empty"
        elif null_rate and null_rate > 0.50:
            profile_status = "warn"
        else:
            profile_status = "pass"
        rows.append(
            {
                "year": task.year,
                "date": task.date,
                "table_name": task.table_name,
                "field_name": field,
                "row_count": row_count,
                "null_count": null_count,
                "blank_count": blank_count,
                "null_rate": null_rate,
                "blank_rate": blank_rate,
                "min_value": metrics[f"{field}__min_value"],
                "max_value": metrics[f"{field}__max_value"],
                "profile_status": profile_status,
            }
        )
    return rows


def rule_specs(task: SchemaTask) -> list[dict[str, Any]]:
    if task.table_name == "trades":
        return [
            {
                "rule_name": "price_gt_0",
                "applicable": True,
                "tested_expr": pl.col("Price").is_not_null(),
                "violating_expr": pl.col("Price").is_not_null() & (pl.col("Price") <= 0),
                "sample_columns": ["Price", "TickID", "Time"],
            },
            {
                "rule_name": "volume_gt_0",
                "applicable": True,
                "tested_expr": pl.col("Volume").is_not_null(),
                "violating_expr": pl.col("Volume").is_not_null() & (pl.col("Volume") <= 0),
                "sample_columns": ["Volume", "TickID", "Time"],
            },
            {
                "rule_name": "tickid_not_null",
                "applicable": True,
                "tested_expr": pl.lit(True),
                "violating_expr": pl.col("TickID").is_null(),
                "sample_columns": ["TickID", "Time"],
            },
            {
                "rule_name": "time_not_null",
                "applicable": True,
                "tested_expr": pl.lit(True),
                "violating_expr": pl.col("Time").is_null(),
                "sample_columns": ["Time", "TickID"],
            },
            {
                "rule_name": "seqnum_not_null",
                "applicable": task.year == "2026",
                "tested_expr": pl.lit(True),
                "violating_expr": pl.col("SeqNum").is_null(),
                "sample_columns": ["SeqNum", "TickID", "Time"],
            },
            {
                "rule_name": "sendtime_not_null",
                "applicable": task.year == "2026",
                "tested_expr": pl.lit(True),
                "violating_expr": pl.col("SendTime").is_null(),
                "sample_columns": ["SendTime", "TickID", "Time"],
            },
        ]

    return [
        {
            "rule_name": "price_gt_0",
            "applicable": True,
            "tested_expr": pl.col("Price").is_not_null(),
            "violating_expr": pl.col("Price").is_not_null() & (pl.col("Price") <= 0),
            "sample_columns": ["Price", "OrderId", "Time"],
        },
        {
            "rule_name": "volume_ge_0",
            "applicable": True,
            "tested_expr": pl.col("Volume").is_not_null(),
            "violating_expr": pl.col("Volume").is_not_null() & (pl.col("Volume") < 0),
            "sample_columns": ["Volume", "OrderId", "Time"],
        },
        {
            "rule_name": "seqnum_not_null",
            "applicable": True,
            "tested_expr": pl.lit(True),
            "violating_expr": pl.col("SeqNum").is_null(),
            "sample_columns": ["SeqNum", "OrderId", "Time"],
        },
        {
            "rule_name": "orderid_not_null",
            "applicable": True,
            "tested_expr": pl.lit(True),
            "violating_expr": pl.col("OrderId").is_null(),
            "sample_columns": ["OrderId", "Time"],
        },
        {
            "rule_name": "time_not_null",
            "applicable": True,
            "tested_expr": pl.lit(True),
            "violating_expr": pl.col("Time").is_null(),
            "sample_columns": ["Time", "OrderId"],
        },
        {
            "rule_name": "sendtime_not_null",
            "applicable": task.year == "2026",
            "tested_expr": pl.lit(True),
            "violating_expr": pl.col("SendTime").is_null(),
            "sample_columns": ["SendTime", "OrderId", "Time"],
        },
        {
            "rule_name": "level_ge_0",
            "applicable": True,
            "tested_expr": pl.col("Level").is_not_null(),
            "violating_expr": pl.col("Level").is_not_null() & (pl.col("Level") < 0),
            "sample_columns": ["Level", "OrderId", "Time"],
        },
        {
            "rule_name": "volumepre_ge_0",
            "applicable": True,
            "tested_expr": pl.col("VolumePre").is_not_null(),
            "violating_expr": pl.col("VolumePre").is_not_null() & (pl.col("VolumePre") < 0),
            "sample_columns": ["VolumePre", "OrderId", "Time"],
        },
    ]


def collect_sample_bad_values(
    task: SchemaTask,
    violating_expr: pl.Expr,
    sample_columns: list[str],
) -> str | None:
    sample_rows = (
        pl.scan_parquet(list(task.parquet_paths))
        .filter(violating_expr)
        .select([pl.col(name).cast(pl.String).alias(name) for name in sample_columns])
        .head(5)
        .collect()
        .to_dicts()
    )
    if not sample_rows:
        return None
    return json.dumps(sample_rows, ensure_ascii=False)


def collect_field_value_rule_rows(task: SchemaTask) -> list[dict[str, Any]]:
    scan = pl.scan_parquet(list(task.parquet_paths))
    row_count = scan.select(pl.len().alias("row_count")).collect().item(0, 0)
    rows: list[dict[str, Any]] = []

    for spec in rule_specs(task):
        if not spec["applicable"]:
            rows.append(
                {
                    "year": task.year,
                    "date": task.date,
                    "table_name": task.table_name,
                    "rule_name": spec["rule_name"],
                    "tested_rows": 0,
                    "violating_rows": 0,
                    "violation_rate": None,
                    "status": "not_applicable",
                    "sample_bad_values": None,
                }
            )
            continue

        metrics = (
            scan.select(
                [
                    spec["tested_expr"].cast(pl.Int64).sum().alias("tested_rows"),
                    spec["violating_expr"].cast(pl.Int64).sum().alias("violating_rows"),
                ]
            )
            .collect()
            .to_dicts()[0]
        )
        tested_rows = int(metrics["tested_rows"] or 0)
        violating_rows = int(metrics["violating_rows"] or 0)
        violation_rate = (violating_rows / tested_rows) if tested_rows else None
        if row_count == 0:
            status = "empty"
        elif tested_rows == 0:
            status = "not_applicable"
        elif violating_rows > 0:
            status = "fail"
        else:
            status = "pass"
        rows.append(
            {
                "year": task.year,
                "date": task.date,
                "table_name": task.table_name,
                "rule_name": spec["rule_name"],
                "tested_rows": tested_rows,
                "violating_rows": violating_rows,
                "violation_rate": violation_rate,
                "status": status,
                "sample_bad_values": collect_sample_bad_values(
                    task,
                    violating_expr=spec["violating_expr"],
                    sample_columns=spec["sample_columns"],
                )
                if violating_rows
                else None,
            }
        )

    return rows


def hour_bucket_profile(task: SchemaTask, use_sendtime: bool) -> str | None:
    scan = pl.scan_parquet(list(task.parquet_paths))
    if use_sendtime:
        hour_expr = (
            pl.col("SendTime")
            .dt.convert_time_zone("Asia/Hong_Kong")
            .dt.hour()
            .alias("hour_bucket")
        )
        grouped = (
            scan.filter(pl.col("SendTime").is_not_null())
            .select(hour_expr)
            .group_by("hour_bucket")
            .len()
            .sort("hour_bucket")
            .collect()
            .to_dicts()
        )
    else:
        hour_expr = (
            pl.col("Time")
            .cast(pl.String)
            .str.slice(0, 2)
            .cast(pl.Int32, strict=False)
            .alias("hour_bucket")
        )
        grouped = (
            scan.filter(pl.col("Time").is_not_null())
            .select(hour_expr)
            .group_by("hour_bucket")
            .len()
            .sort("hour_bucket")
            .collect()
            .to_dicts()
        )
    if not grouped:
        return None
    profile = {str(row["hour_bucket"]): int(row["len"]) for row in grouped if row["hour_bucket"] is not None}
    return json.dumps(profile, ensure_ascii=False, sort_keys=True)


def collect_time_profile_row(task: SchemaTask) -> dict[str, Any]:
    scan = pl.scan_parquet(list(task.parquet_paths))
    metrics = (
        scan.select(
            [
                pl.len().alias("row_count"),
                pl.col("SendTime").is_null().sum().alias("sendtime_null_count"),
                pl.col("Time").is_null().sum().alias("time_null_count"),
                pl.col("SendTime").min().cast(pl.String).alias("min_sendtime"),
                pl.col("SendTime").max().cast(pl.String).alias("max_sendtime"),
                pl.col("Time").min().cast(pl.String).alias("min_time_raw"),
                pl.col("Time").max().cast(pl.String).alias("max_time_raw"),
                (
                    (
                        pl.col("SendTime").is_not_null()
                        & pl.col("SendTime").shift(1).is_not_null()
                        & (pl.col("SendTime") < pl.col("SendTime").shift(1))
                    )
                    .cast(pl.Int64)
                    .sum()
                ).alias("timestamp_inversion_count"),
                (
                    (
                        pl.col("SendTime").is_not_null()
                        & pl.col("SendTime").shift(1).is_not_null()
                    )
                    .cast(pl.Int64)
                    .sum()
                ).alias("timestamp_comparable_count"),
                (
                    (
                        pl.col("SendTime").is_not_null()
                        & pl.col("SendTime").shift(1).is_not_null()
                        & pl.col("SeqNum").is_not_null()
                        & pl.col("SeqNum").shift(1).is_not_null()
                        & (pl.col("SeqNum") > pl.col("SeqNum").shift(1))
                        & (pl.col("SendTime") < pl.col("SendTime").shift(1))
                    )
                    .cast(pl.Int64)
                    .sum()
                ).alias("seq_time_disagreement_count"),
                (
                    (
                        pl.col("SendTime").is_not_null()
                        & pl.col("SendTime").shift(1).is_not_null()
                        & pl.col("SeqNum").is_not_null()
                        & pl.col("SeqNum").shift(1).is_not_null()
                    )
                    .cast(pl.Int64)
                    .sum()
                ).alias("seq_time_comparable_count"),
            ]
        )
        .collect()
        .to_dicts()[0]
    )

    row_count = int(metrics["row_count"] or 0)
    timestamp_comparable_count = int(metrics["timestamp_comparable_count"] or 0)
    seq_time_comparable_count = int(metrics["seq_time_comparable_count"] or 0)
    sendtime_supported = task.year == "2026"

    return {
        "year": task.year,
        "date": task.date,
        "table_name": task.table_name,
        "session": None,
        "row_count": row_count,
        "min_sendtime": metrics["min_sendtime"],
        "max_sendtime": metrics["max_sendtime"],
        "min_time_raw": metrics["min_time_raw"],
        "max_time_raw": metrics["max_time_raw"],
        "sendtime_null_count": int(metrics["sendtime_null_count"] or 0),
        "time_null_count": int(metrics["time_null_count"] or 0),
        "hour_bucket_profile": hour_bucket_profile(task, use_sendtime=sendtime_supported and metrics["min_sendtime"] is not None),
        "timestamp_order_inversion_rate": (
            int(metrics["timestamp_inversion_count"] or 0) / timestamp_comparable_count
            if timestamp_comparable_count
            else None
        ),
        "seq_vs_time_disagreement_rate": (
            int(metrics["seq_time_disagreement_count"] or 0) / seq_time_comparable_count
            if seq_time_comparable_count
            else None
        ),
        "status": "pass" if row_count else "empty",
    }


def process_task(task: SchemaTask) -> dict[str, list[dict[str, Any]]]:
    return {
        "schema_fingerprint": [build_schema_fingerprint_row(task)],
        "field_nulls": collect_field_null_rows(task),
        "field_value_rules": collect_field_value_rule_rows(task),
        "time_profile": [collect_time_profile_row(task)],
    }


def build_summary(state: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    return {
        "pipeline": "dqa_schema",
        "status": state["status"],
        "year": state["year"],
        "generated_at": iso_utc_now(),
        "completed_count": state["completed_count"],
        "failed_count": state["failed_count"],
        "pending_count": state["pending_count"],
        "artifacts": {
            "checkpoint": str(output_dir / "checkpoint.json"),
            "heartbeat": str(output_dir / "heartbeat.json"),
            "schema_fingerprint_jsonl": str(output_dir / "audit_schema_fingerprint.jsonl"),
            "field_nulls_jsonl": str(output_dir / "audit_field_nulls.jsonl"),
            "field_value_rules_jsonl": str(output_dir / "audit_field_value_rules.jsonl"),
            "time_profile_jsonl": str(output_dir / "audit_time_profile.jsonl"),
            "summary": str(output_dir / "summary.json"),
        },
    }


def write_checkpoint(checkpoint_path: Path, heartbeat_path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = iso_utc_now()
    write_json(checkpoint_path, state)
    write_json(
        heartbeat_path,
        {
            "status": state["status"],
            "year": state["year"],
            "updated_at": state["updated_at"],
            "completed_count": state["completed_count"],
            "failed_count": state["failed_count"],
            "pending_count": state["pending_count"],
            "active_task_key": state.get("active_task_key"),
        },
    )


def report_markdown(path: Path, *, year: str, state: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(
        "\n".join(
            [
                f"# DQA Schema {year}",
                "",
                f"- generated_at: {iso_utc_now()}",
                f"- completed_count: {state['completed_count']}",
                f"- failed_count: {state['failed_count']}",
                f"- pending_count: {state['pending_count']}",
                "",
                "First-pass schema, field-rule, and time-profile audit based on stage parquet partitions.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_dqa_schema",
            purpose="Check schema drift, field health, and time integrity against stage contracts.",
            responsibilities=[
                "Snapshot actual parquet schema per date-table partition and compare it with the frozen stage contract.",
                "Compute field null/blank profiles plus mechanical value-rule checks.",
                "Produce a first-pass time profile from SendTime and Time without injecting semantic interpretation.",
            ],
            inputs=[
                "/Volumes/Data/港股Tick数据/candidate_cleaned/{orders,trades}/date=YYYY-MM-DD/*.parquet",
                "Scripts/stage_contract.py",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/dqa/schema/year=<year>/*.parquet",
                "Research/Audits/dqa_schema_<year>.md",
            ],
        )
        return 0

    tasks = discover_tasks(args)
    output_dir = args.output_root / "schema" / f"year={args.year}"
    research_report_path = args.research_root / f"dqa_schema_{args.year}.md"
    log_path = args.log_root / f"dqa_schema_{args.year}.log"
    logger = configure_logger("dqa_schema", log_path)

    if not tasks:
        logger.error("No stage parquet partitions matched the requested selection.")
        return 1

    ensure_dir(output_dir)
    checkpoint_path = output_dir / "checkpoint.json"
    heartbeat_path = output_dir / "heartbeat.json"
    schema_fingerprint_path = output_dir / "audit_schema_fingerprint.jsonl"
    field_nulls_path = output_dir / "audit_field_nulls.jsonl"
    field_value_rules_path = output_dir / "audit_field_value_rules.jsonl"
    time_profile_path = output_dir / "audit_time_profile.jsonl"
    summary_path = output_dir / "summary.json"

    if not args.resume:
        reset_manifest_files(
            [
                checkpoint_path,
                heartbeat_path,
                schema_fingerprint_path,
                field_nulls_path,
                field_value_rules_path,
                time_profile_path,
                summary_path,
            ]
        )
        state = {
            "status": "running",
            "year": args.year,
            "started_at": iso_utc_now(),
            "updated_at": iso_utc_now(),
            "completed_task_keys": [],
            "failed_tasks": {},
            "completed_count": 0,
            "failed_count": 0,
            "pending_count": len(tasks),
            "active_task_key": None,
        }
    else:
        if not checkpoint_path.exists():
            logger.error("Cannot resume because checkpoint is missing: %s", checkpoint_path)
            return 1
        state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        state["status"] = "running"
        state["active_task_key"] = None

    completed_task_keys = set(state.get("completed_task_keys", []))
    write_checkpoint(checkpoint_path, heartbeat_path, state)

    for task in tasks:
        if task.task_key in completed_task_keys:
            continue

        state["active_task_key"] = task.task_key
        state["pending_count"] = max(0, len(tasks) - len(completed_task_keys) - state["failed_count"])
        write_checkpoint(checkpoint_path, heartbeat_path, state)

        try:
            result = process_task(task)
            for row in result["schema_fingerprint"]:
                append_jsonl(schema_fingerprint_path, row)
            for row in result["field_nulls"]:
                append_jsonl(field_nulls_path, row)
            for row in result["field_value_rules"]:
                append_jsonl(field_value_rules_path, row)
            for row in result["time_profile"]:
                append_jsonl(time_profile_path, row)
            completed_task_keys.add(task.task_key)
            logger.info(
                "DQA schema task %s complete: schema=%s field_nulls=%s value_rules=%s time_profile=%s",
                task.task_key,
                len(result["schema_fingerprint"]),
                len(result["field_nulls"]),
                len(result["field_value_rules"]),
                len(result["time_profile"]),
            )
        except Exception as exc:
            state["failed_tasks"][task.task_key] = str(exc)
            logger.error("DQA schema task %s failed: %s", task.task_key, exc)

        state["completed_task_keys"] = sorted(completed_task_keys)
        state["completed_count"] = len(completed_task_keys)
        state["failed_count"] = len(state["failed_tasks"])
        state["pending_count"] = max(0, len(tasks) - state["completed_count"] - state["failed_count"])
        state["active_task_key"] = None
        write_checkpoint(checkpoint_path, heartbeat_path, state)

    schema_rows = read_jsonl_rows(schema_fingerprint_path)
    field_null_rows = read_jsonl_rows(field_nulls_path)
    field_rule_rows = read_jsonl_rows(field_value_rules_path)
    time_profile_rows = read_jsonl_rows(time_profile_path)

    write_parquet(schema_rows, output_dir / "audit_schema_fingerprint.parquet")
    write_parquet(field_null_rows, output_dir / "audit_field_nulls.parquet")
    write_parquet(field_rule_rows, output_dir / "audit_field_value_rules.parquet")
    write_parquet(time_profile_rows, output_dir / "audit_time_profile.parquet")
    report_markdown(research_report_path, year=str(args.year), state=state)

    state["status"] = "completed" if not state["failed_tasks"] else "completed_with_failures"
    write_checkpoint(checkpoint_path, heartbeat_path, state)
    write_json(summary_path, build_summary(state, output_dir))
    logger.info(
        "DQA schema %s for %s: completed=%s failed=%s output=%s",
        state["status"],
        args.year,
        state["completed_count"],
        state["failed_count"],
        output_dir,
    )
    return 0 if not state["failed_tasks"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
