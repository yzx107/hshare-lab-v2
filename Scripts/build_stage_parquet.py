from __future__ import annotations

import argparse
import json
import os
import time
import zipfile
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    DEFAULT_MANIFEST_ROOT,
    append_jsonl,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)
from Scripts.stage_contract import CONTRACTS, NULL_TOKENS, StageColumn, StageTableContract

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
PROGRESS_EMIT_MEMBER_INTERVAL = 100
PROGRESS_EMIT_SECONDS = 5.0


@dataclass(frozen=True)
class StageTask:
    year: str
    trade_date: str
    zip_path: str
    table_name: str
    output_path: str
    row_group_target: int
    overwrite_existing: bool

    @property
    def task_key(self) -> str:
        return f"{self.trade_date}:{self.table_name}"


@dataclass(frozen=True)
class StageBundleTask:
    year: str
    trade_date: str
    zip_path: str
    tasks: tuple[StageTask, ...]
    progress_path: str | None = None

    @property
    def bundle_key(self) -> str:
        return f"{self.trade_date}:{','.join(task.table_name for task in self.tasks)}"


@dataclass
class TableBuildState:
    task: StageTask
    contract: StageTableContract
    output_path: Path
    tmp_output_path: Path
    started_at: str
    ingest_dt: datetime
    buffer_frames: list[pl.DataFrame] = field(default_factory=list)
    buffer_rows: int = 0
    writer: pq.ParquetWriter | None = None
    raw_row_count: int = 0
    rejected_rows: int = 0
    row_count: int = 0
    source_member_count: int = 0
    failed_members: list[dict[str, str]] = field(default_factory=list)
    required_null_counts: Counter[str] = field(default_factory=Counter)
    rejection_reason_counts: Counter[str] = field(default_factory=Counter)
    send_time_parse_failure_count: int = 0
    min_send_time: Any = None
    max_send_time: Any = None
    min_time: Any = None
    max_time: Any = None


def default_workers() -> int:
    cpu_count = os.cpu_count() or 1
    return max(1, min(8, cpu_count - 1))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build stage parquet for raw Trades and Orders with visible, resumable tasks."
    )
    parser.add_argument("--year", required=True, help="Year such as 2025 or 2026.")
    parser.add_argument(
        "--table",
        choices=["all", "orders", "trades"],
        default="all",
        help="Logical table to build.",
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
        default=DEFAULT_STAGE_ROOT,
        help="Root directory for candidate_cleaned / stage parquet output.",
    )
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=DEFAULT_MANIFEST_ROOT,
        help="Root directory for stage manifests and checkpoints.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for stage logs.",
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
        "--workers",
        type=int,
        default=default_workers(),
        help="Number of parallel date-table tasks.",
    )
    parser.add_argument(
        "--row-group-target",
        type=int,
        default=250_000,
        help="Approximate rows per parquet row group flush.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint and skip completed date-table tasks.",
    )
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Overwrite existing stage parquet files for selected tasks.",
    )
    parser.add_argument(
        "--print-plan",
        action="store_true",
        help="Print the intended pipeline plan and exit.",
    )
    return parser.parse_args()


def canonical_date_key(value: str) -> str:
    digits = value.replace("-", "").strip()
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(f"Invalid date token: {value}")
    return digits


def canonical_trade_date(date_key: str) -> str:
    return f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"


def normalize_zip_member_name(name: str) -> str:
    return name.replace("\\", "/")


def raw_group_for_member(year: str, normalized_name: str) -> str | None:
    parts = normalized_name.split("/")
    if year == "2025":
        return parts[1] if len(parts) >= 3 else None
    return parts[0] if len(parts) >= 2 else None


def mapped_tables_by_group(year: str) -> dict[str, list[str]]:
    group_to_tables: dict[str, list[str]] = {}
    for table_name, contract in CONTRACTS.items():
        for group in contract.source_groups_by_year[year]:
            group_to_tables.setdefault(group, []).append(table_name)
    return group_to_tables


def read_csv_member_as_strings(member_stream: Any) -> pl.DataFrame:
    frame = pl.read_csv(
        member_stream,
        null_values=NULL_TOKENS,
        infer_schema=False,
        row_index_name="row_num_in_file",
        row_index_offset=1,
        low_memory=True,
        raise_if_empty=False,
    )
    if "row_num_in_file" not in frame.columns:
        return pl.DataFrame(schema={"row_num_in_file": pl.Int64})
    return frame.with_columns(pl.col("row_num_in_file").cast(pl.Int64))


def normalize_text_expr(column_name: str) -> pl.Expr:
    return (
        pl.col(column_name)
        .cast(pl.String)
        .str.strip_chars()
        .replace("", None)
        .replace("NULL", None)
        .replace("null", None)
        .replace("nan", None)
        .replace("NaN", None)
    )


def expression_for_column(
    column: StageColumn,
    available_columns: set[str],
    *,
    trade_date: str,
    table_name: str,
    source_file: str,
    ingest_dt: datetime,
) -> pl.Expr:
    name = column.name

    if name == "date":
        return pl.lit(date.fromisoformat(trade_date)).alias(name)
    if name == "table_name":
        return pl.lit(table_name).alias(name)
    if name == "source_file":
        return pl.lit(source_file).alias(name)
    if name == "ingest_ts":
        return pl.lit(ingest_dt).cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias(name)
    if name == "row_num_in_file":
        if name in available_columns:
            return pl.col(name).cast(pl.Int64).alias(name)
        return pl.lit(None).cast(pl.Int64).alias(name)
    if name == "SendTimeRaw":
        if "SendTime" in available_columns:
            return normalize_text_expr("SendTime").alias(name)
        return pl.lit(None).cast(pl.String).alias(name)

    if name not in available_columns:
        return pl.lit(None).cast(column.polars_dtype).alias(name)

    text = normalize_text_expr(name)

    if name == "Time":
        return (
            pl.when(text.is_null())
            .then(None)
            .when(text.str.contains(r"^\d{1,6}$"))
            .then(text.str.zfill(6))
            .otherwise(None)
            .alias(name)
        )
    if name == "SendTime":
        return (
            pl.from_epoch(text.cast(pl.Int64, strict=False), time_unit="ns")
            .dt.replace_time_zone("UTC")
            .cast(pl.Datetime(time_unit="ns", time_zone="UTC"))
            .alias(name)
        )
    if column.polars_dtype == pl.String:
        return text.alias(name)
    if column.polars_dtype == pl.Float64:
        return text.cast(pl.Float64, strict=False).alias(name)
    if column.polars_dtype == pl.Int64:
        return text.cast(pl.Int64, strict=False).alias(name)
    if column.polars_dtype == pl.Int32:
        return text.cast(pl.Int32, strict=False).alias(name)
    if column.polars_dtype == pl.Int16:
        return text.cast(pl.Int16, strict=False).alias(name)
    if column.polars_dtype == pl.Int8:
        return text.cast(pl.Int8, strict=False).alias(name)

    return text.cast(column.polars_dtype, strict=False).alias(name)


def standardize_member_frame(
    frame: pl.DataFrame,
    contract: StageTableContract,
    *,
    trade_date: str,
    source_file: str,
    ingest_dt: datetime,
) -> pl.DataFrame:
    available_columns = set(frame.columns)
    expressions = [
        expression_for_column(
            column,
            available_columns,
            trade_date=trade_date,
            table_name=contract.table_name,
            source_file=source_file,
            ingest_dt=ingest_dt,
        )
        for column in contract.all_columns
    ]
    return frame.select(expressions)


def invalid_required_mask(contract: StageTableContract) -> pl.Expr:
    return pl.any_horizontal([pl.col(column).is_null() for column in contract.required_columns])


def inspect_source_inventory(year: str, trade_date: str, zip_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    group_to_tables = mapped_tables_by_group(year)
    group_counts: Counter[str] = Counter()
    example_members: dict[str, str] = {}
    unmapped_members: list[dict[str, Any]] = []

    with zipfile.ZipFile(zip_path) as zf:
        for original_name in sorted(zf.namelist()):
            normalized_name = normalize_zip_member_name(original_name)
            if not normalized_name.lower().endswith(".csv"):
                continue
            raw_group = raw_group_for_member(year, normalized_name)
            group_key = raw_group or "<unparsed>"
            group_counts[group_key] += 1
            example_members.setdefault(group_key, normalized_name)
            mapped_tables = group_to_tables.get(group_key, [])
            if not mapped_tables:
                unmapped_members.append(
                    {
                        "year": year,
                        "date": trade_date,
                        "zip_path": str(zip_path),
                        "source_file": normalized_name,
                        "raw_group": group_key,
                        "skip_reason": "unmapped_source_group",
                    }
                )

    summary_rows = []
    for raw_group, member_count in sorted(group_counts.items()):
        mapped_tables = group_to_tables.get(raw_group, [])
        summary_rows.append(
            {
                "year": year,
                "date": trade_date,
                "zip_path": str(zip_path),
                "raw_group": raw_group,
                "csv_member_count": member_count,
                "mapped_tables": mapped_tables,
                "skip_reason": None if mapped_tables else "unmapped_source_group",
                "example_member": example_members[raw_group],
            }
        )
    return summary_rows, unmapped_members


def discover_source_members(
    zf: zipfile.ZipFile,
    *,
    year: str,
    contract: StageTableContract,
) -> list[tuple[str, str]]:
    groups = set(contract.source_groups_by_year[year])
    members: list[tuple[str, str]] = []
    for original_name in sorted(zf.namelist()):
        normalized_name = normalize_zip_member_name(original_name)
        if not normalized_name.lower().endswith(".csv"):
            continue
        group = raw_group_for_member(year, normalized_name)
        if group in groups:
            members.append((original_name, normalized_name))
    return members


def required_issue_counts(
    raw_frame: pl.DataFrame,
    standardized: pl.DataFrame,
    contract: StageTableContract,
) -> Counter[str]:
    counts: Counter[str] = Counter()
    for column_name in contract.required_columns:
        if column_name not in raw_frame.columns:
            counts[f"missing_required_column:{column_name}"] += raw_frame.height
            continue

        raw_series = raw_frame.select(normalize_text_expr(column_name).alias(column_name)).get_column(column_name)
        standardized_series = standardized.get_column(column_name)
        null_input_count = raw_series.is_null().sum()
        if null_input_count:
            counts[f"null_required_input:{column_name}"] += int(null_input_count)

        failed_cast_mask = raw_series.is_not_null() & standardized_series.is_null()
        failed_cast_count = failed_cast_mask.sum()
        if failed_cast_count:
            reason = (
                f"invalid_required_format:{column_name}"
                if column_name == "Time"
                else f"cast_failed_required:{column_name}"
            )
            counts[reason] += int(failed_cast_count)
    return counts


def flush_buffer(
    *,
    buffer_frames: list[pl.DataFrame],
    writer: pq.ParquetWriter | None,
    output_path: Path,
    schema: pa.Schema,
) -> pq.ParquetWriter:
    if not buffer_frames:
        if writer is not None:
            return writer
        empty_table = pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)
        pq.write_table(empty_table, output_path, compression="zstd")
        return pq.ParquetWriter(output_path, schema=schema, compression="zstd")

    batch = pl.concat(buffer_frames, rechunk=True)
    table = batch.to_arrow().cast(schema)
    if writer is None:
        writer = pq.ParquetWriter(output_path, schema=schema, compression="zstd")
    writer.write_table(table)
    buffer_frames.clear()
    return writer


def bundle_progress_snapshot(
    bundle: StageBundleTask,
    states: dict[str, TableBuildState],
    *,
    member_total: int,
    members_processed: int,
    current_source_file: str | None,
    current_raw_group: str | None,
    status: str,
) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for table_name, state in states.items():
        tmp_output_bytes = 0
        if state.tmp_output_path.exists():
            tmp_output_bytes = state.tmp_output_path.stat().st_size
        elif state.output_path.exists():
            tmp_output_bytes = state.output_path.stat().st_size

        tables[table_name] = {
            "raw_row_count": state.raw_row_count,
            "row_count": state.row_count,
            "rejected_row_count": state.rejected_rows,
            "failed_member_count": len(state.failed_members),
            "source_member_count": state.source_member_count,
            "tmp_output_bytes": tmp_output_bytes,
        }

    return {
        "bundle_key": bundle.bundle_key,
        "year": bundle.year,
        "date": bundle.trade_date,
        "zip_path": bundle.zip_path,
        "status": status,
        "updated_at": iso_utc_now(),
        "member_total": member_total,
        "members_processed": members_processed,
        "current_source_file": current_source_file,
        "current_raw_group": current_raw_group,
        "tables": tables,
    }


def write_bundle_progress(
    bundle: StageBundleTask,
    states: dict[str, TableBuildState],
    *,
    member_total: int,
    members_processed: int,
    current_source_file: str | None,
    current_raw_group: str | None,
    status: str,
) -> None:
    if not bundle.progress_path:
        return
    write_json(
        Path(bundle.progress_path),
        bundle_progress_snapshot(
            bundle,
            states,
            member_total=member_total,
            members_processed=members_processed,
            current_source_file=current_source_file,
            current_raw_group=current_raw_group,
            status=status,
        ),
    )


def read_active_bundle_progress(progress_dir: Path) -> list[dict[str, Any]]:
    if not progress_dir.exists():
        return []

    active_rows: list[dict[str, Any]] = []
    for progress_path in sorted(progress_dir.glob("*.json")):
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
        if payload.get("status") in {"running", "finalizing"}:
            active_rows.append(payload)
    return active_rows


def prepare_build_state(task: StageTask, *, started_at: str, ingest_dt: datetime) -> TableBuildState:
    contract = CONTRACTS[task.table_name]
    output_path = Path(task.output_path)
    tmp_output_path = output_path.with_suffix(".parquet.tmp")
    ensure_dir(output_path.parent)

    if task.overwrite_existing and output_path.exists():
        output_path.unlink()
    if tmp_output_path.exists():
        tmp_output_path.unlink()

    return TableBuildState(
        task=task,
        contract=contract,
        output_path=output_path,
        tmp_output_path=tmp_output_path,
        started_at=started_at,
        ingest_dt=ingest_dt,
    )


def record_failed_member(state: TableBuildState, *, source_file: str, error: Exception) -> None:
    state.failed_members.append({"source_file": source_file, "error": str(error)})


def process_member_frame_for_state(
    state: TableBuildState,
    *,
    raw_frame: pl.DataFrame,
    source_file: str,
) -> None:
    state.raw_row_count += raw_frame.height
    standardized = standardize_member_frame(
        raw_frame,
        state.contract,
        trade_date=state.task.trade_date,
        source_file=source_file,
        ingest_dt=state.ingest_dt,
    )
    state.rejection_reason_counts.update(required_issue_counts(raw_frame, standardized, state.contract))
    if "SendTimeRaw" in standardized.columns and "SendTime" in standardized.columns:
        state.send_time_parse_failure_count += standardized.filter(
            pl.col("SendTimeRaw").is_not_null() & pl.col("SendTime").is_null()
        ).height

    invalid_mask = invalid_required_mask(state.contract)
    invalid_rows = standardized.filter(invalid_mask)
    valid_rows = standardized.filter(~invalid_mask)

    state.rejected_rows += invalid_rows.height
    if invalid_rows.height:
        for column_name in state.contract.required_columns:
            state.required_null_counts[column_name] += invalid_rows.filter(
                pl.col(column_name).is_null()
            ).height

    if valid_rows.height == 0:
        return

    state.row_count += valid_rows.height
    state.buffer_frames.append(valid_rows)
    state.buffer_rows += valid_rows.height

    if "SendTime" in valid_rows.columns:
        send_series = valid_rows.get_column("SendTime").drop_nulls()
        if len(send_series):
            member_min = send_series.min()
            member_max = send_series.max()
            state.min_send_time = member_min if state.min_send_time is None else min(state.min_send_time, member_min)
            state.max_send_time = member_max if state.max_send_time is None else max(state.max_send_time, member_max)

    time_series = valid_rows.get_column("Time").drop_nulls()
    if len(time_series):
        member_min_time = time_series.min()
        member_max_time = time_series.max()
        state.min_time = member_min_time if state.min_time is None else min(state.min_time, member_min_time)
        state.max_time = member_max_time if state.max_time is None else max(state.max_time, member_max_time)

    if state.buffer_rows >= state.task.row_group_target:
        state.writer = flush_buffer(
            buffer_frames=state.buffer_frames,
            writer=state.writer,
            output_path=state.tmp_output_path,
            schema=state.contract.arrow_schema,
        )
        state.buffer_rows = 0


def cleanup_build_state(state: TableBuildState) -> None:
    if state.writer is not None:
        state.writer.close()
        state.writer = None
    if state.tmp_output_path.exists():
        state.tmp_output_path.unlink()


def finalize_build_state(state: TableBuildState) -> dict[str, Any]:
    if state.row_count == 0 and state.failed_members:
        cleanup_build_state(state)
        raise RuntimeError(
            f"All source members failed for {state.task.task_key}; first error: {state.failed_members[0]}"
        )

    try:
        state.writer = flush_buffer(
            buffer_frames=state.buffer_frames,
            writer=state.writer,
            output_path=state.tmp_output_path,
            schema=state.contract.arrow_schema,
        )
        if state.writer is not None:
            state.writer.close()
            state.writer = None
        state.tmp_output_path.replace(state.output_path)
    except Exception:
        cleanup_build_state(state)
        raise

    status = "completed_with_member_errors" if state.failed_members else "completed"

    return {
        "task_key": state.task.task_key,
        "status": status,
        "year": state.task.year,
        "date": state.task.trade_date,
        "table_name": state.task.table_name,
        "zip_path": state.task.zip_path,
        "output_file": str(state.output_path),
        "output_bytes": state.output_path.stat().st_size,
        "raw_row_count": state.raw_row_count,
        "row_count": state.row_count,
        "source_member_count": state.source_member_count,
        "failed_member_count": len(state.failed_members),
        "failed_member_examples": state.failed_members[:10],
        "rejected_row_count": state.rejected_rows,
        "required_null_counts": dict(state.required_null_counts),
        "rejection_reason_counts": dict(state.rejection_reason_counts),
        "send_time_parse_failure_count": state.send_time_parse_failure_count,
        "min_send_time": format_datetime_value(state.min_send_time),
        "max_send_time": format_datetime_value(state.max_send_time),
        "min_time": state.min_time,
        "max_time": state.max_time,
        "started_at": state.started_at,
        "finished_at": iso_utc_now(),
    }


def format_datetime_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def process_stage_bundle(bundle: StageBundleTask) -> dict[str, Any]:
    started_at = iso_utc_now()
    ingest_dt = datetime.now(timezone.utc).replace(microsecond=0)
    states = {
        task.table_name: prepare_build_state(task, started_at=started_at, ingest_dt=ingest_dt)
        for task in bundle.tasks
    }
    group_to_table_names: dict[str, list[str]] = {}
    seen_member_for_table = {task.table_name: False for task in bundle.tasks}
    last_progress_emit = 0.0

    for table_name, state in states.items():
        for group in state.contract.source_groups_by_year[bundle.year]:
            group_to_table_names.setdefault(group, []).append(table_name)

    try:
        with zipfile.ZipFile(bundle.zip_path) as zf:
            bundle_members: list[tuple[str, str, str, tuple[str, ...]]] = []
            for original_name in sorted(zf.namelist()):
                normalized_name = normalize_zip_member_name(original_name)
                if not normalized_name.lower().endswith(".csv"):
                    continue

                raw_group = raw_group_for_member(bundle.year, normalized_name)
                target_tables = tuple(group_to_table_names.get(raw_group or "", []))
                if not target_tables:
                    continue
                bundle_members.append((original_name, normalized_name, raw_group or "", target_tables))

            member_total = len(bundle_members)
            write_bundle_progress(
                bundle,
                states,
                member_total=member_total,
                members_processed=0,
                current_source_file=None,
                current_raw_group=None,
                status="running",
            )

            for member_index, (original_name, normalized_name, raw_group, target_tables) in enumerate(
                bundle_members, start=1
            ):
                for table_name in target_tables:
                    seen_member_for_table[table_name] = True

                try:
                    with zf.open(original_name) as member_stream:
                        raw_frame = read_csv_member_as_strings(member_stream)
                except Exception as exc:
                    for table_name in target_tables:
                        record_failed_member(states[table_name], source_file=normalized_name, error=exc)
                else:
                    for table_name in target_tables:
                        state = states[table_name]
                        try:
                            process_member_frame_for_state(
                                state,
                                raw_frame=raw_frame,
                                source_file=normalized_name,
                            )
                            state.source_member_count += 1
                        except Exception as exc:
                            record_failed_member(state, source_file=normalized_name, error=exc)

                now = time.monotonic()
                should_emit = (
                    member_index == member_total
                    or member_index == 1
                    or member_index % PROGRESS_EMIT_MEMBER_INTERVAL == 0
                    or (now - last_progress_emit) >= PROGRESS_EMIT_SECONDS
                )
                if should_emit:
                    write_bundle_progress(
                        bundle,
                        states,
                        member_total=member_total,
                        members_processed=member_index,
                        current_source_file=normalized_name,
                        current_raw_group=raw_group,
                        status="running",
                    )
                    last_progress_emit = now

        results: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []

        write_bundle_progress(
            bundle,
            states,
            member_total=member_total,
            members_processed=member_total,
            current_source_file=None,
            current_raw_group=None,
            status="finalizing",
        )

        for table_name, state in states.items():
            if not seen_member_for_table[table_name]:
                cleanup_build_state(state)
                failures.append(
                    {
                        "task_key": state.task.task_key,
                        "year": state.task.year,
                        "date": state.task.trade_date,
                        "table_name": state.task.table_name,
                        "zip_path": state.task.zip_path,
                        "error": f"No source members found for {state.task.task_key} in {state.task.zip_path}",
                    }
                )
                continue

            try:
                results.append(finalize_build_state(state))
            except Exception as exc:
                failures.append(
                    {
                        "task_key": state.task.task_key,
                        "year": state.task.year,
                        "date": state.task.trade_date,
                        "table_name": state.task.table_name,
                        "zip_path": state.task.zip_path,
                        "error": str(exc),
                    }
                )

        write_bundle_progress(
            bundle,
            states,
            member_total=member_total,
            members_processed=member_total,
            current_source_file=None,
            current_raw_group=None,
            status="completed" if not failures else "completed_with_failures",
        )

        return {
            "bundle_key": bundle.bundle_key,
            "results": results,
            "failures": failures,
        }
    except Exception:
        write_bundle_progress(
            bundle,
            states,
            member_total=0,
            members_processed=0,
            current_source_file=None,
            current_raw_group=None,
            status="failed",
        )
        raise


def process_stage_task(task: StageTask) -> dict[str, Any]:
    bundle_result = process_stage_bundle(
        StageBundleTask(
            year=task.year,
            trade_date=task.trade_date,
            zip_path=task.zip_path,
            tasks=(task,),
        )
    )
    if bundle_result["failures"]:
        raise RuntimeError(bundle_result["failures"][0]["error"])
    return bundle_result["results"][0]


def initial_state(args: argparse.Namespace, selected_dates: list[str], tasks: list[StageTask]) -> dict[str, Any]:
    return {
        "status": "running",
        "year": args.year,
        "selected_dates": selected_dates,
        "selected_table": args.table,
        "started_at": iso_utc_now(),
        "updated_at": iso_utc_now(),
        "workers": args.workers,
        "row_group_target": args.row_group_target,
        "completed_task_keys": [],
        "failed_tasks": {},
        "completed_count": 0,
        "failed_count": 0,
        "pending_count": len(tasks),
        "executor_mode": "process",
        "active_bundle_progress": [],
    }


def load_state(checkpoint_path: Path, args: argparse.Namespace, selected_dates: list[str]) -> dict[str, Any]:
    if not checkpoint_path.exists():
        return initial_state(args, selected_dates, [])

    state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if state.get("year") != args.year:
        raise ValueError(f"Checkpoint year mismatch: expected {args.year}, found {state.get('year')}")
    if state.get("selected_table") != args.table:
        raise ValueError(
            f"Checkpoint table mismatch: expected {args.table}, found {state.get('selected_table')}"
        )
    if state.get("selected_dates") != selected_dates:
        raise ValueError("Checkpoint selected dates mismatch; rerun without --resume or use same date set.")
    state["status"] = "running"
    state["updated_at"] = iso_utc_now()
    return state


def write_checkpoint(checkpoint_path: Path, heartbeat_path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = iso_utc_now()
    write_json(checkpoint_path, state)
    heartbeat = {
        "updated_at": state["updated_at"],
        "status": state["status"],
        "completed_count": state["completed_count"],
        "failed_count": state["failed_count"],
        "pending_count": state["pending_count"],
        "active_bundles": state.get("active_bundle_progress", []),
    }
    write_json(heartbeat_path, heartbeat)


def parse_selected_dates(args: argparse.Namespace, year_dir: Path) -> list[tuple[str, Path]]:
    zip_paths = sorted(year_dir.glob("*.zip"))
    if not zip_paths:
        return []

    by_date = {canonical_date_key(path.stem): path for path in zip_paths}

    if args.dates:
        selected_keys = [canonical_date_key(token) for token in args.dates.split(",") if token.strip()]
    else:
        selected_keys = sorted(by_date)
        if args.max_days:
            selected_keys = selected_keys[-args.max_days :] if args.latest_days else selected_keys[: args.max_days]

    return [(key, by_date[key]) for key in selected_keys if key in by_date]


def build_tasks(
    *,
    year: str,
    selected_date_paths: list[tuple[str, Path]],
    output_root: Path,
    selected_tables: list[str],
    row_group_target: int,
    overwrite_existing: bool,
    completed_task_keys: set[str],
) -> tuple[list[StageTask], list[str]]:
    tasks: list[StageTask] = []
    conflicts: list[str] = []

    for date_key, zip_path in selected_date_paths:
        trade_date = canonical_trade_date(date_key)
        for table_name in selected_tables:
            output_path = output_root / table_name / f"date={trade_date}" / f"{date_key}_{table_name}.parquet"
            task = StageTask(
                year=year,
                trade_date=trade_date,
                zip_path=str(zip_path),
                table_name=table_name,
                output_path=str(output_path),
                row_group_target=row_group_target,
                overwrite_existing=overwrite_existing,
            )

            if task.task_key in completed_task_keys and output_path.exists():
                continue

            if output_path.exists() and not overwrite_existing:
                conflicts.append(str(output_path))
                continue

            tasks.append(task)

    return tasks, conflicts


def build_task_bundles(tasks: list[StageTask], *, progress_dir: Path) -> list[StageBundleTask]:
    grouped: dict[tuple[str, str, str], list[StageTask]] = {}
    for task in tasks:
        key = (task.year, task.trade_date, task.zip_path)
        grouped.setdefault(key, []).append(task)

    bundles: list[StageBundleTask] = []
    for (year, trade_date, zip_path), grouped_tasks in sorted(grouped.items()):
        table_slug = "-".join(sorted(task.table_name for task in grouped_tasks))
        bundles.append(
            StageBundleTask(
                year=year,
                trade_date=trade_date,
                zip_path=zip_path,
                tasks=tuple(sorted(grouped_tasks, key=lambda item: item.table_name)),
                progress_path=str(progress_dir / f"{trade_date}_{table_slug}.json"),
            )
        )
    return bundles


def build_summary(state: dict[str, Any], manifest_dir: Path) -> dict[str, Any]:
    return {
        "pipeline": "stage_parquet",
        "status": state["status"],
        "year": state["year"],
        "selected_dates": state["selected_dates"],
        "selected_table": state["selected_table"],
        "generated_at": iso_utc_now(),
        "completed_count": state["completed_count"],
        "failed_count": state["failed_count"],
        "pending_count": state["pending_count"],
        "artifacts": {
            "checkpoint": str(manifest_dir / "checkpoint.json"),
            "heartbeat": str(manifest_dir / "heartbeat.json"),
            "partitions_manifest": str(manifest_dir / "partitions.jsonl"),
            "failures_manifest": str(manifest_dir / "failures.jsonl"),
            "source_group_inventory": str(manifest_dir / "source_groups.jsonl"),
            "unmapped_source_members": str(manifest_dir / "unmapped_source_members.jsonl"),
            "bundle_progress_dir": str(manifest_dir / "bundle_progress"),
            "summary": str(manifest_dir / "summary.json"),
        },
    }


def reset_manifest_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="build_stage_parquet",
            purpose="Build a transparent, resumable stage parquet layer for Trades and Orders.",
            responsibilities=[
                "Map 2025 and 2026 raw source tables into logical Trades and Orders outputs.",
                "Apply only minimal engineering standardization: types, nulls, time, partitioning, technical columns.",
                "Write checkpoint, heartbeat, partition manifest, and failure manifest per year.",
                "Parallelize date-table tasks while keeping each task explicit and resumable.",
            ],
            inputs=[
                "/Volumes/Data/港股Tick数据/{2025,2026}/*.zip",
                "STAGE_SCHEMA.md and CLEANING_SPEC.md rules.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/candidate_cleaned/{orders,trades}/date=YYYY-MM-DD/*.parquet",
                "/Volumes/Data/港股Tick数据/manifests/stage_parquet_<year>",
            ],
        )
        return 0

    year_dir = args.raw_root / str(args.year)
    manifest_dir = args.manifest_root / f"stage_parquet_{args.year}"
    log_path = args.log_root / f"stage_parquet_{args.year}.log"
    logger = configure_logger("stage_parquet", log_path)

    if not year_dir.exists():
        logger.error("Raw year directory does not exist: %s", year_dir)
        return 1

    selected_date_paths = parse_selected_dates(args, year_dir)
    if not selected_date_paths:
        logger.error("No zip archives matched the requested date selection.")
        return 1

    selected_dates = [canonical_trade_date(key) for key, _ in selected_date_paths]
    checkpoint_path = manifest_dir / "checkpoint.json"
    heartbeat_path = manifest_dir / "heartbeat.json"
    partitions_manifest_path = manifest_dir / "partitions.jsonl"
    failures_manifest_path = manifest_dir / "failures.jsonl"
    source_groups_path = manifest_dir / "source_groups.jsonl"
    unmapped_members_path = manifest_dir / "unmapped_source_members.jsonl"
    progress_dir = manifest_dir / "bundle_progress"
    summary_path = manifest_dir / "summary.json"

    ensure_dir(manifest_dir)
    ensure_dir(progress_dir)
    if not args.resume:
        reset_manifest_files(
            [
                checkpoint_path,
                heartbeat_path,
                partitions_manifest_path,
                failures_manifest_path,
                source_groups_path,
                unmapped_members_path,
                summary_path,
            ]
        )
        reset_manifest_files(list(progress_dir.glob("*.json")))

    if args.resume and partitions_manifest_path.exists() and not checkpoint_path.exists():
        logger.error(
            "Refusing to resume without checkpoint: manifest exists but checkpoint is missing."
        )
        return 1

    state = (
        load_state(checkpoint_path, args, selected_dates)
        if args.resume
        else initial_state(args, selected_dates, [])
    )
    completed_task_keys = set(state.get("completed_task_keys", []))
    selected_tables = ["orders", "trades"] if args.table == "all" else [args.table]

    if not args.resume or not source_groups_path.exists():
        for date_key, zip_path in selected_date_paths:
            trade_date = canonical_trade_date(date_key)
            group_rows, unmapped_rows = inspect_source_inventory(str(args.year), trade_date, zip_path)
            for row in group_rows:
                append_jsonl(source_groups_path, row)
            for row in unmapped_rows:
                append_jsonl(unmapped_members_path, row)

    tasks, conflicts = build_tasks(
        year=str(args.year),
        selected_date_paths=selected_date_paths,
        output_root=args.output_root,
        selected_tables=selected_tables,
        row_group_target=args.row_group_target,
        overwrite_existing=args.overwrite_existing,
        completed_task_keys=completed_task_keys,
    )

    if conflicts:
        logger.error(
            "Existing output files found. Use --resume or --overwrite-existing. First conflicts: %s",
            conflicts[:5],
        )
        return 1

    state["pending_count"] = len(tasks)
    state["active_bundle_progress"] = []
    write_checkpoint(checkpoint_path, heartbeat_path, state)

    if not tasks:
        state["status"] = "completed"
        write_json(summary_path, build_summary(state, manifest_dir))
        logger.info("All selected stage tasks are already complete.")
        return 0

    bundles = build_task_bundles(tasks, progress_dir=progress_dir)
    logger.info(
        "Prepared %s stage tasks across %s zip bundles using direct zip streaming.",
        len(tasks),
        len(bundles),
    )

    future_to_bundle: dict[Any, StageBundleTask] = {}
    try:
        executor: Any = ProcessPoolExecutor(max_workers=args.workers)
        state["executor_mode"] = "process"
    except (OSError, PermissionError) as exc:
        logger.warning(
            "ProcessPoolExecutor unavailable (%s); falling back to ThreadPoolExecutor.",
            exc,
        )
        executor = ThreadPoolExecutor(max_workers=args.workers)
        state["executor_mode"] = "thread"
        write_checkpoint(checkpoint_path, heartbeat_path, state)

    with executor:
        for bundle in bundles:
            future_to_bundle[executor.submit(process_stage_bundle, bundle)] = bundle

        pending_futures = set(future_to_bundle)
        while pending_futures:
            done_futures, pending_futures = wait(
                pending_futures,
                timeout=PROGRESS_EMIT_SECONDS,
                return_when=FIRST_COMPLETED,
            )
            state["active_bundle_progress"] = read_active_bundle_progress(progress_dir)
            write_checkpoint(checkpoint_path, heartbeat_path, state)

            for future in done_futures:
                bundle = future_to_bundle[future]
                try:
                    bundle_result = future.result()
                    for result in bundle_result["results"]:
                        completed_task_keys.add(result["task_key"])
                        append_jsonl(partitions_manifest_path, result)
                        logger.info(
                            "Stage task %s finished: status=%s raw_rows=%s rows=%s rejected=%s failed_members=%s",
                            result["task_key"],
                            result["status"],
                            result["raw_row_count"],
                            result["row_count"],
                            result["rejected_row_count"],
                            result["failed_member_count"],
                        )

                    for failure in bundle_result["failures"]:
                        state["failed_tasks"][failure["task_key"]] = failure["error"]
                        append_jsonl(failures_manifest_path, failure)
                        logger.error("Stage task %s failed: %s", failure["task_key"], failure["error"])
                except Exception as exc:
                    for task in bundle.tasks:
                        state["failed_tasks"][task.task_key] = str(exc)
                        append_jsonl(
                            failures_manifest_path,
                            {
                                "task_key": task.task_key,
                                "year": task.year,
                                "date": task.trade_date,
                                "table_name": task.table_name,
                                "zip_path": task.zip_path,
                                "error": str(exc),
                            },
                        )
                        logger.error("Stage task %s failed: %s", task.task_key, exc)
                finally:
                    state["completed_task_keys"] = sorted(completed_task_keys)
                    state["completed_count"] = len(completed_task_keys)
                    state["failed_count"] = len(state["failed_tasks"])
                    state["pending_count"] = max(0, len(tasks) - state["completed_count"] - state["failed_count"])
                    state["active_bundle_progress"] = read_active_bundle_progress(progress_dir)
                    write_checkpoint(checkpoint_path, heartbeat_path, state)

    state["status"] = "completed" if not state["failed_tasks"] else "completed_with_failures"
    state["active_bundle_progress"] = []
    write_checkpoint(checkpoint_path, heartbeat_path, state)
    write_json(summary_path, build_summary(state, manifest_dir))
    logger.info(
        "Stage parquet %s: completed=%s failed=%s manifest=%s",
        state["status"],
        state["completed_count"],
        state["failed_count"],
        manifest_dir,
    )
    return 0 if not state["failed_tasks"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
