from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    DEFAULT_MANIFEST_ROOT,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)

DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build DQA coverage tables from stage manifests."
    )
    parser.add_argument("--year", required=True, help="Year such as 2025 or 2026.")
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=DEFAULT_MANIFEST_ROOT,
        help="Root directory that contains stage_parquet_<year> manifests.",
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
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


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


def json_compact(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def status_for_reconciliation(
    *,
    row_diff: int,
    failed_member_count: int,
    unmapped_member_count: int,
    rejected_rows_total: int,
) -> str:
    if row_diff != 0 or failed_member_count > 0 or unmapped_member_count > 0:
        return "fail"
    if rejected_rows_total > 0:
        return "warn"
    return "pass"


def map_rejection_reason(reason: str) -> str:
    if reason.startswith("missing_required_column:") or reason.startswith("null_required_input:"):
        return "required_missing"
    if reason.startswith("cast_failed_required:") or reason.startswith("invalid_required_format:"):
        return "cast_failed"
    return "unknown"


def flatten_source_group_rows(source_group_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened_rows: list[dict[str, Any]] = []
    for row in source_group_rows:
        mapped_tables = row.get("mapped_tables") or []
        if mapped_tables:
            for table_name in mapped_tables:
                flattened_rows.append(
                    {
                        "year": row["year"],
                        "date": row["date"],
                        "table_name": table_name,
                        "source_group": row["raw_group"],
                        "member_count": row["csv_member_count"],
                        "raw_rows": None,
                        "mapped_status": "mapped",
                        "source_member_names": json_compact([row.get("example_member")]),
                        "skip_reason": row.get("skip_reason"),
                    }
                )
        else:
            flattened_rows.append(
                {
                    "year": row["year"],
                    "date": row["date"],
                    "table_name": None,
                    "source_group": row["raw_group"],
                    "member_count": row["csv_member_count"],
                    "raw_rows": None,
                    "mapped_status": "unmapped",
                    "source_member_names": json_compact([row.get("example_member")]),
                    "skip_reason": row.get("skip_reason"),
                }
            )
    return flattened_rows


def build_partition_rows(
    partitions_rows: list[dict[str, Any]],
    source_group_rows: list[dict[str, Any]],
    *,
    run_id: str,
) -> list[dict[str, Any]]:
    source_group_sets: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in source_group_rows:
        for table_name in row.get("mapped_tables") or []:
            source_group_sets[(row["date"], table_name)].append(row["raw_group"])

    output_rows: list[dict[str, Any]] = []
    for row in partitions_rows:
        output_rows.append(
            {
                "date": row["date"],
                "table_name": row["table_name"],
                "year": row["year"],
                "partition_path": row["output_file"],
                "output_file_count": 1,
                "output_bytes": row["output_bytes"],
                "row_count_stage": row["row_count"],
                "min_sendtime": row.get("min_send_time"),
                "max_sendtime": row.get("max_send_time"),
                "build_started_at": row.get("started_at"),
                "build_finished_at": row.get("finished_at"),
                "build_status": row["status"],
                "checkpoint_version": "stage_parquet_v1",
                "source_group_set": json_compact(sorted(set(source_group_sets[(row["date"], row["table_name"])]))),
                "run_id": run_id,
            }
        )
    return output_rows


def build_row_reconciliation_rows(
    partitions_rows: list[dict[str, Any]],
    failures_rows: list[dict[str, Any]],
    unmapped_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    aggregates: dict[tuple[str, str, str], dict[str, Any]] = {}
    date_level_unmapped: dict[str, int] = defaultdict(int)
    for row in unmapped_rows:
        date_level_unmapped[row["date"]] += 1

    for row in partitions_rows:
        key = (row["year"], row["date"], row["table_name"])
        current = aggregates.setdefault(
            key,
            {
                "year": row["year"],
                "date": row["date"],
                "table_name": row["table_name"],
                "raw_rows_total": 0,
                "stage_rows_total": 0,
                "rejected_rows_total": 0,
                "failed_member_count": 0,
            },
        )
        current["raw_rows_total"] += int(row.get("raw_row_count", 0))
        current["stage_rows_total"] += int(row.get("row_count", 0))
        current["rejected_rows_total"] += int(row.get("rejected_row_count", 0))
        current["failed_member_count"] += int(row.get("failed_member_count", 0))

    for row in failures_rows:
        key = (row["year"], row["date"], row["table_name"])
        current = aggregates.setdefault(
            key,
            {
                "year": row["year"],
                "date": row["date"],
                "table_name": row["table_name"],
                "raw_rows_total": 0,
                "stage_rows_total": 0,
                "rejected_rows_total": 0,
                "failed_member_count": 0,
            },
        )
        current["failed_member_count"] += 1

    output_rows: list[dict[str, Any]] = []
    for key in sorted(aggregates):
        row = aggregates[key]
        unmapped_member_count = int(date_level_unmapped.get(row["date"], 0))
        row_diff = row["raw_rows_total"] - row["stage_rows_total"] - row["rejected_rows_total"]
        output_rows.append(
            {
                **row,
                "unmapped_member_count": unmapped_member_count,
                "unmapped_scope": "date_level" if unmapped_member_count else None,
                "skipped_member_count": unmapped_member_count,
                "row_diff": row_diff,
                "reconciliation_status": status_for_reconciliation(
                    row_diff=row_diff,
                    failed_member_count=row["failed_member_count"],
                    unmapped_member_count=unmapped_member_count,
                    rejected_rows_total=row["rejected_rows_total"],
                ),
            }
        )
    return output_rows


def build_failure_rows(
    partitions_rows: list[dict[str, Any]],
    task_failures_rows: list[dict[str, Any]],
    unmapped_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for row in partitions_rows:
        for reason, count in sorted((row.get("rejection_reason_counts") or {}).items()):
            rows.append(
                {
                    "year": row["year"],
                    "date": row["date"],
                    "table_name": row["table_name"],
                    "source_file": None,
                    "source_member": None,
                    "failure_type": map_rejection_reason(reason),
                    "failure_count": int(count),
                    "sample_value": None,
                    "notes": reason,
                }
            )

        send_time_parse_failure_count = int(row.get("send_time_parse_failure_count", 0))
        if send_time_parse_failure_count:
            rows.append(
                {
                    "year": row["year"],
                    "date": row["date"],
                    "table_name": row["table_name"],
                    "source_file": None,
                    "source_member": None,
                    "failure_type": "sendtime_parse_failed",
                    "failure_count": send_time_parse_failure_count,
                    "sample_value": None,
                    "notes": "SendTimeRaw present but SendTime parsed to null.",
                }
            )

        for example in row.get("failed_member_examples") or []:
            rows.append(
                {
                    "year": row["year"],
                    "date": row["date"],
                    "table_name": row["table_name"],
                    "source_file": example.get("source_file"),
                    "source_member": example.get("source_file"),
                    "failure_type": "member_failed",
                    "failure_count": 1,
                    "sample_value": example.get("error"),
                    "notes": "Member-level failure captured in stage task output.",
                }
            )

    for row in task_failures_rows:
        rows.append(
            {
                "year": row["year"],
                "date": row["date"],
                "table_name": row["table_name"],
                "source_file": row.get("zip_path"),
                "source_member": None,
                "failure_type": "task_failed",
                "failure_count": 1,
                "sample_value": row.get("error"),
                "notes": "Top-level stage task failure.",
            }
        )

    for row in unmapped_rows:
        rows.append(
            {
                "year": row["year"],
                "date": row["date"],
                "table_name": None,
                "source_file": row.get("source_file"),
                "source_member": row.get("source_file"),
                "failure_type": "unmapped_source_member",
                "failure_count": 1,
                "sample_value": row.get("raw_group"),
                "notes": row.get("skip_reason"),
            }
        )

    return rows


def write_report_markdown(
    path: Path,
    *,
    year: str,
    reconciliation_rows: list[dict[str, Any]],
    failure_rows: list[dict[str, Any]],
) -> None:
    ensure_dir(path.parent)
    pass_count = sum(1 for row in reconciliation_rows if row["reconciliation_status"] == "pass")
    warn_count = sum(1 for row in reconciliation_rows if row["reconciliation_status"] == "warn")
    fail_count = sum(1 for row in reconciliation_rows if row["reconciliation_status"] == "fail")
    content = "\n".join(
        [
            f"# DQA Coverage {year}",
            "",
            f"- generated_at: {iso_utc_now()}",
            f"- partitions: {len(reconciliation_rows)}",
            f"- pass: {pass_count}",
            f"- warn: {warn_count}",
            f"- fail: {fail_count}",
            f"- failure_rows: {len(failure_rows)}",
            "",
            "First-pass coverage audit based on stage manifests.",
        ]
    )
    path.write_text(content + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_dqa_coverage",
            purpose="Measure ingestion completeness from raw to candidate_cleaned.",
            responsibilities=[
                "Convert stage manifests into partition, reconciliation, source-group, and failure audit tables.",
                "Flag row-count non-conservation, failed members, and unmapped source groups.",
                "Persist reproducible parquet outputs under the DQA layer and a short research-facing summary.",
            ],
            inputs=[
                "manifests/stage_parquet_<year>/partitions.jsonl",
                "manifests/stage_parquet_<year>/source_groups.jsonl",
                "manifests/stage_parquet_<year>/failures.jsonl",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/dqa/coverage/year=<year>/*.parquet",
                "Research/Audits/dqa_coverage_<year>.md",
            ],
        )
        return 0

    manifest_dir = args.manifest_root / f"stage_parquet_{args.year}"
    output_dir = args.output_root / "coverage" / f"year={args.year}"
    research_report_path = args.research_root / f"dqa_coverage_{args.year}.md"
    log_path = args.log_root / f"dqa_coverage_{args.year}.log"
    logger = configure_logger("dqa_coverage", log_path)

    if not manifest_dir.exists():
        logger.error("Stage manifest directory does not exist: %s", manifest_dir)
        return 1

    partitions_path = manifest_dir / "partitions.jsonl"
    source_groups_path = manifest_dir / "source_groups.jsonl"
    failures_path = manifest_dir / "failures.jsonl"
    unmapped_path = manifest_dir / "unmapped_source_members.jsonl"
    checkpoint_path = output_dir / "checkpoint.json"
    heartbeat_path = output_dir / "heartbeat.json"
    summary_path = output_dir / "summary.json"

    ensure_dir(output_dir)
    run_id = f"dqa_coverage_{args.year}_{iso_utc_now().replace(':', '').replace('-', '')}"
    write_json(
        checkpoint_path,
        {
            "status": "running",
            "year": args.year,
            "run_id": run_id,
            "started_at": iso_utc_now(),
        },
    )
    write_json(
        heartbeat_path,
        {
            "status": "running",
            "year": args.year,
            "run_id": run_id,
            "updated_at": iso_utc_now(),
            "active_step": "load_stage_manifests",
        },
    )

    partitions_rows = read_jsonl_rows(partitions_path)
    source_groups_rows = read_jsonl_rows(source_groups_path)
    task_failures_rows = read_jsonl_rows(failures_path)
    unmapped_rows = read_jsonl_rows(unmapped_path)

    flattened_source_groups = flatten_source_group_rows(source_groups_rows)
    partition_audit_rows = build_partition_rows(partitions_rows, source_groups_rows, run_id=run_id)
    reconciliation_rows = build_row_reconciliation_rows(partitions_rows, task_failures_rows, unmapped_rows)
    failure_rows = build_failure_rows(partitions_rows, task_failures_rows, unmapped_rows)

    write_parquet(partition_audit_rows, output_dir / "audit_stage_partitions.parquet")
    write_parquet(reconciliation_rows, output_dir / "audit_stage_row_reconciliation.parquet")
    write_parquet(flattened_source_groups, output_dir / "audit_stage_source_groups.parquet")
    write_parquet(failure_rows, output_dir / "audit_stage_failures.parquet")
    write_report_markdown(
        research_report_path,
        year=str(args.year),
        reconciliation_rows=reconciliation_rows,
        failure_rows=failure_rows,
    )

    summary = {
        "pipeline": "dqa_coverage",
        "status": "completed",
        "year": args.year,
        "run_id": run_id,
        "generated_at": iso_utc_now(),
        "counts": {
            "partition_rows": len(partition_audit_rows),
            "reconciliation_rows": len(reconciliation_rows),
            "source_group_rows": len(flattened_source_groups),
            "failure_rows": len(failure_rows),
        },
        "artifacts": {
            "checkpoint": str(checkpoint_path),
            "heartbeat": str(heartbeat_path),
            "audit_stage_partitions": str(output_dir / "audit_stage_partitions.parquet"),
            "audit_stage_row_reconciliation": str(output_dir / "audit_stage_row_reconciliation.parquet"),
            "audit_stage_source_groups": str(output_dir / "audit_stage_source_groups.parquet"),
            "audit_stage_failures": str(output_dir / "audit_stage_failures.parquet"),
            "research_report": str(research_report_path),
        },
    }
    write_json(summary_path, summary)
    write_json(
        checkpoint_path,
        {
            "status": "completed",
            "year": args.year,
            "run_id": run_id,
            "started_at": json.loads(checkpoint_path.read_text(encoding="utf-8"))["started_at"],
            "finished_at": iso_utc_now(),
        },
    )
    write_json(
        heartbeat_path,
        {
            "status": "completed",
            "year": args.year,
            "run_id": run_id,
            "updated_at": iso_utc_now(),
            "active_step": None,
        },
    )
    logger.info(
        "DQA coverage complete for %s: partitions=%s reconciliation=%s failures=%s output=%s",
        args.year,
        len(partition_audit_rows),
        len(reconciliation_rows),
        len(failure_rows),
        output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
