from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    DEFAULT_MANIFEST_ROOT,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    write_json,
)

DATE_PATTERNS = (
    re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(20\d{2})-(\d{2})-(\d{2})(?!\d)"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a resumable raw-layer inventory manifest for one year."
    )
    parser.add_argument("--year", required=True, help="Raw year directory such as 2025 or 2026.")
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="Root directory that contains year-level raw folders.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_MANIFEST_ROOT,
        help="Root directory for manifest outputs.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for pipeline logs.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=1000,
        help="Write checkpoint and heartbeat after this many files.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint and append to the existing manifest stream.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Optional file limit for smoke tests; 0 means full scan.",
    )
    return parser.parse_args()


def infer_trade_date(relative_path: str) -> str | None:
    normalized = relative_path.replace("\\", "/")
    for pattern in DATE_PATTERNS:
        match = pattern.search(normalized)
        if match is None:
            continue
        try:
            parsed = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            continue
        return parsed.isoformat()
    return None


def iter_files(root: Path) -> Any:
    for current_root, dirnames, filenames in os.walk(root):
        dirnames.sort()
        filenames.sort()
        for filename in filenames:
            yield Path(current_root) / filename


def load_manifest_relative_paths(files_jsonl_path: Path) -> set[str]:
    relative_paths: set[str] = set()
    if not files_jsonl_path.exists():
        return relative_paths

    with files_jsonl_path.open("r", encoding="utf-8") as manifest_handle:
        for line_number, line in enumerate(manifest_handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue

            record = json.loads(stripped)
            relative_path = record.get("relative_path")
            if not relative_path:
                raise ValueError(
                    f"Manifest record at line {line_number} is missing relative_path: {files_jsonl_path}"
                )
            if relative_path in relative_paths:
                raise ValueError(
                    f"Manifest contains duplicate relative_path {relative_path!r}: {files_jsonl_path}"
                )
            relative_paths.add(relative_path)

    return relative_paths


def validate_resume_state(
    *,
    state: dict[str, Any],
    seen_relative_paths: set[str],
    files_jsonl_path: Path,
) -> None:
    expected_files = int(state.get("files_scanned", 0))
    if expected_files != len(seen_relative_paths):
        raise ValueError(
            "Checkpoint / manifest mismatch: "
            f"checkpoint files_scanned={expected_files}, "
            f"manifest unique records={len(seen_relative_paths)} at {files_jsonl_path}"
        )

    last_relative_path = state.get("last_relative_path")
    if last_relative_path and last_relative_path not in seen_relative_paths:
        raise ValueError(
            "Checkpoint / manifest mismatch: "
            f"last_relative_path {last_relative_path!r} is missing from {files_jsonl_path}"
        )


def build_record(path: Path, raw_dir: Path) -> dict[str, Any]:
    stat = path.stat()
    relative_path = path.relative_to(raw_dir).as_posix()
    suffix = "".join(path.suffixes).lower()
    return {
        "relative_path": relative_path,
        "size_bytes": stat.st_size,
        "modified_at": stat.st_mtime,
        "suffix": suffix or "<none>",
        "trade_date": infer_trade_date(relative_path),
    }


def initial_state(year: str, raw_dir: Path) -> dict[str, Any]:
    return {
        "status": "running",
        "year": year,
        "raw_dir": str(raw_dir),
        "started_at": iso_utc_now(),
        "updated_at": iso_utc_now(),
        "last_relative_path": None,
        "files_scanned": 0,
        "bytes_scanned": 0,
        "unknown_date_files": 0,
        "zero_byte_files": 0,
        "suffix_counts": {},
        "date_metrics": {},
    }


def load_state(checkpoint_path: Path, year: str, raw_dir: Path) -> dict[str, Any]:
    if not checkpoint_path.exists():
        return initial_state(year, raw_dir)

    state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    if state.get("year") != year:
        raise ValueError(f"Checkpoint year mismatch: expected {year}, found {state.get('year')}")
    if state.get("raw_dir") != str(raw_dir):
        raise ValueError(
            "Checkpoint raw_dir mismatch: "
            f"expected {raw_dir}, found {state.get('raw_dir')}"
        )
    state["status"] = "running"
    state["updated_at"] = iso_utc_now()
    return state


def write_checkpoint(
    checkpoint_path: Path,
    heartbeat_path: Path,
    state: dict[str, Any],
) -> None:
    state["updated_at"] = iso_utc_now()
    write_json(checkpoint_path, state)
    heartbeat_payload = {
        "updated_at": state["updated_at"],
        "status": state["status"],
        "files_scanned": state["files_scanned"],
        "bytes_scanned": state["bytes_scanned"],
        "last_relative_path": state["last_relative_path"],
    }
    write_json(heartbeat_path, heartbeat_payload)


def build_date_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade_date in sorted(state["date_metrics"]):
        metrics = state["date_metrics"][trade_date]
        rows.append(
            {
                "trade_date": trade_date,
                "file_count": metrics["file_count"],
                "total_bytes": metrics["total_bytes"],
            }
        )
    return rows


def build_summary(state: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    date_rows = build_date_rows(state)
    return {
        "pipeline": "raw_inventory",
        "status": state["status"],
        "year": state["year"],
        "raw_dir": state["raw_dir"],
        "output_dir": str(output_dir),
        "generated_at": iso_utc_now(),
        "files_scanned": state["files_scanned"],
        "bytes_scanned": state["bytes_scanned"],
        "zero_byte_files": state["zero_byte_files"],
        "unknown_date_files": state["unknown_date_files"],
        "date_coverage_start": date_rows[0]["trade_date"] if date_rows else None,
        "date_coverage_end": date_rows[-1]["trade_date"] if date_rows else None,
        "distinct_trade_dates": len(date_rows),
        "suffix_counts": state["suffix_counts"],
        "artifacts": {
            "checkpoint": str(output_dir / "checkpoint.json"),
            "heartbeat": str(output_dir / "heartbeat.json"),
            "date_summary": str(output_dir / "date_summary.json"),
            "file_manifest_jsonl": str(output_dir / "files.jsonl"),
            "file_manifest_parquet": str(output_dir / "files.parquet"),
            "date_summary_parquet": str(output_dir / "date_summary.parquet"),
        },
    }


def try_materialize_parquet(
    *,
    files_jsonl_path: Path,
    date_rows: list[dict[str, Any]],
    files_parquet_path: Path,
    date_summary_parquet_path: Path,
    logger_name: str,
) -> None:
    logger = configure_logger(logger_name)
    try:
        import pyarrow as pa
        import pyarrow.json as pajson
        import pyarrow.parquet as pq
    except ImportError:
        logger.info("PyArrow is not installed yet; skipped parquet materialization.")
        return

    if not files_jsonl_path.exists():
        logger.info("No JSONL manifest exists yet; skipped parquet materialization.")
        return

    try:
        files_table = pajson.read_json(str(files_jsonl_path))
        pq.write_table(files_table, files_parquet_path, compression="zstd")
        date_table = pa.Table.from_pylist(date_rows)
        pq.write_table(date_table, date_summary_parquet_path, compression="zstd")
        logger.info("Materialized parquet manifests at %s", files_parquet_path.parent)
    except Exception as exc:  # pragma: no cover - best-effort export
        logger.warning("Failed to materialize parquet manifests: %s", exc)


def main() -> int:
    args = parse_args()
    year = str(args.year)
    raw_dir = args.raw_root / year
    output_dir = args.output_root / f"raw_inventory_{year}"
    log_path = args.log_root / f"raw_inventory_{year}.log"
    logger = configure_logger("raw_inventory", log_path)

    if not raw_dir.exists():
        logger.error("Raw directory does not exist: %s", raw_dir)
        return 1

    ensure_dir(output_dir)
    files_jsonl_path = output_dir / "files.jsonl"
    checkpoint_path = output_dir / "checkpoint.json"
    heartbeat_path = output_dir / "heartbeat.json"
    date_summary_path = output_dir / "date_summary.json"
    summary_path = output_dir / "summary.json"
    files_parquet_path = output_dir / "files.parquet"
    date_summary_parquet_path = output_dir / "date_summary.parquet"

    if args.resume and files_jsonl_path.exists() and not checkpoint_path.exists():
        logger.error(
            "Refusing to resume without checkpoint: manifest stream exists but checkpoint is missing."
        )
        return 1

    try:
        state = (
            load_state(checkpoint_path, year, raw_dir)
            if args.resume
            else initial_state(year, raw_dir)
        )
        seen_relative_paths = (
            load_manifest_relative_paths(files_jsonl_path) if args.resume else set()
        )
        if args.resume:
            validate_resume_state(
                state=state,
                seen_relative_paths=seen_relative_paths,
                files_jsonl_path=files_jsonl_path,
            )
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    suffix_counts = Counter(state["suffix_counts"])
    date_metrics = defaultdict(lambda: {"file_count": 0, "total_bytes": 0})
    for trade_date, metrics in state["date_metrics"].items():
        date_metrics[trade_date] = {
            "file_count": metrics["file_count"],
            "total_bytes": metrics["total_bytes"],
        }

    mode = "a" if args.resume else "w"
    processed_since_checkpoint = 0
    truncated = False

    with files_jsonl_path.open(mode, encoding="utf-8") as manifest_handle:
        for path in iter_files(raw_dir):
            if args.max_files and state["files_scanned"] >= args.max_files:
                logger.info("Reached max-files=%s; stopping early.", args.max_files)
                truncated = True
                break

            record = build_record(path, raw_dir)
            relative_path = record["relative_path"]

            if relative_path in seen_relative_paths:
                continue

            manifest_handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            seen_relative_paths.add(relative_path)
            state["files_scanned"] += 1
            state["bytes_scanned"] += record["size_bytes"]
            state["last_relative_path"] = relative_path
            processed_since_checkpoint += 1

            suffix_counts[record["suffix"]] += 1
            if record["size_bytes"] == 0:
                state["zero_byte_files"] += 1
            if record["trade_date"] is None:
                state["unknown_date_files"] += 1
            else:
                date_metrics[record["trade_date"]]["file_count"] += 1
                date_metrics[record["trade_date"]]["total_bytes"] += record["size_bytes"]

            if args.max_files and state["files_scanned"] >= args.max_files:
                logger.info("Reached max-files=%s; stopping early.", args.max_files)
                truncated = True
                break

            if processed_since_checkpoint >= args.checkpoint_every:
                manifest_handle.flush()
                state["suffix_counts"] = dict(sorted(suffix_counts.items()))
                state["date_metrics"] = dict(sorted(date_metrics.items()))
                write_checkpoint(checkpoint_path, heartbeat_path, state)
                logger.info(
                    "Progress checkpoint: files=%s bytes=%s last=%s",
                    state["files_scanned"],
                    state["bytes_scanned"],
                    state["last_relative_path"],
                )
                processed_since_checkpoint = 0

    state["suffix_counts"] = dict(sorted(suffix_counts.items()))
    state["date_metrics"] = dict(sorted(date_metrics.items()))
    state["status"] = "truncated" if truncated else "completed"
    write_checkpoint(checkpoint_path, heartbeat_path, state)

    date_rows = build_date_rows(state)
    write_json(date_summary_path, date_rows)
    write_json(summary_path, build_summary(state, output_dir))
    try_materialize_parquet(
        files_jsonl_path=files_jsonl_path,
        date_rows=date_rows,
        files_parquet_path=files_parquet_path,
        date_summary_parquet_path=date_summary_parquet_path,
        logger_name="raw_inventory_parquet",
    )

    logger.info(
        "Raw inventory %s: files=%s bytes=%s output=%s",
        state["status"],
        state["files_scanned"],
        state["bytes_scanned"],
        output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
