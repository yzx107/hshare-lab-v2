from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)

DEFAULT_REFERENCE_ROOT = DEFAULT_DATA_ROOT / "reference" / "tushare"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a Tushare reference landing registry.")
    parser.add_argument("--year", required=True)
    parser.add_argument("--reference-root", type=Path, default=DEFAULT_REFERENCE_ROOT)
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def read_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def schema_fingerprint(parquet_path: Path) -> str | None:
    if not parquet_path.exists():
        return None
    schema = pl.scan_parquet(parquet_path).limit(0).collect().schema
    payload = json.dumps({name: str(dtype) for name, dtype in schema.items()}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def parquet_row_count(parquet_path: Path) -> int | None:
    if not parquet_path.exists():
        return None
    return int(pl.scan_parquet(parquet_path).select(pl.len()).collect().item())


def manifest_matches_year(manifest: dict[str, Any], year: str) -> bool:
    for key in ("trade_date", "as_of_date"):
        value = manifest.get(key)
        if isinstance(value, str) and value.startswith(year):
            return True
    start_date = manifest.get("start_date")
    end_date = manifest.get("end_date")
    if isinstance(start_date, str) and isinstance(end_date, str):
        return start_date[:4] <= year <= end_date[:4]
    return False


def endpoint_key(manifest: dict[str, Any]) -> str:
    value = manifest.get("endpoint")
    return value if isinstance(value, str) else "unknown"


def build_entries(reference_root: Path, year: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for manifest_path in sorted(reference_root.glob("**/manifest.json")):
        manifest = read_manifest(manifest_path)
        if not manifest_matches_year(manifest, year):
            continue
        parquet_path = Path(manifest["output_file"])
        rows = parquet_row_count(parquet_path)
        entries.append(
            {
                "endpoint": endpoint_key(manifest),
                "manifest": str(manifest_path),
                "parquet": str(parquet_path),
                "manifest_row_count": manifest.get("row_count"),
                "parquet_row_count": rows,
                "row_count_status": "pass" if rows == manifest.get("row_count") else "fail",
                "output_bytes": parquet_path.stat().st_size if parquet_path.exists() else 0,
                "schema_fingerprint": schema_fingerprint(parquet_path),
                "trade_date": manifest.get("trade_date"),
                "as_of_date": manifest.get("as_of_date"),
                "start_date": manifest.get("start_date"),
                "end_date": manifest.get("end_date"),
                "source_role": manifest.get("source_role"),
                "generated_at": manifest.get("generated_at"),
            }
        )
    return entries


def summarize(entries: list[dict[str, Any]]) -> dict[str, Any]:
    endpoint_counts: dict[str, int] = {}
    endpoint_rows: dict[str, int] = {}
    failures: list[str] = []
    for entry in entries:
        endpoint = entry["endpoint"]
        endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
        endpoint_rows[endpoint] = endpoint_rows.get(endpoint, 0) + int(
            entry.get("parquet_row_count") or 0
        )
        if entry["row_count_status"] != "pass":
            failures.append(entry["manifest"])
    return {
        "endpoint_counts": endpoint_counts,
        "endpoint_rows": endpoint_rows,
        "row_count_failures": failures,
        "status": "pass" if not failures else "fail",
    }


def print_plan() -> None:
    print_scaffold_plan(
        name="build_tushare_reference_registry",
        purpose="Summarize landed Tushare reference partitions into a year registry.",
        responsibilities=[
            "Scan Tushare manifest files under the external reference root.",
            "Verify manifest row counts against parquet row counts.",
            "Record schema fingerprints, byte sizes, endpoint counts, and date coverage.",
        ],
        inputs=["/Volumes/Data/港股Tick数据/reference/tushare/**/manifest.json"],
        outputs=["/Volumes/Data/港股Tick数据/reference/tushare/_registry/year=<year>.json"],
    )


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_plan()
        return 0
    entries = build_entries(args.reference_root, args.year)
    payload = {
        "pipeline": "build_tushare_reference_registry",
        "generated_at": iso_utc_now(),
        "year": args.year,
        "reference_root": str(args.reference_root),
        "entry_count": len(entries),
        "summary": summarize(entries),
        "entries": entries,
    }
    output_path = args.reference_root / "_registry" / f"year={args.year}.json"
    ensure_dir(output_path.parent)
    write_json(output_path, payload)
    print(json.dumps({k: payload[k] for k in ("year", "entry_count", "summary")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
