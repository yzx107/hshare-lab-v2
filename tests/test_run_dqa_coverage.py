from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl


class DQACoverageTests(unittest.TestCase):
    def write_jsonl(self, path: Path, rows: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def test_run_dqa_coverage_materializes_core_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest_dir = root / "manifests" / "stage_parquet_2026"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"

            self.write_jsonl(
                manifest_dir / "partitions.jsonl",
                [
                    {
                        "year": "2026",
                        "date": "2026-01-05",
                        "table_name": "orders",
                        "output_file": "/tmp/orders.parquet",
                        "output_bytes": 100,
                        "raw_row_count": 10,
                        "row_count": 10,
                        "rejected_row_count": 0,
                        "failed_member_count": 0,
                        "failed_member_examples": [],
                        "rejection_reason_counts": {},
                        "send_time_parse_failure_count": 0,
                        "min_send_time": "2026-01-05T01:20:15+00:00",
                        "max_send_time": "2026-01-05T08:00:00+00:00",
                        "started_at": "2026-03-14T10:00:00+00:00",
                        "finished_at": "2026-03-14T10:10:00+00:00",
                        "status": "completed",
                    },
                    {
                        "year": "2026",
                        "date": "2026-01-05",
                        "table_name": "trades",
                        "output_file": "/tmp/trades.parquet",
                        "output_bytes": 50,
                        "raw_row_count": 5,
                        "row_count": 4,
                        "rejected_row_count": 1,
                        "failed_member_count": 0,
                        "failed_member_examples": [],
                        "rejection_reason_counts": {"invalid_required_format:Time": 1},
                        "send_time_parse_failure_count": 0,
                        "min_send_time": "2026-01-05T01:20:15+00:00",
                        "max_send_time": "2026-01-05T08:00:00+00:00",
                        "started_at": "2026-03-14T10:00:00+00:00",
                        "finished_at": "2026-03-14T10:10:00+00:00",
                        "status": "completed",
                    },
                ],
            )
            self.write_jsonl(
                manifest_dir / "source_groups.jsonl",
                [
                    {
                        "year": "2026",
                        "date": "2026-01-05",
                        "raw_group": "order",
                        "csv_member_count": 10,
                        "mapped_tables": ["orders"],
                        "skip_reason": None,
                        "example_member": "order/00001.csv",
                    },
                    {
                        "year": "2026",
                        "date": "2026-01-05",
                        "raw_group": "trade",
                        "csv_member_count": 8,
                        "mapped_tables": ["trades"],
                        "skip_reason": None,
                        "example_member": "trade/00001.csv",
                    },
                ],
            )
            self.write_jsonl(manifest_dir / "failures.jsonl", [])
            self.write_jsonl(manifest_dir / "unmapped_source_members.jsonl", [])

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_dqa_coverage",
                    "--year",
                    "2026",
                    "--manifest-root",
                    str(root / "manifests"),
                    "--output-root",
                    str(output_root),
                    "--research-root",
                    str(research_root),
                    "--log-root",
                    str(log_root),
                ],
                cwd="/Users/yxin/AI_Workstation/Hshare_Lab_v2",
                check=True,
            )

            coverage_dir = output_root / "coverage" / "year=2026"
            reconciliation = pl.read_parquet(coverage_dir / "audit_stage_row_reconciliation.parquet")
            failures = pl.read_parquet(coverage_dir / "audit_stage_failures.parquet")
            source_groups = pl.read_parquet(coverage_dir / "audit_stage_source_groups.parquet")

            self.assertEqual(reconciliation.height, 2)
            self.assertEqual(
                reconciliation.filter(pl.col("table_name") == "orders").get_column("reconciliation_status").item(),
                "pass",
            )
            self.assertEqual(
                reconciliation.filter(pl.col("table_name") == "trades").get_column("reconciliation_status").item(),
                "warn",
            )
            self.assertEqual(source_groups.height, 2)
            self.assertEqual(failures.get_column("failure_type").to_list(), ["cast_failed"])


if __name__ == "__main__":
    unittest.main()
