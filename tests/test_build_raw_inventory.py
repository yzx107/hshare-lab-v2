from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from Scripts.build_raw_inventory import build_summary, infer_trade_date, main


class BuildRawInventoryTests(unittest.TestCase):
    def test_infer_trade_date_supports_compact_date(self) -> None:
        self.assertEqual(infer_trade_date("trades/20250102/0005.csv"), "2025-01-02")

    def test_infer_trade_date_supports_hyphenated_date(self) -> None:
        self.assertEqual(infer_trade_date("orders/hk/2025-06-17/orders.csv"), "2025-06-17")

    def test_infer_trade_date_returns_none_when_missing(self) -> None:
        self.assertIsNone(infer_trade_date("orders/no_date/orders.csv"))

    def test_build_summary_exposes_expected_artifacts(self) -> None:
        state = {
            "status": "completed",
            "year": "2025",
            "raw_dir": "/tmp/raw/2025",
            "files_scanned": 3,
            "bytes_scanned": 42,
            "zero_byte_files": 1,
            "unknown_date_files": 1,
            "suffix_counts": {".csv": 3},
            "date_metrics": {
                "2025-01-02": {"file_count": 2, "total_bytes": 30},
                "2025-01-03": {"file_count": 1, "total_bytes": 12},
            },
        }

        summary = build_summary(state, Path("/tmp/raw_inventory_2025"))

        self.assertEqual(summary["pipeline"], "raw_inventory")
        self.assertEqual(summary["distinct_trade_dates"], 2)
        self.assertTrue(summary["artifacts"]["file_manifest_parquet"].endswith("files.parquet"))

    def test_resume_appends_new_files_even_when_they_sort_before_checkpoint_tail(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "raw"
            output_root = root / "manifests"
            log_root = root / "logs"
            raw_dir = raw_root / "2026"
            doc_dir = raw_dir / "doc"
            doc_dir.mkdir(parents=True)

            (raw_dir / "20260313.zip").write_bytes(b"zip-20260313")
            (doc_dir / "ReadMe.txt").write_text("doc", encoding="utf-8")

            initial_argv = [
                "build_raw_inventory.py",
                "--year",
                "2026",
                "--raw-root",
                str(raw_root),
                "--output-root",
                str(output_root),
                "--log-root",
                str(log_root),
            ]
            with patch("sys.argv", initial_argv):
                self.assertEqual(main(), 0)

            (raw_dir / "20260314.zip").write_bytes(b"zip-20260314")

            resume_argv = [*initial_argv, "--resume"]
            with patch("sys.argv", resume_argv):
                self.assertEqual(main(), 0)

            manifest_path = output_root / "raw_inventory_2026" / "files.jsonl"
            records = [
                json.loads(line)
                for line in manifest_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            relative_paths = {record["relative_path"] for record in records}
            self.assertEqual(len(records), 3)
            self.assertSetEqual(
                relative_paths,
                {"20260313.zip", "20260314.zip", "doc/ReadMe.txt"},
            )

            summary = json.loads(
                (output_root / "raw_inventory_2026" / "summary.json").read_text(encoding="utf-8")
            )
            self.assertEqual(summary["files_scanned"], 3)
            self.assertEqual(summary["distinct_trade_dates"], 2)
            self.assertEqual(summary["date_coverage_end"], "2026-03-14")


if __name__ == "__main__":
    unittest.main()
