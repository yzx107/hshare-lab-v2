from __future__ import annotations

import unittest
from pathlib import Path

from Scripts.build_raw_inventory import build_summary, infer_trade_date


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


if __name__ == "__main__":
    unittest.main()
