from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import polars as pl

from Scripts.build_tushare_reference_registry import build_entries, summarize
from Scripts.runtime import write_json
from Scripts.run_tushare_daily_universe_reconciliation import stage_universe


class TushareReferenceRegistryTest(unittest.TestCase):
    def test_build_entries_verifies_manifest_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            partition = root / "hk_daily" / "trade_date=2026-05-22"
            partition.mkdir(parents=True)
            parquet_path = partition / "hk_daily.parquet"
            pl.DataFrame({"instrument_key": ["00001", "00002"]}).write_parquet(parquet_path)
            write_json(
                partition / "manifest.json",
                {
                    "endpoint": "hk_daily",
                    "trade_date": "2026-05-22",
                    "row_count": 2,
                    "output_file": str(parquet_path),
                    "source_role": "reference_landing",
                },
            )

            entries = build_entries(root, "2026")
            summary = summarize(entries)

            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["row_count_status"], "pass")
            self.assertEqual(summary["endpoint_counts"], {"hk_daily": 1})

    def test_stage_universe_extracts_symbol_from_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            partition = root / "trades" / "date=2026-05-22"
            partition.mkdir(parents=True)
            pl.DataFrame(
                {"source_file": ["trade/00001.csv", "trade/00002.csv", "trade/00001.csv"]}
            ).write_parquet(partition / "20260522_trades.parquet")

            self.assertEqual(stage_universe(root, "trades", "2026-05-22"), {"00001", "00002"})


if __name__ == "__main__":
    unittest.main()
