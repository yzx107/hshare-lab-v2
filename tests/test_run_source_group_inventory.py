from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

import polars as pl


def write_zip_member(zf: zipfile.ZipFile, name: str, content: str) -> None:
    zf.writestr(name, content)


class SourceGroupInventoryTests(unittest.TestCase):
    def test_run_source_group_inventory_materializes_member_and_daily_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "raw"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"
            year_dir = raw_root / "2025"
            year_dir.mkdir(parents=True)

            zip_path = year_dir / "20251204.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                write_zip_member(
                    zf,
                    "20251204/HKDarkPool/trade_a.csv",
                    "Time,Price,Volume,TickID,BidOrderID,AskOrderID\n092015,10.5,100,1,101,201\n092016,10.6,200,2,102,202\n",
                )
                write_zip_member(
                    zf,
                    "20251204/HKDarkPool/order_a.csv",
                    "SeqNum,OrderId,OrderType,Time,Price,Volume,Level,VolumePre\n1,101,1,092010,10.5,1000,0,0\n2,202,1,092011,10.6,500,0,0\n",
                )
                write_zip_member(
                    zf,
                    "20251204/TradeResumes/main_trade.csv",
                    "Time,Price,Volume,TickID\n092015,10.5,100,1\n",
                )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_source_group_inventory",
                    "--year",
                    "2025",
                    "--group",
                    "HKDarkPool",
                    "--raw-root",
                    str(raw_root),
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

            inventory_dir = output_root / "source_inventory" / "year=2025" / "group=HKDarkPool"
            member_inventory = pl.read_parquet(inventory_dir / "audit_source_member_inventory.parquet")
            daily_summary = pl.read_parquet(inventory_dir / "audit_source_daily_summary.parquet")
            schema_fingerprints = pl.read_parquet(inventory_dir / "audit_source_schema_fingerprints.parquet")
            summary = json.loads((inventory_dir / "summary.json").read_text(encoding="utf-8"))

            self.assertEqual(member_inventory.height, 2)
            self.assertEqual(set(member_inventory.get_column("table_hint").to_list()), {"orders", "trades"})
            self.assertEqual(daily_summary.height, 1)
            self.assertEqual(daily_summary.get_column("matched_member_count").item(), 2)
            self.assertEqual(daily_summary.get_column("matched_row_count").item(), 4)
            self.assertEqual(daily_summary.get_column("status").item(), "present")
            self.assertEqual(schema_fingerprints.height, 2)
            self.assertEqual(summary["matching_dates_count"], 1)
            self.assertEqual(summary["matched_member_count"], 2)
            self.assertEqual(summary["matched_row_count"], 4)


if __name__ == "__main__":
    unittest.main()
