from __future__ import annotations

import subprocess
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from Scripts.stage_contract import CONTRACTS


def write_stage_parquet(path: Path, table_name: str, columns: dict[str, list[object]]) -> None:
    schema = CONTRACTS[table_name].arrow_schema
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, path)


class DQALinkageTests(unittest.TestCase):
    def test_run_dqa_linkage_materializes_daily_feasibility(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"

            orders_dir = stage_root / "orders" / "date=2026-01-05"
            trades_dir = stage_root / "trades" / "date=2026-01-05"
            orders_dir.mkdir(parents=True)
            trades_dir.mkdir(parents=True)

            write_stage_parquet(
                orders_dir / "20260105_orders.parquet",
                "orders",
                {
                    "Channel": [30, 30, 30],
                    "SendTimeRaw": [
                        "1767576010000000000",
                        "1767576020000000000",
                        "1767576030000000000",
                    ],
                    "SendTime": [
                        datetime(2026, 1, 5, 1, 20, 10, tzinfo=timezone.utc),
                        datetime(2026, 1, 5, 1, 20, 20, tzinfo=timezone.utc),
                        datetime(2026, 1, 5, 1, 20, 30, tzinfo=timezone.utc),
                    ],
                    "SeqNum": [1, 2, 3],
                    "OrderId": [1001, 1002, 1003],
                    "OrderType": [1, 1, 1],
                    "Ext": ["110", "110", "110"],
                    "Time": ["092010", "092020", "092030"],
                    "Price": [54.0, 54.1, 54.2],
                    "Volume": [1000, 500, 700],
                    "Level": [0, 0, 0],
                    "BrokerNo": ["3436", "3436", "3436"],
                    "VolumePre": [0, 0, 0],
                    "date": [date(2026, 1, 5), date(2026, 1, 5), date(2026, 1, 5)],
                    "table_name": ["orders", "orders", "orders"],
                    "source_file": ["order/00001.csv", "order/00002.csv", "order/00003.csv"],
                    "ingest_ts": [
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                    ],
                    "row_num_in_file": [1, 1, 1],
                },
            )

            write_stage_parquet(
                trades_dir / "20260105_trades.parquet",
                "trades",
                {
                    "SendTimeRaw": [
                        "1767576015000000000",
                        "1767576040000000000",
                        "1767576050000000000",
                    ],
                    "SendTime": [
                        datetime(2026, 1, 5, 1, 20, 15, tzinfo=timezone.utc),
                        datetime(2026, 1, 5, 1, 20, 40, tzinfo=timezone.utc),
                        datetime(2026, 1, 5, 1, 20, 50, tzinfo=timezone.utc),
                    ],
                    "SeqNum": [10, 11, 12],
                    "TickID": [1, 2, 3],
                    "Time": ["092015", "092040", "092050"],
                    "Price": [54.0, 54.1, 54.2],
                    "Volume": [100, 200, 300],
                    "Dir": [0, 0, 0],
                    "Type": ["U", "U", "U"],
                    "BrokerNo": ["0", "0", "0"],
                    "BidOrderID": [1001, 1002, 0],
                    "BidVolume": [100, 200, 0],
                    "AskOrderID": [0, 1003, 9999],
                    "AskVolume": [0, 200, 300],
                    "date": [date(2026, 1, 5), date(2026, 1, 5), date(2026, 1, 5)],
                    "table_name": ["trades", "trades", "trades"],
                    "source_file": ["trade/00001.csv", "trade/00002.csv", "trade/00003.csv"],
                    "ingest_ts": [
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                    ],
                    "row_num_in_file": [1, 1, 1],
                },
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_dqa_linkage",
                    "--year",
                    "2026",
                    "--stage-root",
                    str(stage_root),
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

            linkage_dir = output_root / "linkage" / "year=2026"
            linkage = pl.read_parquet(linkage_dir / "audit_linkage_feasibility_daily.parquet")

            self.assertEqual(linkage.height, 1)
            self.assertEqual(linkage.get_column("bid_orderid_present_count").item(), 2)
            self.assertEqual(linkage.get_column("bid_orderid_matched_count").item(), 2)
            self.assertEqual(linkage.get_column("ask_orderid_present_count").item(), 2)
            self.assertEqual(linkage.get_column("ask_orderid_matched_count").item(), 1)
            self.assertEqual(linkage.get_column("both_sides_present_count").item(), 1)
            self.assertEqual(linkage.get_column("both_sides_matched_count").item(), 1)
            self.assertEqual(linkage.get_column("negative_time_lag_count").item(), 0)
            self.assertEqual(linkage.get_column("status").item(), "pass")


if __name__ == "__main__":
    unittest.main()
