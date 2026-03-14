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


class SemanticTimeAnchorTests(unittest.TestCase):
    def test_time_anchor_probe_marks_weak_pass_when_time_is_consistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"

            orders_dir = stage_root / "orders" / "date=2025-12-04"
            trades_dir = stage_root / "trades" / "date=2025-12-04"
            orders_dir.mkdir(parents=True)
            trades_dir.mkdir(parents=True)

            write_stage_parquet(
                orders_dir / "20251204_orders.parquet",
                "orders",
                {
                    "Channel": [None, None],
                    "SendTimeRaw": [None, None],
                    "SendTime": [None, None],
                    "SeqNum": [1, 2],
                    "OrderId": [9001, 9002],
                    "OrderType": [1, 1],
                    "Ext": [None, None],
                    "Time": ["155958", "155959"],
                    "Price": [10.0, 10.1],
                    "Volume": [100, 200],
                    "Level": [0, 0],
                    "BrokerNo": ["1", "2"],
                    "VolumePre": [0, 0],
                    "date": [date(2025, 12, 4), date(2025, 12, 4)],
                    "table_name": ["orders", "orders"],
                    "source_file": ["OrderAdd/a.csv", "OrderAdd/b.csv"],
                    "ingest_ts": [
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                    ],
                    "row_num_in_file": [1, 1],
                },
            )

            write_stage_parquet(
                trades_dir / "20251204_trades.parquet",
                "trades",
                {
                    "SendTimeRaw": [None, None],
                    "SendTime": [None, None],
                    "SeqNum": [10, 11],
                    "TickID": [1, 2],
                    "Time": ["155958", "155959"],
                    "Price": [10.0, 10.1],
                    "Volume": [100, 200],
                    "Dir": [1, -1],
                    "Type": ["X", "X"],
                    "BrokerNo": ["1", "2"],
                    "BidOrderID": [9001, 0],
                    "BidVolume": [100, 0],
                    "AskOrderID": [0, 9002],
                    "AskVolume": [0, 200],
                    "date": [date(2025, 12, 4), date(2025, 12, 4)],
                    "table_name": ["trades", "trades"],
                    "source_file": ["TradeResumes/a.csv", "TradeResumes/b.csv"],
                    "ingest_ts": [
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                    ],
                    "row_num_in_file": [1, 1],
                },
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_time_anchor",
                    "--year",
                    "2025",
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

            result = pl.read_parquet(
                output_root / "semantic_time_anchor" / "year=2025" / "semantic_time_anchor_daily.parquet"
            )
            self.assertEqual(result.height, 1)
            row = result.to_dicts()[0]
            self.assertEqual(row["orders_time_nonnull_rate"], 1.0)
            self.assertEqual(row["trades_time_nonnull_rate"], 1.0)
            self.assertEqual(row["matched_both_time_nonnull_rate"], 1.0)
            self.assertEqual(row["matched_same_time_rate"], 1.0)
            self.assertEqual(row["matched_order_time_le_trade_time_rate"], 1.0)
            self.assertEqual(row["matched_negative_second_delta_count"], 0)
            self.assertEqual(row["coarse_time_anchor_status"], "weak_pass")
            self.assertEqual(row["research_time_grade"], "coarse_only")

    def test_time_anchor_probe_marks_unavailable_when_orders_time_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"

            orders_dir = stage_root / "orders" / "date=2025-07-04"
            trades_dir = stage_root / "trades" / "date=2025-07-04"
            orders_dir.mkdir(parents=True)
            trades_dir.mkdir(parents=True)

            write_stage_parquet(
                orders_dir / "20250704_orders.parquet",
                "orders",
                {
                    "Channel": [None],
                    "SendTimeRaw": [None],
                    "SendTime": [None],
                    "SeqNum": [1],
                    "OrderId": [7001],
                    "OrderType": [1],
                    "Ext": [None],
                    "Time": [None],
                    "Price": [10.0],
                    "Volume": [100],
                    "Level": [0],
                    "BrokerNo": ["1"],
                    "VolumePre": [0],
                    "date": [date(2025, 7, 4)],
                    "table_name": ["orders"],
                    "source_file": ["OrderAdd/a.csv"],
                    "ingest_ts": [datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc)],
                    "row_num_in_file": [1],
                },
            )

            write_stage_parquet(
                trades_dir / "20250704_trades.parquet",
                "trades",
                {
                    "SendTimeRaw": [None],
                    "SendTime": [None],
                    "SeqNum": [10],
                    "TickID": [1],
                    "Time": ["155958"],
                    "Price": [10.0],
                    "Volume": [100],
                    "Dir": [1],
                    "Type": ["X"],
                    "BrokerNo": ["1"],
                    "BidOrderID": [7001],
                    "BidVolume": [100],
                    "AskOrderID": [0],
                    "AskVolume": [0],
                    "date": [date(2025, 7, 4)],
                    "table_name": ["trades"],
                    "source_file": ["TradeResumes/a.csv"],
                    "ingest_ts": [datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc)],
                    "row_num_in_file": [1],
                },
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_time_anchor",
                    "--year",
                    "2025",
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

            result = pl.read_parquet(
                output_root / "semantic_time_anchor" / "year=2025" / "semantic_time_anchor_daily.parquet"
            )
            row = result.to_dicts()[0]
            self.assertEqual(row["orders_time_nonnull_rate"], 0.0)
            self.assertEqual(row["matched_both_time_nonnull_rate"], 0.0)
            self.assertEqual(row["coarse_time_anchor_status"], "unavailable")
            self.assertEqual(row["research_time_grade"], "blocked")


if __name__ == "__main__":
    unittest.main()
