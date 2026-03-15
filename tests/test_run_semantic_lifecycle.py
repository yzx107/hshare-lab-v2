from __future__ import annotations

import subprocess
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from Scripts.semantic_contract import SEMANTIC_STATUS_VALUES

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, columns: dict[str, list[object]]) -> None:
    pq.write_table(pa.Table.from_pydict(columns), path)


class SemanticLifecycleRunnerTests(unittest.TestCase):
    def test_runner_outputs_expected_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"
            orders_dir = stage_root / "orders" / "date=2026-03-13"
            trades_dir = stage_root / "trades" / "date=2026-03-13"
            orders_dir.mkdir(parents=True)
            trades_dir.mkdir(parents=True)

            write_parquet(
                orders_dir / "orders.parquet",
                {
                    "OrderId": [1001, 1001, 1002],
                    "OrderType": [1, 2, 1],
                    "SeqNum": [1, 2, 3],
                    "Time": ["093000", "093001", "093002"],
                    "date": [date(2026, 3, 13)] * 3,
                    "table_name": ["orders"] * 3,
                    "source_file": ["a", "a", "b"],
                    "ingest_ts": [datetime(2026, 3, 15, tzinfo=timezone.utc)] * 3,
                    "row_num_in_file": [1, 2, 1],
                },
            )
            write_parquet(
                trades_dir / "trades.parquet",
                {
                    "BidOrderID": [1001, 0],
                    "AskOrderID": [0, 1002],
                    "TickID": [1, 2],
                    "Time": ["093001", "093002"],
                    "date": [date(2026, 3, 13)] * 2,
                    "table_name": ["trades"] * 2,
                    "source_file": ["x", "y"],
                    "ingest_ts": [datetime(2026, 3, 15, tzinfo=timezone.utc)] * 2,
                    "row_num_in_file": [1, 1],
                },
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_lifecycle",
                    "--year",
                    "2026",
                    "--input-root",
                    str(stage_root),
                    "--output-root",
                    str(output_root),
                    "--research-root",
                    str(research_root),
                    "--log-root",
                    str(log_root),
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            frame = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_orderid_lifecycle_daily.parquet")
            row = frame.to_dicts()[0]
            self.assertIn("distinct_orderids", frame.columns)
            self.assertIn("linked_orderids", frame.columns)
            self.assertIn("linked_orderid_rate", frame.columns)
            self.assertIn("lifecycle_status", frame.columns)
            self.assertIn(row["status"], SEMANTIC_STATUS_VALUES)
            self.assertEqual(row["distinct_orderids"], 2)
            self.assertEqual(row["linked_orderids"], 2)
            self.assertEqual(row["linked_orderid_rate"], 1.0)
            self.assertEqual(row["orders_with_multiple_events"], 1)
            self.assertEqual(row["orders_with_multiple_events_rate"], 0.5)
            self.assertEqual(row["orders_with_multiple_trades"], 0)
            self.assertEqual(row["orders_with_single_trade"], 2)
            self.assertEqual(row["orders_with_single_trade_rate"], 1.0)
            self.assertEqual(row["cross_session_candidate_count"], 0)
            self.assertEqual(row["first_order_seqnum_present_rate"], 1.0)
            self.assertEqual(row["last_order_seqnum_present_rate"], 1.0)
            self.assertEqual(row["first_trade_seqnum_present_rate"], 0.0)
            self.assertEqual(row["last_trade_seqnum_present_rate"], 0.0)


if __name__ == "__main__":
    unittest.main()
