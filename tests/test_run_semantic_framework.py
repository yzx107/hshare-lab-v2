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

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_stage_parquet(path: Path, table_name: str, columns: dict[str, list[object]]) -> None:
    schema = CONTRACTS[table_name].arrow_schema
    table = pa.Table.from_pydict(columns, schema=schema)
    pq.write_table(table, path)


class SemanticFrameworkTests(unittest.TestCase):
    def test_run_semantic_framework_dispatches_structured_outputs(self) -> None:
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

            write_stage_parquet(
                orders_dir / "20260313_orders.parquet",
                "orders",
                {
                    "Channel": [1, 1, 1],
                    "SendTimeRaw": ["093000000", "093001000", "093002000"],
                    "SendTime": [
                        datetime(2026, 3, 13, 9, 30, tzinfo=timezone.utc),
                        datetime(2026, 3, 13, 9, 30, 1, tzinfo=timezone.utc),
                        datetime(2026, 3, 13, 9, 30, 2, tzinfo=timezone.utc),
                    ],
                    "SeqNum": [1, 2, 3],
                    "OrderId": [1001, 1001, 1002],
                    "OrderType": [1, 2, 1],
                    "Ext": [None, None, None],
                    "Time": ["093000", "093001", "093002"],
                    "Price": [10.0, 10.0, 10.1],
                    "Volume": [100, 50, 200],
                    "Level": [0, 0, 0],
                    "BrokerNo": ["1", "1", "2"],
                    "VolumePre": [0, 0, 0],
                    "date": [date(2026, 3, 13), date(2026, 3, 13), date(2026, 3, 13)],
                    "table_name": ["orders", "orders", "orders"],
                    "source_file": ["order/a.csv", "order/a.csv", "order/b.csv"],
                    "ingest_ts": [
                        datetime(2026, 3, 15, 1, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 15, 1, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 15, 1, 0, tzinfo=timezone.utc),
                    ],
                    "row_num_in_file": [1, 2, 1],
                },
            )

            write_stage_parquet(
                trades_dir / "20260313_trades.parquet",
                "trades",
                {
                    "SendTimeRaw": ["093001500", "093002500"],
                    "SendTime": [
                        datetime(2026, 3, 13, 9, 30, 1, 500000, tzinfo=timezone.utc),
                        datetime(2026, 3, 13, 9, 30, 2, 500000, tzinfo=timezone.utc),
                    ],
                    "SeqNum": [10, 11],
                    "TickID": [5001, 5002],
                    "Time": ["093001", "093002"],
                    "Price": [10.0, 10.1],
                    "Volume": [50, 200],
                    "Dir": [1, -1],
                    "Type": ["T", "T"],
                    "BrokerNo": ["1", "2"],
                    "BidOrderID": [1001, 0],
                    "BidVolume": [50, 0],
                    "AskOrderID": [0, 1002],
                    "AskVolume": [0, 200],
                    "date": [date(2026, 3, 13), date(2026, 3, 13)],
                    "table_name": ["trades", "trades"],
                    "source_file": ["trade/a.csv", "trade/b.csv"],
                    "ingest_ts": [
                        datetime(2026, 3, 15, 1, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 15, 1, 0, tzinfo=timezone.utc),
                    ],
                    "row_num_in_file": [1, 1],
                },
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_framework",
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
                cwd=str(REPO_ROOT),
                check=True,
            )

            lifecycle = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_orderid_lifecycle_daily.parquet")
            self.assertEqual(lifecycle.height, 1)
            lifecycle_row = lifecycle.to_dicts()[0]
            self.assertEqual(lifecycle_row["distinct_orderids"], 2)
            self.assertEqual(lifecycle_row["orders_with_multiple_events"], 1)
            self.assertEqual(lifecycle_row["lifecycle_status"], "weak_pass")

            tradedir = pl.read_parquet(
                output_root / "semantic" / "year=2026" / "semantic_tradedir_daily.parquet"
            )
            self.assertEqual(tradedir.height, 1)
            self.assertEqual(tradedir.to_dicts()[0]["distinct_tradedir_values"], 2)

            ordertype = pl.read_parquet(
                output_root / "semantic" / "year=2026" / "semantic_ordertype_daily.parquet"
            )
            self.assertEqual(ordertype.height, 1)
            self.assertEqual(ordertype.to_dicts()[0]["multi_ordertype_orderid_count"], 1)

            session = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_session_daily.parquet")
            self.assertEqual(session.height, 1)
            self.assertEqual(session.to_dicts()[0]["status"], "not_run")

            summary = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_yearly_summary.parquet")
            self.assertEqual(summary.height, 4)

            bridge = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_admissibility_bridge.parquet")
            self.assertGreaterEqual(bridge.height, 4)
            self.assertIn("research_module", bridge.columns)


if __name__ == "__main__":
    unittest.main()
