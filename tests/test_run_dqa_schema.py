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


class DQASchemaTests(unittest.TestCase):
    def test_run_dqa_schema_materializes_profiles(self) -> None:
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
                    "Channel": [30, 30],
                    "SendTimeRaw": ["1767576015546000000", "1767576020000000000"],
                    "SendTime": [
                        datetime(2026, 1, 5, 1, 20, 15, 546000, tzinfo=timezone.utc),
                        datetime(2026, 1, 5, 1, 20, 20, tzinfo=timezone.utc),
                    ],
                    "SeqNum": [35143, 35144],
                    "OrderId": [3027201, 3027202],
                    "OrderType": [1, 1],
                    "Ext": ["110", "110"],
                    "Time": ["092015", "092020"],
                    "Price": [54.0, 54.1],
                    "Volume": [1000, 500],
                    "Level": [0, -1],
                    "BrokerNo": ["3436", "3436"],
                    "VolumePre": [0, 0],
                    "date": [date(2026, 1, 5), date(2026, 1, 5)],
                    "table_name": ["orders", "orders"],
                    "source_file": ["order/00001.csv", "order/00002.csv"],
                    "ingest_ts": [
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                        datetime(2026, 3, 14, 10, 0, tzinfo=timezone.utc),
                    ],
                    "row_num_in_file": [1, 1],
                },
            )

            write_stage_parquet(
                trades_dir / "20260105_trades.parquet",
                "trades",
                {
                    "SendTimeRaw": ["1767576015543000000", "1767576021000000000"],
                    "SendTime": [
                        datetime(2026, 1, 5, 1, 20, 15, 543000, tzinfo=timezone.utc),
                        datetime(2026, 1, 5, 1, 20, 21, tzinfo=timezone.utc),
                    ],
                    "SeqNum": [35079, 35080],
                    "TickID": [1, 2],
                    "Time": ["092015", "092021"],
                    "Price": [53.9, 54.0],
                    "Volume": [500, 600],
                    "Dir": [0, 0],
                    "Type": ["U", "U"],
                    "BrokerNo": ["0", "0"],
                    "BidOrderID": [0, 0],
                    "BidVolume": [0, 0],
                    "AskOrderID": [0, 0],
                    "AskVolume": [0, 0],
                    "date": [date(2026, 1, 5), date(2026, 1, 5)],
                    "table_name": ["trades", "trades"],
                    "source_file": ["trade/00001.csv", "trade/00002.csv"],
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
                    "Scripts.run_dqa_schema",
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

            schema_dir = output_root / "schema" / "year=2026"
            schema_fingerprint = pl.read_parquet(schema_dir / "audit_schema_fingerprint.parquet")
            field_rules = pl.read_parquet(schema_dir / "audit_field_value_rules.parquet")
            time_profile = pl.read_parquet(schema_dir / "audit_time_profile.parquet")

            self.assertEqual(schema_fingerprint.height, 2)
            self.assertEqual(set(schema_fingerprint.get_column("schema_status").to_list()), {"pass"})
            self.assertIn("level_ge_0", field_rules.get_column("rule_name").to_list())
            self.assertEqual(
                field_rules.filter(
                    (pl.col("table_name") == "orders") & (pl.col("rule_name") == "level_ge_0")
                ).get_column("status").item(),
                "fail",
            )
            self.assertEqual(time_profile.height, 2)
            self.assertTrue(
                time_profile.filter(pl.col("table_name") == "orders").get_column("hour_bucket_profile").item()
            )


if __name__ == "__main__":
    unittest.main()
