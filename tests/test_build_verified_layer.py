from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.from_dicts(rows).write_parquet(path)


class BuildVerifiedLayerTests(unittest.TestCase):
    def test_builds_admit_now_tables_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "verified"
            research_root = root / "research"
            log_root = root / "logs"

            trade_date = "2026-03-13"
            write_parquet(
                stage_root / "orders" / f"date={trade_date}" / "orders.parquet",
                [
                    {
                        "date": date.fromisoformat(trade_date),
                        "table_name": "orders",
                        "source_file": "a.csv",
                        "ingest_ts": datetime(2026, 3, 18, tzinfo=timezone.utc),
                        "row_num_in_file": 1,
                        "SeqNum": 100,
                        "OrderId": 1001,
                        "Time": "093000",
                        "Price": 12.3,
                        "Volume": 1000,
                        "OrderType": 2,
                        "BrokerNo": "1234",
                    }
                ],
            )
            write_parquet(
                stage_root / "trades" / f"date={trade_date}" / "trades.parquet",
                [
                    {
                        "date": date.fromisoformat(trade_date),
                        "table_name": "trades",
                        "source_file": "b.csv",
                        "ingest_ts": datetime(2026, 3, 18, tzinfo=timezone.utc),
                        "row_num_in_file": 1,
                        "TickID": 9001,
                        "Time": "093001",
                        "Price": 12.4,
                        "Volume": 300,
                        "Dir": 1,
                        "BidOrderID": 1001,
                    }
                ],
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_verified_layer",
                    "--year",
                    "2026",
                    "--dates",
                    trade_date,
                    "--stage-root",
                    str(stage_root),
                    "--output-root",
                    str(output_root),
                    "--research-root",
                    str(research_root),
                    "--log-root",
                    str(log_root),
                    "--workers",
                    "1",
                    "--executor",
                    "thread",
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            orders_out = output_root / "verified_orders" / "year=2026" / f"date={trade_date}" / "part-00000.parquet"
            trades_out = output_root / "verified_trades" / "year=2026" / f"date={trade_date}" / "part-00000.parquet"
            self.assertTrue(orders_out.exists())
            self.assertTrue(trades_out.exists())

            orders_df = pl.read_parquet(orders_out)
            trades_df = pl.read_parquet(trades_out)
            self.assertEqual(
                orders_df.columns,
                ["date", "table_name", "source_file", "ingest_ts", "row_num_in_file", "SeqNum", "OrderId", "Time", "Price", "Volume"],
            )
            self.assertEqual(
                trades_df.columns,
                ["date", "table_name", "source_file", "ingest_ts", "row_num_in_file", "TickID", "Time", "Price", "Volume"],
            )

            summary = json.loads((output_root / "manifests" / "year=2026" / "summary.json").read_text())
            self.assertEqual(summary["completed_count"], 2)
            self.assertEqual(summary["failed_count"], 0)
            self.assertIn("verified_orders", summary["tables"])
            self.assertIn("verified_trades", summary["tables"])

    def test_resume_skips_existing_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "verified"
            research_root = root / "research"
            log_root = root / "logs"

            trade_date = "2025-12-04"
            write_parquet(
                stage_root / "orders" / f"date={trade_date}" / "orders.parquet",
                [
                    {
                        "date": date.fromisoformat(trade_date),
                        "table_name": "orders",
                        "source_file": "a.csv",
                        "ingest_ts": datetime(2026, 3, 18, tzinfo=timezone.utc),
                        "row_num_in_file": 1,
                        "SeqNum": 100,
                        "OrderId": 1001,
                        "Time": "093000",
                        "Price": 12.3,
                        "Volume": 1000,
                    }
                ],
            )

            cmd = [
                "python3",
                "-m",
                "Scripts.build_verified_layer",
                "--year",
                "2025",
                "--table",
                "orders",
                "--dates",
                trade_date,
                "--stage-root",
                str(stage_root),
                "--output-root",
                str(output_root),
                "--research-root",
                str(research_root),
                "--log-root",
                str(log_root),
                "--workers",
                "1",
                "--executor",
                "thread",
            ]
            subprocess.run(cmd, cwd=str(REPO_ROOT), check=True)
            subprocess.run([*cmd, "--resume"], cwd=str(REPO_ROOT), check=True)

            parts_path = output_root / "manifests" / "year=2025" / "verified_partitions.jsonl"
            rows = [json.loads(line) for line in parts_path.read_text().splitlines() if line.strip()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["task_key"], f"{trade_date}:orders")


if __name__ == "__main__":
    unittest.main()
