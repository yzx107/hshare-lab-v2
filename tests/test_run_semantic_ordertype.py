from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from Scripts.semantic_contract import SEMANTIC_STATUS_VALUES


def write_parquet(path: Path, columns: dict[str, list[object]]) -> None:
    pq.write_table(pa.Table.from_pydict(columns), path)


class SemanticOrderTypeRunnerTests(unittest.TestCase):
    def test_runner_profiles_ordertype_trajectories(self) -> None:
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
                },
            )
            write_parquet(trades_dir / "trades.parquet", {"TickID": [1], "Time": ["093001"]})

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_ordertype",
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
                cwd="/private/tmp/hshare_semantic_2026_runner",
                check=True,
            )
            frame = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_ordertype_daily.parquet")
            row = frame.to_dicts()[0]
            self.assertEqual(row["multi_ordertype_orderid_count"], 1)
            self.assertEqual(row["single_ordertype_orderid_count"], 1)
            self.assertIn("ordertype_transition_pattern_sample", frame.columns)
            self.assertIn(row["status"], SEMANTIC_STATUS_VALUES)
            self.assertEqual(row["ordertype_nonnull_count"], 3)
            self.assertEqual(row["ordertype_nonnull_rate"], 1.0)
            self.assertEqual(row["distinct_ordertype_values"], 2)
            self.assertEqual(row["single_ordertype_orderid_rate"], 0.5)
            self.assertEqual(row["multi_ordertype_orderid_rate"], 0.5)
            self.assertEqual(row["top_ordertype_values"], "1:2,2:1")
            self.assertEqual(row["ordertype_transition_pattern_count"], 2)
            self.assertEqual(row["ordertype_transition_pattern_sample"], "1:1,1,2:1")


if __name__ == "__main__":
    unittest.main()
