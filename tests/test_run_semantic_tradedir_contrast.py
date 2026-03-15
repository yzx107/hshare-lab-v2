from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, columns: dict[str, list[object]]) -> None:
    pq.write_table(pa.Table.from_pydict(columns), path)


class SemanticTradeDirContrastRunnerTests(unittest.TestCase):
    def test_runner_profiles_dir_contrast_and_candidate_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"
            dates = ("2026-01-05", "2026-02-24", "2026-03-13")

            for trade_date in dates:
                orders_dir = stage_root / "orders" / f"date={trade_date}"
                trades_dir = stage_root / "trades" / f"date={trade_date}"
                orders_dir.mkdir(parents=True)
                trades_dir.mkdir(parents=True)
                write_parquet(
                    orders_dir / "orders.parquet",
                    {
                        "OrderId": [1],
                        "OrderType": [1],
                        "SeqNum": [1],
                        "Time": ["093000"],
                    },
                )
                write_parquet(
                    trades_dir / "trades.parquet",
                    {
                        "SeqNum": [1, 2, 3, 4, 5, 6, 7, 8],
                        "TickID": [1, 2, 3, 4, 5, 6, 7, 8],
                        "Time": ["090000", "093000", "093001", "093002", "130000", "130001", "130002", "160001"],
                        "Price": [100.0, 101.0, 100.0, 100.0, 101.0, 102.0, 101.0, 101.0],
                        "Volume": [10, 20, 20, 20, 30, 30, 30, 10],
                        "Dir": [0, 1, 2, 1, 2, 1, 2, 0],
                        "BidOrderID": [0, 11, 21, 12, 22, 13, 23, 0],
                        "AskOrderID": [0, 31, 41, 32, 42, 33, 43, 0],
                    },
                )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_tradedir_contrast",
                    "--year",
                    "2026",
                    "--dates",
                    ",".join(dates),
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

            daily = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_tradedir_contrast_daily.parquet")
            summary = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_tradedir_contrast_summary.parquet")

            self.assertEqual(daily.height, 9)
            dir1 = daily.filter((pl.col("date") == "2026-01-05") & (pl.col("dir_value") == 1)).to_dicts()[0]
            self.assertEqual(dir1["row_count"], 3)
            self.assertEqual(dir1["both_present_rate"], 1.0)
            self.assertAlmostEqual(dir1["uptick_rate"], 2 / 3)
            self.assertIn("0930_1159:", dir1["session_mix"])
            self.assertIn("1300_1559:", dir1["session_mix"])

            dir0 = daily.filter((pl.col("date") == "2026-01-05") & (pl.col("dir_value") == 0)).to_dicts()[0]
            self.assertEqual(dir0["neither_side_rate"], 1.0)
            self.assertIn("1600_plus:", dir0["session_mix"])

            summary_row = summary.to_dicts()[0]
            self.assertEqual(summary_row["observed_dir_values"], "0,1,2")
            self.assertEqual(summary_row["status"], "candidate_directional_signal")
            self.assertEqual(summary_row["admissibility_impact"], "requires_manual_review")
            self.assertTrue(summary_row["dir1_vs_dir2_uptick_gap_sign_consistent_flag"])
            self.assertEqual(summary_row["dir1_vs_dir2_bucket_uptick_consistent_day_count"], 3)
            self.assertEqual(summary_row["dir1_vs_dir2_linkage_gap_avg"], 0.0)
            self.assertGreater(summary_row["dir0_specialness_score"], 0.0)


if __name__ == "__main__":
    unittest.main()
