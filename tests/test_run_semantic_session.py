from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from Scripts.semantic_contract import SEMANTIC_STATUS_VALUES, STATUS_NOT_RUN

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, columns: dict[str, list[object]]) -> None:
    pq.write_table(pa.Table.from_pydict(columns), path)


class SemanticSessionRunnerTests(unittest.TestCase):
    def test_runner_marks_not_run_when_session_missing(self) -> None:
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
            write_parquet(orders_dir / "orders.parquet", {"OrderId": [1001], "OrderType": [1], "SeqNum": [1], "Time": ["093000"]})
            write_parquet(trades_dir / "trades.parquet", {"TickID": [1], "Time": ["093001"]})

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_session",
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
            frame = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_session_daily.parquet")
            row = frame.to_dicts()[0]
            self.assertEqual(row["status"], STATUS_NOT_RUN)

    def test_runner_handles_session_column_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "dqa"
            research_root = root / "research"
            log_root = root / "logs"
            orders_dir = stage_root / "orders" / "date=2026-03-14"
            trades_dir = stage_root / "trades" / "date=2026-03-14"
            orders_dir.mkdir(parents=True)
            trades_dir.mkdir(parents=True)
            write_parquet(
                orders_dir / "orders.parquet",
                {
                    "OrderId": [1001, 1002],
                    "OrderType": [1, 1],
                    "SeqNum": [1, 2],
                    "Time": ["093000", "130000"],
                    "Session": ["AM", "PM"],
                },
            )
            write_parquet(trades_dir / "trades.parquet", {"TickID": [1], "Time": ["093001"], "Session": ["AM"]})

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.run_semantic_session",
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
            frame = pl.read_parquet(output_root / "semantic" / "year=2026" / "semantic_session_daily.parquet")
            row = frame.to_dicts()[0]
            self.assertEqual(row["distinct_session_values"], 2)
            self.assertIn("session_split_required_flag", frame.columns)
            self.assertIn(row["status"], SEMANTIC_STATUS_VALUES)


if __name__ == "__main__":
    unittest.main()
