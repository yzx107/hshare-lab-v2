from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from Scripts import build_verified_layer as verified_layer


REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.from_dicts(rows).write_parquet(path)


class BuildVerifiedLayerTests(unittest.TestCase):
    def test_parse_selected_dates_respects_range_and_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned" / "orders"
            for trade_date in [
                "2025-01-02",
                "2025-01-05",
                "2025-01-06",
                "2025-01-07",
                "2025-01-08",
                "2025-01-09",
            ]:
                (stage_root / f"date={trade_date}").mkdir(parents=True, exist_ok=True)

            args = argparse.Namespace(
                year="2025",
                stage_root=root / "candidate_cleaned",
                dates=None,
                start_date="2025-01-05",
                end_date="2025-01-09",
                max_days=0,
                latest_days=False,
                date_batch_size=2,
                date_batch_index=2,
            )

            selected_dates = verified_layer.parse_selected_dates(args, "orders")
            self.assertEqual(selected_dates, ["2025-01-07", "2025-01-08"])

    def test_prepare_task_inputs_reuses_existing_scratch_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_path = root / "candidate_cleaned" / "orders" / "date=2026-03-13" / "orders.parquet"
            write_parquet(
                source_path,
                [
                    {
                        "date": date.fromisoformat("2026-03-13"),
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

            task = verified_layer.VerifiedTask(
                year="2026",
                table_name="orders",
                date="2026-03-13",
                input_paths=(str(source_path),),
                output_path=str(root / "verified_orders" / "year=2026" / "date=2026-03-13" / "part-00000.parquet"),
                allowed_columns=("date",),
                excluded_columns=(),
                scratch_root=str(root / "scratch"),
                input_read_mode="scratch_prefetch",
            )

            first_prefetch = verified_layer.prepare_task_inputs(task)
            second_prefetch = verified_layer.prepare_task_inputs(task)

            self.assertEqual(first_prefetch.prefetch_copied_files, 1)
            self.assertEqual(first_prefetch.prefetch_reused_files, 0)
            self.assertEqual(second_prefetch.prefetch_copied_files, 0)
            self.assertEqual(second_prefetch.prefetch_reused_files, 1)
            self.assertEqual(first_prefetch.effective_input_paths, second_prefetch.effective_input_paths)
            self.assertTrue(Path(first_prefetch.effective_input_paths[0]).exists())

    def test_interleave_tasks_by_table_keeps_trades_from_starving(self) -> None:
        tasks = [
            verified_layer.VerifiedTask(
                year="2026",
                table_name="orders",
                date="2026-01-02",
                input_paths=("orders-1.parquet",),
                output_path="verified-orders-1.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
            ),
            verified_layer.VerifiedTask(
                year="2026",
                table_name="orders",
                date="2026-02-09",
                input_paths=("orders-2.parquet",),
                output_path="verified-orders-2.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
            ),
            verified_layer.VerifiedTask(
                year="2026",
                table_name="trades",
                date="2026-01-02",
                input_paths=("trades-1.parquet",),
                output_path="verified-trades-1.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
            ),
            verified_layer.VerifiedTask(
                year="2026",
                table_name="trades",
                date="2026-02-09",
                input_paths=("trades-2.parquet",),
                output_path="verified-trades-2.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
            ),
        ]

        ordered_task_keys = [task.task_key for task in verified_layer.interleave_tasks_by_table(tasks)]
        self.assertEqual(
            ordered_task_keys,
            [
                "2026-01-02:orders",
                "2026-01-02:trades",
                "2026-02-09:orders",
                "2026-02-09:trades",
            ],
        )

    def test_interleave_tasks_by_table_prefers_larger_inputs_within_each_table(self) -> None:
        tasks = [
            verified_layer.VerifiedTask(
                year="2026",
                table_name="orders",
                date="2026-01-02",
                input_paths=("orders-1.parquet",),
                output_path="verified-orders-1.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
                input_bytes=100,
            ),
            verified_layer.VerifiedTask(
                year="2026",
                table_name="orders",
                date="2026-02-09",
                input_paths=("orders-2.parquet",),
                output_path="verified-orders-2.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
                input_bytes=300,
            ),
            verified_layer.VerifiedTask(
                year="2026",
                table_name="trades",
                date="2026-01-02",
                input_paths=("trades-1.parquet",),
                output_path="verified-trades-1.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
                input_bytes=50,
            ),
            verified_layer.VerifiedTask(
                year="2026",
                table_name="trades",
                date="2026-02-09",
                input_paths=("trades-2.parquet",),
                output_path="verified-trades-2.parquet",
                allowed_columns=("date",),
                excluded_columns=(),
                input_bytes=200,
            ),
        ]

        ordered_task_keys = [task.task_key for task in verified_layer.interleave_tasks_by_table(tasks)]
        self.assertEqual(
            ordered_task_keys,
            [
                "2026-02-09:orders",
                "2026-02-09:trades",
                "2026-01-02:orders",
                "2026-01-02:trades",
            ],
        )

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

    def test_prefetches_orders_to_scratch_without_changing_trades(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "verified"
            research_root = root / "research"
            log_root = root / "logs"
            scratch_root = root / "scratch"

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
                    "--scratch-root",
                    str(scratch_root),
                    "--scratch-table",
                    "orders",
                    "--workers",
                    "1",
                    "--executor",
                    "thread",
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            scratch_orders = (
                scratch_root
                / "verified_prefetch"
                / "year=2026"
                / "orders"
                / f"date={trade_date}"
                / "orders.parquet"
            )
            scratch_trades = (
                scratch_root
                / "verified_prefetch"
                / "year=2026"
                / "trades"
                / f"date={trade_date}"
                / "trades.parquet"
            )
            self.assertTrue(scratch_orders.exists())
            self.assertFalse(scratch_trades.exists())

            parts_path = output_root / "manifests" / "year=2026" / "verified_partitions.jsonl"
            rows = [json.loads(line) for line in parts_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            rows_by_task = {row["task_key"]: row for row in rows}

            orders_row = rows_by_task[f"{trade_date}:orders"]
            self.assertEqual(orders_row["input_read_mode"], "scratch_prefetch")
            self.assertEqual(orders_row["prefetch_copied_files"], 1)
            self.assertEqual(orders_row["prefetch_reused_files"], 0)
            self.assertEqual(orders_row["scratch_input_paths"], [str(scratch_orders)])
            self.assertEqual(orders_row["effective_input_paths"], [str(scratch_orders)])
            self.assertGreaterEqual(orders_row["prefetch_seconds"], 0.0)
            self.assertGreaterEqual(orders_row["materialize_seconds"], 0.0)
            self.assertGreaterEqual(orders_row["total_task_seconds"], orders_row["materialize_seconds"])

            trades_row = rows_by_task[f"{trade_date}:trades"]
            self.assertEqual(trades_row["input_read_mode"], "direct_stage")
            self.assertEqual(trades_row["scratch_input_paths"], [])
            self.assertEqual(trades_row["effective_input_paths"], trades_row["input_paths"])
            self.assertEqual(trades_row["prefetch_copied_files"], 0)
            self.assertEqual(trades_row["prefetch_reused_files"], 0)

    def test_partial_batch_run_writes_selection_scoped_summary_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "verified"
            research_root = root / "research"
            log_root = root / "logs"

            for trade_date, seq_num in [
                ("2025-01-05", 101),
                ("2025-01-06", 102),
                ("2025-01-07", 103),
            ]:
                write_parquet(
                    stage_root / "orders" / f"date={trade_date}" / "orders.parquet",
                    [
                        {
                            "date": date.fromisoformat(trade_date),
                            "table_name": "orders",
                            "source_file": "a.csv",
                            "ingest_ts": datetime(2026, 3, 18, tzinfo=timezone.utc),
                            "row_num_in_file": 1,
                            "SeqNum": seq_num,
                            "OrderId": 1000 + seq_num,
                            "Time": "093000",
                            "Price": 12.3,
                            "Volume": 1000,
                        }
                    ],
                )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_verified_layer",
                    "--year",
                    "2025",
                    "--table",
                    "orders",
                    "--start-date",
                    "2025-01-05",
                    "--end-date",
                    "2025-01-07",
                    "--date-batch-size",
                    "2",
                    "--date-batch-index",
                    "2",
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

            selected_output = output_root / "verified_orders" / "year=2025" / "date=2025-01-07" / "part-00000.parquet"
            skipped_output = output_root / "verified_orders" / "year=2025" / "date=2025-01-05" / "part-00000.parquet"
            self.assertTrue(selected_output.exists())
            self.assertFalse(skipped_output.exists())

            summary = json.loads((output_root / "manifests" / "year=2025" / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["completed_count"], 1)
            self.assertEqual(summary["pending_count"], 0)
            self.assertEqual(summary["selection"]["label"], "orders__from_2025-01-05__to_2025-01-07__batch_2_of_size_2")
            self.assertEqual(summary["selection"]["selected_date_count"], 1)
            self.assertEqual(summary["tables"]["verified_orders"]["dates"], ["2025-01-07"])
            self.assertEqual(summary["tables"]["verified_orders"]["partitions"], 1)

            report_path = (
                research_root
                / "verified_layer_2025__orders__from_2025-01-05__to_2025-01-07__batch_2_of_size_2.md"
            )
            self.assertTrue(report_path.exists())
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("label: orders__from_2025-01-05__to_2025-01-07__batch_2_of_size_2", report_text)
            self.assertIn("selected_date_count: 1", report_text)

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

    def test_resume_rebuilds_when_manifest_row_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "verified"
            research_root = root / "research"
            log_root = root / "logs"

            trade_date = "2026-03-13"
            task_key = f"{trade_date}:orders"
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

            manifest_root = output_root / "manifests" / "year=2026"
            manifest_root.mkdir(parents=True, exist_ok=True)
            output_path = output_root / "verified_orders" / "year=2026" / f"date={trade_date}" / "part-00000.parquet"
            stale_row = {
                "task_key": task_key,
                "date": trade_date,
                "year": "2026",
                "table_name": "orders",
                "verified_table_name": "verified_orders",
                "output_path": str(output_path),
                "input_paths": [str(stage_root / "orders" / f"date={trade_date}" / "orders.parquet")],
                "input_row_count": 1,
                "output_row_count": 1,
                "output_bytes": 1,
                "included_columns": ["date"],
                "excluded_columns": [],
                "verified_policy_version": "2026-03-15",
                "source_layer": "candidate_cleaned",
                "admission_rule": "admit_now_only",
                "contains_caveat_fields": False,
                "reference_join_applied": False,
                "research_time_grade": "fine_ok",
                "generated_at": "2026-03-18T00:00:00Z",
            }
            (manifest_root / "verified_partitions.jsonl").write_text(json.dumps(stale_row) + "\n", encoding="utf-8")
            (manifest_root / "checkpoint.json").write_text(
                json.dumps(
                    {
                        "status": "completed_with_failures",
                        "year": "2026",
                        "started_at": "2026-03-18T00:00:00Z",
                        "updated_at": "2026-03-18T00:00:00Z",
                        "workers": 1,
                        "executor_mode": "thread",
                        "completed_task_keys": [task_key],
                        "completed_count": 1,
                        "failed_tasks": {task_key: "old failure"},
                        "failed_count": 1,
                        "pending_count": 0,
                        "active_task_key": None,
                        "active_task_keys": [],
                    }
                ),
                encoding="utf-8",
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_verified_layer",
                    "--year",
                    "2026",
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
                    "--resume",
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            rows = [
                json.loads(line)
                for line in (manifest_root / "verified_partitions.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["task_key"], task_key)

            summary = json.loads((manifest_root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["completed_count"], 1)
            self.assertEqual(summary["failed_count"], 0)

    def test_refuses_orphan_output_without_manifest_row(self) -> None:
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
                    }
                ],
            )
            write_parquet(
                output_root / "verified_orders" / "year=2026" / f"date={trade_date}" / "part-00000.parquet",
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

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_verified_layer",
                    "--year",
                    "2026",
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
                    "--resume",
                ],
                cwd=str(REPO_ROOT),
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Found verified outputs without matching manifest rows", result.stderr)


if __name__ == "__main__":
    unittest.main()
