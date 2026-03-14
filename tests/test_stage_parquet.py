from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

import polars as pl

from Scripts.build_stage_parquet import (
    StageBundleTask,
    StageTask,
    inspect_source_inventory,
    process_stage_bundle,
    process_stage_task,
    read_active_bundle_progress,
)


class StageParquetTests(unittest.TestCase):
    def build_zip(self, root: Path, name: str, members: dict[str, str]) -> Path:
        zip_path = root / name
        with zipfile.ZipFile(zip_path, "w") as zf:
            for member_name, content in members.items():
                zf.writestr(member_name, content)
        return zip_path

    def test_process_stage_task_supports_2025_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            zip_path = self.build_zip(
                root,
                "20250218.zip",
                {
                    "20250218\\OrderAdd\\00001.csv": (
                        "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                        "1,1001,1,000,92048,39.450,500,0,0746,0\n"
                    ),
                    "20250218\\OrderModifyDelete\\00001.csv": (
                        "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                        "2,1001,3,110,93000,39.450,500,0,6389,0\n"
                    ),
                    "20250218\\TradeResumes\\00001.csv": (
                        "Time,Price,Volume,Dir,Type,BrokerNo,TickID,BidOrderID,BidVolume,AskOrderID,AskVolume\n"
                        "92048,39.350,4000,0,U,,1,0,0,0,0\n"
                    ),
                },
            )

            orders_output = root / "stage" / "orders" / "date=2025-02-18" / "20250218_orders.parquet"
            trades_output = root / "stage" / "trades" / "date=2025-02-18" / "20250218_trades.parquet"

            orders_result = process_stage_task(
                StageTask(
                    year="2025",
                    trade_date="2025-02-18",
                    zip_path=str(zip_path),
                    table_name="orders",
                    output_path=str(orders_output),
                    row_group_target=10,
                    overwrite_existing=True,
                )
            )
            trades_result = process_stage_task(
                StageTask(
                    year="2025",
                    trade_date="2025-02-18",
                    zip_path=str(zip_path),
                    table_name="trades",
                    output_path=str(trades_output),
                    row_group_target=10,
                    overwrite_existing=True,
                )
            )

            orders = pl.read_parquet(orders_output)
            trades = pl.read_parquet(trades_output)

            self.assertEqual(orders_result["row_count"], 2)
            self.assertEqual(trades_result["row_count"], 1)
            self.assertEqual(list(orders["Time"]), ["092048", "093000"])
            self.assertEqual(list(orders["BrokerNo"]), ["0746", "6389"])
            self.assertEqual(list(orders["Ext"]), ["000", "110"])
            self.assertEqual(list(trades["Time"]), ["092048"])
            self.assertEqual(trades["TickID"].to_list(), [1])
            self.assertIn("20250218/OrderAdd/00001.csv", orders["source_file"].to_list())
            self.assertEqual(orders["row_num_in_file"].to_list(), [1, 1])

    def test_process_stage_task_supports_2026_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            zip_path = self.build_zip(
                root,
                "20260105.zip",
                {
                    "order/00001.csv": (
                        "Channel,SendTime,SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                        "30,1767576015546000000,35143,3027201,1,110,092015,54.000,1000,0,3436,0\n"
                    ),
                    "trade/00001.csv": (
                        "SendTime,SeqNum,TickID,Time,Price,Volume,Dir,Type,BrokerNo,BidOrderID,BidVolume,AskOrderID,AskVolume\n"
                        "1767576015543000000,35079,1,092015,53.900,500,0,U,0,0,0,0,0\n"
                    ),
                },
            )

            orders_output = root / "stage" / "orders" / "date=2026-01-05" / "20260105_orders.parquet"
            trades_output = root / "stage" / "trades" / "date=2026-01-05" / "20260105_trades.parquet"

            process_stage_task(
                StageTask(
                    year="2026",
                    trade_date="2026-01-05",
                    zip_path=str(zip_path),
                    table_name="orders",
                    output_path=str(orders_output),
                    row_group_target=10,
                    overwrite_existing=True,
                )
            )
            process_stage_task(
                StageTask(
                    year="2026",
                    trade_date="2026-01-05",
                    zip_path=str(zip_path),
                    table_name="trades",
                    output_path=str(trades_output),
                    row_group_target=10,
                    overwrite_existing=True,
                )
            )

            orders = pl.read_parquet(orders_output)
            trades = pl.read_parquet(trades_output)

            self.assertEqual(orders["Channel"].to_list(), [30])
            self.assertEqual(orders["SendTimeRaw"].to_list(), ["1767576015546000000"])
            self.assertEqual(orders["Time"].to_list(), ["092015"])
            self.assertEqual(trades["SeqNum"].to_list(), [35079])
            self.assertEqual(trades["SendTimeRaw"].to_list(), ["1767576015543000000"])
            self.assertEqual(trades["Type"].to_list(), ["U"])
            self.assertEqual(str(trades.schema["SendTime"]), "Datetime(time_unit='ns', time_zone='UTC')")

    def test_process_stage_bundle_builds_orders_and_trades_in_one_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            zip_path = self.build_zip(
                root,
                "20260105.zip",
                {
                    "order/00001.csv": (
                        "Channel,SendTime,SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                        "30,1767576015546000000,35143,3027201,1,110,092015,54.000,1000,0,3436,0\n"
                    ),
                    "trade/00001.csv": (
                        "SendTime,SeqNum,TickID,Time,Price,Volume,Dir,Type,BrokerNo,BidOrderID,BidVolume,AskOrderID,AskVolume\n"
                        "1767576015543000000,35079,1,092015,53.900,500,0,U,0,0,0,0,0\n"
                    ),
                },
            )

            orders_output = root / "stage" / "orders" / "date=2026-01-05" / "20260105_orders.parquet"
            trades_output = root / "stage" / "trades" / "date=2026-01-05" / "20260105_trades.parquet"

            bundle_result = process_stage_bundle(
                StageBundleTask(
                    year="2026",
                    trade_date="2026-01-05",
                    zip_path=str(zip_path),
                    tasks=(
                        StageTask(
                            year="2026",
                            trade_date="2026-01-05",
                            zip_path=str(zip_path),
                            table_name="orders",
                            output_path=str(orders_output),
                            row_group_target=10,
                            overwrite_existing=True,
                        ),
                        StageTask(
                            year="2026",
                            trade_date="2026-01-05",
                            zip_path=str(zip_path),
                            table_name="trades",
                            output_path=str(trades_output),
                            row_group_target=10,
                            overwrite_existing=True,
                        ),
                    ),
                )
            )

            result_by_table = {row["table_name"]: row for row in bundle_result["results"]}
            self.assertEqual(bundle_result["failures"], [])
            self.assertEqual(sorted(result_by_table), ["orders", "trades"])
            self.assertEqual(result_by_table["orders"]["row_count"], 1)
            self.assertEqual(result_by_table["trades"]["row_count"], 1)

            orders = pl.read_parquet(orders_output)
            trades = pl.read_parquet(trades_output)
            self.assertEqual(orders["OrderId"].to_list(), [3027201])
            self.assertEqual(trades["TickID"].to_list(), [1])

    def test_process_stage_bundle_writes_progress_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            zip_path = self.build_zip(
                root,
                "20260105.zip",
                {
                    "order/00001.csv": (
                        "Channel,SendTime,SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                        "30,1767576015546000000,35143,3027201,1,110,092015,54.000,1000,0,3436,0\n"
                    ),
                },
            )
            progress_path = root / "bundle_progress" / "2026-01-05_orders.json"
            orders_output = root / "stage" / "orders" / "date=2026-01-05" / "20260105_orders.parquet"

            bundle_result = process_stage_bundle(
                StageBundleTask(
                    year="2026",
                    trade_date="2026-01-05",
                    zip_path=str(zip_path),
                    tasks=(
                        StageTask(
                            year="2026",
                            trade_date="2026-01-05",
                            zip_path=str(zip_path),
                            table_name="orders",
                            output_path=str(orders_output),
                            row_group_target=10,
                            overwrite_existing=True,
                        ),
                    ),
                    progress_path=str(progress_path),
                )
            )

            self.assertEqual(bundle_result["failures"], [])
            self.assertTrue(progress_path.exists())
            payload = json.loads(progress_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["member_total"], 1)
            self.assertEqual(payload["members_processed"], 1)
            self.assertEqual(payload["tables"]["orders"]["row_count"], 1)

    def test_read_active_bundle_progress_filters_completed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            progress_dir = Path(tmpdir)
            (progress_dir / "running.json").write_text(
                json.dumps({"bundle_key": "a", "status": "running", "tables": {}}),
                encoding="utf-8",
            )
            (progress_dir / "completed.json").write_text(
                json.dumps({"bundle_key": "b", "status": "completed", "tables": {}}),
                encoding="utf-8",
            )
            (progress_dir / "finalizing.json").write_text(
                json.dumps({"bundle_key": "c", "status": "finalizing", "tables": {}}),
                encoding="utf-8",
            )

            active_rows = read_active_bundle_progress(progress_dir)

            self.assertEqual(sorted(row["bundle_key"] for row in active_rows), ["a", "c"])

    def test_invalid_required_time_is_rejected_with_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            zip_path = self.build_zip(
                root,
                "20250218.zip",
                {
                    "20250218\\TradeResumes\\00001.csv": (
                        "Time,Price,Volume,Dir,Type,BrokerNo,TickID,BidOrderID,BidVolume,AskOrderID,AskVolume\n"
                        "09:20:48,39.350,4000,0,U,,1,0,0,0,0\n"
                    ),
                },
            )
            trades_output = root / "stage" / "trades" / "date=2025-02-18" / "20250218_trades.parquet"

            result = process_stage_task(
                StageTask(
                    year="2025",
                    trade_date="2025-02-18",
                    zip_path=str(zip_path),
                    table_name="trades",
                    output_path=str(trades_output),
                    row_group_target=10,
                    overwrite_existing=True,
                )
            )

            self.assertEqual(result["raw_row_count"], 1)
            self.assertEqual(result["row_count"], 0)
            self.assertEqual(result["rejected_row_count"], 1)
            self.assertEqual(result["rejection_reason_counts"]["invalid_required_format:Time"], 1)

    def test_inspect_source_inventory_reports_unmapped_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            zip_path = self.build_zip(
                root,
                "20260105.zip",
                {
                    "order/00001.csv": (
                        "Channel,SendTime,SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                    ),
                    "mystery/00001.csv": "foo\n1\n",
                },
            )

            group_rows, unmapped_rows = inspect_source_inventory("2026", "2026-01-05", zip_path)

            mapped_groups = {row["raw_group"]: row["mapped_tables"] for row in group_rows}
            self.assertEqual(mapped_groups["order"], ["orders"])
            self.assertEqual(mapped_groups["mystery"], [])
            self.assertEqual(unmapped_rows[0]["skip_reason"], "unmapped_source_group")


if __name__ == "__main__":
    unittest.main()
