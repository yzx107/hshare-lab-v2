from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl

from Scripts.build_opend_agent_caveat_handoff import materialize_rows

REPO_ROOT = Path(__file__).resolve().parents[1]


class BuildOpenDAgentCaveatHandoffTests(unittest.TestCase):
    def test_materializes_best_bid_ask_with_size_and_ready_gate(self) -> None:
        rows = materialize_rows(
            date="2026-05-22",
            symbol="00001",
            order_rows=[
                order("09:30:00.000", 1, "000", 1, 10.0, 100),
                order("09:30:01.000", 2, "100", 1, 11.0, 200),
            ],
            trade_rows=[trade("09:30:02.000", 5, 10.5, 20)],
            sort_mode="send_seq_order_first",
            side_bit=0,
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["BestBidReplay"], 10.0)
        self.assertEqual(row["BestBidSizeReplay"], 100)
        self.assertEqual(row["BestAskReplay"], 11.0)
        self.assertEqual(row["BestAskSizeReplay"], 200)
        self.assertTrue(row["TopOfBookValidFlag"])
        self.assertEqual(row["ReplayQualityScore"], 1.0)
        self.assertTrue(row["CaveatHandoffReadyFlag"])

    def test_crossed_book_blocks_caveat_handoff(self) -> None:
        row = materialize_rows(
            date="2026-05-22",
            symbol="00001",
            order_rows=[
                order("09:30:00.000", 1, "000", 1, 12.0, 100),
                order("09:30:01.000", 2, "100", 1, 11.0, 200),
            ],
            trade_rows=[trade("09:30:02.000", 5, 11.5, 20)],
            sort_mode="send_seq_order_first",
            side_bit=0,
        )[0]
        self.assertTrue(row["CrossedWindowFlag"])
        self.assertTrue(row["ReplayWindowExcludedFlag"])
        self.assertFalse(row["TopOfBookValidFlag"])
        self.assertFalse(row["CaveatHandoffReadyFlag"])

    def test_same_millisecond_batch_blocks_caveat_handoff(self) -> None:
        row = materialize_rows(
            date="2026-05-22",
            symbol="00001",
            order_rows=[
                order("09:30:00.000", 1, "000", 1, 10.0, 100),
                order("09:30:00.000", 2, "100", 1, 11.0, 200),
            ],
            trade_rows=[trade("09:30:00.000", 5, 10.5, 20)],
            sort_mode="send_seq_order_first",
            side_bit=0,
        )[0]
        self.assertTrue(row["SameMillisecondBatchRiskFlag"])
        self.assertFalse(row["TopOfBookValidFlag"])
        self.assertFalse(row["CaveatHandoffReadyFlag"])

    def test_missing_size_blocks_caveat_handoff(self) -> None:
        row = materialize_rows(
            date="2026-05-22",
            symbol="00001",
            order_rows=[order("09:30:00.000", 1, "000", 1, 10.0, 100)],
            trade_rows=[trade("09:30:02.000", 5, 10.0, 20)],
            sort_mode="send_seq_order_first",
            side_bit=0,
        )[0]
        self.assertIsNone(row["BestAskSizeReplay"])
        self.assertFalse(row["TopOfBookValidFlag"])
        self.assertFalse(row["CaveatHandoffReadyFlag"])

    def test_resume_does_not_duplicate_completed_partitions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "caveat"
            universe_path = root / "universe.parquet"
            trade_date = "2026-05-22"
            write_stage(stage_root, trade_date)
            pl.DataFrame(
                [
                    {
                        "symbol": "HK.00001",
                        "instrument_key": "00001",
                        "universe_status": "included",
                    }
                ]
            ).write_parquet(universe_path)

            command = [
                "python3",
                "-m",
                "Scripts.build_opend_agent_caveat_handoff",
                "--universe-path",
                str(universe_path),
                "--dates",
                trade_date,
                "--symbols",
                "HK.00001",
                "--stage-root",
                str(stage_root),
                "--output-root",
                str(output_root),
                "--resume",
                "--heartbeat",
            ]
            subprocess.run(command, cwd=str(REPO_ROOT), check=True)
            subprocess.run(command, cwd=str(REPO_ROOT), check=True)

            namespace = output_root / "orderbook_replay__top_of_book_with_size_caveat"
            partition_lines = (
                (namespace / "manifests" / "partitions.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            )
            self.assertEqual(len(partition_lines), 1)
            summary = json.loads((namespace / "manifests" / "summary.json").read_text())
            self.assertEqual(summary["total_rows"], 1)
            self.assertEqual(summary["ready_rows"], 1)


def order(
    time_value: str,
    order_id: int,
    ext: str,
    order_type: int,
    price: float,
    volume: int,
) -> dict[str, object]:
    return {
        "SendTime": f"2026-05-22T{time_value}",
        "Time": time_value.replace(":", "").replace(".", ""),
        "SeqNum": order_id,
        "OrderId": order_id,
        "OrderType": order_type,
        "Ext": ext,
        "Price": price,
        "Volume": volume,
        "source_file": "order/00001.csv",
    }


def trade(time_value: str, seq: int, price: float, volume: int) -> dict[str, object]:
    return {
        "SendTime": f"2026-05-22T{time_value}",
        "Time": time_value.replace(":", "").replace(".", ""),
        "SeqNum": seq,
        "TickID": seq,
        "Price": price,
        "Volume": volume,
        "source_file": "trade/00001.csv",
    }


def write_stage(stage_root: Path, trade_date: str) -> None:
    orders_path = stage_root / "orders" / f"date={trade_date}" / "20260522_orders.parquet"
    trades_path = stage_root / "trades" / f"date={trade_date}" / "20260522_trades.parquet"
    orders_path.parent.mkdir(parents=True, exist_ok=True)
    trades_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [
            order("09:30:00.000", 1, "000", 1, 10.0, 100),
            order("09:30:01.000", 2, "100", 1, 11.0, 200),
        ]
    ).write_parquet(orders_path)
    pl.DataFrame([trade("09:30:02.000", 5, 10.5, 20)]).write_parquet(trades_path)


if __name__ == "__main__":
    unittest.main()
