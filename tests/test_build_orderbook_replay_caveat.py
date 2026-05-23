from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)


class BuildOrderbookReplayCaveatTests(unittest.TestCase):
    def test_materializes_lifecycle_and_trade_linkage_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            stage_root = root / "candidate_cleaned"
            output_root = root / "caveat"
            research_root = root / "research"
            log_root = root / "logs"
            trade_date = "2026-05-22"
            write_parquet(
                stage_root / "orders" / f"date={trade_date}" / "20260522_orders.parquet",
                [
                    order("09:30:00", 1, "000", 1, 10.0, 100, 0),
                    order("09:30:01", 2, "100", 1, 11.0, 80, 0),
                    order("09:30:02", 1, "000", 2, 10.0, 70, 0, volume_pre=100),
                    order("09:30:04", 2, "100", 3, 11.0, 80, 0),
                ],
            )
            write_parquet(
                stage_root / "trades" / f"date={trade_date}" / "20260522_trades.parquet",
                [
                    {
                        "SendTime": f"{trade_date}T09:30:03",
                        "Time": "093003",
                        "SeqNum": 5,
                        "TickID": 5001,
                        "Price": 10.5,
                        "Volume": 20,
                        "BidOrderID": 1,
                        "AskOrderID": 2,
                        "source_file": "trade/00001.csv",
                    }
                ],
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_orderbook_replay_caveat",
                    "--dates",
                    trade_date,
                    "--symbols",
                    "HK.00001",
                    "--stage-root",
                    str(stage_root),
                    "--output-root",
                    str(output_root),
                    "--research-root",
                    str(research_root),
                    "--log-root",
                    str(log_root),
                    "--write-research-report",
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            namespace = output_root / "orderbook_replay__caveat_lifecycle_linkage"
            lifecycle = pl.read_parquet(
                namespace
                / "lifecycle_events"
                / "year=2026"
                / f"date={trade_date}"
                / "symbol=00001"
                / "part-00000.parquet"
            )
            linkage = pl.read_parquet(
                namespace
                / "trade_linkage"
                / "year=2026"
                / f"date={trade_date}"
                / "symbol=00001"
                / "part-00000.parquet"
            )
            self.assertEqual(lifecycle.height, 4)
            self.assertEqual(linkage.height, 1)
            modify = lifecycle.filter(pl.col("OrderType") == 2).to_dicts()[0]
            self.assertTrue(modify["volume_pre_matches_prior_active_order"])
            linked = linkage.to_dicts()[0]
            self.assertTrue(linked["bid_orderid_present"])
            self.assertTrue(linked["ask_orderid_present"])
            self.assertTrue(linked["bid_order_active"])
            self.assertTrue(linked["ask_order_active"])
            self.assertTrue(linked["bid_order_side_matches"])
            self.assertTrue(linked["ask_order_side_matches"])
            self.assertEqual(linked["bid_order_volume"], 70)
            self.assertEqual(linked["admission_rule"], "admit_now_plus_caveat_only")
            self.assertEqual(
                linked["replay_depth_admission"],
                "blocked_until_crossed_book_residue_is_explained_or_bounded",
            )

            summary = json.loads((namespace / "manifests" / "summary.json").read_text())
            self.assertEqual(summary["lifecycle_rows"], 4)
            self.assertEqual(summary["trade_linkage_rows"], 1)
            self.assertTrue(summary["contains_caveat_fields"])
            self.assertTrue((research_root / "orderbook_replay_caveat_20260522.md").exists())


def order(
    time_value: str,
    order_id: int,
    ext: str,
    order_type: int,
    price: float,
    volume: int,
    level: int,
    *,
    volume_pre: int = 0,
) -> dict[str, object]:
    return {
        "SendTime": f"2026-05-22T{time_value}",
        "Time": time_value.replace(":", ""),
        "SeqNum": order_id,
        "OrderId": order_id,
        "OrderType": order_type,
        "Ext": ext,
        "Price": price,
        "Volume": volume,
        "Level": level,
        "VolumePre": volume_pre,
        "source_file": "order/00001.csv",
    }


if __name__ == "__main__":
    unittest.main()
