from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl

from Scripts.build_orderbook_top_of_book_only import materialize_rows

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)


class BuildOrderbookTopOfBookOnlyTests(unittest.TestCase):
    def test_materializes_top_of_book_only_rows_with_quality_gates(self) -> None:
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
                    order("09:30:00.000", 1, "000", 1, 10.0, 100),
                    order("09:30:01.000", 2, "100", 1, 11.0, 100),
                ],
            )
            write_parquet(
                stage_root / "trades" / f"date={trade_date}" / "20260522_trades.parquet",
                [
                    {
                        "SendTime": f"{trade_date}T09:30:02.000",
                        "Time": "093002000",
                        "SeqNum": 5,
                        "TickID": 5001,
                        "Price": 10.5,
                        "Volume": 20,
                        "source_file": "trade/00001.csv",
                    }
                ],
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_orderbook_top_of_book_only",
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

            namespace = output_root / "orderbook_replay__top_of_book_only"
            frame = pl.read_parquet(
                namespace
                / "top_of_book_events"
                / "year=2026"
                / f"date={trade_date}"
                / "symbol=00001"
                / "part-00000.parquet"
            )
            self.assertEqual(frame.height, 1)
            row = frame.to_dicts()[0]
            self.assertEqual(row["BestBidReplay"], 10.0)
            self.assertEqual(row["BestAskReplay"], 11.0)
            self.assertEqual(row["ReplaySpread"], 1.0)
            self.assertEqual(row["ReplayMid"], 10.5)
            self.assertTrue(row["TradeInsideBestBookFlag"])
            self.assertTrue(row["TopOfBookValidFlag"])
            self.assertEqual(row["ReplayQualityScore"], 1.0)
            self.assertFalse(row["CrossedWindowFlag"])
            self.assertFalse(row["ReplayResidueFlag"])
            self.assertFalse(row["ReplayWindowExcludedFlag"])
            self.assertFalse(row["SameMillisecondBatchRiskFlag"])
            forbidden_columns = {"Level", "BidVolume", "AskVolume", "FullReconstructedDepth"}
            self.assertFalse(forbidden_columns & set(frame.columns))

            summary = json.loads((namespace / "manifests" / "summary.json").read_text())
            self.assertEqual(summary["release_bucket"], "admit_top_of_book_only")
            self.assertEqual(summary["top_of_book_rows"], 1)
            self.assertTrue((research_root / "orderbook_top_of_book_only_20260522.md").exists())

    def test_invalid_same_millisecond_window_is_explicitly_flagged(self) -> None:
        trade_date = "2026-05-22"
        rows = materialize_rows(
            date=trade_date,
            symbol="00001",
            order_rows=[
                order("09:30:00.000", 1, "000", 1, 10.0, 100),
                order("09:30:00.000", 2, "100", 1, 11.0, 100),
            ],
            trade_rows=[
                {
                    "SendTime": f"{trade_date}T09:30:00.000",
                    "Time": "093000000",
                    "SeqNum": 5,
                    "TickID": 5001,
                    "Price": 10.5,
                    "Volume": 20,
                    "source_file": "trade/00001.csv",
                }
            ],
            sort_mode="send_seq_order_first",
            side_bit=0,
        )
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertFalse(row["TopOfBookValidFlag"])
        self.assertEqual(row["ReplayQualityScore"], 0.0)
        self.assertTrue(row["SameMillisecondBatchRiskFlag"])
        self.assertIsNone(row["ReplaySpread"])
        self.assertIsNone(row["ReplayMid"])
        self.assertIsNone(row["TradeInsideBestBookFlag"])

    def test_missing_registry_entry_fails_loudly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            registry = json.loads(
                (REPO_ROOT / "manifests" / "field_release_registry.json").read_text()
            )
            registry["objects"] = [
                entry
                for entry in registry["objects"]
                if entry["object_name"] != "orderbook_replay__top_of_book_only"
            ]
            bad_registry = root / "field_release_registry.json"
            bad_registry.write_text(json.dumps(registry), encoding="utf-8")
            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_orderbook_top_of_book_only",
                    "--dates",
                    "2026-05-22",
                    "--symbols",
                    "HK.00001",
                    "--stage-root",
                    str(root / "candidate_cleaned"),
                    "--output-root",
                    str(root / "caveat"),
                    "--field-release-registry",
                    str(bad_registry),
                ],
                cwd=str(REPO_ROOT),
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unknown release object", result.stderr)

    def test_missing_gating_object_fails_loudly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            registry = json.loads(
                (REPO_ROOT / "manifests" / "field_release_registry.json").read_text()
            )
            registry["objects"] = [
                entry
                for entry in registry["objects"]
                if entry["object_name"] != "ReplayQualityScore"
            ]
            bad_registry = root / "field_release_registry.json"
            bad_registry.write_text(json.dumps(registry), encoding="utf-8")
            result = run_builder_with_registry(root, bad_registry)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unknown release object: ReplayQualityScore", result.stderr)

    def test_keep_out_object_fails_before_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            registry = json.loads(
                (REPO_ROOT / "manifests" / "field_release_registry.json").read_text()
            )
            for entry in registry["objects"]:
                if entry["object_name"] == "BestBidReplay":
                    entry["release_bucket"] = "keep_out_for_now"
            bad_registry = root / "field_release_registry.json"
            bad_registry.write_text(json.dumps(registry), encoding="utf-8")
            result = run_builder_with_registry(root, bad_registry)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("keep_out_for_now object cannot be materialized", result.stderr)


def run_builder_with_registry(
    root: Path,
    registry_path: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "python3",
            "-m",
            "Scripts.build_orderbook_top_of_book_only",
            "--dates",
            "2026-05-22",
            "--symbols",
            "HK.00001",
            "--stage-root",
            str(root / "candidate_cleaned"),
            "--output-root",
            str(root / "caveat"),
            "--field-release-registry",
            str(registry_path),
        ],
        cwd=str(REPO_ROOT),
        check=False,
        capture_output=True,
        text=True,
    )


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


if __name__ == "__main__":
    unittest.main()
