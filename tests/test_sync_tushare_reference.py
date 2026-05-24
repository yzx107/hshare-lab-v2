from __future__ import annotations

import argparse
import unittest
from pathlib import Path

import polars as pl

from Scripts.sync_tushare_reference import (
    date_token_to_compact,
    date_token_to_iso,
    normalize_hk_adjfactor_frame,
    normalize_hk_basic_frame,
    normalize_hk_daily_frame,
    normalize_hk_tradecal_frame,
    output_paths,
)


class SyncTushareReferenceTest(unittest.TestCase):
    def test_date_token_normalization(self) -> None:
        self.assertEqual(date_token_to_iso("20260522"), "2026-05-22")
        self.assertEqual(date_token_to_compact("2026-05-22"), "20260522")

    def test_normalize_hk_basic_adds_reference_columns(self) -> None:
        frame = pl.DataFrame(
            {
                "ts_code": ["00001.HK"],
                "list_date": ["19721101"],
                "delist_date": [None],
            }
        )

        rows = normalize_hk_basic_frame(frame, as_of_date="2026-05-24").to_dicts()

        self.assertEqual(rows[0]["instrument_key"], "00001")
        self.assertEqual(rows[0]["listing_date"], "1972-11-01")
        self.assertIsNone(rows[0]["delisting_date"])
        self.assertEqual(rows[0]["source_label"], "tushare_hk_basic")

    def test_normalize_hk_daily_adds_date_and_source_label(self) -> None:
        frame = pl.DataFrame({"ts_code": ["00001.HK"], "trade_date": ["20190904"], "close": [69.0]})

        rows = normalize_hk_daily_frame(frame).to_dicts()

        self.assertEqual(rows[0]["instrument_key"], "00001")
        self.assertEqual(rows[0]["date"], "2019-09-04")
        self.assertEqual(rows[0]["source_label"], "tushare_hk_daily")

    def test_normalize_hk_adjfactor_adds_date_and_source_label(self) -> None:
        frame = pl.DataFrame(
            {"ts_code": ["00001.HK"], "trade_date": ["20260522"], "cum_adjfactor": [1.0]}
        )

        rows = normalize_hk_adjfactor_frame(frame).to_dicts()

        self.assertEqual(rows[0]["instrument_key"], "00001")
        self.assertEqual(rows[0]["date"], "2026-05-22")
        self.assertEqual(rows[0]["source_label"], "tushare_hk_adjfactor")

    def test_normalize_hk_tradecal_adds_date_and_source_label(self) -> None:
        frame = pl.DataFrame(
            {"cal_date": ["20260102"], "is_open": [1], "pretrade_date": ["20251231"]}
        )

        rows = normalize_hk_tradecal_frame(frame).to_dicts()

        self.assertEqual(rows[0]["date"], "2026-01-02")
        self.assertEqual(rows[0]["source_label"], "tushare_hk_tradecal")

    def test_output_paths_partition_by_endpoint_date(self) -> None:
        args = argparse.Namespace(
            endpoint="hk_daily",
            trade_date="20260522",
            as_of_date="2026-05-24",
            output_root=Path("root"),
        )

        parquet_path, manifest_path = output_paths(args)

        self.assertEqual(str(parquet_path), "root/hk_daily/trade_date=2026-05-22/hk_daily.parquet")
        self.assertEqual(str(manifest_path), "root/hk_daily/trade_date=2026-05-22/manifest.json")

    def test_hk_basic_output_path_includes_list_status(self) -> None:
        args = argparse.Namespace(
            endpoint="hk_basic",
            trade_date=None,
            list_status="L",
            as_of_date="2026-05-24",
            output_root=Path("root"),
        )

        parquet_path, _ = output_paths(args)

        self.assertEqual(
            str(parquet_path),
            "root/hk_basic/list_status=L/as_of_date=2026-05-24/hk_basic.parquet",
        )

    def test_hk_tradecal_output_path_partitions_by_year(self) -> None:
        args = argparse.Namespace(
            endpoint="hk_tradecal",
            trade_date=None,
            start_date="20260101",
            end_date="20260524",
            list_status="L",
            as_of_date="2026-05-24",
            output_root=Path("root"),
        )

        parquet_path, _ = output_paths(args)

        self.assertEqual(str(parquet_path), "root/hk_tradecal/year=2026/hk_tradecal.parquet")


if __name__ == "__main__":
    unittest.main()
