from __future__ import annotations

import unittest

import polars as pl

from Scripts.sync_instrument_profile_seed import merge_seed, normalize_seed_frame, opend_seed_from_basicinfo


class SyncInstrumentProfileSeedTest(unittest.TestCase):
    def test_merge_seed_preserves_existing_non_null_values(self) -> None:
        base = normalize_seed_frame(
            pl.DataFrame(
                {
                    "instrument_key": ["00823"],
                    "listing_date": ["2005-11-25"],
                    "instrument_family": ["reit_or_unit_trust_non_etf"],
                    "source_label": ["manual_seed"],
                }
            )
        )
        incoming = normalize_seed_frame(
            pl.DataFrame(
                {
                    "instrument_key": ["00823", "00001"],
                    "listing_date": ["2000-01-01", "1972-11-01"],
                    "source_label": ["tushare_hk_basic", "tushare_hk_basic"],
                }
            )
        )

        merged = merge_seed(base, incoming).sort("instrument_key")
        rows = merged.to_dicts()

        self.assertEqual(rows[0]["instrument_key"], "00001")
        self.assertEqual(rows[0]["listing_date"], "1972-11-01")
        self.assertEqual(rows[0]["source_label"], "tushare_hk_basic")
        self.assertEqual(rows[1]["instrument_key"], "00823")
        self.assertEqual(rows[1]["listing_date"], "2005-11-25")
        self.assertEqual(rows[1]["instrument_family"], "reit_or_unit_trust_non_etf")
        self.assertEqual(rows[1]["source_label"], "manual_seed")

    def test_opend_seed_from_basicinfo_maps_etf_and_clears_placeholder_listing_date(self) -> None:
        frame = pl.DataFrame(
            {
                "code": ["HK.02800", "HK.00001"],
                "name": ["盈富基金", "长和"],
                "stock_type": ["ETF", "STOCK"],
                "listing_date": ["1999-11-12", "1970-01-01"],
            }
        )

        rows = opend_seed_from_basicinfo(frame, "2026-04-07").sort("instrument_key").to_dicts()

        self.assertEqual(rows[0]["instrument_key"], "00001")
        self.assertIsNone(rows[0]["listing_date"])
        self.assertIsNone(rows[0]["instrument_family"])
        self.assertEqual(rows[0]["source_label"], "opend_security_snapshot")

        self.assertEqual(rows[1]["instrument_key"], "02800")
        self.assertEqual(rows[1]["listing_date"], "1999-11-12")
        self.assertEqual(rows[1]["instrument_family"], "exchange_traded_fund")
        self.assertEqual(rows[1]["instrument_family_source"], "opend_security_snapshot")


if __name__ == "__main__":
    unittest.main()
