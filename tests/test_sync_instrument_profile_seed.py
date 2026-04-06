from __future__ import annotations

import unittest

import polars as pl

from Scripts.sync_instrument_profile_seed import merge_seed, normalize_seed_frame


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


if __name__ == "__main__":
    unittest.main()
