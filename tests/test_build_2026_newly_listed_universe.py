from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]


class Build2026NewlyListedUniverseTests(unittest.TestCase):
    def test_builds_fail_closed_universe_with_tick_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            profile_path = root / "instrument_profile.parquet"
            seed_path = root / "instrument_profile_seed.csv"
            stage_root = root / "candidate_cleaned"
            raw_root = root / "raw"
            output_root = root / "reference" / "newly_listed_hk"
            trade_date = "2026-05-22"

            pl.DataFrame(
                [
                    profile_row("00068", "2026-04-17", True),
                    profile_row("00069", "2025-04-17", True),
                    profile_row("00070", "2026-05-05", True),
                    profile_row("03000", "2026-05-05", False, "exchange_traded_fund"),
                ]
            ).write_parquet(profile_path)
            pl.DataFrame(
                [
                    {
                        "instrument_key": "01609",
                        "listing_date": "2026-05-05",
                        "source_label": "tushare_hk_basic",
                    }
                ]
            ).write_csv(seed_path)
            write_empty_partition(stage_root, "orders", trade_date)
            write_empty_partition(stage_root, "trades", trade_date)
            write_zip(raw_root, trade_date, orders=["00068", "01609"], trades=["00068", "01609"])

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.build_2026_newly_listed_universe",
                    "--profile-path",
                    str(profile_path),
                    "--seed-path",
                    str(seed_path),
                    "--stage-root",
                    str(stage_root),
                    "--raw-root",
                    str(raw_root),
                    "--output-root",
                    str(output_root),
                    "--dates",
                    trade_date,
                    "--symbols",
                    "HK.00068,HK.00069,HK.00070,HK.03000,HK.01609",
                    "--write-research-report",
                    "--research-root",
                    str(root / "Research"),
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            universe_path = output_root / "year=2026" / "newly_listed_hk_2026.parquet"
            manifest_path = output_root / "year=2026" / "newly_listed_hk_2026_manifest.json"
            frame = pl.read_parquet(universe_path)
            by_symbol = {row["symbol"]: row for row in frame.to_dicts()}
            self.assertEqual(by_symbol["HK.00068"]["universe_status"], "included")
            self.assertEqual(by_symbol["HK.00068"]["candidate_cleaned_trade_dates"], [trade_date])
            self.assertEqual(by_symbol["HK.00070"]["universe_status"], "excluded_no_tick_data")
            self.assertEqual(by_symbol["HK.03000"]["universe_status"], "ambiguous_instrument_type")
            self.assertEqual(by_symbol["HK.01609"]["universe_status"], "included")
            self.assertEqual(by_symbol["HK.00069"]["universe_status"], "missing_listing_date")
            self.assertTrue((output_root / "year=2026" / "newly_listed_hk_2026.csv").exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["included_count"], 2)
            self.assertEqual(
                manifest["target_symbol_status"]["HK.01609"]["universe_status"], "included"
            )


def profile_row(
    instrument_key: str,
    listing_date: str,
    stock_research_candidate: bool,
    instrument_family: str = "listed_security_unclassified",
) -> dict[str, object]:
    return {
        "instrument_key": instrument_key,
        "listing_date": listing_date,
        "stock_research_candidate": stock_research_candidate,
        "instrument_family": instrument_family,
        "instrument_family_status": "seed_classified",
        "source_label": "test_profile",
    }


def write_empty_partition(stage_root: Path, table: str, trade_date: str) -> None:
    path = (
        stage_root / table / f"date={trade_date}" / f"{trade_date.replace('-', '')}_{table}.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"source_file": []}, schema={"source_file": pl.Utf8}).write_parquet(path)


def write_zip(raw_root: Path, trade_date: str, *, orders: list[str], trades: list[str]) -> None:
    path = raw_root / trade_date[:4] / f"{trade_date.replace('-', '')}.zip"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        for key in orders:
            archive.writestr(f"order/{key}.csv", "")
        for key in trades:
            archive.writestr(f"trade/{key}.csv", "")


if __name__ == "__main__":
    unittest.main()
