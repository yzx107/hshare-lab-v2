from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)


class SemanticReportTests(unittest.TestCase):
    def test_report_merges_probe_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dqa_root = root / "dqa"
            year_dir = dqa_root / "semantic" / "year=2026"
            research_root = root / "research"
            common = {
                "date": "2026-03-13",
                "year": "2026",
                "scope": "test",
                "confidence": "low",
                "blocking_level": "blocking",
                "tested_rows": 10,
                "pass_rows": 5,
                "fail_rows": 0,
                "unknown_rows": 5,
                "summary": "ok",
                "evidence_path": "x",
            }
            write_parquet(year_dir / "semantic_orderid_lifecycle_daily.parquet", [{**common, "semantic_area": "orderid_lifecycle", "status": "weak_pass", "admissibility_impact": "allow_with_caveat"}])
            write_parquet(year_dir / "semantic_tradedir_daily.parquet", [{**common, "semantic_area": "tradedir", "status": "unknown", "admissibility_impact": "requires_manual_review"}])
            write_parquet(year_dir / "semantic_ordertype_daily.parquet", [{**common, "semantic_area": "ordertype", "status": "weak_pass", "admissibility_impact": "allow_with_caveat"}])
            write_parquet(year_dir / "semantic_session_daily.parquet", [{**common, "semantic_area": "session", "status": "not_run", "admissibility_impact": "requires_session_split"}])

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.semantic_report",
                    "--year",
                    "2026",
                    "--input-root",
                    str(dqa_root),
                    "--research-root",
                    str(research_root),
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            daily = pl.read_parquet(year_dir / "semantic_daily_summary.parquet")
            yearly = pl.read_parquet(year_dir / "semantic_yearly_summary.parquet")
            bridge = pl.read_parquet(year_dir / "semantic_admissibility_bridge.parquet")
            self.assertEqual(daily.height, 4)
            self.assertEqual(yearly.height, 4)
            self.assertEqual(bridge.height, 11)
            self.assertIn("research_module", bridge.columns)
            self.assertIn("final_research_status", bridge.columns)


if __name__ == "__main__":
    unittest.main()
