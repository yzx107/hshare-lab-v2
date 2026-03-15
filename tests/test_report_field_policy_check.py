from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class ReportFieldPolicyCheckTests(unittest.TestCase):
    def test_script_flags_unverified_fields_and_avoid_phrases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_path = root / "report.md"
            output_path = root / "summary.json"
            report_path.write_text(
                "\n".join(
                    [
                        "# Test Report",
                        "",
                        "We study `OrderId`, `Dir`, `BrokerNo`, and `BidOrderID`.",
                        "This draft should avoid saying confirmed official mapping.",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.report_field_policy_check",
                    "--report",
                    str(report_path),
                    "--output-json",
                    str(output_path),
                ],
                cwd=str(REPO_ROOT),
                check=True,
            )

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            mentioned = {item["field"] for item in payload["field_mentions"]}
            self.assertIn("OrderId", mentioned)
            self.assertIn("Dir", mentioned)
            self.assertIn("BrokerNo", mentioned)
            self.assertIn("BidOrderID", mentioned)
            self.assertIn("Dir", payload["mentioned_unverified_fields"])
            self.assertIn("confirmed_official_mapping", payload["avoid_phrase_hits"])


if __name__ == "__main__":
    unittest.main()
