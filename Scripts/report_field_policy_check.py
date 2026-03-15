from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from Scripts.runtime import iso_utc_now, print_scaffold_plan, write_json

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_PATH = REPO_ROOT / "Research" / "Validation" / "field_policy_2026-03-15.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check a markdown report against field policy guidance.")
    parser.add_argument("--report", type=Path, help="Path to the markdown report to inspect.")
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def load_policy(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def detect_field_mentions(text: str, policy: dict[str, Any]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for table_name in ("orders", "trades"):
        table_policy = policy[table_name]
        for field_name, field_policy in table_policy.items():
            pattern = re.compile(rf"(?<![A-Za-z0-9_])`?{re.escape(field_name)}`?(?![A-Za-z0-9_])")
            count = len(pattern.findall(text))
            if count == 0:
                continue
            matches.append(
                {
                    "table": table_name,
                    "field": field_name,
                    "count": count,
                    "statuses": field_policy["statuses"],
                    "allowed_uses": field_policy["allowed_uses"],
                    "forbidden_claims": field_policy["forbidden_claims"],
                }
            )
    return sorted(matches, key=lambda item: (item["table"], item["field"]))


def detect_avoid_phrases(text: str, policy: dict[str, Any]) -> list[str]:
    normalized = re.sub(r"\s+", " ", text).lower()
    hits: list[str] = []
    for raw in policy["wording_preferences"]["avoid"]:
        phrase = raw.replace("_", " ")
        if phrase in normalized:
            hits.append(raw)
    return hits


def build_summary(report_path: Path, policy_path: Path, policy: dict[str, Any], text: str) -> dict[str, Any]:
    field_mentions = detect_field_mentions(text, policy)
    avoid_hits = detect_avoid_phrases(text, policy)
    summary = {
        "generated_at": iso_utc_now(),
        "report_path": str(report_path),
        "policy_path": str(policy_path),
        "field_mentions": field_mentions,
        "avoid_phrase_hits": avoid_hits,
        "mentioned_unverified_fields": [
            item["field"]
            for item in field_mentions
            if "unverified-semantic" in item["statuses"]
        ],
        "mentioned_vendor_defined_fields": [
            item["field"]
            for item in field_mentions
            if "vendor-defined" in item["statuses"]
        ],
    }
    return summary


def print_summary(summary: dict[str, Any]) -> None:
    print(f"# Field Policy Check")
    print("")
    print(f"- generated_at: {summary['generated_at']}")
    print(f"- report_path: {summary['report_path']}")
    print(f"- policy_path: {summary['policy_path']}")
    print(f"- mentioned_fields: {len(summary['field_mentions'])}")
    print(f"- mentioned_unverified_fields: {len(summary['mentioned_unverified_fields'])}")
    print(f"- avoid_phrase_hits: {len(summary['avoid_phrase_hits'])}")
    if summary["field_mentions"]:
        print("")
        print("## Mentioned Fields")
        for item in summary["field_mentions"]:
            statuses = ", ".join(item["statuses"])
            print(f"- {item['table']}.{item['field']}: count={item['count']}; statuses={statuses}")
    if summary["avoid_phrase_hits"]:
        print("")
        print("## Avoid Phrase Hits")
        for item in summary["avoid_phrase_hits"]:
            print(f"- {item}")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="report_field_policy_check",
            purpose="Inspect a markdown report for field mentions and policy-sensitive wording.",
            responsibilities=[
                "Load the machine-readable field policy file.",
                "Detect order/trade field mentions in a report.",
                "Flag mentioned unverified fields and discouraged wording.",
            ],
            inputs=[
                "Research report markdown",
                "Research/Validation/field_policy_2026-03-15.json",
            ],
            outputs=[
                "stdout summary",
                "optional JSON summary via --output-json",
            ],
        )
        return 0
    if not args.report:
        raise SystemExit("--report is required unless --print-plan is used.")
    policy = load_policy(args.policy)
    text = args.report.read_text(encoding="utf-8")
    summary = build_summary(args.report, args.policy, policy, text)
    print_summary(summary)
    if args.output_json is not None:
        write_json(args.output_json, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
