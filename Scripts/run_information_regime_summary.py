from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from Scripts.runtime import ensure_dir, iso_utc_now, print_scaffold_plan, write_json

REPO_ROOT = Path(__file__).resolve().parents[1]

METHODS = ("entropy", "mutual_information", "transfer_entropy")
DEFAULT_ALLOWED_FIELDS = [
    "Price",
    "Volume",
    "instrument_key",
    "Time",
    "OrderId/TickID/SeqNum (grouping/indexing only)",
]
CAVEAT_FIELDS = [
    "Dir",
    "OrderType",
    "OrderSideVendor",
    "Type",
]
BLOCKED_FIELDS = [
    "BrokerNo",
    "Level",
    "VolumePre",
    "BidOrderID/AskOrderID",
    "verified_trade_order_linkage",
    "Ext(full field)",
    "queue/depth/latency-like derived state",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按年份输出信息论方法的 admissibility / feasibility 摘要。")
    parser.add_argument("--year", required=True, choices=["2025", "2026"])
    parser.add_argument("--method", default="all", choices=["all", *METHODS])
    parser.add_argument("--output-root", type=Path, default=None, help="可选输出目录；若不传则打印到 stdout。")
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def research_time_grade(year: str) -> str:
    return "fine_ok" if year == "2026" else "coarse_only"


def default_fields(year: str) -> list[str]:
    values = list(DEFAULT_ALLOWED_FIELDS)
    if year == "2026":
        values.insert(4, "SendTime")
    return values


def method_rule(year: str, method: str) -> dict[str, Any]:
    if year == "2025":
        if method == "entropy":
            return {
                "status": "allowed_with_caveat",
                "allowed_layer": "verified default; caveat lane only with explicit label",
                "summary_wording": "coarse uncertainty/concentration or coarse dependence regime",
                "blocked_wording": "fine-grained timing regime or directional information-flow",
            }
        if method == "mutual_information":
            return {
                "status": "allowed_with_caveat",
                "allowed_layer": "verified default; caveat lane only with explicit label",
                "summary_wording": "coarse temporal dependence or coarse co-movement",
                "blocked_wording": "fine lead-lag or directional information-flow",
            }
        return {
            "status": "blocked",
            "allowed_layer": "not allowed for formal downstream use",
            "summary_wording": "none",
            "blocked_wording": "transfer entropy / directional information-flow",
        }
    if method == "transfer_entropy":
        return {
            "status": "allowed_with_caveat",
            "allowed_layer": "verified only, with SendTime and explicit boundary metadata",
            "summary_wording": "directional information-flow summary under project-level admissible fields",
            "blocked_wording": "causal truth, confirmed signed-flow causality, queue/latency mechanism proof",
        }
    if method == "mutual_information":
        return {
            "status": "allowed",
            "allowed_layer": "verified default; caveat lane allowed with explicit label",
            "summary_wording": "fine-grained temporal dependence or lead-lag dependence summary",
            "blocked_wording": "official semantic proof or queue/depth mechanism claim",
        }
    return {
        "status": "allowed",
        "allowed_layer": "verified default; caveat lane allowed with explicit label",
        "summary_wording": "fine-grained entropy / uncertainty regime summary",
        "blocked_wording": "official semantic proof or queue/depth mechanism claim",
    }


def selected_methods(method_arg: str) -> list[str]:
    return list(METHODS) if method_arg == "all" else [method_arg]


def build_payload(year: str, method_arg: str) -> dict[str, Any]:
    methods = selected_methods(method_arg)
    return {
        "generated_at": iso_utc_now(),
        "year": year,
        "research_time_grade": research_time_grade(year),
        "formal_input_layer": "verified_only",
        "stage_usage": "internal_feasibility_only",
        "default_allowed_fields": default_fields(year),
        "caveat_fields": CAVEAT_FIELDS,
        "blocked_fields": BLOCKED_FIELDS,
        "methods": {method: method_rule(year, method) for method in methods},
        "downstream_requirements": [
            "record year and source_layer",
            "record input_tables and input_field_class",
            "record time resolution, window definition, and discretization rule",
            "record lag grid for transfer entropy style summaries",
            "record null/drop rule and special bucket handling",
            "record sample sizes and generated_at",
        ],
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# 信息论 Regime 摘要 {payload['year']}",
        "",
        f"- 生成时间: {payload['generated_at']}",
        f"- research_time_grade: `{payload['research_time_grade']}`",
        f"- formal_input_layer: `{payload['formal_input_layer']}`",
        f"- stage_usage: `{payload['stage_usage']}`",
        "",
        "## 默认允许字段",
        "",
    ]
    lines.extend(f"- `{value}`" for value in payload["default_allowed_fields"])
    lines.extend(
        [
            "",
            "## Caveat 字段",
            "",
        ]
    )
    lines.extend(f"- `{value}`" for value in payload["caveat_fields"])
    lines.extend(
        [
            "",
            "## Blocked 字段",
            "",
        ]
    )
    lines.extend(f"- `{value}`" for value in payload["blocked_fields"])
    lines.extend(
        [
            "",
            "## 方法规则",
            "",
        ]
    )
    for method, rule in payload["methods"].items():
        lines.extend(
            [
                f"### {method}",
                f"- status: `{rule['status']}`",
                f"- allowed_layer: `{rule['allowed_layer']}`",
                f"- summary_wording: `{rule['summary_wording']}`",
                f"- blocked_wording: `{rule['blocked_wording']}`",
                "",
            ]
        )
    lines.extend(
        [
            "## 正式消费要求",
            "",
        ]
    )
    lines.extend(f"- {value}" for value in payload["downstream_requirements"])
    return "\n".join(lines).rstrip() + "\n"


def output_paths(output_root: Path, year: str) -> tuple[Path, Path]:
    return (
        output_root / f"information_regime_summary_{year}.md",
        output_root / f"information_regime_summary_{year}.json",
    )


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_information_regime_summary",
            purpose="输出信息论方法的年份边界与 feasibility 摘要，不进入因子研究。",
            responsibilities=[
                "输出按年份划分的 entropy / MI / TE regime summary。",
                "显式保留 `2025 coarse_only / 2026 fine_ok` 年度边界。",
                "说明 default、caveat、blocked 三类字段 lane。",
            ],
            inputs=[
                "Research/Validation/information_theory_admissibility.md",
                "Research/Audits/research_admissibility_matrix.md",
                "Research/Validation/verified_admission_matrix_2026-03-18.md",
            ],
            outputs=[
                "stdout markdown/json summary",
                "optional information_regime_summary_<year>.md/json",
            ],
        )
        return 0

    payload = build_payload(args.year, args.method)
    markdown = render_markdown(payload)
    if args.output_root is not None:
        ensure_dir(args.output_root)
        markdown_path, json_path = output_paths(args.output_root, args.year)
        markdown_path.write_text(markdown, encoding="utf-8")
        write_json(json_path, payload)
        print(json.dumps({"markdown_path": str(markdown_path), "json_path": str(json_path)}, ensure_ascii=False))
        return 0

    print(markdown, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
