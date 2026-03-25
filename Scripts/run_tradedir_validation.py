from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, ensure_dir, iso_utc_now, print_scaffold_plan, write_json

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
DEFAULT_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"
DEFAULT_NOTES_ROOT = REPO_ROOT / "Research" / "Notes"
DEFAULT_VALIDATION_ROOT = REPO_ROOT / "Research" / "Validation"

DECISION_CANDIDATE_ONLY = "candidate_directional_signal_only"
DECISION_STRUCTURE_ONLY = "stable_code_structure_only"
DECISION_NOT_READY = "not_ready"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize TradeDir research readiness from existing semantic artifacts.")
    parser.add_argument("--year", required=False, help="Year such as 2025 or 2026.")
    parser.add_argument("--dqa-root", type=Path, default=DEFAULT_DQA_ROOT)
    parser.add_argument("--audits-root", type=Path, default=DEFAULT_AUDITS_ROOT)
    parser.add_argument("--notes-root", type=Path, default=DEFAULT_NOTES_ROOT)
    parser.add_argument("--validation-root", type=Path, default=DEFAULT_VALIDATION_ROOT)
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def read_single_parquet_row(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    frame = pl.read_parquet(path)
    if frame.is_empty():
        return None
    return frame.to_dicts()[0]


def load_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def format_float(value: float | None, digits: int = 6) -> str:
    if value is None:
        return "na"
    text = f"{value:.{digits}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def parse_contrast_witnesses(markdown_path: Path) -> list[dict[str, Any]]:
    text = load_text(markdown_path)
    if not text:
        return []
    witnesses: list[dict[str, Any]] = []
    current_date: str | None = None
    current_uptick_gap: float | None = None
    current_bucket_summary: str | None = None
    current_dir0_specialness: float | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("### "):
            if current_date is not None:
                witnesses.append(
                    {
                        "date": current_date,
                        "dir1_vs_dir2_uptick_gap": current_uptick_gap,
                        "dir1_vs_dir2_bucket_uptick_gap_summary": current_bucket_summary,
                        "dir0_specialness": current_dir0_specialness,
                    }
                )
            current_date = line.removeprefix("### ").strip()
            current_uptick_gap = None
            current_bucket_summary = None
            current_dir0_specialness = None
            continue
        if line.startswith("- `dir1_vs_dir2_uptick_gap = "):
            value = line.removeprefix("- `dir1_vs_dir2_uptick_gap = ").removesuffix("`")
            current_uptick_gap = safe_float(value)
            continue
        if line.startswith("- `dir1_vs_dir2_bucket_uptick_gap_summary = "):
            current_bucket_summary = line.removeprefix("- `dir1_vs_dir2_bucket_uptick_gap_summary = ").removesuffix("`")
            continue
        if line.startswith("- `dir0_specialness = "):
            value = line.removeprefix("- `dir0_specialness = ").removesuffix("`")
            current_dir0_specialness = safe_float(value)
    if current_date is not None:
        witnesses.append(
            {
                "date": current_date,
                "dir1_vs_dir2_uptick_gap": current_uptick_gap,
                "dir1_vs_dir2_bucket_uptick_gap_summary": current_bucket_summary,
                "dir0_specialness": current_dir0_specialness,
            }
        )
    return witnesses


def parse_contrast_markdown_summary(markdown_path: Path) -> dict[str, Any] | None:
    text = load_text(markdown_path)
    if not text:
        return None
    patterns = {
        "status": r"- status: `([^`]+)`",
        "admissibility_impact": r"- admissibility_impact: `([^`]+)`",
        "observed_dir_values": r"- observed_dir_values: `([^`]+)`",
        "dir1_vs_dir2_uptick_gap_avg": r"- `dir1_vs_dir2_uptick_gap_avg = ([^`\n]+)`",
        "dir1_vs_dir2_linkage_gap_avg": r"- `dir1_vs_dir2_linkage_gap_avg = ([^`\n]+)`",
        "dir1_vs_dir2_uptick_gap_sign_consistent_flag": r"- `dir1_vs_dir2_uptick_gap_sign_consistent_flag = ([^`\n]+)`",
        "dir1_vs_dir2_bucket_uptick_consistent_day_count": r"- `dir1_vs_dir2_bucket_uptick_consistent_day_count = ([^`\n]+)`",
        "dir0_specialness_score": r"- `dir0_specialness_score = ([^`\n]+)`",
    }
    parsed: dict[str, Any] = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if not match:
            continue
        parsed[key] = match.group(1).strip()
    if not parsed:
        return None
    if "dir1_vs_dir2_uptick_gap_avg" in parsed:
        parsed["dir1_vs_dir2_uptick_gap_avg"] = safe_float(parsed["dir1_vs_dir2_uptick_gap_avg"])
    if "dir1_vs_dir2_linkage_gap_avg" in parsed:
        parsed["dir1_vs_dir2_linkage_gap_avg"] = safe_float(parsed["dir1_vs_dir2_linkage_gap_avg"])
    if "dir0_specialness_score" in parsed:
        parsed["dir0_specialness_score"] = safe_float(parsed["dir0_specialness_score"])
    if "dir1_vs_dir2_bucket_uptick_consistent_day_count" in parsed:
        parsed["dir1_vs_dir2_bucket_uptick_consistent_day_count"] = safe_int(parsed["dir1_vs_dir2_bucket_uptick_consistent_day_count"])
    if "dir1_vs_dir2_uptick_gap_sign_consistent_flag" in parsed:
        parsed["dir1_vs_dir2_uptick_gap_sign_consistent_flag"] = parsed["dir1_vs_dir2_uptick_gap_sign_consistent_flag"].lower() == "true"
    return parsed


def collect_probe_inputs(
    *,
    year: str,
    dqa_root: Path,
    audits_root: Path,
    notes_root: Path,
) -> dict[str, Any]:
    semantic_dir = dqa_root / "semantic" / f"year={year}"
    tradedir_summary_path = semantic_dir / "semantic_tradedir_summary.parquet"
    contrast_summary_path = semantic_dir / "semantic_tradedir_contrast_summary.parquet"
    tradedir_summary = read_single_parquet_row(tradedir_summary_path)
    contrast_summary = read_single_parquet_row(contrast_summary_path)

    doc_note_path = notes_root / "vendor_hkex_doc_analysis_2026-03-15.md"
    doc_note_text = load_text(doc_note_path)
    doc_vendor_defined_flag = "`Dir` | vendor-defined" in doc_note_text or "Dir` | vendor-defined" in doc_note_text
    doc_no_aggressor_flag = "不可把 vendor `Dir=1/2/0` 直接升级成官方 aggressor-side truth" in doc_note_text
    doc_export_fields_flag = "`Dir`" in doc_note_text and "vendor `ReadMe.txt`" in doc_note_text

    tradedir_report_path = audits_root / f"semantic_tradedir_{year}.md"
    tradedir_report_text = load_text(tradedir_report_path)
    report_distinct_match = re.search(r"distinct_tradedir_values: (\d+)", tradedir_report_text)
    report_distinct_values = safe_int(report_distinct_match.group(1)) if report_distinct_match else None

    contrast_report_path = audits_root / f"semantic_tradedir_contrast_{year}.md"
    contrast_report_text = load_text(contrast_report_path)
    contrast_candidate_flag = "`candidate_directional_signal`" in contrast_report_text
    contrast_manual_review_flag = "`requires_manual_review`" in contrast_report_text
    witnesses = parse_contrast_witnesses(contrast_report_path)
    if contrast_summary is None or not contrast_summary.get("status"):
        contrast_summary = parse_contrast_markdown_summary(contrast_report_path)
        if contrast_summary is not None and "days_run" not in contrast_summary:
            contrast_summary["days_run"] = len(witnesses) or None

    return {
        "semantic_dir": semantic_dir,
        "tradedir_summary_path": tradedir_summary_path,
        "contrast_summary_path": contrast_summary_path,
        "tradedir_report_path": tradedir_report_path,
        "contrast_report_path": contrast_report_path,
        "doc_note_path": doc_note_path,
        "doc_vendor_defined_flag": doc_vendor_defined_flag,
        "doc_no_aggressor_flag": doc_no_aggressor_flag,
        "doc_export_fields_flag": doc_export_fields_flag,
        "tradedir_summary": tradedir_summary,
        "contrast_summary": contrast_summary,
        "report_distinct_values": report_distinct_values,
        "contrast_candidate_flag": contrast_candidate_flag,
        "contrast_manual_review_flag": contrast_manual_review_flag,
        "witnesses": witnesses,
    }


def choose_decision(inputs: dict[str, Any]) -> tuple[str, str]:
    tradedir_summary = inputs["tradedir_summary"] or {}
    contrast_summary = inputs["contrast_summary"] or {}
    tradedir_status = tradedir_summary.get("status")
    tradedir_nonnull_rate_avg = safe_float(tradedir_summary.get("tradedir_nonnull_rate_avg"))
    contrast_status = contrast_summary.get("status")
    contrast_manual_review = contrast_summary.get("admissibility_impact") == "requires_manual_review" or inputs["contrast_manual_review_flag"]
    if (
        tradedir_status == "weak_pass"
        and tradedir_nonnull_rate_avg == 1.0
        and contrast_status == "candidate_directional_signal"
        and contrast_manual_review
        and inputs["doc_no_aggressor_flag"]
    ):
        return (
            DECISION_CANDIDATE_ONLY,
            "TradeDir is a stable vendor direction code with consistent contrast evidence, but signed-side mapping remains blocked.",
        )
    if tradedir_status in {"weak_pass", "pass"} and tradedir_nonnull_rate_avg == 1.0:
        return (
            DECISION_STRUCTURE_ONLY,
            "TradeDir is structurally stable enough for descriptive checks, but not ready for directional truth claims.",
        )
    return (
        DECISION_NOT_READY,
        "TradeDir validation inputs are incomplete or unstable; keep it out of directional research flows.",
    )


def build_payload(year: str, inputs: dict[str, Any]) -> dict[str, Any]:
    tradedir_summary = inputs["tradedir_summary"] or {}
    contrast_summary = inputs["contrast_summary"] or {}
    decision, rationale = choose_decision(inputs)
    return {
        "generated_at": iso_utc_now(),
        "year": year,
        "decision": decision,
        "decision_rationale": rationale,
        "signed_flow_status": "blocked",
        "aggressor_side_status": "blocked",
        "manual_review_required": True,
        "doc_evidence": {
            "note_path": str(inputs["doc_note_path"]),
            "vendor_defined_flag": inputs["doc_vendor_defined_flag"],
            "no_aggressor_truth_flag": inputs["doc_no_aggressor_flag"],
            "vendor_export_field_flag": inputs["doc_export_fields_flag"],
        },
        "semantic_probe": {
            "summary_path": str(inputs["tradedir_summary_path"]),
            "report_path": str(inputs["tradedir_report_path"]),
            "status": tradedir_summary.get("status"),
            "admissibility_impact": tradedir_summary.get("admissibility_impact"),
            "days_run": safe_int(tradedir_summary.get("days_run")),
            "tested_rows_total": safe_int(tradedir_summary.get("tested_rows_total")),
            "tradedir_nonnull_rate_avg": safe_float(tradedir_summary.get("tradedir_nonnull_rate_avg")),
            "distinct_tradedir_values": inputs["report_distinct_values"],
            "linked_side_consistency_rate_avg": safe_float(tradedir_summary.get("linked_side_consistency_rate_avg")),
        },
        "contrast_probe": {
            "summary_path": str(inputs["contrast_summary_path"]),
            "report_path": str(inputs["contrast_report_path"]),
            "status": contrast_summary.get("status"),
            "admissibility_impact": contrast_summary.get("admissibility_impact"),
            "days_run": safe_int(contrast_summary.get("days_run")),
            "observed_dir_values": contrast_summary.get("observed_dir_values"),
            "dir1_vs_dir2_uptick_gap_avg": safe_float(contrast_summary.get("dir1_vs_dir2_uptick_gap_avg")),
            "dir1_vs_dir2_linkage_gap_avg": safe_float(contrast_summary.get("dir1_vs_dir2_linkage_gap_avg")),
            "dir1_vs_dir2_uptick_gap_sign_consistent_flag": contrast_summary.get("dir1_vs_dir2_uptick_gap_sign_consistent_flag"),
            "dir1_vs_dir2_bucket_uptick_consistent_day_count": safe_int(contrast_summary.get("dir1_vs_dir2_bucket_uptick_consistent_day_count")),
            "dir0_specialness_score": safe_float(contrast_summary.get("dir0_specialness_score")),
        },
        "allowed_research_uses": [
            "descriptive distribution checks",
            "candidate directional signal exploration with explicit caveat",
            "manual-review-gated feature ideation",
        ],
        "blocked_research_uses": [
            "signed flow factor production",
            "aggressor-side truth labeling",
            "verified-layer default admission",
        ],
        "witness_dates": inputs["witnesses"],
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    semantic_probe = payload["semantic_probe"]
    contrast_probe = payload["contrast_probe"]
    doc_evidence = payload["doc_evidence"]
    lines = [
        f"# TradeDir Validation {payload['year']}",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- decision: `{payload['decision']}`",
        f"- manual_review_required: `{str(payload['manual_review_required']).lower()}`",
        f"- signed_flow_status: `{payload['signed_flow_status']}`",
        f"- aggressor_side_status: `{payload['aggressor_side_status']}`",
        f"- rationale: {payload['decision_rationale']}",
        "",
        "## Evidence",
        "",
        f"- doc note: [{Path(doc_evidence['note_path']).name}]({doc_evidence['note_path']})",
        f"- vendor_defined_flag: `{str(doc_evidence['vendor_defined_flag']).lower()}`",
        f"- no_aggressor_truth_flag: `{str(doc_evidence['no_aggressor_truth_flag']).lower()}`",
        f"- vendor_export_field_flag: `{str(doc_evidence['vendor_export_field_flag']).lower()}`",
        f"- semantic probe status: `{semantic_probe['status']}`",
        f"- semantic probe days_run: `{semantic_probe['days_run']}`",
        f"- semantic nonnull avg: `{format_float(semantic_probe['tradedir_nonnull_rate_avg'])}`",
        f"- semantic distinct values: `{semantic_probe['distinct_tradedir_values']}`",
        f"- contrast probe status: `{contrast_probe['status']}`",
        f"- contrast days_run: `{contrast_probe['days_run']}`",
        f"- contrast uptick gap avg: `{format_float(contrast_probe['dir1_vs_dir2_uptick_gap_avg'])}`",
        f"- contrast linkage gap avg: `{format_float(contrast_probe['dir1_vs_dir2_linkage_gap_avg'])}`",
        "",
        "## Boundary",
        "",
        "- Allowed:",
    ]
    for item in payload["allowed_research_uses"]:
        lines.append(f"  - {item}")
    lines.extend(["- Blocked:"])
    for item in payload["blocked_research_uses"]:
        lines.append(f"  - {item}")
    if payload["witness_dates"]:
        lines.extend(["", "## Witness Dates", ""])
        for witness in payload["witness_dates"]:
            lines.extend(
                [
                    f"- `{witness['date']}` uptick_gap=`{format_float(safe_float(witness['dir1_vs_dir2_uptick_gap']))}` dir0_specialness=`{format_float(safe_float(witness['dir0_specialness']))}`",
                    f"  bucket_gap_summary: `{witness['dir1_vs_dir2_bucket_uptick_gap_summary'] or 'na'}`",
                ]
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_tradedir_validation",
            purpose="Summarize TradeDir research readiness from existing semantic outputs and document anchors.",
            responsibilities=[
                "Read checked-in document anchors about vendor Dir semantics.",
                "Read existing TradeDir semantic and contrast summaries.",
                "Emit a compact go/no-go style validation summary without re-running heavy probes.",
            ],
            inputs=[
                "Research/Notes/vendor_hkex_doc_analysis_2026-03-15.md",
                "dqa/semantic/year=<year>/semantic_tradedir_summary.parquet",
                "dqa/semantic/year=<year>/semantic_tradedir_contrast_summary.parquet",
            ],
            outputs=[
                "Research/Validation/tradedir_validation_<year>.md",
                "Research/Validation/tradedir_validation_<year>.json",
            ],
        )
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")
    inputs = collect_probe_inputs(
        year=str(args.year),
        dqa_root=args.dqa_root,
        audits_root=args.audits_root,
        notes_root=args.notes_root,
    )
    payload = build_payload(str(args.year), inputs)
    markdown_path = args.validation_root / f"tradedir_validation_{args.year}.md"
    json_path = args.validation_root / f"tradedir_validation_{args.year}.json"
    write_markdown(markdown_path, payload)
    write_json(json_path, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
