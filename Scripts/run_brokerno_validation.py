from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, ensure_dir, iso_utc_now, print_scaffold_plan, write_json
from Scripts.semantic_contract import parse_selected_dates

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_VALIDATION_ROOT = REPO_ROOT / "Research" / "Validation"
DEFAULT_NOTES_ROOT = REPO_ROOT / "Research" / "Notes"
DEFAULT_REFERENCE_ROOT = REPO_ROOT / "Research" / "References" / "normalized"

DECISION_LOOKUP_ONLY = "reference_lookup_only"
DECISION_NOT_ALPHA_READY = "not_alpha_ready"
DECISION_INSUFFICIENT = "insufficient_evidence"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a lightweight BrokerNo readiness smoke using existing policy docs and a few real dates.")
    parser.add_argument("--year", required=False, help="Year such as 2025 or 2026.")
    parser.add_argument("--dates")
    parser.add_argument("--max-days", type=int, default=3)
    parser.add_argument("--latest-days", action="store_true", default=True)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--validation-root", type=Path, default=DEFAULT_VALIDATION_ROOT)
    parser.add_argument("--notes-root", type=Path, default=DEFAULT_NOTES_ROOT)
    parser.add_argument("--reference-root", type=Path, default=DEFAULT_REFERENCE_ROOT)
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def load_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def safe_rate(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return numerator / denominator


def format_float(value: float | None, digits: int = 6) -> str:
    if value is None:
        return "na"
    text = f"{value:.{digits}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def load_reference_codes(path: Path) -> set[str]:
    if not path.exists():
        return set()
    rows = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            value = line.split(",", 1)[0].strip()
            if value:
                rows.append(value.zfill(4) if value.isdigit() else value)
    return set(rows)


def normalize_code_expr() -> pl.Expr:
    return (
        pl.col("BrokerNo")
        .cast(pl.Utf8)
        .str.strip_chars()
        .alias("broker_code_raw")
    )


def investigate_table(
    *,
    table_name: str,
    trade_date: str,
    stage_root: Path,
    reference_codes: set[str],
    limit_rows: int,
) -> dict[str, Any]:
    paths = sorted((stage_root / table_name / f"date={trade_date}").glob("*.parquet"))
    if not paths:
        return {
            "table_name": table_name,
            "date": trade_date,
            "tested_rows": 0,
            "nonnull_rows": 0,
            "zero_rows": 0,
            "distinct_codes": 0,
            "matched_distinct_codes": 0,
            "unmatched_distinct_codes": 0,
            "zero_rate": None,
            "nonnull_rate": None,
            "matched_distinct_rate": None,
            "top_unmatched_codes": [],
        }
    frame = pl.scan_parquet([str(path) for path in paths]).select(["BrokerNo"])
    if limit_rows > 0:
        frame = frame.limit(limit_rows)
    normalized = (
        frame.with_columns(
            [
                normalize_code_expr(),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("broker_code_raw").is_null() | (pl.col("broker_code_raw") == ""))
                .then(pl.lit(None, dtype=pl.Utf8))
                .when(pl.col("broker_code_raw").str.contains(r"^\d+$"))
                .then(pl.col("broker_code_raw").str.zfill(4))
                .otherwise(pl.col("broker_code_raw"))
                .alias("broker_code"),
            ]
        )
    )
    stats = normalized.select(
        [
            pl.len().alias("tested_rows"),
            pl.col("broker_code").is_not_null().sum().alias("nonnull_rows"),
            pl.col("broker_code").is_in(["0", "0000"]).sum().alias("zero_rows"),
            pl.col("broker_code").drop_nulls().n_unique().alias("distinct_codes"),
        ]
    ).collect().to_dicts()[0]
    code_counts = (
        normalized.filter(pl.col("broker_code").is_not_null())
        .group_by("broker_code")
        .agg(pl.len().alias("row_count"))
        .sort(["row_count", "broker_code"], descending=[True, False])
        .collect()
    )
    code_rows = code_counts.to_dicts()
    matched_distinct = 0
    unmatched: list[dict[str, Any]] = []
    for row in code_rows:
        code = str(row["broker_code"])
        if code in reference_codes:
            matched_distinct += 1
        else:
            unmatched.append({"broker_code": code, "row_count": int(row["row_count"] or 0)})
    tested_rows = int(stats["tested_rows"] or 0)
    nonnull_rows = int(stats["nonnull_rows"] or 0)
    distinct_codes = int(stats["distinct_codes"] or 0)
    zero_rows = int(stats["zero_rows"] or 0)
    return {
        "table_name": table_name,
        "date": trade_date,
        "tested_rows": tested_rows,
        "nonnull_rows": nonnull_rows,
        "zero_rows": zero_rows,
        "distinct_codes": distinct_codes,
        "matched_distinct_codes": matched_distinct,
        "unmatched_distinct_codes": max(distinct_codes - matched_distinct, 0),
        "zero_rate": safe_rate(zero_rows, tested_rows),
        "nonnull_rate": safe_rate(nonnull_rows, tested_rows),
        "matched_distinct_rate": safe_rate(matched_distinct, distinct_codes),
        "top_unmatched_codes": unmatched[:10],
    }


def collect_inputs(
    *,
    year: str,
    notes_root: Path,
    reference_root: Path,
) -> dict[str, Any]:
    field_status_text = load_text(notes_root / "field_status_matrix_2026-03-15.md")
    zero_hypothesis_text = load_text(notes_root / "brokerno_zero_external_hypotheses.md")
    readonly_boundary_text = load_text(REPO_ROOT / "Research" / "Validation" / "broker_reference_readonly_boundary_2026-03-17.md")
    reference_codes = load_reference_codes(reference_root / "brokerno.utf8.csv")
    return {
        "field_status_vendor_defined": "`BrokerNo` | `vendor_defined` + `unverified_semantic`" in field_status_text,
        "field_status_no_official": "已确认等于官方 `BrokerID`" in field_status_text,
        "zero_not_global_fact": "`BrokerNo=0` 目前仍不应被提升成全项目通用、已验证的 broker 语义事实。" in zero_hypothesis_text,
        "query_safe_unattributed": "`unattributed / no-seat-record`" in zero_hypothesis_text,
        "readonly_boundary_lookup_only": "lookup enrichments" in readonly_boundary_text and "do not by themselves prove" in readonly_boundary_text,
        "reference_codes": reference_codes,
        "reference_path": reference_root / "brokerno.utf8.csv",
    }


def choose_decision(rows: list[dict[str, Any]], doc_inputs: dict[str, Any]) -> tuple[str, str]:
    any_rows = any(int(row["tested_rows"] or 0) > 0 for row in rows)
    if not any_rows:
        return DECISION_INSUFFICIENT, "No BrokerNo rows were available for the selected smoke dates."
    high_zero = any((row["zero_rate"] or 0.0) > 0.05 for row in rows)
    imperfect_match = any((row["matched_distinct_rate"] or 0.0) < 0.95 for row in rows if (row["distinct_codes"] or 0) > 0)
    if doc_inputs["field_status_vendor_defined"] and doc_inputs["zero_not_global_fact"] and doc_inputs["readonly_boundary_lookup_only"]:
        if high_zero or imperfect_match:
            return DECISION_LOOKUP_ONLY, "BrokerNo is suitable for lookup enrichment and ambiguity tracking, but not for direct broker-alpha semantics."
        return DECISION_NOT_ALPHA_READY, "BrokerNo coverage looks decent on smoke dates, but policy and semantic boundaries still keep it out of direct alpha use."
    return DECISION_INSUFFICIENT, "BrokerNo policy anchors were incomplete; keep it out of semantic upgrades."


def build_payload(year: str, selected_dates: list[str], rows: list[dict[str, Any]], doc_inputs: dict[str, Any]) -> dict[str, Any]:
    decision, rationale = choose_decision(rows, doc_inputs)
    return {
        "generated_at": iso_utc_now(),
        "year": year,
        "decision": decision,
        "decision_rationale": rationale,
        "manual_review_required": True,
        "verified_default_admission": "blocked",
        "allowed_research_uses": [
            "reference lookup enrichment",
            "coverage and ambiguity analysis",
            "descriptive seat-summary style query outputs",
        ],
        "blocked_research_uses": [
            "official broker identity claims",
            "BrokerNo=0 universal semantic claims",
            "direct broker alpha production without extra validation",
        ],
        "selected_dates": selected_dates,
        "doc_evidence": {
            "field_status_vendor_defined": doc_inputs["field_status_vendor_defined"],
            "field_status_contains_official_claim_text": doc_inputs["field_status_no_official"],
            "zero_not_global_fact": doc_inputs["zero_not_global_fact"],
            "query_safe_unattributed": doc_inputs["query_safe_unattributed"],
            "readonly_boundary_lookup_only": doc_inputs["readonly_boundary_lookup_only"],
            "reference_path": str(doc_inputs["reference_path"]),
            "reference_code_count": len(doc_inputs["reference_codes"]),
        },
        "smoke_rows": rows,
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    lines = [
        f"# BrokerNo Validation {payload['year']}",
        "",
        f"- generated_at: {payload['generated_at']}",
        f"- decision: `{payload['decision']}`",
        f"- manual_review_required: `{str(payload['manual_review_required']).lower()}`",
        f"- verified_default_admission: `{payload['verified_default_admission']}`",
        f"- rationale: {payload['decision_rationale']}",
        f"- selected_dates: `{', '.join(payload['selected_dates'])}`",
        "",
        "## Evidence",
        "",
        f"- field_status_vendor_defined: `{str(payload['doc_evidence']['field_status_vendor_defined']).lower()}`",
        f"- zero_not_global_fact: `{str(payload['doc_evidence']['zero_not_global_fact']).lower()}`",
        f"- query_safe_unattributed: `{str(payload['doc_evidence']['query_safe_unattributed']).lower()}`",
        f"- readonly_boundary_lookup_only: `{str(payload['doc_evidence']['readonly_boundary_lookup_only']).lower()}`",
        f"- reference_codes: `{payload['doc_evidence']['reference_code_count']}` from [{Path(payload['doc_evidence']['reference_path']).name}]({payload['doc_evidence']['reference_path']})",
        "",
        "## Smoke",
        "",
    ]
    for row in payload["smoke_rows"]:
        lines.extend(
            [
                f"### {row['table_name']} {row['date']}",
                f"- tested_rows: `{row['tested_rows']}`",
                f"- nonnull_rate: `{format_float(row['nonnull_rate'])}`",
                f"- zero_rate: `{format_float(row['zero_rate'])}`",
                f"- distinct_codes: `{row['distinct_codes']}`",
                f"- matched_distinct_rate: `{format_float(row['matched_distinct_rate'])}`",
                f"- unmatched_distinct_codes: `{row['unmatched_distinct_codes']}`",
                f"- top_unmatched_codes: `{json.dumps(row['top_unmatched_codes'], ensure_ascii=False)}`",
                "",
            ]
        )
    lines.extend(["## Boundary", "", "- Allowed:"])
    for item in payload["allowed_research_uses"]:
        lines.append(f"  - {item}")
    lines.append("- Blocked:")
    for item in payload["blocked_research_uses"]:
        lines.append(f"  - {item}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_brokerno_validation",
            purpose="Run a lightweight BrokerNo readiness smoke using policy anchors, reference coverage, and a few real dates.",
            responsibilities=[
                "Read BrokerNo policy and reference-usage boundaries.",
                "Scan a few stage dates for BrokerNo zero-rate and reference coverage.",
                "Emit a compact readiness summary without asserting semantic truth.",
            ],
            inputs=[
                "Research/Notes/brokerno_zero_external_hypotheses.md",
                "Research/Validation/broker_reference_readonly_boundary_2026-03-17.md",
                "Research/References/normalized/brokerno.utf8.csv",
                "candidate_cleaned/orders|trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "Research/Validation/brokerno_validation_<year>.md",
                "Research/Validation/brokerno_validation_<year>.json",
            ],
        )
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")
    selected_dates = parse_selected_dates(
        stage_root=args.input_root,
        year=str(args.year),
        dates=args.dates,
        max_days=args.max_days,
        latest_days=args.latest_days,
    )
    if not selected_dates:
        raise SystemExit("No overlapping stage dates matched the requested selection.")
    doc_inputs = collect_inputs(year=str(args.year), notes_root=args.notes_root, reference_root=args.reference_root)
    rows: list[dict[str, Any]] = []
    for trade_date in selected_dates:
        rows.append(
            investigate_table(
                table_name="orders",
                trade_date=trade_date,
                stage_root=args.input_root,
                reference_codes=doc_inputs["reference_codes"],
                limit_rows=args.limit_rows,
            )
        )
        rows.append(
            investigate_table(
                table_name="trades",
                trade_date=trade_date,
                stage_root=args.input_root,
                reference_codes=doc_inputs["reference_codes"],
                limit_rows=args.limit_rows,
            )
        )
    payload = build_payload(str(args.year), selected_dates, rows, doc_inputs)
    markdown_path = args.validation_root / f"brokerno_validation_{args.year}.md"
    json_path = args.validation_root / f"brokerno_validation_{args.year}.json"
    write_markdown(markdown_path, payload)
    write_json(json_path, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
