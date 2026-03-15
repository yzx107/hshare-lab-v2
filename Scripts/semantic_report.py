from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.runtime import DEFAULT_DATA_ROOT, ensure_dir, iso_utc_now, print_scaffold_plan, write_json
from Scripts.semantic_contract import (
    ADMISSIBILITY_BRIDGE_COLUMNS,
    SEMANTIC_AREA_NAMES,
    SUMMARY_TABLE_BY_AREA,
    TOTAL_SUMMARY_COLUMNS,
    STATUS_SEVERITY,
    area_modules,
    build_empty_record,
    get_daily_columns,
)

DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_AUDITS_ROOT = REPO_ROOT / "Research" / "Audits"

AREA_DAILY_FILES = {
    "orderid_lifecycle": "semantic_orderid_lifecycle_daily.parquet",
    "tradedir": "semantic_tradedir_daily.parquet",
    "ordertype": "semantic_ordertype_daily.parquet",
    "session": "semantic_session_daily.parquet",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate semantic probe outputs into unified summaries.")
    parser.add_argument("--year", help="Year such as 2025 or 2026.")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_DQA_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_AUDITS_ROOT)
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


def safe_avg(values: list[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def read_probe_rows(year: str, input_root: Path) -> list[dict[str, Any]]:
    output_dir = input_root / "semantic" / f"year={year}"
    rows: list[dict[str, Any]] = []
    for area, file_name in AREA_DAILY_FILES.items():
        path = output_dir / file_name
        if not path.exists():
            continue
        frame = pl.read_parquet(path)
        expected = get_daily_columns(area)
        available = [column for column in expected if column in frame.columns]
        data = frame.select(available).to_dicts()
        for row in data:
            padded = build_empty_record(area, "daily")
            padded.update(row)
            rows.append(padded)
    return rows


def build_area_summary(area: str, year: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = build_empty_record(area, "summary")
    if not rows:
        base.update({"year": year, "semantic_area": area})
        return base
    worst = max(rows, key=lambda row: STATUS_SEVERITY[row["status"]])
    modules = area_modules(area)
    base.update(
        {
            "year": year,
            "semantic_area": area,
            "status": worst["status"],
            "confidence": worst["confidence"],
            "blocking_level": worst["blocking_level"],
            "days_total": len(rows),
            "days_run": len(rows),
            "days_pass": sum(1 for row in rows if row["status"] == "pass"),
            "days_weak_pass": sum(1 for row in rows if row["status"] == "weak_pass"),
            "days_fail": sum(1 for row in rows if row["status"] == "fail"),
            "days_unknown": sum(1 for row in rows if row["status"] in {"unknown", "not_run"}),
            "tested_rows_total": sum(int(row["tested_rows"] or 0) for row in rows),
            "summary": worst["summary"],
            "admissibility_impact": worst["admissibility_impact"],
            "recommended_modules": ",".join(modules["recommended"]),
            "blocked_modules": ",".join(modules["blocked"]),
        }
    )
    if area == "orderid_lifecycle":
        base.update(
            {
                "linked_orderids_total": sum(int(row["linked_orderids"] or 0) for row in rows),
                "linked_orderid_rate_avg": safe_avg([row["linked_orderid_rate"] for row in rows]),
                "orders_with_multiple_events_rate_avg": safe_avg([row["orders_with_multiple_events_rate"] for row in rows]),
                "orders_with_multiple_trades_rate_avg": safe_avg([row["orders_with_multiple_trades_rate"] for row in rows]),
                "cross_session_candidate_rate_avg": safe_avg([row["cross_session_candidate_rate"] for row in rows]),
            }
        )
    elif area == "tradedir":
        base.update(
            {
                "tradedir_nonnull_rate_avg": safe_avg([row["tradedir_nonnull_rate"] for row in rows]),
                "tradedir_zero_rate_avg": safe_avg([row["tradedir_zero_rate"] for row in rows]),
                "tradedir_pos_rate_avg": safe_avg([row["tradedir_pos_rate"] for row in rows]),
                "tradedir_neg_rate_avg": safe_avg([row["tradedir_neg_rate"] for row in rows]),
                "linked_side_consistency_rate_avg": safe_avg([row["linked_side_consistency_rate"] for row in rows]),
            }
        )
    elif area == "ordertype":
        distinct_values = sorted({int(row["distinct_ordertype_values"]) for row in rows if row["distinct_ordertype_values"] is not None})
        base.update(
            {
                "distinct_ordertype_values_union": ",".join(str(value) for value in distinct_values) if distinct_values else None,
                "single_ordertype_orderid_rate_avg": safe_avg([row["single_ordertype_orderid_rate"] for row in rows]),
                "multi_ordertype_orderid_rate_avg": safe_avg([row["multi_ordertype_orderid_rate"] for row in rows]),
                "ordertype_transition_pattern_count_total": sum(int(row["ordertype_transition_pattern_count"] or 0) for row in rows),
            }
        )
    elif area == "session":
        distinct_values = sorted({int(row["distinct_session_values"]) for row in rows if row["distinct_session_values"] is not None})
        base.update(
            {
                "distinct_session_values_union": ",".join(str(value) for value in distinct_values) if distinct_values else None,
                "cross_session_linkage_rate_avg": safe_avg([row["cross_session_linkage_rate"] for row in rows]),
                "session_time_window_consistent_day_rate": safe_avg([1.0 if row["session_time_window_consistent_flag"] is True else 0.0 if row["session_time_window_consistent_flag"] is False else None for row in rows]),
                "session_split_required_flag": True,
            }
        )
    return base


def build_total_summary(area_summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: list[dict[str, Any]] = []
    for row in area_summary_rows:
        record = {column: row.get(column) for column in TOTAL_SUMMARY_COLUMNS}
        totals.append(record)
    return totals


def build_bridge_rows(year: str, area_summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary in area_summary_rows:
        if not summary.get("semantic_area"):
            continue
        modules = area_modules(summary["semantic_area"])
        for module in modules["recommended"] + modules["blocked"]:
            final_status = "blocked" if module in modules["blocked"] and summary["admissibility_impact"] in {"block", "requires_manual_review"} else summary["admissibility_impact"]
            record = {column: None for column in ADMISSIBILITY_BRIDGE_COLUMNS}
            record.update(
                {
                    "year": year,
                    "semantic_area": summary["semantic_area"],
                    "research_module": module,
                    "semantic_status": summary["status"],
                    "blocking_level": summary["blocking_level"],
                    "admissibility_impact": summary["admissibility_impact"],
                    "final_research_status": final_status,
                    "reason": summary["summary"],
                    "notes": f"recommended={summary['recommended_modules']}; blocked={summary['blocked_modules']}",
                }
            )
            rows.append(record)
    return rows


def write_markdown(path: Path, *, year: str, area_summary_rows: list[dict[str, Any]], bridge_rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    lines = [f"# Semantic Summary {year}", "", f"- generated_at: {iso_utc_now()}", f"- semantic_areas: {len([row for row in area_summary_rows if row.get('semantic_area')])}", "", "This summary aggregates semantic probe status into admissibility-facing gating signals."]
    for row in area_summary_rows:
        if not row.get("semantic_area"):
            continue
        lines.extend(["", f"## {row['semantic_area']}", f"- status: {row['status']}", f"- confidence: {row['confidence']}", f"- blocking_level: {row['blocking_level']}", f"- admissibility_impact: {row['admissibility_impact']}", f"- summary: {row['summary']}", f"- recommended_modules: {row['recommended_modules']}", f"- blocked_modules: {row['blocked_modules']}"])
    lines.append("")
    lines.append("## Admissibility Bridge")
    for row in bridge_rows:
        lines.append(f"- {row['semantic_area']} / {row['research_module']}: {row['final_research_status']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(name="semantic_report", purpose="Aggregate semantic probe outputs into daily summary, yearly summary, and admissibility bridge tables.", responsibilities=["Read lifecycle, tradedir, ordertype, and session probe outputs.", "Merge them into unified summary tables.", "Expose admissibility-facing bridge artifacts and markdown notes."], inputs=["dqa/semantic/year=<year>/semantic_*_daily.parquet"], outputs=["dqa/semantic/year=<year>/semantic_daily_summary.parquet", "Research/Audits/semantic_<year>_summary.md"])
        return 0
    if not args.year:
        raise SystemExit("--year is required unless --print-plan is used.")
    output_dir = args.input_root / "semantic" / f"year={args.year}"
    daily_rows = read_probe_rows(str(args.year), args.input_root)
    area_summary_rows = [build_area_summary(area, str(args.year), [row for row in daily_rows if row["semantic_area"] == area]) for area in SEMANTIC_AREA_NAMES]
    total_summary_rows = build_total_summary(area_summary_rows)
    bridge_rows = build_bridge_rows(str(args.year), area_summary_rows)
    write_parquet(daily_rows, output_dir / "semantic_daily_summary.parquet")
    for area, row in zip(SEMANTIC_AREA_NAMES, area_summary_rows):
        if row.get("semantic_area"):
            write_parquet([row], output_dir / SUMMARY_TABLE_BY_AREA[area])
    write_parquet(total_summary_rows, output_dir / "semantic_yearly_summary.parquet")
    write_parquet(bridge_rows, output_dir / "semantic_admissibility_bridge.parquet")
    report_path = args.research_root / f"semantic_{args.year}_summary.md"
    write_markdown(report_path, year=str(args.year), area_summary_rows=area_summary_rows, bridge_rows=bridge_rows)
    write_json(output_dir / "semantic_summary.json", {"pipeline": "semantic_report", "year": str(args.year), "artifacts": {"daily_summary": str(output_dir / 'semantic_daily_summary.parquet'), "yearly_summary": str(output_dir / 'semantic_yearly_summary.parquet'), "admissibility_bridge": str(output_dir / 'semantic_admissibility_bridge.parquet'), "report": str(report_path)}})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
