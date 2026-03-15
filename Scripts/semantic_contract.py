from __future__ import annotations

from pathlib import Path
from typing import Any

SEMANTIC_AREA_LIFECYCLE = "orderid_lifecycle"
SEMANTIC_AREA_TRADEDIR = "tradedir"
SEMANTIC_AREA_ORDERTYPE = "ordertype"
SEMANTIC_AREA_SESSION = "session"
SEMANTIC_AREA_NAMES = (
    SEMANTIC_AREA_LIFECYCLE,
    SEMANTIC_AREA_TRADEDIR,
    SEMANTIC_AREA_ORDERTYPE,
    SEMANTIC_AREA_SESSION,
)
SEMANTIC_AREAS = SEMANTIC_AREA_NAMES

STATUS_PASS = "pass"
STATUS_WEAK_PASS = "weak_pass"
STATUS_FAIL = "fail"
STATUS_UNKNOWN = "unknown"
STATUS_NOT_APPLICABLE = "not_applicable"
STATUS_NOT_RUN = "not_run"
SEMANTIC_STATUS_VALUES = (
    STATUS_PASS,
    STATUS_WEAK_PASS,
    STATUS_FAIL,
    STATUS_UNKNOWN,
    STATUS_NOT_APPLICABLE,
    STATUS_NOT_RUN,
)
SEMANTIC_STATUSES = SEMANTIC_STATUS_VALUES

CONFIDENCE_HIGH = "high"
CONFIDENCE_MEDIUM = "medium"
CONFIDENCE_LOW = "low"
SEMANTIC_CONFIDENCE_LEVELS = (
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    CONFIDENCE_LOW,
)

BLOCKING_LEVEL_BLOCKING = "blocking"
BLOCKING_LEVEL_NON_BLOCKING = "non_blocking"
BLOCKING_LEVEL_CONTEXT_ONLY = "context_only"
BLOCKING_LEVELS = (
    BLOCKING_LEVEL_BLOCKING,
    BLOCKING_LEVEL_NON_BLOCKING,
    BLOCKING_LEVEL_CONTEXT_ONLY,
)

ADMISSIBILITY_ALLOW = "allow"
ADMISSIBILITY_ALLOW_WITH_CAVEAT = "allow_with_caveat"
ADMISSIBILITY_BLOCK = "block"
ADMISSIBILITY_REQUIRES_SESSION_SPLIT = "requires_session_split"
ADMISSIBILITY_REQUIRES_TIME_ANCHOR = "requires_time_anchor"
ADMISSIBILITY_REQUIRES_MANUAL_REVIEW = "requires_manual_review"
ADMISSIBILITY_IMPACTS = (
    ADMISSIBILITY_ALLOW,
    ADMISSIBILITY_ALLOW_WITH_CAVEAT,
    ADMISSIBILITY_BLOCK,
    ADMISSIBILITY_REQUIRES_SESSION_SPLIT,
    ADMISSIBILITY_REQUIRES_TIME_ANCHOR,
    ADMISSIBILITY_REQUIRES_MANUAL_REVIEW,
)

COMMON_DAILY_COLUMNS = (
    "date",
    "year",
    "semantic_area",
    "scope",
    "status",
    "confidence",
    "blocking_level",
    "tested_rows",
    "pass_rows",
    "fail_rows",
    "unknown_rows",
    "summary",
    "admissibility_impact",
    "evidence_path",
)

AREA_DAILY_COLUMNS = {
    SEMANTIC_AREA_LIFECYCLE: (
        "distinct_orderids",
        "linked_orderids",
        "linked_orderid_rate",
        "orders_with_multiple_events",
        "orders_with_multiple_events_rate",
        "orders_with_multiple_trades",
        "orders_with_multiple_trades_rate",
        "orders_with_single_trade",
        "orders_with_single_trade_rate",
        "cross_session_candidate_count",
        "cross_session_candidate_rate",
        "first_order_seqnum_present_rate",
        "last_order_seqnum_present_rate",
        "first_trade_seqnum_present_rate",
        "last_trade_seqnum_present_rate",
        "lifecycle_status",
    ),
    SEMANTIC_AREA_TRADEDIR: (
        "tradedir_nonnull_count",
        "tradedir_nonnull_rate",
        "distinct_tradedir_values",
        "tradedir_zero_count",
        "tradedir_pos_count",
        "tradedir_neg_count",
        "tradedir_other_count",
        "tradedir_zero_rate",
        "tradedir_pos_rate",
        "tradedir_neg_rate",
        "linked_edge_count",
        "linked_edge_rate",
        "linked_side_consistency_tested",
        "linked_side_consistency_pass",
        "linked_side_consistency_fail",
        "linked_side_consistency_rate",
        "tradedir_status",
    ),
    SEMANTIC_AREA_ORDERTYPE: (
        "ordertype_nonnull_count",
        "ordertype_nonnull_rate",
        "distinct_ordertype_values",
        "single_ordertype_orderid_count",
        "multi_ordertype_orderid_count",
        "single_ordertype_orderid_rate",
        "multi_ordertype_orderid_rate",
        "top_ordertype_values",
        "ordertype_transition_pattern_count",
        "ordertype_transition_pattern_sample",
        "ordertype_status",
    ),
    SEMANTIC_AREA_SESSION: (
        "distinct_session_values",
        "session_value_count",
        "cross_session_linkage_count",
        "cross_session_linkage_rate",
        "session_time_window_consistent_flag",
        "session_split_required_flag",
        "orders_session_nonnull_rate",
        "trades_session_nonnull_rate",
        "linked_edges_with_session_rate",
        "session_status",
    ),
}

AREA_SUMMARY_COLUMNS = {
    SEMANTIC_AREA_LIFECYCLE: (
        "year",
        "semantic_area",
        "status",
        "confidence",
        "blocking_level",
        "days_total",
        "days_run",
        "days_pass",
        "days_weak_pass",
        "days_fail",
        "days_unknown",
        "tested_rows_total",
        "linked_orderids_total",
        "linked_orderid_rate_avg",
        "orders_with_multiple_events_rate_avg",
        "orders_with_multiple_trades_rate_avg",
        "cross_session_candidate_rate_avg",
        "summary",
        "admissibility_impact",
        "recommended_modules",
        "blocked_modules",
    ),
    SEMANTIC_AREA_TRADEDIR: (
        "year",
        "semantic_area",
        "status",
        "confidence",
        "blocking_level",
        "days_total",
        "days_run",
        "days_pass",
        "days_weak_pass",
        "days_fail",
        "days_unknown",
        "tested_rows_total",
        "tradedir_nonnull_rate_avg",
        "tradedir_zero_rate_avg",
        "tradedir_pos_rate_avg",
        "tradedir_neg_rate_avg",
        "linked_side_consistency_rate_avg",
        "summary",
        "admissibility_impact",
        "recommended_modules",
        "blocked_modules",
    ),
    SEMANTIC_AREA_ORDERTYPE: (
        "year",
        "semantic_area",
        "status",
        "confidence",
        "blocking_level",
        "days_total",
        "days_run",
        "days_pass",
        "days_weak_pass",
        "days_fail",
        "days_unknown",
        "tested_rows_total",
        "distinct_ordertype_values_union",
        "single_ordertype_orderid_rate_avg",
        "multi_ordertype_orderid_rate_avg",
        "ordertype_transition_pattern_count_total",
        "summary",
        "admissibility_impact",
        "recommended_modules",
        "blocked_modules",
    ),
    SEMANTIC_AREA_SESSION: (
        "year",
        "semantic_area",
        "status",
        "confidence",
        "blocking_level",
        "days_total",
        "days_run",
        "days_pass",
        "days_weak_pass",
        "days_fail",
        "days_unknown",
        "tested_rows_total",
        "distinct_session_values_union",
        "cross_session_linkage_rate_avg",
        "session_time_window_consistent_day_rate",
        "session_split_required_flag",
        "summary",
        "admissibility_impact",
        "recommended_modules",
        "blocked_modules",
    ),
}

SUMMARY_TABLE_BY_AREA = {
    SEMANTIC_AREA_LIFECYCLE: "semantic_lifecycle_summary.parquet",
    SEMANTIC_AREA_TRADEDIR: "semantic_tradedir_summary.parquet",
    SEMANTIC_AREA_ORDERTYPE: "semantic_ordertype_summary.parquet",
    SEMANTIC_AREA_SESSION: "semantic_session_summary.parquet",
}

TOTAL_SUMMARY_COLUMNS = (
    "year",
    "semantic_area",
    "status",
    "confidence",
    "blocking_level",
    "days_run",
    "days_pass",
    "days_weak_pass",
    "days_fail",
    "days_unknown",
    "tested_rows_total",
    "summary",
    "admissibility_impact",
    "recommended_modules",
    "blocked_modules",
)

ADMISSIBILITY_BRIDGE_COLUMNS = (
    "year",
    "semantic_area",
    "research_module",
    "semantic_status",
    "blocking_level",
    "admissibility_impact",
    "final_research_status",
    "reason",
    "notes",
)
SEMANTIC_BRIDGE_COLUMNS = ADMISSIBILITY_BRIDGE_COLUMNS

STATUS_SEVERITY = {
    STATUS_FAIL: 6,
    STATUS_UNKNOWN: 5,
    STATUS_NOT_RUN: 4,
    STATUS_WEAK_PASS: 3,
    STATUS_PASS: 2,
    STATUS_NOT_APPLICABLE: 1,
}

AREA_RESEARCH_MODULES = {
    SEMANTIC_AREA_LIFECYCLE: {
        "recommended": ("order_lifecycle_shape_by_event_count",),
        "blocked": ("execution_realism_or_fill_simulation", "strict_ordering_sensitive_causality"),
    },
    SEMANTIC_AREA_TRADEDIR: {
        "recommended": ("trade_dir_weak_consistency_check",),
        "blocked": ("signed_flow", "aggressor_side_inference"),
    },
    SEMANTIC_AREA_ORDERTYPE: {
        "recommended": ("ordertype_weak_consistency_check", "order_lifecycle_shape_by_event_count"),
        "blocked": ("event_semantics_inference",),
    },
    SEMANTIC_AREA_SESSION: {
        "recommended": ("matched_edge_session_profile",),
        "blocked": ("cross_session_unaware_research",),
    },
}


def canonical_date(value: str) -> str:
    digits = value.replace("-", "").strip()
    if len(digits) != 8 or not digits.isdigit():
        raise ValueError(f"Invalid date token: {value}")
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def parse_selected_dates(
    *,
    stage_root: Path,
    year: str,
    dates: str | None,
    max_days: int,
    latest_days: bool,
) -> list[str]:
    order_root = stage_root / "orders"
    trade_root = stage_root / "trades"
    order_dates = {
        path.name.split("=", 1)[1]
        for path in order_root.glob("date=*")
        if path.name.split("=", 1)[1].startswith(f"{year}-")
    }
    trade_dates = {
        path.name.split("=", 1)[1]
        for path in trade_root.glob("date=*")
        if path.name.split("=", 1)[1].startswith(f"{year}-")
    }
    available_dates = sorted(order_dates & trade_dates)
    if dates:
        selected_dates = [canonical_date(token) for token in dates.split(",") if token.strip()]
    else:
        selected_dates = available_dates[-max_days:] if max_days and latest_days else available_dates
        if max_days and not latest_days:
            selected_dates = selected_dates[:max_days]
    return [value for value in selected_dates if value in available_dates]


def validate_choice(value: str, allowed_values: tuple[str, ...], label: str) -> str:
    if value not in allowed_values:
        raise ValueError(f"Unsupported {label}: {value}")
    return value


def get_daily_columns(area: str) -> tuple[str, ...]:
    validate_choice(area, SEMANTIC_AREA_NAMES, "semantic area")
    return COMMON_DAILY_COLUMNS + AREA_DAILY_COLUMNS[area]


def get_summary_columns(area: str) -> tuple[str, ...]:
    validate_choice(area, SEMANTIC_AREA_NAMES, "semantic area")
    return AREA_SUMMARY_COLUMNS[area]


def build_empty_record(area: str, level: str) -> dict[str, Any]:
    if level == "daily":
        columns = get_daily_columns(area)
    elif level == "summary":
        columns = get_summary_columns(area)
    elif level == "total_summary":
        columns = TOTAL_SUMMARY_COLUMNS
    elif level == "bridge":
        columns = ADMISSIBILITY_BRIDGE_COLUMNS
    else:
        raise ValueError(f"Unsupported record level: {level}")
    return {column: None for column in columns}


def validate_columns(area: str, cols: list[str] | tuple[str, ...], level: str) -> None:
    expected = set(build_empty_record(area, level).keys())
    actual = set(cols)
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(f"Missing columns for {area} {level}: {missing}")


def map_semantic_result_to_admissibility(*, semantic_area: str, status: str, blocking_level: str) -> str:
    validate_choice(semantic_area, SEMANTIC_AREA_NAMES, "semantic area")
    validate_choice(status, SEMANTIC_STATUS_VALUES, "status")
    validate_choice(blocking_level, BLOCKING_LEVELS, "blocking level")
    if semantic_area == SEMANTIC_AREA_SESSION:
        if status == STATUS_FAIL:
            return ADMISSIBILITY_BLOCK
        return ADMISSIBILITY_REQUIRES_SESSION_SPLIT
    if semantic_area == SEMANTIC_AREA_TRADEDIR:
        if status == STATUS_FAIL:
            return ADMISSIBILITY_BLOCK
        return ADMISSIBILITY_REQUIRES_MANUAL_REVIEW
    if semantic_area == SEMANTIC_AREA_ORDERTYPE:
        if status == STATUS_FAIL:
            return ADMISSIBILITY_BLOCK
        return ADMISSIBILITY_ALLOW_WITH_CAVEAT
    if status == STATUS_FAIL:
        return ADMISSIBILITY_BLOCK
    if status == STATUS_PASS:
        return ADMISSIBILITY_ALLOW
    if status == STATUS_WEAK_PASS:
        return ADMISSIBILITY_ALLOW_WITH_CAVEAT
    if status == STATUS_NOT_APPLICABLE:
        return ADMISSIBILITY_ALLOW_WITH_CAVEAT
    if status in {STATUS_UNKNOWN, STATUS_NOT_RUN}:
        if blocking_level == BLOCKING_LEVEL_BLOCKING:
            return ADMISSIBILITY_REQUIRES_MANUAL_REVIEW
        return ADMISSIBILITY_ALLOW_WITH_CAVEAT
    return ADMISSIBILITY_REQUIRES_MANUAL_REVIEW


def populate_record(area: str, level: str, **values: Any) -> dict[str, Any]:
    record = build_empty_record(area, level)
    record.update(values)
    return record


def build_daily_result(area: str, **kwargs: Any) -> dict[str, Any]:
    record = populate_record(area, "daily", **kwargs)
    validate_choice(record["semantic_area"], SEMANTIC_AREA_NAMES, "semantic area")
    validate_choice(record["status"], SEMANTIC_STATUS_VALUES, "status")
    validate_choice(record["confidence"], SEMANTIC_CONFIDENCE_LEVELS, "confidence")
    validate_choice(record["blocking_level"], BLOCKING_LEVELS, "blocking level")
    validate_choice(record["admissibility_impact"], ADMISSIBILITY_IMPACTS, "admissibility impact")
    return record


def build_summary_result(area: str, **kwargs: Any) -> dict[str, Any]:
    record = populate_record(area, "summary", **kwargs)
    validate_choice(record["semantic_area"], SEMANTIC_AREA_NAMES, "semantic area")
    validate_choice(record["status"], SEMANTIC_STATUS_VALUES, "status")
    validate_choice(record["confidence"], SEMANTIC_CONFIDENCE_LEVELS, "confidence")
    validate_choice(record["blocking_level"], BLOCKING_LEVELS, "blocking level")
    validate_choice(record["admissibility_impact"], ADMISSIBILITY_IMPACTS, "admissibility impact")
    return record


def area_modules(semantic_area: str) -> dict[str, tuple[str, ...]]:
    validate_choice(semantic_area, SEMANTIC_AREA_NAMES, "semantic area")
    return AREA_RESEARCH_MODULES[semantic_area]
