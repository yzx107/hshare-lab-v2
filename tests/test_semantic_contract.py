from __future__ import annotations

import unittest

from Scripts.semantic_contract import (
    ADMISSIBILITY_ALLOW,
    ADMISSIBILITY_ALLOW_WITH_CAVEAT,
    ADMISSIBILITY_IMPACTS,
    ADMISSIBILITY_REQUIRES_MANUAL_REVIEW,
    ADMISSIBILITY_REQUIRES_SESSION_SPLIT,
    BLOCKING_LEVEL_BLOCKING,
    BLOCKING_LEVEL_CONTEXT_ONLY,
    BLOCKING_LEVELS,
    COMMON_DAILY_COLUMNS,
    SEMANTIC_AREA_NAMES,
    SEMANTIC_AREA_LIFECYCLE,
    SEMANTIC_AREA_SESSION,
    SEMANTIC_AREA_TRADEDIR,
    SEMANTIC_CONFIDENCE_LEVELS,
    SEMANTIC_STATUS_VALUES,
    STATUS_PASS,
    STATUS_UNKNOWN,
    STATUS_WEAK_PASS,
    build_daily_result,
    build_empty_record,
    build_summary_result,
    get_daily_columns,
    get_summary_columns,
    map_semantic_result_to_admissibility,
)


class SemanticContractTests(unittest.TestCase):
    def test_contract_enums_exist(self) -> None:
        self.assertIn(SEMANTIC_AREA_LIFECYCLE, SEMANTIC_AREA_NAMES)
        self.assertIn(STATUS_PASS, SEMANTIC_STATUS_VALUES)
        self.assertIn(STATUS_WEAK_PASS, SEMANTIC_STATUS_VALUES)
        self.assertIn(STATUS_UNKNOWN, SEMANTIC_STATUS_VALUES)
        self.assertIn(BLOCKING_LEVEL_BLOCKING, BLOCKING_LEVELS)
        self.assertIn(ADMISSIBILITY_ALLOW_WITH_CAVEAT, ADMISSIBILITY_IMPACTS)

    def test_contract_schemas_expose_core_columns(self) -> None:
        self.assertIn("semantic_area", COMMON_DAILY_COLUMNS)
        self.assertIn("admissibility_impact", COMMON_DAILY_COLUMNS)
        self.assertIn("recommended_modules", get_summary_columns(SEMANTIC_AREA_LIFECYCLE))
        self.assertIn("linked_orderids", get_daily_columns(SEMANTIC_AREA_LIFECYCLE))

    def test_mapping_function_is_available(self) -> None:
        self.assertEqual(
            map_semantic_result_to_admissibility(
                semantic_area=SEMANTIC_AREA_LIFECYCLE,
                status=STATUS_PASS,
                blocking_level=BLOCKING_LEVEL_BLOCKING,
            ),
            ADMISSIBILITY_ALLOW,
        )
        self.assertEqual(
            map_semantic_result_to_admissibility(
                semantic_area=SEMANTIC_AREA_TRADEDIR,
                status=STATUS_WEAK_PASS,
                blocking_level=BLOCKING_LEVEL_BLOCKING,
            ),
            ADMISSIBILITY_REQUIRES_MANUAL_REVIEW,
        )
        self.assertEqual(
            map_semantic_result_to_admissibility(
                semantic_area=SEMANTIC_AREA_SESSION,
                status=STATUS_UNKNOWN,
                blocking_level=BLOCKING_LEVEL_CONTEXT_ONLY,
            ),
            ADMISSIBILITY_REQUIRES_SESSION_SPLIT,
        )

    def test_builders_validate_required_fields(self) -> None:
        daily = build_daily_result(
            SEMANTIC_AREA_LIFECYCLE,
            date="2026-03-13",
            year="2026",
            semantic_area=SEMANTIC_AREA_LIFECYCLE,
            scope="test",
            status=STATUS_PASS,
            confidence="medium",
            blocking_level=BLOCKING_LEVEL_BLOCKING,
            tested_rows=1,
            pass_rows=1,
            fail_rows=0,
            unknown_rows=0,
            summary="ok",
            admissibility_impact=ADMISSIBILITY_ALLOW,
            evidence_path="x",
        )
        self.assertEqual(daily["status"], STATUS_PASS)
        self.assertIn("linked_orderids", daily)
        summary = build_summary_result(
            SEMANTIC_AREA_LIFECYCLE,
            year="2026",
            semantic_area=SEMANTIC_AREA_LIFECYCLE,
            status=STATUS_PASS,
            confidence="medium",
            blocking_level=BLOCKING_LEVEL_BLOCKING,
            summary="ok",
            admissibility_impact=ADMISSIBILITY_ALLOW,
            recommended_modules="a",
            blocked_modules="b",
        )
        self.assertEqual(summary["semantic_area"], SEMANTIC_AREA_LIFECYCLE)
        self.assertIn("days_total", summary)
        self.assertIn("linked_orderids_total", summary)

    def test_build_empty_record_exposes_full_schema(self) -> None:
        daily = build_empty_record(SEMANTIC_AREA_LIFECYCLE, "daily")
        summary = build_empty_record(SEMANTIC_AREA_SESSION, "summary")
        self.assertIn("linked_orderid_rate", daily)
        self.assertIn("session_split_required_flag", summary)


if __name__ == "__main__":
    unittest.main()
