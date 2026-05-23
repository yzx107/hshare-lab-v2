from __future__ import annotations

import copy
import subprocess
import tempfile
import unittest
from pathlib import Path

from Scripts import build_verified_layer
from Scripts.field_release_registry import (
    RELEASE_BUCKETS,
    FieldReleaseRegistryError,
    assert_release_objects_for_namespace,
    find_object,
    load_registry,
    objects_by_name,
    validate_registry,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


class FieldReleaseMachineryTests(unittest.TestCase):
    def test_loads_registry_and_required_release_objects(self) -> None:
        registry = load_registry()
        self.assertEqual(RELEASE_BUCKETS, set(registry["release_buckets"]))
        objects = objects_by_name(registry)
        for object_name in (
            "OrderTypeLifecycleEventCode",
            "OrderSideVendor",
            "TradeToActiveOrderLinkageEvidence",
            "PriorActiveVolumeCheck",
            "orderbook_replay__caveat_lifecycle_linkage",
            "CrossedWindowFlag",
            "orderbook_replay__top_of_book_only",
            "TopOfBookValidFlag",
            "ReplayQualityScore",
            "FullReconstructedDepth",
        ):
            self.assertIn(object_name, objects)
        self.assertEqual(validate_registry(registry), [])
        quality_score = objects["ReplayQualityScore"]
        self.assertEqual(quality_score["release_bucket"], "admit_with_explicit_caveat_only")
        self.assertEqual(
            quality_score["downstream_namespaces"],
            ["orderbook_replay__top_of_book_only"],
        )

    def test_dossier_generation_outputs_chinese_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.generate_field_release_dossier",
                    "--object",
                    "OrderSideVendor",
                    "--output-root",
                    tmpdir,
                ],
                cwd=str(REPO_ROOT),
                check=True,
                capture_output=True,
                text=True,
            )
            output_path = Path(result.stdout.strip())
            text = output_path.read_text(encoding="utf-8")
            self.assertIn("## 放开的对象", text)
            self.assertIn("## 允许用途", text)
            self.assertIn("OrderSideVendor", text)

    def test_validator_cli_accepts_object_and_namespace(self) -> None:
        for object_name in ("OrderSideVendor", "orderbook_replay__caveat_lifecycle_linkage"):
            subprocess.run(
                [
                    "python3",
                    "-m",
                    "Scripts.validate_field_release",
                    "--object",
                    object_name,
                ],
                cwd=str(REPO_ROOT),
                check=True,
                capture_output=True,
                text=True,
            )

    def test_blocked_object_cannot_enter_verified_default_namespace(self) -> None:
        registry = copy.deepcopy(load_registry())
        blocked = find_object(registry, "FullReconstructedDepth")
        blocked["verified_default_admission"] = True
        errors = validate_registry(registry, object_name="FullReconstructedDepth")
        self.assertTrue(
            any("cannot enter verified default namespace" in error for error in errors),
            errors,
        )

    def test_caveat_namespace_requires_forbidden_claims_and_downstream_namespace(self) -> None:
        registry = copy.deepcopy(load_registry())
        namespace = find_object(registry, "orderbook_replay__caveat_lifecycle_linkage")
        namespace["forbidden_claims"] = []
        namespace["downstream_namespaces"] = []
        errors = validate_registry(
            registry,
            object_name="orderbook_replay__caveat_lifecycle_linkage",
        )
        self.assertTrue(any("forbidden_claims" in error for error in errors), errors)
        self.assertTrue(any("downstream_namespaces" in error for error in errors), errors)

    def test_verified_gate_rejects_caveat_object_in_default_namespace(self) -> None:
        task = build_verified_layer.VerifiedTask(
            year="2026",
            table_name="orders",
            date="2026-05-22",
            input_paths=("orders.parquet",),
            output_path="verified_orders.parquet",
            input_columns=("Ext",),
            output_columns=("OrderSideVendor",),
            excluded_columns=(),
        )
        with self.assertRaises(FieldReleaseRegistryError):
            build_verified_layer.enforce_task_release_registry(task, load_registry())

    def test_verified_gate_rejects_top_of_book_object_in_default_namespace(self) -> None:
        task = build_verified_layer.VerifiedTask(
            year="2026",
            table_name="trades",
            date="2026-05-22",
            input_paths=("trades.parquet",),
            output_path="verified_trades.parquet",
            input_columns=("Price",),
            output_columns=("BestBidReplay",),
            excluded_columns=(),
        )
        with self.assertRaises(FieldReleaseRegistryError):
            build_verified_layer.enforce_task_release_registry(task, load_registry())

    def test_verified_gate_allows_registered_explicit_caveat_namespace(self) -> None:
        task = build_verified_layer.VerifiedTask(
            year="2026",
            table_name="orders",
            date="2026-05-22",
            input_paths=("orders.parquet",),
            output_path="verified_orders__caveat_ordertype_ordersidevendor.parquet",
            input_columns=("OrderType", "Ext"),
            output_columns=("OrderType", "OrderSideVendor"),
            excluded_columns=(),
            caveat_columns=("OrderType", "OrderSideVendor"),
            variant_label="caveat_ordertype_ordersidevendor",
        )
        build_verified_layer.enforce_task_release_registry(task, load_registry())

    def test_verified_gate_fails_when_release_object_is_missing(self) -> None:
        registry = copy.deepcopy(load_registry())
        registry["objects"] = [
            entry for entry in registry["objects"] if entry["object_name"] != "OrderSideVendor"
        ]
        task = build_verified_layer.VerifiedTask(
            year="2026",
            table_name="orders",
            date="2026-05-22",
            input_paths=("orders.parquet",),
            output_path="verified_orders__caveat_ordersidevendor.parquet",
            input_columns=("Ext",),
            output_columns=("OrderSideVendor",),
            excluded_columns=(),
            caveat_columns=("OrderSideVendor",),
            variant_label="caveat_ordersidevendor",
        )
        with self.assertRaises(FieldReleaseRegistryError):
            build_verified_layer.enforce_task_release_registry(task, registry)

    def test_verified_gate_fails_when_caveat_column_has_no_registry_mapping(self) -> None:
        task = build_verified_layer.VerifiedTask(
            year="2026",
            table_name="trades",
            date="2026-05-22",
            input_paths=("trades.parquet",),
            output_path="verified_trades__caveat_dir.parquet",
            input_columns=("Dir",),
            output_columns=("Dir",),
            excluded_columns=(),
            caveat_columns=("Dir",),
            variant_label="caveat_dir",
        )
        with self.assertRaises(FieldReleaseRegistryError):
            build_verified_layer.enforce_task_release_registry(task, load_registry())

    def test_top_of_book_bucket_cannot_materialize_full_depth(self) -> None:
        with self.assertRaises(FieldReleaseRegistryError):
            assert_release_objects_for_namespace(
                load_registry(),
                object_names=["FullReconstructedDepth"],
                namespace="orderbook_replay__top_of_book_only",
                allowed_buckets={"admit_top_of_book_only"},
            )


if __name__ == "__main__":
    unittest.main()
