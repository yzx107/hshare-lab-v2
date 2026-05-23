from __future__ import annotations

import copy
import subprocess
import tempfile
import unittest
from pathlib import Path

from Scripts.field_release_registry import (
    RELEASE_BUCKETS,
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
            "TopOfBookValidFlag",
            "FullReconstructedDepth",
        ):
            self.assertIn(object_name, objects)
        self.assertEqual(validate_registry(registry), [])

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


if __name__ == "__main__":
    unittest.main()
