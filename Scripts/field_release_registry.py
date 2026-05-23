from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY_PATH = REPO_ROOT / "manifests" / "field_release_registry.json"

RELEASE_BUCKETS = {
    "admit_now",
    "admit_with_explicit_caveat_only",
    "admit_top_of_book_only",
    "keep_out_for_now",
}
OBJECT_TYPES = {"raw_field", "derived_field", "caveat_namespace"}
REQUIRED_OBJECT_FIELDS = {
    "object_name",
    "object_type",
    "source_layer",
    "release_bucket",
    "allowed_uses",
    "forbidden_claims",
    "evidence_docs",
    "downstream_namespaces",
    "blocker",
    "manual_review_required",
    "contains_caveat_fields",
}
DEFAULT_VERIFIED_NAMESPACES = {"verified_orders", "verified_trades", "verified_default"}


class FieldReleaseRegistryError(ValueError):
    pass


def load_registry(path: Path | str = DEFAULT_REGISTRY_PATH) -> dict[str, Any]:
    registry_path = Path(path)
    return json.loads(registry_path.read_text(encoding="utf-8"))


def objects_by_name(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    objects = registry.get("objects")
    if not isinstance(objects, list):
        raise FieldReleaseRegistryError("registry must contain an objects list")
    by_name: dict[str, dict[str, Any]] = {}
    for entry in objects:
        name = entry.get("object_name")
        if not isinstance(name, str) or not name:
            raise FieldReleaseRegistryError("every object must have object_name")
        if name in by_name:
            raise FieldReleaseRegistryError(f"duplicate object_name: {name}")
        by_name[name] = entry
    return by_name


def find_object(registry: dict[str, Any], object_name: str) -> dict[str, Any]:
    objects = objects_by_name(registry)
    if object_name not in objects:
        known = ", ".join(sorted(objects))
        raise FieldReleaseRegistryError(f"unknown release object: {object_name}; known: {known}")
    return objects[object_name]


def repo_path(path_text: str, repo_root: Path = REPO_ROOT) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return repo_root / path


def release_dossier_path(object_name: str, repo_root: Path = REPO_ROOT) -> Path:
    slug = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in object_name)
    return repo_root / "Research" / "Validation" / f"field_release_dossier_{slug}.md"


def assert_release_objects_for_namespace(
    registry: dict[str, Any],
    *,
    object_names: list[str],
    namespace: str,
    allowed_buckets: set[str],
    expected_builder: str | None = None,
    require_dossier: bool = False,
    repo_root: Path = REPO_ROOT,
) -> None:
    errors: list[str] = []
    for object_name in object_names:
        errors.extend(validate_registry(registry, object_name=object_name, repo_root=repo_root))
        if errors:
            continue
        entry = find_object(registry, object_name)
        bucket = str(entry["release_bucket"])
        if bucket not in allowed_buckets:
            errors.append(
                f"{object_name}: release_bucket {bucket} is not allowed for namespace {namespace}"
            )
        if namespace in DEFAULT_VERIFIED_NAMESPACES and bucket != "admit_now":
            errors.append(f"{object_name}: non-admit_now object cannot enter {namespace}")
        elif namespace not in entry.get("downstream_namespaces", []):
            errors.append(f"{object_name}: namespace {namespace} is not registered downstream")
        if bucket == "keep_out_for_now":
            errors.append(f"{object_name}: keep_out_for_now object cannot be materialized")
        if entry.get("object_type") == "caveat_namespace":
            builder = entry.get("builder")
            if not builder:
                errors.append(f"{object_name}: caveat_namespace must declare builder")
            if expected_builder is not None and builder != expected_builder:
                errors.append(
                    f"{object_name}: builder mismatch; expected {expected_builder}, got {builder}"
                )
            if require_dossier and not release_dossier_path(object_name, repo_root).exists():
                errors.append(f"{object_name}: missing release dossier")
    if errors:
        raise FieldReleaseRegistryError("; ".join(errors))


def validate_registry(
    registry: dict[str, Any],
    *,
    object_name: str | None = None,
    repo_root: Path = REPO_ROOT,
    require_evidence: bool = True,
) -> list[str]:
    errors: list[str] = []
    bucket_keys = set(registry.get("release_buckets", {}))
    missing_buckets = sorted(RELEASE_BUCKETS - bucket_keys)
    if missing_buckets:
        errors.append(f"registry release_buckets missing: {', '.join(missing_buckets)}")
    type_keys = set(registry.get("object_types", {}))
    missing_types = sorted(OBJECT_TYPES - type_keys)
    if missing_types:
        errors.append(f"registry object_types missing: {', '.join(missing_types)}")

    try:
        entries = (
            [find_object(registry, object_name)]
            if object_name
            else list(objects_by_name(registry).values())
        )
    except FieldReleaseRegistryError as exc:
        return errors + [str(exc)]

    for entry in entries:
        errors.extend(
            validate_object(entry, repo_root=repo_root, require_evidence=require_evidence)
        )
    return errors


def validate_object(
    entry: dict[str, Any],
    *,
    repo_root: Path = REPO_ROOT,
    require_evidence: bool = True,
) -> list[str]:
    name = entry.get("object_name", "<unknown>")
    errors: list[str] = []
    missing = sorted(field for field in REQUIRED_OBJECT_FIELDS if field not in entry)
    if missing:
        errors.append(f"{name}: missing required fields: {', '.join(missing)}")
        return errors

    bucket = entry["release_bucket"]
    object_type = entry["object_type"]
    if bucket not in RELEASE_BUCKETS:
        errors.append(f"{name}: unsupported release_bucket: {bucket}")
    if object_type not in OBJECT_TYPES:
        errors.append(f"{name}: unsupported object_type: {object_type}")

    for field in ("allowed_uses", "forbidden_claims", "evidence_docs", "downstream_namespaces"):
        if not isinstance(entry[field], list):
            errors.append(f"{name}: {field} must be a list")
    if not isinstance(entry["manual_review_required"], bool):
        errors.append(f"{name}: manual_review_required must be boolean")
    if not isinstance(entry["contains_caveat_fields"], bool):
        errors.append(f"{name}: contains_caveat_fields must be boolean")

    if bucket != "admit_now" and not entry["forbidden_claims"]:
        errors.append(f"{name}: non-admit_now object must declare forbidden_claims")
    if bucket == "keep_out_for_now" and not entry["blocker"]:
        errors.append(f"{name}: keep_out_for_now object must declare blocker")
    if entry["contains_caveat_fields"] and not entry["downstream_namespaces"]:
        errors.append(f"{name}: caveat-bearing object must declare downstream_namespaces")

    if object_type == "caveat_namespace":
        if not entry.get("builder"):
            errors.append(f"{name}: caveat_namespace must declare builder")
        if not entry["forbidden_claims"]:
            errors.append(f"{name}: caveat_namespace must declare forbidden_claims")
        if not entry["downstream_namespaces"]:
            errors.append(f"{name}: caveat_namespace must declare downstream_namespaces")

    if entry.get("verified_default_admission") and bucket != "admit_now":
        errors.append(f"{name}: blocked/caveat object cannot enter verified default namespace")
    default_namespaces = set(entry.get("downstream_namespaces", [])) & DEFAULT_VERIFIED_NAMESPACES
    if default_namespaces and bucket != "admit_now":
        errors.append(
            f"{name}: non-admit_now object names verified default namespace: "
            f"{', '.join(sorted(default_namespaces))}"
        )

    if require_evidence:
        if not entry["evidence_docs"]:
            errors.append(f"{name}: evidence_docs must not be empty")
        for evidence_doc in entry["evidence_docs"]:
            if not repo_path(str(evidence_doc), repo_root).exists():
                errors.append(f"{name}: missing evidence doc: {evidence_doc}")
    return errors
