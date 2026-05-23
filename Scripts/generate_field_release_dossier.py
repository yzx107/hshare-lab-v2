from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from Scripts.field_release_registry import (
    DEFAULT_REGISTRY_PATH,
    FieldReleaseRegistryError,
    find_object,
    load_registry,
    validate_registry,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "Research" / "Validation"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a Chinese field release dossier.")
    parser.add_argument("--object", dest="object_name", required=True)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--stdout", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_registry(args.registry)
    errors = validate_registry(registry, object_name=args.object_name)
    if errors:
        for error in errors:
            print(f"- {error}")
        return 1
    try:
        entry = find_object(registry, args.object_name)
    except FieldReleaseRegistryError as exc:
        print(str(exc))
        return 1
    markdown = render_dossier(entry)
    if args.stdout:
        print(markdown)
        return 0
    args.output_root.mkdir(parents=True, exist_ok=True)
    output_path = args.output_root / f"field_release_dossier_{slug(args.object_name)}.md"
    output_path.write_text(markdown, encoding="utf-8")
    print(str(output_path))
    return 0


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")


def render_dossier(entry: dict[str, Any]) -> str:
    lines = [
        f"# Field Release Dossier: {entry['object_name']}",
        "",
        "## 放开的对象",
        "",
        f"- object_name: `{entry['object_name']}`",
        f"- object_type: `{entry['object_type']}`",
        f"- release_bucket: `{entry['release_bucket']}`",
        f"- caveat_level: `{entry.get('caveat_level', 'unspecified')}`",
        f"- manual_review_required: `{entry['manual_review_required']}`",
        f"- contains_caveat_fields: `{entry['contains_caveat_fields']}`",
        "",
        "## 来源层",
        "",
        f"- source_layer: `{entry['source_layer']}`",
    ]
    if entry.get("raw_fields"):
        lines.append(f"- raw_fields: {join_code(entry['raw_fields'])}")
    if entry.get("builder"):
        lines.append(f"- builder: `{entry['builder']}`")
    lines.extend(
        [
            "",
            "## 派生逻辑",
            "",
            entry.get("derived_logic") or "无派生逻辑；该对象直接作为受限 release object 管理。",
            "",
            "## 证据材料",
            "",
        ]
    )
    lines.extend(markdown_list(entry["evidence_docs"], code=True))
    lines.extend(["", "## 允许用途 (allowed uses)", ""])
    lines.extend(markdown_list(entry["allowed_uses"]))
    lines.extend(["", "## 禁止宣称 (forbidden claims)", ""])
    lines.extend(markdown_list(entry["forbidden_claims"]))
    lines.extend(["", "## 当前 blocker", "", entry["blocker"] or "当前无 blocker。"])
    lines.extend(["", "## 下游使用边界", ""])
    lines.extend(markdown_list(entry.get("downstream_boundary", [])))
    lines.extend(["", "## 下游 namespace", ""])
    lines.extend(markdown_list(entry["downstream_namespaces"], code=True))
    lines.extend(
        [
            "",
            "## verified default 边界",
            "",
            f"- verified_default_admission: `{entry.get('verified_default_admission', False)}`",
            "- 除非 registry 后续显式升级为 `admit_now`，否则不得静默并入 "
            "`verified_orders` / `verified_trades` 默认表。",
            "",
        ]
    )
    return "\n".join(lines)


def markdown_list(values: list[Any], *, code: bool = False) -> list[str]:
    if not values:
        return ["- 无"]
    if code:
        return [f"- `{value}`" for value in values]
    return [f"- {value}" for value in values]


def join_code(values: list[Any]) -> str:
    return ", ".join(f"`{value}`" for value in values)


if __name__ == "__main__":
    raise SystemExit(main())
