from __future__ import annotations

import argparse
import json
from pathlib import Path

from Scripts.field_release_registry import (
    DEFAULT_REGISTRY_PATH,
    load_registry,
    validate_registry,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Hshare field release registry entries.")
    parser.add_argument("--object", dest="object_name", help="Release object name to validate.")
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable validation output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_registry(args.registry)
    errors = validate_registry(registry, object_name=args.object_name)
    payload = {
        "registry": str(args.registry),
        "object_name": args.object_name,
        "ok": not errors,
        "errors": errors,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    elif errors:
        print("Field release validation failed:")
        for error in errors:
            print(f"- {error}")
    else:
        target = args.object_name or "all objects"
        print(f"Field release validation passed: {target}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
