from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_REGISTRY_PATH = REPO_ROOT / "config" / "reference_sources.example.json"
DEFAULT_LOCAL_SOURCE_CONFIG_PATH = REPO_ROOT / "config" / "reference_sources.local.json"


def load_json(path: Path, *, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return {} if default is None else default
    return json.loads(path.read_text(encoding="utf-8"))


def load_source_registry(path: Path = DEFAULT_SOURCE_REGISTRY_PATH) -> dict[str, Any]:
    payload = load_json(path)
    if not payload:
        raise SystemExit(f"Missing source registry config: {path}")
    if "sources" not in payload or not isinstance(payload["sources"], dict):
        raise SystemExit(f"Invalid source registry config, expected object field 'sources': {path}")
    return payload


def load_local_source_config(path: Path = DEFAULT_LOCAL_SOURCE_CONFIG_PATH) -> dict[str, Any]:
    return load_json(path)


def get_nested(payload: dict[str, Any], dotted_key: str | None) -> Any:
    if not dotted_key:
        return None
    current: Any = payload
    for token in dotted_key.split("."):
        if not isinstance(current, dict) or token not in current:
            return None
        current = current[token]
    return current


def get_registered_source(source_id: str, registry: dict[str, Any]) -> dict[str, Any]:
    source = registry.get("sources", {}).get(source_id)
    if not isinstance(source, dict):
        raise SystemExit(f"Unknown source id: {source_id}")
    return source


def resolve_source_secret(
    source_id: str,
    *,
    registry: dict[str, Any],
    local_config: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
) -> str | None:
    source = get_registered_source(source_id, registry)
    env = os.environ if env is None else env
    local_config = {} if local_config is None else local_config
    local_value = get_nested(local_config, source.get("local_credential_key"))
    if isinstance(local_value, str) and local_value.strip():
        return local_value.strip()
    env_name = source.get("credential_env")
    if isinstance(env_name, str):
        value = env.get(env_name)
        if value:
            return value.strip()
    return None


def resolve_source_endpoint(
    source_id: str,
    *,
    registry: dict[str, Any],
    local_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = get_registered_source(source_id, registry)
    local_config = {} if local_config is None else local_config
    host = get_nested(local_config, source.get("local_host_key")) or source.get("default_host")
    port = get_nested(local_config, source.get("local_port_key")) or source.get("default_port")
    return {"host": host, "port": port}


def enabled_source_ids(registry: dict[str, Any]) -> list[str]:
    return [source_id for source_id, meta in registry.get("sources", {}).items() if meta.get("enabled", False)]
