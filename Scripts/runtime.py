from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_DATA_ROOT = Path("/Volumes/Data/港股Tick数据")
DEFAULT_MANIFEST_ROOT = DEFAULT_DATA_ROOT / "manifests"
DEFAULT_LOG_ROOT = DEFAULT_DATA_ROOT / "logs"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def configure_logger(name: str, log_path: Path | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_path is not None:
        ensure_dir(log_path.parent)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def append_jsonl(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def print_scaffold_plan(
    *,
    name: str,
    purpose: str,
    responsibilities: list[str],
    inputs: list[str],
    outputs: list[str],
) -> None:
    print(f"{name}: {purpose}")
    print("Responsibilities:")
    for item in responsibilities:
        print(f"- {item}")
    print("Inputs:")
    for item in inputs:
        print(f"- {item}")
    print("Outputs:")
    for item in outputs:
        print(f"- {item}")
