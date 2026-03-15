from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a resumable long task under a heartbeat watchdog. "
            "If the child exits unexpectedly or the heartbeat goes stale, restart it."
        )
    )
    parser.add_argument("--heartbeat-path", required=True, type=Path)
    parser.add_argument("--checkpoint-path", required=True, type=Path)
    parser.add_argument("--stale-seconds", type=int, default=900)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--restart-delay-seconds", type=int, default=10)
    parser.add_argument("--max-restarts", type=int, default=0, help="0 means unlimited.")
    parser.add_argument("--label", default="watchdog")
    parser.add_argument("--log-path", type=Path)
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to run after '--', for example: -- python3 -m Scripts.run_dqa_schema ...",
    )
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("A child command is required after '--'.")
    return args


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_heartbeat_status(path: Path) -> tuple[str | None, float | None, dict[str, Any] | None]:
    payload = read_json(path)
    if not payload:
        return None, None, None
    updated_at = payload.get("updated_at")
    stale_age = None
    if isinstance(updated_at, str):
        try:
            stale_age = time.time() - datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp()
        except ValueError:
            stale_age = None
    return payload.get("status"), stale_age, payload


def append_log(path: Path | None, line: str) -> None:
    stamped = f"{utc_now()} | {line}"
    print(stamped, flush=True)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(stamped + "\n")


def start_child(command: list[str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        start_new_session=True,
    )


def terminate_child(process: subprocess.Popen[str], timeout: float = 15.0) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.5)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def main() -> int:
    args = parse_args()
    restart_count = 0
    child: subprocess.Popen[str] | None = None

    append_log(
        args.log_path,
        f"[{args.label}] watchdog start heartbeat={args.heartbeat_path} checkpoint={args.checkpoint_path} "
        f"stale={args.stale_seconds}s poll={args.poll_seconds}s",
    )

    while True:
        status, stale_age, payload = read_heartbeat_status(args.heartbeat_path)
        if status in {"completed", "completed_with_failures"}:
            append_log(args.log_path, f"[{args.label}] heartbeat status={status}; watchdog exit")
            return 0

        if child is None or child.poll() is not None:
            if child is not None:
                exit_code = child.poll()
                append_log(args.log_path, f"[{args.label}] child exited code={exit_code}")
            if args.max_restarts and restart_count >= args.max_restarts:
                append_log(args.log_path, f"[{args.label}] max restarts reached; watchdog exit")
                return 1
            append_log(args.log_path, f"[{args.label}] starting child: {' '.join(args.command)}")
            child = start_child(args.command)
            restart_count += 1
            time.sleep(args.restart_delay_seconds)
            continue

        if stale_age is not None and stale_age > args.stale_seconds:
            active = None if payload is None else payload.get("active_task_keys") or payload.get("active_task_key")
            append_log(
                args.log_path,
                f"[{args.label}] stale heartbeat age={int(stale_age)}s active={active}; restarting child",
            )
            terminate_child(child)
            child = None
            time.sleep(args.restart_delay_seconds)
            continue

        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
