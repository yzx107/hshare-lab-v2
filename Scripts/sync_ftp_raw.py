from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    DEFAULT_MANIFEST_ROOT,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    write_json,
)

LISTING_PATTERN = re.compile(r"^\s*(\d+)\s+\w+\s+\d+\s+\d{1,2}:\d{2}\s+(.+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incrementally sync raw L2 zip files from the configured FTP source."
    )
    parser.add_argument("--year", required=True, help="Remote year folder such as 2026.")
    parser.add_argument(
        "--ftp-config",
        type=Path,
        default=Path("config/ftp_source.local.json"),
        help="Local FTP credential config path.",
    )
    parser.add_argument(
        "--source-key",
        default="hk_tick_ftp",
        help="Top-level key inside the local FTP config.",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="Root directory that contains year-level raw folders.",
    )
    parser.add_argument(
        "--manifest-root",
        type=Path,
        default=DEFAULT_MANIFEST_ROOT,
        help="Root directory for sync state artifacts.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for sync logs.",
    )
    parser.add_argument(
        "--bind-interface",
        default="en0",
        help="Physical interface used to bypass TUN hijack, e.g. en0.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Optional limit for smoke tests; 0 means sync all pending files.",
    )
    parser.add_argument(
        "--include-doc",
        action="store_true",
        help="Also sync the remote Doc/ directory after zip files.",
    )
    return parser.parse_args()


def load_ftp_config(config_path: Path, source_key: str) -> dict[str, Any]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    source = payload[source_key]
    required_keys = ("host", "port", "username", "password")
    missing = [key for key in required_keys if not source.get(key)]
    if missing:
        raise ValueError(f"FTP config is missing required keys: {missing}")
    return source


def run_command(command: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=True,
        env=env,
    )


def clean_proxy_env() -> dict[str, str]:
    env = dict(os.environ)
    for key in (
        "ALL_PROXY",
        "all_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "FTP_PROXY",
        "ftp_proxy",
        "NO_PROXY",
        "no_proxy",
    ):
        env[key] = ""
    return env


def remote_listing(
    *,
    ftp: dict[str, Any],
    remote_dir: str,
    bind_ip: str,
) -> list[dict[str, Any]]:
    lftp_script = (
        f"set net:socket-bind-ipv4 {bind_ip}; "
        "set net:timeout 15; "
        "set net:max-retries 1; "
        "set ftp:passive-mode true; "
        f"open -u {ftp['username']},{ftp['password']} -p {ftp['port']} {ftp['host']}; "
        f"cls -l {remote_dir}; "
        "bye"
    )
    result = subprocess.run(
        ["lftp", "-f", "/dev/stdin"],
        input=lftp_script,
        check=True,
        text=True,
        capture_output=True,
        env=clean_proxy_env(),
    )
    rows: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if not stripped or stripped.endswith("/"):
            continue
        match = LISTING_PATTERN.match(stripped)
        if match is None:
            continue
        size = int(match.group(1))
        remote_path = match.group(2)
        rows.append(
            {
                "remote_path": remote_path,
                "filename": Path(remote_path).name,
                "size": size,
            }
        )
    return rows


def interface_ipv4(interface: str) -> str:
    result = run_command(["ifconfig", interface])
    lines = result.stdout.splitlines()
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("inet ") and "netmask" in stripped:
            return stripped.split()[1]
    raise ValueError(f"Failed to detect IPv4 for interface {interface}")


def write_state(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = iso_utc_now()
    write_json(path, payload)


def sync_file(
    *,
    ftp: dict[str, Any],
    bind_interface: str,
    remote_path: str,
    local_path: Path,
) -> None:
    ensure_dir(local_path.parent)
    netrc_content = f"machine {ftp['host']} login {ftp['username']} password {ftp['password']}\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
        handle.write(netrc_content)
        netrc_path = Path(handle.name)
    os.chmod(netrc_path, 0o600)
    try:
        run_command(
            [
                "curl",
                "--interface",
                bind_interface,
                "--disable-epsv",
                "--fail",
                "--silent",
                "--show-error",
                "--continue-at",
                "-",
                "--netrc-file",
                str(netrc_path),
                "--output",
                str(local_path),
                f"ftp://{ftp['host']}:{ftp['port']}/{remote_path}",
            ],
            env=clean_proxy_env(),
        )
    finally:
        netrc_path.unlink(missing_ok=True)


def main() -> None:
    args = parse_args()
    ftp = load_ftp_config(args.ftp_config, args.source_key)
    bind_ip = interface_ipv4(args.bind_interface)

    year_dir = args.raw_root / args.year
    ensure_dir(year_dir)

    manifest_dir = args.manifest_root / f"ftp_sync_{args.year}"
    ensure_dir(manifest_dir)
    checkpoint_path = manifest_dir / "checkpoint.json"
    heartbeat_path = manifest_dir / "heartbeat.json"
    summary_path = manifest_dir / "summary.json"
    pending_path = manifest_dir / "pending_files.json"

    log_path = args.log_root / f"ftp_sync_{args.year}.log"
    logger = configure_logger(f"ftp_sync_{args.year}", log_path=log_path)

    logger.info("Listing remote year directory %s via %s (%s)", args.year, args.bind_interface, bind_ip)
    remote_files = remote_listing(ftp=ftp, remote_dir=args.year, bind_ip=bind_ip)
    remote_files.sort(key=lambda item: item["filename"])

    pending: list[dict[str, Any]] = []
    for item in remote_files:
        local_path = year_dir / item["filename"]
        local_size = local_path.stat().st_size if local_path.exists() else 0
        if local_size == item["size"]:
            continue
        pending.append(
            {
                **item,
                "local_path": str(local_path),
                "local_size": local_size,
            }
        )

    if args.max_files > 0:
        pending = pending[: args.max_files]

    write_json(pending_path, pending)

    state = {
        "pipeline": "ftp_sync",
        "status": "running",
        "year": args.year,
        "bind_interface": args.bind_interface,
        "bind_ip": bind_ip,
        "remote_file_count": len(remote_files),
        "pending_file_count": len(pending),
        "downloaded_file_count": 0,
        "downloaded_bytes": 0,
        "last_completed_file": None,
        "started_at": iso_utc_now(),
    }
    write_state(checkpoint_path, state)
    write_state(heartbeat_path, state)

    for index, item in enumerate(pending, start=1):
        local_path = Path(item["local_path"])
        logger.info(
            "Downloading %s (%s/%s, remote=%s, local=%s)",
            item["filename"],
            index,
            len(pending),
            item["size"],
            item["local_size"],
        )
        sync_file(
            ftp=ftp,
            bind_interface=args.bind_interface,
            remote_path=item["remote_path"],
            local_path=local_path,
        )
        final_size = local_path.stat().st_size
        if final_size != item["size"]:
            raise RuntimeError(
                f"Downloaded file size mismatch for {local_path}: expected {item['size']}, got {final_size}"
            )
        state["downloaded_file_count"] += 1
        state["downloaded_bytes"] += item["size"] - item["local_size"]
        state["last_completed_file"] = item["filename"]
        write_state(checkpoint_path, state)
        write_state(heartbeat_path, state)

    if args.include_doc:
        logger.info("Doc sync is not implemented yet; skipping remote Doc/ directory.")

    state["status"] = "completed"
    summary = {
        **state,
        "log_path": str(log_path),
        "manifest_dir": str(manifest_dir),
        "raw_year_dir": str(year_dir),
        "pending_path": str(pending_path),
        "completed_at": iso_utc_now(),
    }
    write_state(checkpoint_path, state)
    write_state(heartbeat_path, state)
    write_json(summary_path, summary)
    logger.info(
        "Sync completed for %s: downloaded %s files, %s bytes",
        args.year,
        state["downloaded_file_count"],
        state["downloaded_bytes"],
    )


if __name__ == "__main__":
    main()
