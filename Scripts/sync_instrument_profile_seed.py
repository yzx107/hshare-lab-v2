from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import requests

from Scripts.reference_sources import (
    DEFAULT_LOCAL_SOURCE_CONFIG_PATH,
    DEFAULT_SOURCE_REGISTRY_PATH,
    enabled_source_ids,
    get_registered_source,
    load_local_source_config,
    load_source_registry,
    resolve_source_endpoint,
    resolve_source_secret,
)
from Scripts.runtime import DEFAULT_LOG_ROOT, configure_logger, iso_utc_now, print_scaffold_plan

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEED_PATH = REPO_ROOT / "Research" / "References" / "normalized" / "instrument_profile_seed.csv"
DEFAULT_SOUTHBOUND_SEED_PATH = REPO_ROOT / "Research" / "References" / "normalized" / "hkex_southbound_seed.csv"
DEFAULT_MARKET_CAP_SEED_PATH = REPO_ROOT / "Research" / "References" / "normalized" / "hk_market_snapshot_seed.csv"
DEFAULT_FUTU_HOME = REPO_ROOT / ".tmp" / "futu_home"


SEED_SCHEMA: dict[str, pl.DataType] = {
    "instrument_key": pl.Utf8,
    "listing_date": pl.Utf8,
    "float_mktcap_hkd": pl.Utf8,
    "total_mktcap_hkd": pl.Utf8,
    "circulating_mktcap_hkd": pl.Utf8,
    "market_cap_field_name": pl.Utf8,
    "market_cap_as_of_date": pl.Utf8,
    "market_cap_source_label": pl.Utf8,
    "market_cap_currency": pl.Utf8,
    "market_cap_admissibility_note": pl.Utf8,
    "latest_turnover_hkd": pl.Utf8,
    "latest_volume_shares": pl.Utf8,
    "liquidity_field_name": pl.Utf8,
    "liquidity_as_of_date": pl.Utf8,
    "liquidity_source_label": pl.Utf8,
    "liquidity_currency": pl.Utf8,
    "southbound_eligible": pl.Utf8,
    "southbound_as_of_date": pl.Utf8,
    "southbound_source_label": pl.Utf8,
    "instrument_family": pl.Utf8,
    "instrument_family_source": pl.Utf8,
    "instrument_family_note": pl.Utf8,
    "as_of_date": pl.Utf8,
    "source_label": pl.Utf8,
}

SOUTHBOUND_SEED_COLUMNS = ["instrument_key", "southbound_eligible", "as_of_date", "source_label"]

MARKET_SNAPSHOT_COLUMNS = [
    "instrument_key",
    "float_mktcap_hkd",
    "total_mktcap_hkd",
    "circulating_mktcap_hkd",
    "market_cap_field_name",
    "market_cap_as_of_date",
    "market_cap_source_label",
    "market_cap_currency",
    "market_cap_admissibility_note",
    "latest_turnover_hkd",
    "latest_volume_shares",
    "liquidity_field_name",
    "liquidity_as_of_date",
    "liquidity_source_label",
    "liquidity_currency",
    "as_of_date",
    "source_label",
]

SOUTHBOUND_REPLACE_COLUMNS = {
    "southbound_eligible",
    "southbound_as_of_date",
    "southbound_source_label",
}

MARKET_SNAPSHOT_REPLACE_COLUMNS = {
    "float_mktcap_hkd",
    "total_mktcap_hkd",
    "circulating_mktcap_hkd",
    "market_cap_field_name",
    "market_cap_as_of_date",
    "market_cap_source_label",
    "market_cap_currency",
    "market_cap_admissibility_note",
    "latest_turnover_hkd",
    "latest_volume_shares",
    "liquidity_field_name",
    "liquidity_as_of_date",
    "liquidity_source_label",
    "liquidity_currency",
}


def normalize_instrument_key_expr(column_name: str = "instrument_key") -> pl.Expr:
    return pl.col(column_name).cast(pl.Utf8).str.strip_chars().str.zfill(5).alias(column_name)


def normalize_instrument_key_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.zfill(5)


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "NA", "N/A", "NULL", "null"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync instrument_profile seed from registered reference sources such as Tushare, curated HKEX seed CSVs, or OpenD current snapshots."
    )
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=DEFAULT_SEED_PATH,
        help="Target instrument profile seed CSV path.",
    )
    parser.add_argument(
        "--registry-path",
        type=Path,
        default=DEFAULT_SOURCE_REGISTRY_PATH,
        help="Reference source registry config path.",
    )
    parser.add_argument(
        "--local-config-path",
        type=Path,
        default=DEFAULT_LOCAL_SOURCE_CONFIG_PATH,
        help="Local private source config path.",
    )
    parser.add_argument(
        "--sources",
        default="enabled",
        help="Comma-separated source ids to run, or 'enabled' for all enabled sources in the registry.",
    )
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="As-of date recorded for source rows when the upstream source does not provide one.",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=DEFAULT_LOG_ROOT,
        help="Root directory for run logs.",
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def normalize_seed_frame(frame: pl.DataFrame | None) -> pl.DataFrame:
    if frame is None or frame.height == 0:
        return pl.DataFrame(schema=SEED_SCHEMA)
    normalized = frame
    for column_name, dtype in SEED_SCHEMA.items():
        if column_name not in normalized.columns:
            normalized = normalized.with_columns(pl.lit(None, dtype=dtype).alias(column_name))
        else:
            normalized = normalized.with_columns(pl.col(column_name).cast(dtype, strict=False).alias(column_name))
    normalized = normalized.select(list(SEED_SCHEMA))
    if "instrument_key" in normalized.columns:
        normalized = normalized.with_columns(normalize_instrument_key_expr())
    return normalized


def load_existing_seed(seed_path: Path) -> pl.DataFrame:
    if not seed_path.exists() or seed_path.stat().st_size == 0:
        return normalize_seed_frame(None)
    return normalize_seed_frame(pl.read_csv(seed_path, null_values=["", "NA", "N/A", "NULL", "null"]))


def merge_seed(
    base: pl.DataFrame,
    incoming: pl.DataFrame,
    *,
    replace_columns: set[str] | None = None,
) -> pl.DataFrame:
    if incoming.height == 0:
        return base
    replace_columns = set() if replace_columns is None else replace_columns
    left = base.rename({column: f"base__{column}" for column in base.columns if column != "instrument_key"})
    right = incoming.with_columns(pl.lit(True).alias("_incoming_present")).rename(
        {column: f"incoming__{column}" for column in incoming.columns if column != "instrument_key"}
    )
    joined = left.join(right, on="instrument_key", how="full", coalesce=True)
    expressions = [pl.col("instrument_key")]
    for column_name in SEED_SCHEMA:
        if column_name == "instrument_key":
            continue
        if column_name in replace_columns:
            expressions.append(
                pl.when(pl.col("_incoming_present") == True)
                .then(pl.col(f"incoming__{column_name}"))
                .otherwise(pl.col(f"base__{column_name}"))
                .alias(column_name)
            )
        else:
            expressions.append(pl.coalesce(f"base__{column_name}", f"incoming__{column_name}").alias(column_name))
    return normalize_seed_frame(joined.select(expressions)).unique(subset=["instrument_key"], keep="first").sort("instrument_key")


def fetch_tushare_hk_basic(token: str, as_of_date: str) -> pl.DataFrame:
    try:
        import pandas as pd
        import tushare as ts
    except ImportError as exc:
        raise SystemExit("Tushare dependencies are missing. Install the optional reference dependencies first.") from exc

    pro = ts.pro_api(token)
    pandas_frame = pro.hk_basic(
        fields="ts_code,name,fullname,market,list_status,list_date,trade_unit,isin,curr_type"
    )
    if pandas_frame is None or len(pandas_frame) == 0:
        return normalize_seed_frame(None)
    frame = pl.DataFrame(pd.DataFrame(pandas_frame).to_dict(orient="records"))
    return normalize_seed_frame(
        frame.with_columns(
            pl.col("ts_code").cast(pl.Utf8).str.replace(r"\.HK$", "").str.zfill(5).alias("instrument_key"),
            pl.col("list_date")
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m-%d")
            .alias("listing_date"),
            pl.lit(as_of_date).alias("as_of_date"),
            pl.lit("tushare_hk_basic").alias("source_label"),
        ).select(
            "instrument_key",
            "listing_date",
            pl.lit(None, dtype=pl.Utf8).alias("float_mktcap_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("total_mktcap_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("circulating_mktcap_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_field_name"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_as_of_date"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_source_label"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_currency"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_admissibility_note"),
            pl.lit(None, dtype=pl.Utf8).alias("latest_turnover_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("latest_volume_shares"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_field_name"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_as_of_date"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_source_label"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_currency"),
            pl.lit(None, dtype=pl.Utf8).alias("southbound_eligible"),
            pl.lit(None, dtype=pl.Utf8).alias("southbound_as_of_date"),
            pl.lit(None, dtype=pl.Utf8).alias("southbound_source_label"),
            pl.lit(None, dtype=pl.Utf8).alias("instrument_family"),
            pl.lit(None, dtype=pl.Utf8).alias("instrument_family_source"),
            pl.lit(None, dtype=pl.Utf8).alias("instrument_family_note"),
            "as_of_date",
            "source_label",
        )
    )


def load_curated_seed(path: Path) -> pl.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return normalize_seed_frame(None)
    return normalize_seed_frame(pl.read_csv(path, null_values=["", "NA", "N/A", "NULL", "null"]))


def normalize_opend_listing_date(frame: pl.DataFrame) -> pl.DataFrame:
    if "listing_date" not in frame.columns:
        return frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias("listing_date"))
    return frame.with_columns(
        pl.when(pl.col("listing_date").cast(pl.Utf8, strict=False).str.strip_chars() == "1970-01-01")
        .then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.col("listing_date").cast(pl.Utf8, strict=False).str.strip_chars())
        .alias("listing_date")
    )


def opend_instrument_family_expr() -> pl.Expr:
    return (
        pl.when(pl.col("stock_type") == "ETF")
        .then(pl.lit("exchange_traded_fund"))
        .when(pl.col("stock_type") == "IDX")
        .then(pl.lit("market_index"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("instrument_family")
    )


def opend_instrument_family_note_expr() -> pl.Expr:
    return (
        pl.when(pl.col("stock_type") == "ETF")
        .then(
            pl.lit(
                "Current OpenD security snapshot classifies this instrument as ETF. Use as secondary reference only; curated HKEX REIT seed still has higher priority."
            )
        )
        .when(pl.col("stock_type") == "IDX")
        .then(pl.lit("Current OpenD security snapshot classifies this instrument as market index."))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("instrument_family_note")
    )


def opend_seed_from_basicinfo(frame: pl.DataFrame, as_of_date: str) -> pl.DataFrame:
    if frame.height == 0:
        return normalize_seed_frame(None)
    normalized = normalize_opend_listing_date(frame)
    return normalize_seed_frame(
        normalized.with_columns(
            pl.col("code").cast(pl.Utf8).str.replace(r"^HK\.", "").str.zfill(5).alias("instrument_key"),
            opend_instrument_family_expr(),
            pl.when(pl.col("stock_type").is_in(["ETF", "IDX"]))
            .then(pl.lit("opend_security_snapshot"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("instrument_family_source"),
            opend_instrument_family_note_expr(),
            pl.lit(as_of_date).alias("as_of_date"),
            pl.lit("opend_security_snapshot").alias("source_label"),
        ).select(
            "instrument_key",
            "listing_date",
            pl.lit(None, dtype=pl.Utf8).alias("float_mktcap_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("total_mktcap_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("circulating_mktcap_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_field_name"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_as_of_date"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_source_label"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_currency"),
            pl.lit(None, dtype=pl.Utf8).alias("market_cap_admissibility_note"),
            pl.lit(None, dtype=pl.Utf8).alias("latest_turnover_hkd"),
            pl.lit(None, dtype=pl.Utf8).alias("latest_volume_shares"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_field_name"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_as_of_date"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_source_label"),
            pl.lit(None, dtype=pl.Utf8).alias("liquidity_currency"),
            pl.lit(None, dtype=pl.Utf8).alias("southbound_eligible"),
            pl.lit(None, dtype=pl.Utf8).alias("southbound_as_of_date"),
            pl.lit(None, dtype=pl.Utf8).alias("southbound_source_label"),
            "instrument_family",
            "instrument_family_source",
            "instrument_family_note",
            "as_of_date",
            "source_label",
        )
    )


def fetch_opend_security_snapshot(host: str, port: int, as_of_date: str) -> pl.DataFrame:
    previous_home = os.environ.get("HOME")
    DEFAULT_FUTU_HOME.mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = str(DEFAULT_FUTU_HOME)
    context = None
    try:
        import pandas as pd
        from futu import Market, OpenQuoteContext, RET_OK, SecurityType
    except ImportError as exc:
        raise RuntimeError("OpenD dependencies are missing. Install the optional reference dependencies first.") from exc
    try:
        context = OpenQuoteContext(host=host, port=port)
        frames: list[pl.DataFrame] = []
        for security_type in (
            SecurityType.STOCK,
            SecurityType.ETF,
            SecurityType.WARRANT,
            SecurityType.BOND,
            SecurityType.IDX,
        ):
            ret, data = context.get_stock_basicinfo(Market.HK, security_type)
            if ret != RET_OK or data is None or len(data) == 0:
                continue
            frames.append(pl.DataFrame(pd.DataFrame(data).to_dict(orient="records")))
        if not frames:
            return normalize_seed_frame(None)
        combined = pl.concat(frames, how="diagonal_relaxed")
        seed = opend_seed_from_basicinfo(combined, as_of_date)
        return normalize_seed_frame(
            seed.group_by("instrument_key")
            .agg(
                pl.col("listing_date").drop_nulls().first().alias("listing_date"),
                pl.col("float_mktcap_hkd").drop_nulls().first().alias("float_mktcap_hkd"),
                pl.col("southbound_eligible").drop_nulls().first().alias("southbound_eligible"),
                pl.col("instrument_family").drop_nulls().first().alias("instrument_family"),
                pl.col("instrument_family_source").drop_nulls().first().alias("instrument_family_source"),
                pl.col("instrument_family_note").drop_nulls().first().alias("instrument_family_note"),
                pl.col("as_of_date").drop_nulls().first().alias("as_of_date"),
                pl.col("source_label").drop_nulls().first().alias("source_label"),
            )
            .sort("instrument_key")
        )
    finally:
        try:
            context.close()  # type: ignore[name-defined]
        except Exception:
            pass
        if previous_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = previous_home


def parse_jsonp_payload(text: str) -> dict[str, Any]:
    match = re.match(r"^[^(]+\((.*)\)\s*$", text, re.S)
    return json.loads(match.group(1) if match else text)


def get_with_retries(
    url: str,
    *,
    params: dict[str, Any],
    headers: dict[str, str],
    timeout_seconds: int,
    attempts: int = 4,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, params=params, timeout=timeout_seconds, headers=headers)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"GET failed after {attempts} attempts: {url}") from last_error


def fetch_sse_southbound_rows(timeout_seconds: int = 8) -> tuple[list[dict[str, Any]], str | None]:
    params = {
        "jsonCallBack": "jsonpCallback",
        "isPagination": "true",
        "sqlId": "COMMON_SSE_JYFW_HGT_XXPL_BDZQQD_L",
        "pageHelp.pageSize": "5000",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "1",
        "keyword": "",
    }
    response = get_with_retries(
        "http://query.sse.com.cn/commonQuery.do",
        params=params,
        timeout_seconds=timeout_seconds,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "http://www.sse.com.cn/services/hkexsc/disclo/eligible/",
        },
        attempts=2,
    )
    payload = parse_jsonp_payload(response.text)
    rows = payload.get("result") or payload.get("pageHelp", {}).get("data") or []
    update_dates = sorted({str(row.get("UPDATE_DATE", "")).strip() for row in rows if row.get("UPDATE_DATE")})
    return rows, update_dates[-1] if update_dates else None


def fetch_szse_southbound_rows(timeout_seconds: int = 8) -> tuple[list[dict[str, Any]], str | None]:
    rows: list[dict[str, Any]] = []
    as_of_date: str | None = None
    page_no = 1
    page_count = 1
    while page_no <= page_count:
        response = get_with_retries(
            "https://www.szse.cn/api/report/ShowReport/data",
            params={
                "CATALOGID": "SGT_GGTBDQD",
                "TABKEY": "tab1",
                "PAGENO": str(page_no),
                "random": str(time.time()),
            },
            timeout_seconds=timeout_seconds,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.szse.cn/szhk/hkbussiness/underlylist/",
            },
            attempts=2,
        )
        payload = response.json()
        table = payload[0]
        metadata = table.get("metadata", {})
        as_of_date = as_of_date or str(metadata.get("subname") or "").strip() or None
        page_count = int(metadata.get("pagecount") or 1)
        rows.extend(table.get("data") or [])
        page_no += 1
    return rows, as_of_date


def southbound_frame_from_seed(seed: pl.DataFrame) -> pl.DataFrame:
    return normalize_seed_frame(
        seed.with_columns(
            pl.col("as_of_date").alias("southbound_as_of_date"),
            pl.col("source_label").alias("southbound_source_label"),
            pl.lit(None, dtype=pl.Utf8).alias("float_mktcap_hkd"),
        )
    )


def stock_connect_southbound_seed(as_of_date: str, output_path: Path, logger) -> pl.DataFrame:
    try:
        sse_rows, sse_as_of = fetch_sse_southbound_rows()
        szse_rows, szse_as_of = fetch_szse_southbound_rows()
    except Exception as exc:
        if output_path.exists() and output_path.stat().st_size > 0:
            cached = pl.read_csv(output_path, null_values=["", "NA", "N/A", "NULL", "null"])
            logger.warning("Southbound official fetch failed; reusing cached seed path=%s reason=%s", output_path, exc)
            return southbound_frame_from_seed(cached)
        raise
    merged: dict[str, dict[str, Any]] = {}
    for row in sse_rows:
        key = normalize_instrument_key_value(row.get("SECURITY_CODE"))
        if not key:
            continue
        merged.setdefault(key, {"instrument_key": key, "source_labels": set(), "as_of_dates": set()})
        merged[key]["source_labels"].add("sse_southbound_eligible")
        if sse_as_of:
            merged[key]["as_of_dates"].add(sse_as_of)
    for row in szse_rows:
        key = normalize_instrument_key_value(row.get("zqdm"))
        if not key:
            continue
        merged.setdefault(key, {"instrument_key": key, "source_labels": set(), "as_of_dates": set()})
        merged[key]["source_labels"].add("szse_southbound_eligible")
        if szse_as_of:
            merged[key]["as_of_dates"].add(szse_as_of)

    rows = []
    for key, row in sorted(merged.items()):
        as_of_dates = sorted(row["as_of_dates"])
        source_labels = sorted(row["source_labels"])
        rows.append(
            {
                "instrument_key": key,
                "southbound_eligible": "true",
                "as_of_date": as_of_dates[-1] if as_of_dates else as_of_date,
                "source_label": ";".join(source_labels),
            }
        )
    seed = pl.DataFrame(rows, schema={column: pl.Utf8 for column in SOUTHBOUND_SEED_COLUMNS})
    ensure_parent(output_path)
    seed.select(SOUTHBOUND_SEED_COLUMNS).write_csv(output_path)
    logger.info(
        "Southbound official seed refreshed: rows=%s sse_rows=%s szse_rows=%s output=%s",
        seed.height,
        len(sse_rows),
        len(szse_rows),
        output_path,
    )
    return southbound_frame_from_seed(seed)


def fetch_eastmoney_hk_snapshot_rows(timeout_seconds: int = 8) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page_no = 1
    page_size = 500
    total = None
    while total is None or len(rows) < total:
        params = {
            "pn": str(page_no),
            "pz": str(page_size),
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "fid": "f12",
            "fs": "m:128 t:3,m:128 t:4,m:128 t:1,m:128 t:2",
            "fields": "f12,f14,f2,f3,f5,f6,f20,f21,f26,f100,f152",
        }
        last_error: Exception | None = None
        response = None
        for host in ("https://33.push2.eastmoney.com", "https://push2.eastmoney.com"):
            for attempt in range(2):
                try:
                    session = requests.Session()
                    session.trust_env = False
                    response = session.get(
                        f"{host}/api/qt/clist/get",
                        params=params,
                        timeout=timeout_seconds,
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    response.raise_for_status()
                    break
                except requests.RequestException as exc:
                    last_error = exc
                    response = None
                    time.sleep(0.5 * (attempt + 1))
            if response is not None:
                break
        if response is None:
            raise RuntimeError(f"Eastmoney HK snapshot page {page_no} failed after retries: {last_error}") from last_error
        payload = response.json().get("data") or {}
        total = int(payload.get("total") or 0)
        batch = payload.get("diff") or []
        if not batch:
            break
        rows.extend(batch)
        page_no += 1
    return rows


def eastmoney_market_snapshot_seed(as_of_date: str, output_path: Path, logger) -> pl.DataFrame:
    rows = []
    try:
        source_rows = fetch_eastmoney_hk_snapshot_rows()
    except Exception as exc:
        if output_path.exists() and output_path.stat().st_size > 0:
            cached = pl.read_csv(output_path, null_values=["", "NA", "N/A", "NULL", "null"])
            logger.warning("Market snapshot fetch failed; reusing cached seed path=%s reason=%s", output_path, exc)
            return normalize_seed_frame(cached)
        raise
    for row in source_rows:
        key = normalize_instrument_key_value(row.get("f12"))
        if not key:
            continue
        code_int = int(key)
        if code_int >= 10000:
            continue
        total_mktcap = parse_number(row.get("f20"))
        circulating_mktcap = parse_number(row.get("f21"))
        turnover = parse_number(row.get("f6"))
        volume = parse_number(row.get("f5"))
        if total_mktcap is None and circulating_mktcap is None and turnover is None and volume is None:
            continue
        rows.append(
            {
                "instrument_key": key,
                "float_mktcap_hkd": None,
                "total_mktcap_hkd": f"{total_mktcap:.6f}" if total_mktcap is not None else None,
                "circulating_mktcap_hkd": f"{circulating_mktcap:.6f}" if circulating_mktcap is not None else None,
                "market_cap_field_name": "eastmoney_f20_total_market_cap;eastmoney_f21_circulating_market_cap",
                "market_cap_as_of_date": as_of_date,
                "market_cap_source_label": "eastmoney_hk_spot_market_snapshot",
                "market_cap_currency": "HKD",
                "market_cap_admissibility_note": (
                    "Public Eastmoney HK quote snapshot. f20/f21 are treated as total/circulating market cap reference only; "
                    "do not promote to float_mktcap_hkd or verified fact truth."
                ),
                "latest_turnover_hkd": f"{turnover:.6f}" if turnover is not None else None,
                "latest_volume_shares": f"{volume:.6f}" if volume is not None else None,
                "liquidity_field_name": "eastmoney_f6_turnover;eastmoney_f5_volume",
                "liquidity_as_of_date": as_of_date,
                "liquidity_source_label": "eastmoney_hk_spot_market_snapshot",
                "liquidity_currency": "HKD",
                "as_of_date": as_of_date,
                "source_label": "eastmoney_hk_spot_market_snapshot",
            }
        )
    seed = pl.DataFrame(rows, schema={column: pl.Utf8 for column in MARKET_SNAPSHOT_COLUMNS})
    ensure_parent(output_path)
    seed.select(MARKET_SNAPSHOT_COLUMNS).write_csv(output_path)
    logger.info("Market snapshot seed refreshed: rows=%s output=%s", seed.height, output_path)
    return normalize_seed_frame(seed)


def selected_source_ids(args: argparse.Namespace, registry: dict[str, Any]) -> list[str]:
    if args.sources == "enabled":
        return enabled_source_ids(registry)
    values = [token.strip() for token in args.sources.split(",") if token.strip()]
    if not values:
        raise SystemExit("At least one source id is required.")
    return values


def run_sources(
    *,
    source_ids: list[str],
    registry: dict[str, Any],
    local_config: dict[str, Any],
    as_of_date: str,
    base_seed: pl.DataFrame,
    logger,
) -> tuple[list[dict[str, Any]], pl.DataFrame]:
    summaries: list[dict[str, Any]] = []
    merged = base_seed
    for source_id in source_ids:
        source = get_registered_source(source_id, registry)
        if source_id == "tushare_hk_basic":
            token = resolve_source_secret(source_id, registry=registry, local_config=local_config)
            if not token:
                raise SystemExit(
                    "Missing Tushare token. Set TUSHARE_TOKEN or populate config/reference_sources.local.json."
                )
            frame = fetch_tushare_hk_basic(token, as_of_date)
        elif source.get("kind") == "curated_csv":
            source_path = REPO_ROOT / Path(source["path"])
            frame = load_curated_seed(source_path)
        elif source_id == "stock_connect_southbound_official":
            output_path = REPO_ROOT / Path(source.get("path", DEFAULT_SOUTHBOUND_SEED_PATH))
            frame = stock_connect_southbound_seed(as_of_date, output_path, logger)
        elif source_id == "eastmoney_hk_spot_market_snapshot":
            output_path = REPO_ROOT / Path(source.get("path", DEFAULT_MARKET_CAP_SEED_PATH))
            frame = eastmoney_market_snapshot_seed(as_of_date, output_path, logger)
        elif source_id == "opend_security_snapshot":
            endpoint = resolve_source_endpoint(source_id, registry=registry, local_config=local_config)
            try:
                frame = fetch_opend_security_snapshot(
                    str(endpoint["host"]),
                    int(endpoint["port"]),
                    as_of_date,
                )
            except Exception as exc:
                logger.warning("OpenD source skipped: host=%s port=%s reason=%s", endpoint["host"], endpoint["port"], exc)
                frame = normalize_seed_frame(None)
        else:
            raise SystemExit(f"Unsupported source id for seed sync: {source_id}")
        logger.info("Source %s rows=%s", source_id, frame.height)
        summaries.append({"source_id": source_id, "row_count": int(frame.height)})
        if source_id == "stock_connect_southbound_official":
            replace_columns = SOUTHBOUND_REPLACE_COLUMNS
        elif source_id == "eastmoney_hk_spot_market_snapshot":
            replace_columns = MARKET_SNAPSHOT_REPLACE_COLUMNS
        else:
            replace_columns = set()
        merged = merge_seed(merged, frame, replace_columns=replace_columns)
    return summaries, merged


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="sync_instrument_profile_seed",
            purpose="Sync instrument_profile seed from registered reference sources such as Tushare and curated HKEX seed CSVs.",
            responsibilities=[
                "Load the registered reference source registry and local private config.",
                "Fetch or load source rows from enabled sources.",
                "Merge source rows into Research/References/normalized/instrument_profile_seed.csv without overwriting existing non-null values by default.",
                "Keep source roles explicit for listing_date, southbound, and instrument-family enrichment.",
                "Treat OpenD only as secondary current-snapshot reference, not as semantic proof.",
            ],
            inputs=[
                "config/reference_sources.example.json",
                "config/reference_sources.local.json",
                "Research/References/normalized/instrument_profile_seed.csv",
                "Research/References/normalized/hkex_reit_seed.csv",
                "Research/References/normalized/hkex_southbound_seed.csv",
                "Research/References/normalized/hk_market_snapshot_seed.csv",
                "local OpenD quote service (optional)",
            ],
            outputs=[
                "Research/References/normalized/instrument_profile_seed.csv",
            ],
        )
        return 0

    logger = configure_logger("sync_instrument_profile_seed", args.log_root / "sync_instrument_profile_seed.log")
    registry = load_source_registry(args.registry_path)
    local_config = load_local_source_config(args.local_config_path)
    source_ids = selected_source_ids(args, registry)
    base_seed = load_existing_seed(args.seed_path)
    source_summaries, merged = run_sources(
        source_ids=source_ids,
        registry=registry,
        local_config=local_config,
        as_of_date=args.as_of_date,
        base_seed=base_seed,
        logger=logger,
    )
    merged.write_csv(args.seed_path)
    logger.info(
        "Instrument profile seed sync complete: rows=%s generated_at=%s sources=%s",
        merged.height,
        iso_utc_now(),
        ",".join(source_id for source_id in source_ids),
    )
    for summary in source_summaries:
        logger.info("Source summary: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
