# Tushare Data Foundation Expansion 2026-05-24

## Goal

把 Tushare 纳入 Hshare Lab v2 的数据底座，但只作为 `reference / sidecar` 层使用，不改写
`raw -> candidate cleaned -> DQA -> semantic verification -> verified layer` 的主链合同。

## Current Smoke Evidence

- `tushare_hk_basic` token / dependency smoke passed on 2026-05-24.
- Temporary output: `/private/tmp/hshare_tushare_smoke_seed.csv`.
- Returned rows: `2739`.
- Normalized seed columns:
  - `instrument_key`
  - `listing_date`
  - `float_mktcap_hkd`
  - `southbound_eligible`
  - `instrument_family`
  - `instrument_family_source`
  - `instrument_family_note`
  - `as_of_date`
  - `source_label`
- `hk_daily` single-symbol permission smoke also passed with 1 row for `00001.HK` on `20190904`.
- `sync_tushare_reference` temporary landing smoke passed:
  - `hk_basic` as of `2026-05-24`: `2739` rows.
  - `hk_daily` trade date `2019-09-04`: `1928` rows.
  - `hk_daily` trade date `2026-05-22`: `2316` rows.

## Upstream Docs Checked

- Tushare `hk_basic`: <https://tushare.pro/document/2?doc_id=191>
- Tushare `hk_daily`: <https://tushare.pro/document/2?doc_id=192>
- Tushare `hk_tradecal`: <https://tushare.pro/document/2?doc_id=250>
- Tushare `hk_adjfactor`: <https://tushare.pro/document/2?doc_id=339>

## Source Boundary

Tushare can expand the data foundation in four lanes:

1. `security master` lane: `hk_basic`
   - Allowed use: listing date, status, ISIN, currency, name/full name, market label.
   - First consumer: `instrument_profile` sidecar and universe research.
   - Not allowed: direct proof of tick field semantics.

2. `daily bar reference` lane: `hk_daily`
   - Allowed use: end-of-day cross-section, low-frequency reconciliation, gap checks, liquidity filters.
   - First consumer: coverage/reconciliation reports and optional daily sidecars.
   - Not allowed: replacing tick/order/trade verified facts or proving intraday semantics.

3. `calendar` lane: `hk_tradecal`
   - Allowed use: trading-day calendar, incremental scheduling, date coverage checks.
   - First consumer: Tushare registry and DQA coverage alignment.
   - Not allowed: proving raw availability by itself.

4. `corporate-action / adjustment` lane: `hk_adjfactor`
   - Allowed use: landed adjustment-factor reference only.
   - Required before use: separate contract and adjusted-return policy.
   - Not allowed: adjusted return or adjusted price truth before that policy exists.

## Implementation Contract

New landing entry:

```bash
python3 -m Scripts.sync_tushare_reference --endpoint hk_basic --as-of-date 2026-05-24
python3 -m Scripts.sync_tushare_reference --endpoint hk_daily --trade-date 20260522
python3 -m Scripts.sync_tushare_reference --endpoint hk_tradecal --start-date 20260101 --end-date 20260524
python3 -m Scripts.sync_tushare_reference --endpoint hk_adjfactor --trade-date 20260522
```

Default output root:

```text
/Volumes/Data/港股Tick数据/reference/tushare/
```

Expected partitions:

```text
reference/tushare/hk_basic/list_status=L/as_of_date=YYYY-MM-DD/hk_basic.parquet
reference/tushare/hk_basic/list_status=L/as_of_date=YYYY-MM-DD/manifest.json
reference/tushare/hk_daily/trade_date=YYYY-MM-DD/hk_daily.parquet
reference/tushare/hk_daily/trade_date=YYYY-MM-DD/manifest.json
reference/tushare/hk_tradecal/year=YYYY/hk_tradecal.parquet
reference/tushare/hk_tradecal/year=YYYY/manifest.json
reference/tushare/hk_adjfactor/trade_date=YYYY-MM-DD/hk_adjfactor.parquet
reference/tushare/hk_adjfactor/trade_date=YYYY-MM-DD/manifest.json
```

Each manifest must include:

- endpoint
- row count
- columns
- output path and bytes
- generated timestamp
- `as_of_date` or `trade_date`
- source role: `reference_landing`

## Admission Rules

- Tushare token stays only in `config/reference_sources.local.json` or `TUSHARE_TOKEN`.
- `config/reference_sources.local.json` stays ignored by Git.
- `tushare_hk_daily` / `tushare_hk_tradecal` / `tushare_hk_adjfactor` are registered but disabled
  in the default source chain, because
  `sync_instrument_profile_seed --sources enabled` is an `instrument_profile` seed workflow.
- No Tushare field enters `verified` without a separate semantic/admission document.
- Long or multi-date runs must start from a single-day real-data smoke and keep manifests resumable.

## Completed Local Landing 2026-05-24

- `hk_basic`:
  - `L` rows: `2739`
  - `D` rows: `753`
  - `P` rows: `0`
- `hk_tradecal`:
  - rows: `144`
  - open trading days from `2026-01-02` to `2026-05-22`: `94`
- `hk_daily`:
  - partitions: `94`
  - rows: `212416`
- `hk_adjfactor`:
  - partitions: `94`
  - rows: `396854`
- Registry:
  - path: `/Volumes/Data/港股Tick数据/reference/tushare/_registry/year=2026.json`
  - entries: `192`
  - row count failures: `0`
- Universe reconciliation:
  - path: `/Volumes/Data/港股Tick数据/dqa/reference/tushare/year=2026/tushare_daily_universe_reconciliation.parquet`
  - report: `Research/Audits/tushare_daily_universe_2026.md`
  - stage zero dates before backfill: `10`
  - stage zero dates after backfill: `0`
  - total daily missing stage after backfill: `222`
  - max daily missing profile: `0`

## Stage Gap Backfill Result

The first reconciliation exposed `10` trading days where Tushare daily bars existed but
`candidate_cleaned/orders` and `candidate_cleaned/trades` were missing. The follow-up stage
backfill confirmed that raw zip files already existed locally, then rebuilt stage and DQA for
`2026-04-13` through `2026-04-24`.

Backfill result:

- stage partitions: completed for orders and trades on all 10 dates.
- stage cleaning: `rejected=0`, `failed_members=0`.
- schema DQA: `completed_count=168`, `failed_count=0`.
- linkage DQA: `completed_count=84`, `failed_count=0`.
- Tushare reconciliation: `stage_zero_date_count=0`, `stage_zero_dates_json=[]`.
- `total_daily_missing_stage`: reduced from `22448` to `222`.

This validates the intended role of the Tushare layer: it did not replace raw/stage, but it
surfaced a real raw-to-stage coverage gap that was then repaired through the main pipeline.

## Recommended Next Step

Use the reconciliation report to separate:

- true raw/stage missing dates
- stock-like instruments
- non-stock instruments such as warrants, CBBCs, ETFs, REITs, and other structured products

This should remain a report/audit surface first, not a verified data rewrite.
