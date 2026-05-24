# OpenD Trading Agent Handoff Contract 20260524

## Output Roots

- universe parquet: `/Volumes/Data/港股Tick数据/reference/newly_listed_hk/year=2026/newly_listed_hk_2026.parquet`
- universe csv: `/Volumes/Data/港股Tick数据/reference/newly_listed_hk/year=2026/newly_listed_hk_2026.csv`
- handoff root: `/Volumes/Data/港股Tick数据/caveat/orderbook_replay__top_of_book_with_size_caveat`
- handoff namespace: `orderbook_replay__top_of_book_with_size_caveat`
- handoff partitions: `top_of_book_events/year=YYYY/date=YYYY-MM-DD/symbol=NNNNN/part-00000.parquet`
- manifests: `manifests/partitions.jsonl`, `manifests/summary.json`, `manifests/heartbeat.json`

## Universe Contract

Downstream should treat only `universe_status == "included"` as the 2026 newly listed stock research universe. All other statuses are fail-closed.

Required universe fields:

- `symbol`
- `instrument_key`
- `listing_date`
- `source_label`
- `stock_research_candidate`
- `candidate_cleaned_trade_dates`
- `candidate_cleaned_order_dates`
- `first_trade_date`
- `last_trade_date`
- `coverage_days`
- `universe_status`
- `caveat`

Current target-symbol status:

- `HK.01609`: `included`, listing_date `2026-05-05`, coverage_days `14`.
- `HK.01879`: not included for 2026 newly listed flow; reference listing_date is `2019-03-29`, kept as watched/unresolved.

## Handoff Schema

Required handoff fields:

- `date`
- `symbol`
- `SendTime`
- `TickID`
- `SeqNum`
- `TradePrice`
- `TradeVolume`
- `BestBidReplay`
- `BestBidSizeReplay`
- `BestAskReplay`
- `BestAskSizeReplay`
- `ReplaySpread`
- `ReplayMid`
- `TopOfBookValidFlag`
- `ReplayQualityScore`
- `CrossedWindowFlag`
- `ReplayResidueFlag`
- `ReplayWindowExcludedFlag`
- `SameMillisecondBatchRiskFlag`
- `SizeSemanticsCaveat`
- `StrategyHandoffEligibleFlag`

## Required Downstream Filter

Use this exact row-level filter before strategy optimization:

```text
StrategyHandoffEligibleFlag == true
TopOfBookValidFlag == true
ReplayQualityScore == 1.0
CrossedWindowFlag == false
ReplayResidueFlag == false
ReplayWindowExcludedFlag == false
SameMillisecondBatchRiskFlag == false
BestBidReplay > 0
BestAskReplay > 0
BestAskReplay >= BestBidReplay
BestBidSizeReplay > 0
BestAskSizeReplay > 0
```

Rows that fail any condition must be excluded from strategy signals.

## Size Boundary

Bid/ask size is bounded active-order replay volume at the best price using the existing Ext[0] side candidate and lifecycle replay. It is useful for strict strategy gating only after `StrategyHandoffEligibleFlag == true`.

It remains caveated:

- not verified executable queue size
- not full-depth reconstruction
- not a verified orderbook semantic release
- not a guarantee of fillability or PnL

Current May 2026 handoff summary:

- processed dates: `2026-05-04` through `2026-05-22` local available dates
- processed symbols: `54`
- total rows: `3,067,538`
- eligible rows: `33,703`
- eligible ratio: `0.010986986958270769`
- blockers: same-millisecond batch ordering, crossed windows, missing/non-positive best size

## futu-opend-execution Notes

The current adapter at `/Users/yxin/AI_Workstation/futu-opend-execution/src/futu_opend_execution/data/hshare_top_of_book.py` can read a root that directly contains `top_of_book_events`, so the new namespace root is path-compatible.

The current newly-listed flow still builds its universe from `--instrument-profile`. Hshare Lab refreshed `/Volumes/Data/港股Tick数据/reference/instrument_profile/latest/instrument_profile.parquet` after refreshing the Tushare-backed seed, so the default profile now contains `HK.01609`.

If futu-opend-execution later wants to consume the Hshare-native universe parquet directly, add a `--universe-path` option in `src/futu_opend_execution/agent/newly_listed.py` and route it before `_load_profile_candidates`.

## Example Commands

```bash
PYTHONPATH=src python -m futu_opend_execution.cli.main newly-listed-universe \
  --listing-year 2026 \
  --top-of-book-root /Volumes/Data/港股Tick数据/caveat/orderbook_replay__top_of_book_with_size_caveat \
  --output-json reports/agent/newly_listed_universe_2026_handoff.json \
  --output-md reports/agent/newly_listed_universe_2026_handoff.md
```

```bash
PYTHONPATH=src python -m futu_opend_execution.cli.main optimize-newly-listed \
  --listing-year 2026 \
  --top-of-book-root /Volumes/Data/港股Tick数据/caveat/orderbook_replay__top_of_book_with_size_caveat \
  --max-symbols 20 \
  --max-dates-per-symbol 5 \
  --overextension-grid 1.5,2.0,2.5 \
  --pullback-grid 0.3,0.5,0.8 \
  --rebuy-anchor-grid 0.5,1.0 \
  --safety-buffer-grid 20,30 \
  --max-sell-ratio-grid 0.25,0.5 \
  --report-json reports/agent/newly_listed_optimizer_handoff.json \
  --report-md reports/agent/newly_listed_optimizer_handoff.md
```
