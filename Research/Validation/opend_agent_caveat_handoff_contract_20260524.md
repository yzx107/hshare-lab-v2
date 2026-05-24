# OpenD Agent Caveat Handoff Contract 20260524

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
- `CaveatHandoffReadyFlag`

## Caveat Readiness Rule

`CaveatHandoffReadyFlag` is true only when every row-level predicate below is true:

```text
CaveatHandoffReadyFlag == true
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

Rows that fail any condition are reference-only and must remain fail-closed for this Hshare output.

## Size Boundary

Bid/ask size is bounded active-order replay volume at the best price using the existing Ext[0] side candidate and lifecycle replay. It is only a caveated reference field after `CaveatHandoffReadyFlag == true`.

It remains caveated:

- not verified executable queue size
- not full-depth reconstruction
- not a verified orderbook semantic release
- not a guarantee of executable liquidity, downstream return, or risk behavior

Current May 2026 handoff summary:

- processed dates: `2026-05-04` through `2026-05-22` local available dates
- processed symbols: `54`
- total rows: `3,067,538`
- ready rows: `33,703`
- ready ratio: `0.010986986958270769`
- blockers: same-millisecond batch ordering, crossed windows, missing/non-positive best size

## Consumer Boundary

This branch defines a reference/caveat handoff only. Any consumer workflow must define its own contract before consuming these fields.
