# Orderbook Replay Semantic Release 2026-05-23

## Scope

本页记录 `orders` 生命周期流与 active-order-book replay 验证后的语义松闸边界。

它只释放 `orderbook_replay` / DQA / caveat-only 命名空间，不改变 verified 默认表准入。

## Evidence

- input layer: `candidate_cleaned`
- evidence report: `Research/Audits/hshare_orderbook_reconstruction_probe_20260522.md`
- date: `2026-05-22`
- symbols: `HK.01879`, `HK.01609`, `HK.09988`, `HK.00700`, `HK.00001`, `HK.02382`, `HK.01024`
- tested orders: `1,721,328`
- tested trades: `235,270`

Aggregate `Ext[0]` replay result:

- recommended side candidate: `7/7`
- `BidOrderID` active rate: `100.0000%`
- `AskOrderID` active rate: `99.9996%`
- bid side match rate: `100.0000%`
- ask side match rate: `100.0000%`
- `VolumePre` match rate: `100.0000%`
- `Level` match rate: `93.9303%`
- crossed book rate: `5.4881%`
- trade price inside book rate: `99.2619%`

Aggregate `Ext[1]` replay result:

- crossed book rate: `98.5688%`
- bid side match rate: `79.3296%`
- ask side match rate: `35.7098%`

## Released With Caveat

| field_or_derivation | released_use | boundary |
| --- | --- | --- |
| `OrderType` | lifecycle replay event code: `1=add`, `2=modify`, `3=delete` | vendor event code only; not official native message identity |
| `Ext[0] -> OrderSideVendor` | order side proxy: `0=BID`, `1=ASK` | derived bit only; full `Ext` remains unverified |
| `BidOrderID / AskOrderID` | trade-to-active-order linkage and side-match DQA | caveat-only replay namespace; not verified default; not official native field identity |
| `VolumePre` | previous active order quantity check on modify events | caveat-only DQA; not verified default; not official native field identity |

## Still Blocked

- `Level` as ten-depth truth or queue position
- full `Ext` semantics
- `BrokerNo` broker-alpha or official participant identity
- reconstructed depth as strategy / production replay input
- signed-flow alpha from `Dir` without the existing caveat
- execution realism and strict ordering-sensitive causality from lifecycle evidence alone

## Policy Decision

Machine-readable release entries are now registered in:

- `manifests/field_release_registry.json`

The release unit is the object / namespace, not the raw field alone.

Update `verified_field_policy_2026-03-15.json` as follows:

- `VolumePre`: `admit_with_explicit_caveat_only`
- `BidOrderID`: `admit_with_explicit_caveat_only`
- `AskOrderID`: `admit_with_explicit_caveat_only`
- `Level`: remains `keep_out_for_now`
- `BrokerNo`: remains `keep_out_for_now`
- `BidVolume / AskVolume`: remain `keep_out_for_now`

Default verified tables remain unchanged:

- `verified_orders`
- `verified_trades`

Allowed follow-up namespace:

- `orderbook_replay__caveat_lifecycle_linkage`
- builder: `python -m Scripts.build_orderbook_replay_caveat`
- `orderbook_replay__top_of_book_only`
- builder: `python -m Scripts.build_orderbook_top_of_book_only`

The namespace materializes two evidence tables:

- `lifecycle_events`: order lifecycle rows with `OrderType`, derived `OrderSideVendor`, and `VolumePre` prior-active-volume checks
- `trade_linkage`: trade rows with `BidOrderID / AskOrderID` active-order presence and side-match checks

The top-of-book-only namespace materializes:

- `top_of_book_events`: trade rows with `BestBidReplay / BestAskReplay / ReplaySpread / ReplayMid / TradeInsideBestBookFlag / TopOfBookValidFlag`
- quality gates: `CrossedWindowFlag / ReplayResidueFlag / ReplayWindowExcludedFlag / SameMillisecondBatchRiskFlag`

The namespace must carry explicit manifest metadata:

- `admission_rule = admit_now_plus_caveat_only`
- `contains_caveat_fields = true`
- `source_layer = candidate_cleaned`
- `replay_depth_admission = blocked_until_crossed_book_residue_is_explained_or_bounded`

## Next Gate

Before reconstructed depth can enter replay or strategy workflows, crossed-book residue must be explained or bounded by a contract-level threshold.

The immediate next check should focus on crossed windows:

- adjacent trades
- same-millisecond vendor batch ordering
- `SeqNum / TickID` ordering sufficiency
- whether trade consumption must be applied to active order quantities
