# Downstream Research Surface Contract 2026-05-24

## 结论

`verified default` 现在刻意很窄，但 repo 仍然可以继续向下游 research 提供有用输入。正确消费方式不是把更多 raw/vendor 字段直接塞进 `verified`，而是把可研究对象分成：

- `verified_default`: 默认事实入口，低风险、可无脑读取。
- `explicit_caveat_research`: 字段名和 vendor 定义明确，但仍必须带 caveat。
- `top_of_book_bounded`: 只放开 best bid/ask、spread、mid 和质量 gate，不放开 full depth。
- `blocked_keep_out`: 只保留为 future work 或 blocker，不得作为研究输入。

本页是下游 research 的入口 contract；机器可读版本见 [downstream_research_surface_contract.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/manifests/downstream_research_surface_contract.json)。

## Source Of Truth

- [verified_field_policy_2026-03-15.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_field_policy_2026-03-15.json)
- [verified_admission_matrix_2026-03-18.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_matrix_2026-03-18.md)
- [field_release_registry.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/manifests/field_release_registry.json)
- [field_release_mechanism_2026-05.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/field_release_mechanism_2026-05.md)
- [orderbook_research_handoff_2026-05.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/orderbook_research_handoff_2026-05.md)
- [information_theory_admissibility.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/information_theory_admissibility.md)
- [l2_field_semantics_evidence_matrix_2026-05-24.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/l2_field_semantics_evidence_matrix_2026-05-24.md)

## Surface Matrix

| surface | namespace / table | status | allowed variables | allowed research | hard boundary |
| --- | --- | --- | --- | --- | --- |
| `verified_default` | `verified_orders`, `verified_trades` | default formal input | tech/provenance columns, `instrument_key`, `Time`, `Price`, `Volume`, `SeqNum`, `OrderId`, `TickID`, 2026-only `SendTime` | price/volume/event-count research, grouping, coarse/fine timing where year policy allows, entropy/MI baseline | not a full official native dictionary; 2025 stays `coarse_only` |
| `explicit_caveat_research` | `verified_orders__caveat_ordertype_ordersidevendor`, `verified_trades__caveat_dir`, `orderbook_replay__caveat_lifecycle_linkage` | usable only with caveat metadata | `OrderTypeLifecycleEventCode`, `OrderSideVendor`, `Dir`, `Type`, `TradeToActiveOrderLinkageEvidence`, `PriorActiveVolumeCheck` | lifecycle shape, vendor-side order flow proxy, trade direction proxy, active-order linkage DQA, special trade-type buckets | no confirmed HKEX native event semantics, no default verified admission |
| `top_of_book_bounded` | `orderbook_replay__top_of_book_only.top_of_book_events` | bounded release | `BestBidReplay`, `BestAskReplay`, `ReplaySpread`, `ReplayMid`, `TradeInsideBestBookFlag`, `TopOfBookValidFlag`, `ReplayQualityScore`, quality flags | spread/mid descriptive checks, trade-inside-best-book reports, bounded microstructure diagnostics | must filter quality gates; no full reconstructed depth, queue, or fill realism |
| `stage_internal` | `candidate_cleaned.orders`, `candidate_cleaned.trades` | internal evidence / feasibility only | all retained stage fields | DQA, semantic verification, future admission probes | not a downstream formal research layer |
| `blocked_keep_out` | none | blocked | `BrokerNo`, `Level`, full `Ext`, `BidVolume`, `AskVolume`, `FullReconstructedDepth`, queue position, fill realism | blocker tracking and manual evidence collection only | cannot be repackaged into strategy or default research input |

Explicit unverified caveat: the caveat and blocked fields above are vendor-defined or derived objects whose names are informative but whose downstream business semantics are not fully verified. They may be retained in stage, DQA, caveat namespace, or bounded top-of-book outputs, but they must not be silently promoted into default verified facts.

## Year Boundary

| year | default timing status | usable default surface | caveat/bounded surface |
| --- | --- | --- | --- |
| 2025 | `coarse_only`; raw layout is `OrderAdd` / `OrderModifyDelete` / `TradeResumes` | `verified_orders` and `verified_trades` without default `SendTime` | lifecycle/event-count/coarse linkage caveats only; no fine lead-lag or latency-like research |
| 2026 | `fine_ok`; raw layout is `order` / `trade` | `verified_orders` and `verified_trades` with default `SendTime` | fine timing, caveat lifecycle/linkage, and top-of-book bounded research are allowed when quality gates pass |

`candidate_cleaned` may present a common table shape across years, but downstream code must keep the year caveat. A 2026 variable admission does not automatically backfill 2025.

## Default Verified Columns

`verified_orders`:

- 2025: `date`, `table_name`, `source_file`, `ingest_ts`, `row_num_in_file`, `SeqNum`, `OrderId`, `Time`, `Price`, `Volume`, `instrument_key`
- 2026: `date`, `table_name`, `source_file`, `ingest_ts`, `row_num_in_file`, `SeqNum`, `OrderId`, `Time`, `SendTime`, `Price`, `Volume`, `instrument_key`

`verified_trades`:

- 2025: `date`, `table_name`, `source_file`, `ingest_ts`, `row_num_in_file`, `TickID`, `Time`, `Price`, `Volume`, `instrument_key`
- 2026: `date`, `table_name`, `source_file`, `ingest_ts`, `row_num_in_file`, `TickID`, `Time`, `SendTime`, `Price`, `Volume`, `instrument_key`

## Caveat Research Variables

These objects are useful enough to develop downstream research, but every output must record `contains_caveat_fields=true`:

- `OrderTypeLifecycleEventCode`: vendor lifecycle code only, `1=Add`, `2=Modify`, `3=Delete`.
- `OrderSideVendor`: derived from `Ext[0]`; vendor-side order side proxy only.
- `Dir`: vendor-derived aggressor proxy; not confirmed signed-flow truth.
- `Type`: vendor public-trade-type bucket.
- `TradeToActiveOrderLinkageEvidence`: DQA/replay evidence for `BidOrderID` / `AskOrderID` linkage.
- `PriorActiveVolumeCheck`: modify-event consistency evidence using `VolumePre`.

Allowed downstream modules:

- order lifecycle intensity by event count
- trade/order arrival imbalance under year caveat
- vendor-side order flow proxy
- trade direction proxy with `Dir=0` and special `Type` buckets separated
- active-order linkage coverage and side-match reporting
- entropy / MI / TE only under [information_theory_admissibility.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/information_theory_admissibility.md)

## Top-Of-Book Bounded Variables

Default consumption filter:

```text
TopOfBookValidFlag = true
ReplayQualityScore = 1.0
CrossedWindowFlag = false
ReplayResidueFlag = false
ReplayWindowExcludedFlag = false
SameMillisecondBatchRiskFlag = false
```

Allowed variables:

- `BestBidReplay`
- `BestAskReplay`
- `ReplaySpread`
- `ReplayMid`
- `TradeInsideBestBookFlag`
- `TopOfBookValidFlag`
- `ReplayQualityScore`
- `CrossedWindowFlag`
- `ReplayResidueFlag`
- `ReplayWindowExcludedFlag`
- `SameMillisecondBatchRiskFlag`

Allowed downstream modules:

- bounded spread / mid descriptive diagnostics
- trade-inside-best-book checks
- filter-aware top-of-book sample construction
- manual review queue for crossed / residue / same-millisecond risk

`ReplayQualityScore` is a gate, not an alpha signal and not a continuous quality score.

## Blocked Objects

The following remain outside downstream formal research:

- `BrokerNo`
- `Level`
- full `Ext`
- `BidVolume`
- `AskVolume`
- `FullReconstructedDepth`
- queue position
- queue depletion
- fill priority
- execution realism
- broker alpha

They may appear in stage, DQA, or blocker notes, but not as default verified inputs or strategy-ready fields.

## Completion Gate For Future Expansion

Any future expansion must update the policy/registry before code consumes the new object:

1. Add or update a `field_release_registry.json` entry.
2. State allowed uses, forbidden claims, evidence docs, downstream namespace, and blocker.
3. Run `python3 -m Scripts.validate_field_release --json`.
4. Run `python3 -m Scripts.report_field_policy_check --report <changed report>`.
5. Verify year-specific behavior separately for 2025 and 2026.

No downstream repo should treat `candidate_cleaned` as a shortcut around this contract.
