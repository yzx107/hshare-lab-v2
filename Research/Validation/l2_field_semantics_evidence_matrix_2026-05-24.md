# L2 字段语义证据矩阵 2026-05-24

## 结论

当前可以确认的是：

- 当前数据属于 HKEX `OMD-C` / `Securities FullTick` 体系的 L2 / market-by-order 数据来源。
- 当前 `candidate_cleaned.orders` / `candidate_cleaned.trades` 与 HKEX Historical Full Book / OMD-C 的核心 order / trade 能力兼容。
- 当前 vendor CSV 字段不应被当作 HKEX OMD-C binary message 或 HKEX Historical Full Book CSV 的 `1:1` 原样字段字典。
- 本页不是单一跨年原始字段字典。2025 与 2026 的 raw source layout / vendor export dictionary 不同，`candidate_cleaned` 只是把它们归一化成共同 stage contract。

最重要的边界：

- `OrderType` 在当前 vendor CSV 中是 `1=Add / 2=Modify / 3=Delete` 的 lifecycle event code；它不是 HKEX OMD-C Add Order message 里的 `OrderType=1 Market / 2 Limit` 字段。
- `Dir` 是 vendor-derived aggressor proxy，不是 HKEX OMD-C 原生 aggressor-side 字段。
- `Level` 不能直接解释成官方 `OrderBookPosition`。HKEX OMD-C v1.45 对 Securities instruments 说明 `OrderBookPosition` always set to zero，而当前 vendor `Level` 明显承载了 vendor 层盘口档位提示。
- `BrokerNo` 不能直接解释成 OMD-C board-lot order/trade 原生字段。HKEX v1.45 的 board-lot `Add/Modify/Delete Order` 与 `Trade` message 不包含 `BrokerID`；`BrokerID` 明确出现在 `Broker Queue` 与 odd-lot order 语境。
- `BidOrderID / AskOrderID` 在真实数据中已支持强 linkage DQA，但它们不是 HKEX OMD-C `Trade (50)` message 的原生字段。

## Evidence Standard

本页使用三类证据：

- `official`: HKEX 官方 OMD-C / Historical Full Book / trading mechanism 文档。
- `vendor_export`: 当前供应商 `ReadMe` / notice / local CSV file specification。
- `local_validation`: 本 repo 对真实数据的 DQA / semantic / replay 结果。

字段状态：

- `official_native`: 可直接指向 HKEX 官方 message field。
- `official_family_compatible`: 与官方产品族和字段能力兼容，但当前 CSV 列名/落地方式未证明 1:1。
- `vendor_defined`: vendor 明确定义，但不是官方原生字段。
- `project_structural`: 项目内可作为结构键、时间、数量、价格等保守列使用。
- `caveat_only`: 只能进入显式 caveat namespace / DQA / replay evidence。
- `blocked`: 不得进入默认 verified 或 alpha 语义。

## Year-Specific Source / Dictionary Boundary

`candidate_cleaned` 提供的是 common stage table contract，不证明 2025 和 2026 raw exports 共享同一个 native dictionary。

| year | raw source layout | stage mapping | dictionary implications |
| --- | --- | --- | --- |
| 2025 | `OrderAdd`, `OrderModifyDelete`, `TradeResumes`; repo inventory 另观测并隔离 `HKDarkPool` source-group label | `OrderAdd` + `OrderModifyDelete` -> `orders`; `TradeResumes` -> `trades` | 无 raw `Channel`；trade-side `SendTime` 与 `SeqNum` 缺失/置空；`Time` 是 coarse `HHMMSS` anchor；项目口径为 `coarse_only`；不得用 2026 fine timing 或 `order/trade` 两目录语义反推 2025 |
| 2026 | `order`, `trade` | `order` -> `orders`; `trade` -> `trades` | 供应商把 order lifecycle 合并进 `order`，逐笔成交目录改为 `trade`；`Channel` / `SendTime` / trade `SeqNum` 在 raw 存在时可保留；项目口径可在 DQA 通过后进入 `fine_ok`；仍然是 vendor export dictionary，不是官方 native message dictionary |

Cross-year usage rules:

- 2025 `OrderType` 同时受 `OrderAdd` / `OrderModifyDelete` source layout 影响；2026 `OrderType` 位于合并后的 `order` export。它们只能在 vendor lifecycle code 层比较，不能升级成 HKEX OMD-C Add Order `OrderType`。
- 2025 `Time` / linkage 结论是 coarse-only；2026 `SendTime` 支撑 fine timing DQA。不得把 2026 `SendTime` 假设回填到 2025。
- 2025 `TradeResumes` 与 2026 `trade` 是跨年字典漂移边界，尤其影响 `SeqNum`, `SendTime`, `Type`, `Dir`, `BidOrderID`, `AskOrderID` 的解释。
- 2025 观测到的 `HKDarkPool` 是隔离 source-group label；除非另有证据，不得把它并入 2026 `trade` 字典或默认 verified 口径。

## Official Sources Checked

- HKEX OMD-C infrastructure page: current technical document is `OMD Interface Specifications ... Binary Protocol (version 1.45)`.
  - https://www.hkex.com.hk/Services/Market-Data-Services/Infrastructure/HKEX-Orion-Market-Data-Platform-Securities-Market-OMD-C
- HKEX OMD-C Binary Interface Specifications v1.45.
  - https://www.hkex.com.hk/eng/prod/dataprod/Documents/HKEX_OMD-C_Binary_Interface_Specifications_v1.45.pdf
- HKEX OMD-C FAQ: FullTick is market-by-order and provides individual orders/trade information only; broker queue broker number is integer format.
  - https://www.hkex.com.hk/Global/Exchange/FAQ/Market-Data/Getting-Market-Data/Orion-Market-Data-Platform-Securities-Market-OMDC?sc_lang=en
- HKEX Historical Full Book - Securities Market CSV product page.
  - https://www.hkex.com.hk/eng/ods/historicalDataProfile.aspx?ProductID=PIV6Zq4So8Xt8KrhPg%2BGnaLk7a0yqT0DbuhjtJjwndI%3D&SchemeID=G1jju8Px9615F17McV2bG1mKNhX6xspAh4dZvUKA44g%3D
- HKEX Data Marketplace: Historical Full Book - Securities Market is tick-by-tick data for HKEX securities orders and trades in CSV format.
  - https://www.hkex.com.hk/Services/Market-Data-Services/Historical-Data-Services/HKEX-Data-Marketplace?sc_lang=en
- HKEX Trading Mechanism: public trade types disseminated by OMD-C.
  - https://www.hkex.com.hk/Services/Trading/Securities/Overview/Trading-Mechanism?sc_lang=en

## Local Sources Checked

- [stage_contract.py](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Scripts/stage_contract.py)
- [ReadMe.utf8.txt](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/normalized/ReadMe.utf8.txt)
- [CFBC_File_Specification_wef_20250630.pdf](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/vendor/CFBC_File_Specification_wef_20250630.pdf)
- [verified_field_policy_2026-03-15.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_field_policy_2026-03-15.json)
- [verified_admission_matrix_2026-03-18.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_matrix_2026-03-18.md)
- [dqa_linkage_2026.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/dqa_linkage_2026.md)
- [semantic_tradedir_contrast_2026.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/semantic_tradedir_contrast_2026.md)
- [hshare_orderbook_reconstruction_probe_20260522.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/hshare_orderbook_reconstruction_probe_20260522.md)
- [orderbook_replay_semantic_release_2026-05-23.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/orderbook_replay_semantic_release_2026-05-23.md)

## Official OMD-C Message Baseline

From HKEX OMD-C v1.45:

- `Add Order (30)` is FullTick-only and contains `SecurityCode`, `OrderId`, `Price`, `Quantity`, `Side`, `OrderType`, `OrderBookPosition`.
- `Modify Order (31)` is FullTick-only and identifies the existing order by `OrderId`; the only modifiable attribute is quantity.
- `Delete Order (32)` identifies an existing order by `OrderId`.
- `Trade (50)` is generated for each performed trade and contains `SecurityCode`, `TradeID`, `Price`, `Quantity`, `TrdType`, `TradeTime`.
- `Broker Queue (54)` contains up to 40 broker IDs for a side, but it is a separate conflated broker queue service, not the core FullTick order/trade message.
- `Aggregate Order Book Update (53)` contains `AggregateQuantity`, `Price`, `NumberOfOrders`, `Side`, `PriceLevel`, `UpdateAction`; this is market-by-price, not the same surface as FullTick individual orders.

From local `CFBC_File_Specification_wef_20250630.pdf`:

- Historical Full Book CSV `MC30-38_AllFB_YYYYMMDD.csv` is `Securities Full Order Book`.
- Its fields include `TradeID`, `OrderId`, `Price`, `Quantity`, `TrdType`, `TradeTime`, `Side`, `OrderType`, `OrderBookPosition`.
- This official-style CSV layout still does not match the current vendor `order/*.csv` / `trade/*.csv` columns one-to-one.

## Orders Field Matrix

| stage field | current status | safest semantics | evidence | safe use | forbidden claim |
| --- | --- | --- | --- | --- | --- |
| `Channel` | `vendor_defined` | 2026-only vendor/export channel or feed shard field; 2025 absent/null | stage contract only | partition/debug/DQA on 2026 rows | official OMD-C native field without mapping; any 2025 channel semantics |
| `SendTimeRaw` | `project_structural` | raw vendor timestamp literal retained for audit; 2026 raw only for current order export | stage contract + HFB uses `SendTime` in packet/header context | trace/sanity checks where raw exists | business event time truth in all years |
| `SendTime` | `project_structural` | parsed vendor timestamp; 2026 verified default only; 2025 absent/null | verified policy | 2026 timing research with caveat | 2025 fine-grained time anchor |
| `SeqNum` | `project_structural` | vendor sequence / structural ordering column | stage + DQA | sorting/checkpoint/DQA | official message sequence identity |
| `OrderId` | `official_family_compatible` + `project_structural` | order identity key compatible with OMD-C FullTick `OrderId` | OMD-C Add/Modify/Delete + linkage/lifecycle | verified structural key | official native mapping fully proven |
| `OrderType` | `caveat_only` | vendor lifecycle code: `1=Add`, `2=Modify`, `3=Delete`; 2025 split by source dirs, 2026 merged in `order` | vendor ReadMe + semantic ordertype + replay | lifecycle DQA/replay caveat with year retained | HKEX Add Order `OrderType=1 Market / 2 Limit`; cross-year dictionary equivalence |
| `Ext` | `caveat_only` | vendor bitfield; only `Ext[0]` currently released as `OrderSideVendor` | vendor ReadMe + replay | derive `OrderSideVendor` in caveat namespace | full `Ext` semantics verified |
| `Time` | `project_structural` | vendor HHMMSS time string; 2025 is coarse-only anchor, 2026 should prefer admitted `SendTime` for fine timing | vendor ReadMe + DQA | coarse time/session buckets; 2025 coarse research only | nanosecond event-time truth |
| `Price` | `official_family_compatible` | order price / trade price measure after vendor export | OMD-C Price + HFB CSV + stage | verified structural measure | exact official decimal/zero semantics in all vendor rows |
| `Volume` | `official_family_compatible` | vendor order/trade quantity; compatible with official `Quantity` | OMD-C Quantity + HFB CSV + stage | verified structural measure | queue-ahead or fill semantics |
| `Level` | `blocked` | vendor level hint only | vendor ReadMe + OMD-C says Securities `OrderBookPosition` is always zero + replay residue | DQA hint only | official depth / reliable ten-depth / queue position |
| `BrokerNo` | `blocked` | vendor broker/seat field | vendor ReadMe + OMD-C Broker Queue/odd-lot context | reference lookup coverage only | board-lot order native `BrokerID` or broker alpha input |
| `VolumePre` | `caveat_only` | previous active volume consistency candidate on modify rows | vendor ReadMe + replay `VolumePre` checks | `PriorActiveVolumeCheck` DQA | official native field / queue-ahead |
| tech columns | `project_structural` | `date`, `table_name`, `source_file`, `ingest_ts`, `row_num_in_file` | stage contract | traceability/rebuild/reconciliation | market semantics |

## Trades Field Matrix

| stage field | current status | safest semantics | evidence | safe use | forbidden claim |
| --- | --- | --- | --- | --- | --- |
| `SendTimeRaw` | `project_structural` | raw vendor timestamp literal retained for audit; 2026 raw only for current trade export | stage contract | trace/sanity checks where raw exists | universal event-time truth |
| `SendTime` | `project_structural` | parsed vendor timestamp; 2026 verified default only; 2025 absent/null | verified policy + DQA | 2026 fine timing with caveat | 2025 fine timing |
| `SeqNum` | `project_structural` | vendor sequence / structural ordering column; 2026 raw only for trades, 2025 absent/null | stage contract | 2026 sorting/DQA; nullable cross-year contract | official sequence identity; 2025 trade sequence proof |
| `TickID` | `official_family_compatible` + `project_structural` | vendor trade identifier, plausibly aligned with OMD-C `TradeID` role | OMD-C `TradeID` + verified policy | verified structural trade key | official `TradeID` mapping fully proven |
| `Time` | `project_structural` | vendor HHMMSS time string; 2025 is coarse-only anchor, 2026 may be paired with admitted `SendTime` | vendor ReadMe + DQA | coarse buckets/session; 2025 coarse research only | exact official `TradeTime` replacement |
| `Price` | `official_family_compatible` | trade price | OMD-C `Price` + HFB CSV | verified structural measure | all official price semantics exhausted |
| `Volume` | `official_family_compatible` | traded quantity, compatible with official `Quantity` | OMD-C `Quantity` + HFB CSV | verified structural measure | side/queue semantics |
| `Dir` | `caveat_only` | vendor-derived aggressor proxy: `1=sell`, `2=buy`, `0=other`; read per year/source layout | vendor ReadMe + semantic contrast | caveat-only directional buckets with year retained | HKEX native signed side / signed-flow truth |
| `Type` | `caveat_only` | vendor public trade type bucket, compatible with public trade-type letters; read per year/source layout | vendor ReadMe + HKEX public trade type docs | special type separation with year retained | confirmed raw official `TrdType` integer mapping |
| `BrokerNo` | `blocked` | vendor broker/seat field | vendor ReadMe; OMD-C `Trade (50)` has no broker field | reference lookup coverage only | trade-native broker identity |
| `BidOrderID` | `caveat_only` | vendor buy-side linked order id, strong local linkage evidence | DQA linkage + replay | trade-to-active-order DQA/replay | official `Trade (50)` native field |
| `BidVolume` | `blocked` | vendor buy-side order/trade quantity field | vendor ReadMe only | retain in stage/DQA | verified fill/queue size |
| `AskOrderID` | `caveat_only` | vendor sell-side linked order id, strong local linkage evidence | DQA linkage + replay | trade-to-active-order DQA/replay | official `Trade (50)` native field |
| `AskVolume` | `blocked` | vendor sell-side order/trade quantity field | vendor ReadMe only | retain in stage/DQA | verified fill/queue size |
| tech columns | `project_structural` | `date`, `table_name`, `source_file`, `ingest_ts`, `row_num_in_file` | stage contract | traceability/rebuild/reconciliation | market semantics |

## Current Verified Admission Implication

Default `verified_orders` / `verified_trades` should continue to expose only:

- technical traceability fields
- project structural keys
- `instrument_key`
- `Time`, `Price`, `Volume`
- `OrderId` / `TickID`
- `SeqNum`
- `SendTime` only where year-level policy admits it, currently 2026 default

Consumers must retain the year/source caveat. A field admitted on 2026 rows is not automatically admitted on 2025 rows, and a common
stage column must not be read as proof of a common raw dictionary.

The following must remain outside default verified:

- `OrderType`, `Ext`, `Dir`, `Type`
- `BrokerNo`
- `Level`
- `VolumePre`
- `BidOrderID`, `BidVolume`, `AskOrderID`, `AskVolume`
- reconstructed depth / queue semantics / execution realism

Explicit unverified caveat: the keep-out fields above are vendor-defined / unverified fields in this repo. They may be retained
in `candidate_cleaned` and mentioned in DQA, but they must not be promoted to research-verified semantics or default verified
inputs without a separate policy update and validation gate.

## What Would Close The Remaining Gaps

To upgrade from current safe semantics to exact field dictionary, we need at least one of:

1. A vendor-supplied export mapping that explicitly maps each year/version separately: 2025 `OrderAdd` / `OrderModifyDelete` / `TradeResumes` fields and 2026 `order` / `trade` fields to HKEX OMD-C / Historical Full Book fields, including `SeqNum`, `Channel`, `SendTime`, `Ext`, `BrokerNo`, `BidOrderID`, `BidVolume`, `AskOrderID`, `AskVolume`.
2. Official HKEX Historical Full Book CSV sample/file specification for the exact subscribed product version and proof that the vendor CSV is a direct renamed projection of that product.
3. Additional real-data validation gates:
   - `TickID` monotonic/consecutive per security/day vs official `TradeID` behavior.
   - `Type` value mapping against `TrdType` / public trade type labels.
   - `SeqNum` vs packet/channel sequencing.
   - `BrokerNo` distribution and join semantics by table/source group.
   - `Level` and `BidVolume/AskVolume` replay consistency on representative multi-day samples.

Until those gates pass, the current policy is correct: default research consumes `verified`; high-risk fields stay in explicit caveat namespaces.
