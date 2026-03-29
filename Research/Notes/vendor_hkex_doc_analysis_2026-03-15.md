# HKEX / Vendor Technical Document Analysis 2026-03-15

## Scope

本笔记只做 HKEX / vendor 技术文档分析与规则提炼，不启动 staging / DQA / full-year 扫描，不回写 candidate_cleaned / dqa / verified。

分析材料：

- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/vendor/CFBC_File_Specification_wef_20250630.pdf`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/vendor/ReadMe.txt`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/raw_vendor_notice_2026-01-01.txt`
- HKEX OMD-C 官方页面与 binary interface 文档

## Repo-Safe Conclusions

### 1. 可以升级成官方 / 技术层支撑的结论

- 当前上游来源可以写成 HKEX `OMD-C` family。
- 当前数据内容与 HKEX `Securities FullTick (SF)` 的 market-by-order / full-depth 能力兼容。
- vendor `ReadMe.txt` 与 `raw_vendor_notice_2026-01-01.txt` 可以支撑一个明确的 vendor export drift:
  - `2026-01-01` 起，vendor 目录从 `OrderAdd + OrderModifyDelete + TradeResumes` 调整为 `order + trade`
  - vendor 声称新数据提供“全部完整的委托和成交记录”
- local vendor PDF `CFBC_File_Specification_wef_20250630.pdf` 可以支撑：
  - HKEX Historical Full Book (Securities Market) 存在官方 / 官方风格 CSV 产品形态
  - 其内容包括 Securities Reference、Trading Session Status、Securities Full Order Book、Odd Lot Order
  - 其参考来源显式指向 OMD binary protocol

### 2. 仍然只能算 vendor-defined 的字段 / 结论

- `OrderType`
- `Ext`
- `Dir`
- `Type`
- `Level`
- `VolumePre`
- `BrokerNo`

对这些字段，目前最多只能写到：

- vendor 文档给出了导出侧定义
- 可以用于 stage 保留、DQA 分层检查、reference join
- 不可直接升级成 research-verified semantics

### 3. 当前不能直接放行的推断

- 不可把 vendor `Dir=1/2/0` 直接升级成官方 aggressor-side truth
- 不可把 vendor `Dir` 写成 HKEX `Trade (50)` / `Trade Ticker (52)` 原生字段；官方 OMD-C 在这两类消息里提供 `TrdType`，但不提供成交 aggressor side
- 不可把 `BrokerNo` 直接写死为 HKEX binary `BrokerID`
- 不可把 `Level` 直接写死为官方 order book depth level
- 不可把 `VolumePre` 直接写死为官方 binary 原生字段
- 不可把 `BidOrderID / AskOrderID` 直接写死为官方 trade message 原生字段
- 不可把 vendor `OrderType=1/2/3` 直接写死为已完成官方 message semantic mapping

### 4. 当前可以安全升级到 caveat-only 的表述

- 结合 HKEX 官方文档“Trade / Trade Ticker 无原生 aggressor-side 字段”与 vendor `ReadMe` 的 `Dir` 释义，当前可以把 `Dir` 写成：
  - vendor-derived aggressor proxy
  - `Dir=1` = vendor-coded sell aggressor
  - `Dir=2` = vendor-coded buy aggressor
  - `Dir=0` = other / vendor-unclassified bucket
- 这一级表述仍然不是：
  - 官方 HKEX native field mapping
  - confirmed signed-trade truth
  - 无 caveat 的 signed-flow alpha input

## Evidence Mapping

### A. HKEX 官方页面 / 文档可直接支撑

- HKEX OMD-C 页面明确说明 securities market 下有 `Securities Standard`、`Securities Premium`、`Securities FullTick`，其中 `SF` 属于 streaming market-by-order datafeed。
- HKEX Overview 页面明确说明 `Securities FullTick (SF)` 是 `Market by order (tick-by-tick full depth)`。
- HKEX OMD-C binary interface 文档明确存在：
  - `Add Order (30)`
  - `Modify Order (31)`
  - `Delete Order (32)`
  - `Trade (50)`
  - `Broker Queue (54)`
- HKEX `Trade (50)` / `Trade Ticker (52)` message definitions提供 `TrdType` / Public Trade Type 语境，但没有 native `Dir/Side/Direction` aggressor-side 字段。
- 这足以支撑 “OMD-C family + FullTick-compatible order-level content” 这一级 provenance。

### B. Vendor readme / notice 可直接支撑

- `ReadMe.txt` 给出了 vendor CSV 字段名与 vendor 侧释义：
  - orders: `SeqNum / OrderId / OrderType / Ext / Time / Price / Volume / Level / BrokerNo / VolumePre`
  - trades: `Time / Price / Volume / Dir / Type / BrokerNo / TickID / BidOrderID / BidVolume / AskOrderID / AskVolume`
- vendor `ReadMe.txt` 还给出了 `Dir` 的 export-layer 释义：
  - `1` = 卖方主动成交
  - `2` = 买方主动成交
  - `0` = 其它
- `raw_vendor_notice_2026-01-01.txt` 明确记录了 `2026-01-01` 的目录变更与“完整委托和成交记录”通知。

### C. Local vendor PDF 可直接支撑

- `CFBC_File_Specification_wef_20250630.pdf` 描述的是 HKEX Historical Full Book (Securities Market) CSV 产品。
- 其中 `MC30-38_AllFB_YYYYMMDD.csv` 为 `Securities Full Order Book`。
- 其中字段和消息类型来自 OMD binary family，例如：
  - `TradeID`
  - `OrderId`
  - `Price`
  - `Quantity`
  - `TrdType`
  - `Side`
  - `OrderBookPosition`
- 但该 PDF 的文件命名和列布局与当前 vendor `order/trade/*.csv` 并不一致。

## Required Guardrails

### DATA_CONTRACT

建议明确补充：

- `CFBC_File_Specification_wef_20250630.pdf` 主要用于产品族与 file-layout provenance，不作为当前 vendor CSV 的逐字段官方字典。
- 若 HKEX official CSV spec 与 vendor CSV shape 不一致，应记录为 export mismatch，不做强行 1:1 字段映射。
- `BidOrderID / AskOrderID / BidVolume / AskVolume / VolumePre / Level / BrokerNo / Dir / Type / Ext` 默认归类为 `vendor-defined-unverified`。

### DQA_SPEC

建议明确补充：

- DQA 可检查这些字段的 coverage、枚举稳定性、null pattern、join feasibility、cross-field consistency。
- DQA 不得把这些字段的统计稳定性升级成官方语义确认。
- 对 `BidOrderID / AskOrderID -> OrderId`，结论只能是 linkage feasibility / consistency，不是官方 schema confirmation。
- 对 `BrokerNo` 与 broker reference 的 join，结论只能是 mapping coverage，不是 official identity proof。

### Provenance Notes

建议明确补充：

- vendor `ReadMe` 与 vendor notice 是 export-layer source contract，不是 HKEX binary field dictionary。
- local vendor PDF 支撑的是 Historical Full Book / OMD family 产品边界，而不是当前 vendor CSV 已与官方 `MC30-38` 文件同构。
- 后续若要把某字段升级为官方 / 技术层 confirmed，需要满足：
  - 能指向 HKEX 官方字段定义
  - 能解释当前 vendor CSV 的列落地方式
  - 能排除 vendor 再封装 / 重命名 / 合并导出

## Field Status Matrix

| Field | Current status | Safe wording |
| --- | --- | --- |
| `OrderId` | official-family-compatible | order-level identifier compatible with OMD-C FullTick-style content |
| `TickID` | vendor-defined with plausible official-family fit | vendor trade identifier, not yet confirmed as official native field mapping |
| `OrderType` | vendor-defined | stable vendor event code, not yet official semantic mapping |
| `Ext` | vendor-defined | vendor extension bitfield, not research-verified |
| `Dir` | vendor-defined | vendor-derived aggressor proxy (`1=sell`, `2=buy`, `0=other`), not official native field or confirmed signed-trade truth |
| `Type` | vendor-defined | vendor public-trade-type code compatible with HKEX public trade type letters, not raw official `TrdType` integer mapping |
| `Level` | vendor-defined | vendor level field, not confirmed official order book position |
| `VolumePre` | vendor-defined | vendor pre-modify volume field, not confirmed official native field |
| `BrokerNo` | vendor-defined | vendor broker/seat field, not yet confirmed equal to official `BrokerID` |
| `BidOrderID` / `AskOrderID` | vendor-defined with linkage value | vendor linkage fields, useful for feasibility checks only |

## Recommended Repo Wording

> The upstream source is HKEX OMD-C family data, and the observed order/trade content is compatible with Securities FullTick-style order-level coverage. However, the current vendor CSV export is not yet treated as a 1:1 official HKEX message dump, and vendor field definitions are not automatically promoted to research-verified semantics.
