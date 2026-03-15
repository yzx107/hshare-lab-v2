# Field Status Matrix 2026-03-15

## Scope

本表只基于以下材料整理字段状态，不基于 staging / DQA / full-year 扫描结果：

- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/vendor/ReadMe.txt`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/raw_vendor_notice_2026-01-01.txt`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/vendor/CFBC_File_Specification_wef_20250630.pdf`
- HKEX OMD-C 官方页面与 binary interface 文档

状态标签：

- `officially_grounded`: 可直接找到 HKEX 官方产品/字段/消息层支撑
- `official_family_compatible`: 与 HKEX OMD-C / FullTick 能力兼容，但非当前 vendor 列名的 1:1 官方映射
- `vendor_defined`: 仅有 vendor 导出层定义
- `unverified_semantic`: 仍不可直接按业务语义放行

## Orders

| Field | Current status | Evidence basis | Safe usage | Do not claim |
| --- | --- | --- | --- | --- |
| `SeqNum` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 提供字段名；官方 OMD-C 存在消息流顺序语境，但当前列名未完成官方映射 | sequence / monotonicity / gap checks | 官方 native sequence field 已确认 |
| `OrderId` | `official_family_compatible` | vendor `ReadMe` 有该字段；HKEX OMD-C 官方消息存在 `OrderId` | order-level linkage、生命周期骨架、same-order consistency | 当前 vendor `OrderId` 已与官方 binary 逐字段对齐 |
| `OrderType` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义 `1/2/3 = 增加/修改/删除`；官方也有 Add/Modify/Delete message family | 枚举稳定性、生命周期形状、弱一致性检查 | `1/2/3` 已正式等于官方 message semantic mapping |
| `Ext` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 提供 bit 位说明 | coverage、枚举统计、cross-field consistency | bit 位已完成官方字段映射 |
| `Time` | `vendor_defined` + `official_family_compatible` | vendor `ReadMe` 定义 `HHMMSS`；HKEX 官方产品存在 time / send time / trade time 语境 | session bucket、time-format checks、与技术时间列对照 | 当前 `Time` 已确认等同某个官方原生时间字段 |
| `Price` | `official_family_compatible` | vendor `ReadMe` 有订单价格；HKEX OMD-C Add Order / related messages存在 `Price` | 非空/正值/异常率、基础价格分布 | 当前 vendor `Price` 已与官方 binary price field 逐字段对齐 |
| `Volume` | `official_family_compatible` | vendor `ReadMe` 有订单量；HKEX OMD-C Add Order / related messages存在 `Quantity` 语境 | 非空/正值/异常率、基础量分布 | 当前 `Volume` 已确认等同官方 `Quantity` |
| `Level` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为买卖盘档位；官方 Historical Full Book 有 `OrderBookPosition`，但未与当前列完成映射 | 枚举分布、null pattern、稳定性对比 | 已确认等于官方 depth level / `OrderBookPosition` |
| `BrokerNo` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为经纪商席位号；可与 broker reference 做只读对照 | coverage、reference join、ambiguity checks | 已确认等于官方 `BrokerID`；`0` 有官方固定含义 |
| `VolumePre` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为修改前的量 | 修改事件下的完整性/条件出现率检查 | 官方原生字段已确认；可直接用于成交/剩余量语义研究 |

## Trades

| Field | Current status | Evidence basis | Safe usage | Do not claim |
| --- | --- | --- | --- | --- |
| `Time` | `vendor_defined` + `official_family_compatible` | vendor `ReadMe` 定义 `HHMMSS`；官方存在 `TradeTime` 等时间字段 | session bucket、time-format checks | 当前 `Time` 已确认等同官方 `TradeTime` |
| `Price` | `official_family_compatible` | vendor `ReadMe` 有成交价；官方 Trade message存在 `Price` | 非空/正值/异常率、基础成交分布 | 当前 vendor `Price` 已与官方 binary price field 逐字段对齐 |
| `Volume` | `official_family_compatible` | vendor `ReadMe` 有成交量；官方 Trade message存在 `Quantity` | 非空/正值/异常率、基础成交量分布 | 当前 `Volume` 已确认等同官方 `Quantity` |
| `Dir` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 提供 `0/1/2` 释义 | 枚举稳定性、分层统计、候选方向信号探索 | 已确认 aggressor side / signed trade truth |
| `Type` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 有字段但未在文本中给出完整释义；官方 Historical Full Book 存在 `TrdType` | coverage、枚举、与其他字段共现关系 | 当前 `Type` 已确认等于官方 `TrdType` |
| `BrokerNo` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为经纪商席位号 | coverage、reference join、ambiguity checks | 已确认等于官方 `BrokerID` |
| `TickID` | `vendor_defined` + `official_family_compatible` | vendor `ReadMe` 定义为成交明细 ID；官方 Historical Full Book 存在 `TradeID` | 去重、唯一性、同日粒度标识检查 | 当前 `TickID` 已确认等于官方 `TradeID` |
| `BidOrderID` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为买盘委托 ID | linkage feasibility、match rate、lag pattern | 官方 trade message 原生字段已确认 |
| `BidVolume` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为买盘委托量 | linkage consistency、null pattern | 官方 trade message 原生字段已确认 |
| `AskOrderID` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为卖盘委托 ID | linkage feasibility、match rate、lag pattern | 官方 trade message 原生字段已确认 |
| `AskVolume` | `vendor_defined` + `unverified_semantic` | vendor `ReadMe` 定义为卖盘委托量 | linkage consistency、null pattern | 官方 trade message 原生字段已确认 |

## Immediate Implications

- 可以放心把 `OrderId`、`Price`、`Volume` 放进 `official-family-compatible` 口径，但不要写成已完成官方逐字段映射。
- `OrderType`、`Ext`、`Dir`、`Type`、`Level`、`BrokerNo`、`VolumePre` 仍应视为 `vendor_defined + unverified_semantic`。
- `BidOrderID / AskOrderID / BidVolume / AskVolume` 有很强 linkage 价值，但当前仍不是官方 schema confirmation。

## Suggested Contract Wording

> The current vendor CSV headers can be separated into two buckets: fields that are compatible with HKEX OMD-C FullTick-style order-level content, and fields that are only vendor-defined export-layer labels. Compatibility does not imply 1:1 official field mapping, and vendor-defined fields remain semantically unverified until explicitly proven otherwise.
