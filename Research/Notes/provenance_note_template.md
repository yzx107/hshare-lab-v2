# 来源说明模板（Provenance Note Template）

## 用途（Purpose）

本模板用于在研究笔记、DQA 报告、字段说明中统一描述当前港股数据的来源边界、字段边界与可用性边界。

它适用于：

- research note
- DQA summary
- semantic verification note
- contract appendix

它不用于：

- 宣布字段已完成 semantic verification
- 宣布 vendor CSV 已与 HKEX 官方 message `1:1` 对齐

## 中文优先说明

- 默认优先使用中文版本
- 如果需要精确保留术语，可在中文句子中直接保留 `OMD-C`、`FullTick`、`export layer` 等英文
- 英文短版/中版保留为对外沟通或引用模板，不要求内部文档优先使用

## 中文短版（推荐）

> 当前数据在 provenance 层可以确认属于 HKEX `OMD-C` family，且其订单级内容与 `Securities FullTick` 风格能力兼容。但当前 vendor CSV 导出形态尚不视为 HKEX 官方 message 的 `1:1` 原样落地；vendor 文档给出的字段定义仅代表 export layer contract，不自动升级为 research-verified semantics。

## 中文中版

> 当前数据在 provenance 层可以确认属于 HKEX `OMD-C` family，且其订单级内容与 `Securities FullTick` 风格能力兼容。vendor 参考材料定义的是当前 CSV 导出层的字段形态与标签，但这些材料描述的是 export layer，而不是 HKEX 官方字段身份的最终证明。因此，凡是只存在于 vendor 定义层的字段，除非另有明确验证，否则都不能自动升级为 research-verified semantics。

## Short Form

> The upstream source is HKEX OMD-C family data. The observed order/trade content is compatible with Securities FullTick-style order-level coverage. However, the current vendor CSV export is not yet treated as a 1:1 official HKEX message dump, and vendor-defined fields are not automatically promoted to research-verified semantics.

## Medium Form

> The current dataset is sourced from the HKEX OMD-C family at the provenance level, and its order-level content is compatible with Securities FullTick-style coverage. Vendor reference materials define the current CSV export shape and field labels, but these materials describe the export layer rather than constituting final proof of official field identity. Therefore, fields that are only vendor-defined remain semantically unverified unless explicitly validated.

## 可选补充句（Optional Add-On Lines）

只在相关时使用这些句子。

### When discussing `OrderId` / linkage

> `OrderId` can be used as an order-level linkage skeleton compatible with OMD-C FullTick-style content, but the current vendor header is still not treated as a formally confirmed native field mapping.

### When discussing `BidOrderID` / `AskOrderID`

> `BidOrderID` / `AskOrderID` currently support linkage feasibility analysis only. They are not yet treated as confirmed official trade-message-native fields.

### When discussing `Dir`

> `Dir` 当前可写成 `vendor-derived aggressor proxy`：`1=sell aggressor`、`2=buy aggressor`、`0=other / special bucket`。它可以支持方向分桶与描述性分析，但仍不是 HKEX 原生成交方向真值。

### When discussing `BrokerNo`

> `BrokerNo` is currently treated as a vendor-defined broker/seat field. Reference joins may support coverage analysis, but not official identity confirmation.

### When discussing `Level`

> `Level` is currently treated as a vendor-defined level field. It is not yet confirmed as the official book-depth position field.

### When discussing `OrderType` / `Ext` / `Type` / `VolumePre`

> `OrderType` 当前可写成 `stable vendor event code`：`1=Add`、`2=Modify`、`3=Delete`，但仍不是官方 event semantics。`Ext.bit0` 可单独写成 `vendor order-side proxy`，而整列 `Ext` 仍未完成语义验证。`Type` 与 HKEX public trade type letter bucket 兼容，但不应写成已确认等于官方原始 `TrdType` 整数字段。`VolumePre` 仍停留在 vendor-defined 语境。

## 推荐表述规则（Recommended Report Wording Rules）

- Prefer `compatible with` over `confirmed as`
- Prefer `vendor-defined` over `officially mapped`
- Prefer `observed stable pattern` over `verified semantic meaning`
- Prefer `linkage feasibility` over `official schema confirmation`
- Prefer `candidate interpretation` over `proven business truth`

## 最小引用包（Minimal Citation Bundle）

When using this template, the recommended supporting references are:

- [vendor_hkex_doc_analysis_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/vendor_hkex_doc_analysis_2026-03-15.md)
- [field_status_matrix_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/field_status_matrix_2026-03-15.md)
- [omdc_provenance_and_message_numbering.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/omdc_provenance_and_message_numbering.md)
- [DATA_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DATA_CONTRACT.md)
- [DQA_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DQA_SPEC.md)
