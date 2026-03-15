# Provenance Note Template

## Purpose

本模板用于在研究笔记、DQA 报告、字段说明中统一描述当前港股数据的来源边界、字段边界与可用性边界。

它适用于：

- research note
- DQA summary
- semantic verification note
- contract appendix

它不用于：

- 宣布字段已完成 semantic verification
- 宣布 vendor CSV 已与 HKEX 官方 message `1:1` 对齐

## Short Form

> The upstream source is HKEX OMD-C family data. The observed order/trade content is compatible with Securities FullTick-style order-level coverage. However, the current vendor CSV export is not yet treated as a 1:1 official HKEX message dump, and vendor-defined fields are not automatically promoted to research-verified semantics.

## Medium Form

> The current dataset is sourced from the HKEX OMD-C family at the provenance level, and its order-level content is compatible with Securities FullTick-style coverage. Vendor reference materials define the current CSV export shape and field labels, but these materials describe the export layer rather than constituting final proof of official field identity. Therefore, fields that are only vendor-defined remain semantically unverified unless explicitly validated.

## Chinese Form

> 当前数据在 provenance 层可以确认属于 HKEX `OMD-C` family，且其订单级内容与 `Securities FullTick` 风格能力兼容。但当前 vendor CSV 导出形态尚不视为 HKEX 官方 message 的 `1:1` 原样落地；vendor 文档给出的字段定义仅代表 export layer contract，不自动升级为 research-verified semantics。

## Optional Add-On Lines

Use these lines only when relevant.

### When discussing `OrderId` / linkage

> `OrderId` can be used as an order-level linkage skeleton compatible with OMD-C FullTick-style content, but the current vendor header is still not treated as a formally confirmed native field mapping.

### When discussing `BidOrderID` / `AskOrderID`

> `BidOrderID` / `AskOrderID` currently support linkage feasibility analysis only. They are not yet treated as confirmed official trade-message-native fields.

### When discussing `Dir`

> `Dir` is currently treated as a vendor-defined direction code. It may support stable descriptive contrasts, but it is not yet promoted to confirmed aggressor-side truth.

### When discussing `BrokerNo`

> `BrokerNo` is currently treated as a vendor-defined broker/seat field. Reference joins may support coverage analysis, but not official identity confirmation.

### When discussing `Level`

> `Level` is currently treated as a vendor-defined level field. It is not yet confirmed as the official book-depth position field.

### When discussing `OrderType` / `Ext` / `Type` / `VolumePre`

> These fields are currently treated as vendor-defined export-layer labels or codes. They may be stable and analytically useful, but their business semantics remain unverified.

## Recommended Report Wording Rules

- Prefer `compatible with` over `confirmed as`
- Prefer `vendor-defined` over `officially mapped`
- Prefer `observed stable pattern` over `verified semantic meaning`
- Prefer `linkage feasibility` over `official schema confirmation`
- Prefer `candidate interpretation` over `proven business truth`

## Minimal Citation Bundle

When using this template, the recommended supporting references are:

- [vendor_hkex_doc_analysis_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/vendor_hkex_doc_analysis_2026-03-15.md)
- [field_status_matrix_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/field_status_matrix_2026-03-15.md)
- [omdc_provenance_and_message_numbering.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/omdc_provenance_and_message_numbering.md)
- [DATA_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DATA_CONTRACT.md)
- [DQA_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DQA_SPEC.md)
