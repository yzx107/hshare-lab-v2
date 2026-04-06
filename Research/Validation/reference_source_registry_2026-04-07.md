# Reference Source Registry 2026-04-07

## Purpose

本页记录当前 `instrument_profile` sidecar 允许接入的外部 reference source，
以及它们各自的角色、边界与禁止越界用途。

它解决的是：

- `listing_date / southbound_eligible / instrument_family` 这类 sidecar enrichment 从哪里来
- 哪些 source 是长期注册的数据源
- 哪些 source 只能作为 `reference_lookup / enrichment`

它不解决：

- tick 字段语义证明
- verified fact 表默认扩列
- 下游因子定义

## Registered Sources

### 1. `tushare_hk_basic`

角色：

- `instrument_profile_enrichment`

当前允许：

- `listing_date`
- 基础 security metadata 辅助核对

当前不允许：

- 用 `hk_basic` 直接证明 `security_type`
- 用 `hk_basic` 直接证明 tick 字段语义

当前定位：

- 长期注册 source
- 上游 seed 同步可直接消费

### 2. `hkex_reit_manual_seed`

角色：

- `instrument_family_exception_seed`

当前允许：

- 补低位 `REIT / unit trust` 例外
- 修正 `stock_research_candidate` 中的低位非普通股票污染

当前不允许：

- 越界写成 verified fact truth

当前定位：

- 长期注册 source
- 通过 curated CSV 维护

### 3. `hkex_southbound_manual_seed`

角色：

- `southbound_eligibility_seed`

当前允许：

- `southbound_eligible`
- 以 `as_of_date` 为边界的时点资格 enrichment

当前不允许：

- 把 `southbound_eligible` 当作永恒真值

当前定位：

- 长期注册 source
- 通过 curated CSV 维护

### 4. `opend_security_snapshot`

角色：

- `secondary_security_reference`

当前允许：

- 运维侧 / 对账侧 / secondary lookup cross-check
- future fallback enrichment discussion

当前不允许：

- 在没有单独 policy 的情况下直接覆盖 seed
- 在没有单独验证的情况下升级成 semantic proof

当前定位：

- 已注册 source
- 当前为预留入口，默认不启用

## Global Rules

- 外部 source 进入上游后，默认只允许落到 `instrument_profile` sidecar
- 外部 source 必须保留 `source_label`
- 时变属性必须保留 `as_of_date`
- 新 source 若要进入默认同步链，必须先在本页或 machine-readable config 中注册
- `tushare / OpenD / manual seed / HKEX list` 都是 `reference source`，不是 `semantic proof`

## Current Pipeline

当前正式入口：

- `config/reference_sources.example.json`
- `config/reference_sources.local.json`（本地私有，不入库）
- `python -m Scripts.sync_instrument_profile_seed`

当前推荐顺序：

1. 先跑 `tushare_hk_basic`，补 `listing_date`
2. 再补 `hkex_reit_manual_seed`
3. 再补 `hkex_southbound_manual_seed`
4. 最后重建 `instrument_profile`
