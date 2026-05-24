# Reference Source Registry 2026-04-07

## Purpose

本页记录当前 `instrument_profile` sidecar 允许接入的外部 reference source，
以及它们各自的角色、边界与禁止越界用途。

它解决的是：

- `listing_date / southbound_eligible / market-cap / liquidity / instrument_family` 这类 sidecar enrichment 从哪里来
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

### 2. `tushare_hk_daily`

角色：

- `daily_bar_reference_landing`

当前允许：

- 日线行情截面 reference landing
- 低频 coverage / reconciliation
- 流动性、停牌、交易日 universe 辅助 sidecar

当前不允许：

- 替代 tick / order / trade verified fact
- 证明 tick 字段语义
- 在没有独立 admission policy 前进入 verified 默认表

当前定位：

- 已注册 source
- 默认不进入 `sync_instrument_profile_seed --sources enabled`
- 通过 `Scripts.sync_tushare_reference` 落地 parquet + manifest

### 3. `tushare_hk_tradecal`

角色：

- `calendar_reference_landing`

当前允许：

- 港股交易日历 reference landing
- 增量任务调度基准
- DQA / coverage 的日期全集对照

当前不允许：

- 替代 raw inventory 本身
- 在未检查 source coverage 前直接推断 raw 缺失原因

当前定位：

- 已注册 source
- 默认不进入 `sync_instrument_profile_seed --sources enabled`
- 通过 `Scripts.sync_tushare_reference` 落地 parquet + manifest

### 4. `tushare_hk_adjfactor`

角色：

- `adjustment_factor_reference_landing`

当前允许：

- 复权因子 reference landing
- future adjustment policy 的输入

当前不允许：

- 在没有独立 adjustment contract 前生成 adjusted return truth
- 默认进入 verified 或 alpha 表

当前定位：

- 已注册 source
- 默认不进入 `sync_instrument_profile_seed --sources enabled`
- 只落地，不在本阶段消费

### 5. `hkex_reit_manual_seed`

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
- 当前 seed 已补入官方 HKEX REIT 名单对应的 REIT override，用于修正低位 `REIT / unit trust` 被误落入股票候选池的问题

### 6. `hkex_southbound_manual_seed`

角色：

- `southbound_eligibility_seed`

当前允许：

- `southbound_eligible`
- 以 `as_of_date` 为边界的时点资格 enrichment

当前不允许：

- 把 `southbound_eligible` 当作永恒真值

当前定位：

- legacy curated CSV source
- 默认不启用；当前优先使用 `stock_connect_southbound_official` 刷新同一路径

### 7. `stock_connect_southbound_official`

角色：

- `southbound_eligibility_seed`

当前允许：

- `southbound_eligible`
- `southbound_as_of_date`
- `southbound_source_label`
- 以 `as_of_date` 为边界的 Stock Connect Southbound 当前资格 enrichment

source：

- SSE 港股通标的证券名单 API：`COMMON_SSE_JYFW_HGT_XXPL_BDZQQD_L`
- SZSE 港股通标的证券名单 API：`SGT_GGTBDQD`
- normalized seed：`Research/References/normalized/hkex_southbound_seed.csv`

refresh cadence：

- 按需 refresh；source 自带更新日期
- 当前 seed 为 point-in-time snapshot，不是历史 eligibility panel

as_of semantics：

- `as_of_date` 来自交易所名单的更新日期
- 若更新日期为非港股通交易日，按交易所页面说明，该名单适用于更新日期后一港股通交易日

当前允许的下游用途：

- current Southbound eligibility bucket
- 港股分层研究的 point-in-time reference sidecar
- 与 market-cap / liquidity sidecar 共同做横截面分层

当前不允许：

- 把 `southbound_eligible=true` 当作永久真值
- 把未出现在 seed 的 instrument 静默写成 `false`
- 写入 verified fact table 或 tick semantic proof

### 8. `eastmoney_hk_spot_market_snapshot`

角色：

- `market_cap_and_liquidity_reference_snapshot`

当前允许：

- `total_mktcap_hkd`
- `circulating_mktcap_hkd`
- `latest_turnover_hkd`
- `latest_volume_shares`
- field-level source labels / as-of dates / currency

source：

- Eastmoney HK spot quote snapshot API
- normalized seed：`Research/References/normalized/hk_market_snapshot_seed.csv`

refresh cadence：

- 按需 refresh；当前仅作为 latest snapshot sidecar
- source 网络失败时允许复用已有 normalized seed，但必须保留原 seed 的 `as_of_date/source_label`

as_of semantics：

- `market_cap_as_of_date` / `liquidity_as_of_date` 是本地 refresh 的 as-of date
- 本 source 不提供严格历史 trade-date panel；不得倒推历史截面真值

当前允许的下游用途：

- 港股横截面 size bucket reference
- liquidity bucket reference
- DQA / coverage / universe research 的 enrichment sidecar

当前不允许：

- 把 `circulating_mktcap_hkd` 冒充 `float_mktcap_hkd`
- 把 `latest_turnover_hkd` / `latest_volume_shares` 冒充 market cap
- 写入 verified fact table 或 tick semantic proof
- 在没有独立 admission policy 前做历史 point-in-time 回测真值

### 9. `opend_security_snapshot`

角色：

- `secondary_security_reference`

当前允许：

- `listing_date` 二次补全
- 当前 security snapshot 下的 `ETF / index` 辅助分类
- 运维侧 / 对账侧 / secondary lookup cross-check

当前不允许：

- 把 OpenD current snapshot 当作历史全时段真值
- 在没有单独 policy 的情况下越界成 tick semantic proof
- 用 OpenD 去替代 HKEX curated REIT override

当前定位：

- 已注册 source
- 当前已接线
- 继续保持 `secondary_security_reference`
- 对于 `REIT`，优先级低于 `hkex_reit_manual_seed`，因为 OpenD 会把 REIT 归在 `ETF` 桶里

## Global Rules

- 外部 source 进入上游后，默认只允许落到 `instrument_profile` sidecar
- 外部 source 必须保留 `source_label`
- 时变属性必须保留 `as_of_date`
- 新 source 若要进入默认同步链，必须先在本页或 machine-readable config 中注册
- `tushare / OpenD / manual seed / Stock Connect list / Eastmoney snapshot` 都是 `reference source`，不是 `semantic proof`

## Current Pipeline

当前正式入口：

- `config/reference_sources.example.json`
- `config/reference_sources.local.json`（本地私有，不入库）
- `python -m Scripts.sync_instrument_profile_seed`

当前推荐顺序：

1. 先跑 `tushare_hk_basic`，补 `listing_date`
2. 再补 `hkex_reit_manual_seed`
3. 跑 `stock_connect_southbound_official` 刷新 Southbound point-in-time seed
4. 跑 `eastmoney_hk_spot_market_snapshot` 刷新 market-cap / liquidity reference seed
5. 再跑 `opend_security_snapshot`，做 current snapshot 下的 secondary classification / listing-date backfill；若本地 OpenD dependency / daemon 不可用，允许 warning 后跳过
6. 最后重建 `instrument_profile`

`tushare_hk_daily` / `tushare_hk_tradecal` / `tushare_hk_adjfactor` 不属于
`instrument_profile_seed` 默认同步顺序；它们先进入
`reference/tushare/<endpoint>/<partition>/`，再由 coverage / reconciliation / adjustment policy
显式消费。
