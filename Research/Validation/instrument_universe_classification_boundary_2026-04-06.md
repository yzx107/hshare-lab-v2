# Instrument Universe Classification Boundary 2026-04-06

## Scope

本页定义上游如何把 raw universe 中的 `instrument_key` 做成可下游消费的产品分类 sidecar，
以及哪些分类可以直接用、哪些仍必须保守写成 `unclassified`。

它解决的问题只有一个：

- 当前 tick universe 里不只有普通股票，还混有 ETF / REIT / structured products / debt / 其他场内证券

它不解决这些问题：

- 不证明 tick 字段语义
- 不把 `instrument_profile` 变成 verified fact 表
- 不把所有低位代码默认判成普通股票

## Output Surface

当前 `instrument_profile` sidecar 允许输出以下分类列：

- `instrument_family`
- `instrument_family_status`
- `instrument_family_source`
- `instrument_family_note`
- `stock_research_candidate`
- `stock_research_candidate_status`
- `stock_research_candidate_note`

这些列属于：

- `reference / sidecar / stratification layer`

不属于：

- `verified default fact columns`

## Source Ladder

### 1. `seed_classified`

最强来源是：

- `Research/References/normalized/instrument_profile_seed.csv`

允许用途：

- 人工维护的 `instrument_family` enrichment
- 下游 universe segmentation
- ETP / REIT / southbound / market-cap stratification

禁止越界：

- 把 seed 内容写成 tick field semantics proof
- 把 seed 内容静默并入 verified fact 表

### 2. `official_range_classified`

如果 seed 没给，但 `instrument_key` 命中 HKEX `Stock Code Allocation Plan`
中可直接支持的产品编码区间，则允许写成 `official_range_classified`。

当前 builder 只对“非普通股票/特殊证券较明确”的区间做保守分类，例如：

- `02800-02849 / 03000-03199 / 03400-03499` -> `exchange_traded_fund`
- `07200-07399 / 07500-07599 / 07700-07799 / 87200-87399 / 87500-87599 / 87700-87799` -> `leveraged_and_inverse_product`
- `04000-04199` -> `exchange_fund_note`
- `04200-04299` -> `government_bond`
- `04700-04799` -> `debt_security_public`
- `04800-04999` -> `spac_warrant`
- `05000-06029` 与部分 `04300-04599` -> `debt_security_professional_only`
- `06200-06299` -> `hdr`
- `87000-87099` -> `reit_or_unit_trust_non_etf`
- `10000-29999` 与 `89200-89599` -> `derivative_warrant`
- `47000-48999` -> `inline_warrant`
- `49500-69999` -> `cbbc`
- `90000-99999` -> `stock_connect_security`

这里的含义是：

- `official_range_classified` 只说明该代码落在 HKEX 官方产品编码分配区间里
- 它是 universe segmentation 证据，不是 security master truth
- 它不证明经济暴露、也不证明 ticker name / issuer / benchmark 已确认

### 3. `listed_security_unclassified`

如果既没有 seed enrichment，也没有命中当前可安全使用的官方产品区间，
则必须写成：

- `instrument_family = listed_security_unclassified`
- `instrument_family_status = listed_security_unclassified`

这类对象的含义是：

- 上游能证明“它在 raw universe 里存在”
- 但还不能安全地区分它是普通股票、REIT、基金、其他证券还是特殊 counter

下游必须遵守：

- 不能把 `listed_security_unclassified` 默认当成普通股票
- 不能把全 universe 直接叫做 `equity universe`

## Current Admissibility

允许下游正式消费的用途：

- universe filtering
- 把明显的 structured products / debt / ETF / REIT 从股票研究池里排除或单独成桶
- 信息论 / boundary / strategy 模块中的 stratification
- 研究报告中显式标注 `instrument_family_status`

允许但必须带 caveat 的用途：

- 用 `official_range_classified` 做粗粒度产品分桶
- 用 `listed_security_unclassified` 做 “unknown / unresolved listed security” 桶

当前不允许的用途：

- 把 `listed_security_unclassified` 直接当成 `common_equity`
- 把 `official_range_classified` 写成 security master confirmed truth
- 把 `instrument_profile` 侧分类列静默并入 verified 默认 fact 表
- 用产品分类去反推 tick field semantics

## Stock Research Target Lane

如果下游当前明确需要“股票作为研究标的”，上游允许提供一条保守 lane：

- `stock_research_candidate = true`

当前定义是：

- `instrument_family = listed_security_unclassified`
- `instrument_key < 10000`

它的含义是：

- 已排除当前能被官方产品编码区间明确识别的大批非股票/特殊产品
- 保留了低位代码里的上市证券候选池，供股票研究先行使用

但它仍然不是：

- pure common-equity proof
- 官方 security master
- 最终无污染股票池

因此下游正确写法应是：

- `stock research candidate universe`

而不是：

- `fully verified equity universe`

当前必须继续承认：

- `00823` 这类低位 REIT / 非普通股票例外，若未被 seed/reference 明确补齐，仍可能落在 `stock_research_candidate`
- 后续应优先通过 seed enrichment 把低位非股票例外逐步剥离

## Recommended Wording

优先写：

- `instrument universe classification sidecar`
- `product-family stratification`
- `official_range_classified`
- `listed_security_unclassified`

避免写：

- `all instruments are stocks`
- `security type fully verified`
- `product semantics proven from ticker code alone`

## Immediate Repo Rule

从现在开始：

- 上游必须承认 tick universe 不是纯股票池
- 下游若需要股票研究池，必须显式使用 `instrument_profile` sidecar 做 universe 选择
- 在缺少更强 reference 前，`listed_security_unclassified` 不能自动进入“普通股票”叙事
- 默认 target universe 应写成 `equity target`，当前由 `stock_research_candidate = true` 保守承载
- non-equity objects 不应回流进默认股票主线；若要使用，只能作为 `explicit source lane`
- `ETF / REIT / warrant / CBBC / bond / other listed security -> equity target` 的 cross-security 研究是允许的，但必须显式声明：
  - `target = equity only`
  - `source = non-equity source universe`
  - `not in default core scoreboard`
