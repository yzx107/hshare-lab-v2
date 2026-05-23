"""
Alpha Probe 001: Broker Flow Candidate v0

Hypothesis:
Broker seats with strong net inflow (Dir=1/2) have predictive power for future short-term returns.
This factor exploits the HKEX Broker Queue and Trade direction data.

Data Source:
- /Volumes/Data/港股Tick数据/candidate_cleaned/trades/year=2026/date=*

Factor Logic:
For each (date, stock):
  1. Filter Dir in (1, 2), BrokerNo not in ("0", "0000")
  2. Compute Net Flow per Broker (Buy Vol - Sell Vol)
  3. Aggregate Top5 Broker Net Flow Ratio (Net Amount / Total Turnover)

Evaluation:
- Rank IC on forward daily returns (T+1, T+3, T+5)
"""

import argparse
import json
import logging
import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

OUTPUT_DIR = "Research/AlphaProbes/broker_flow_v0"


def setup_env():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    con = duckdb.connect(database=":memory:")
    # Polars is installed, but DuckDB can read parquet directly nicely.
    return con


def run_probe(con: duckdb.DuckDBPyConnection, mode: str, date_filter: str = None):
    # Determine date pattern
    if mode == "smoke" and date_filter:
        date_pattern = f"date={date_filter}"
    else:
        date_pattern = "date=*"

    parquet_path = f"/Volumes/Data/港股Tick数据/candidate_cleaned/trades/year=2026/{date_pattern}/*.parquet"

    # Check if files exist
    try:
        count = con.execute(f"SELECT count(*) FROM glob('{parquet_path}')").fetchone()[0]
        if count == 0:
            logging.error(f"No parquet files found matching {parquet_path}")
            return
        logging.info(f"Found files, running query on {count} files matching the pattern.")
    except Exception as e:
        logging.error(f"Failed to load parquet data: {e}")
        return

    logging.info("Step 1: Extract trades, calculate Broker Net Flow")

    # We use source_file name to extract stock code: like "00700_trades.csv" -> "00700"
    # Actually, in candidate_cleaned, the mapping usually involves source_file.

    query_broker_flow = f"""
    WITH raw_trades AS (
        SELECT
            date,
            regexp_extract(source_file, '([0-9]{{5}})', 1) AS stock_code,
            Price,
            Volume,
            Price * Volume as Amount,
            Dir,
            BrokerNo
        FROM read_parquet('{parquet_path}')
        WHERE Dir IN (1, 2)
          AND BrokerNo IS NOT NULL
          AND BrokerNo NOT IN ('0', '0000')
    ),
    broker_stats AS (
        SELECT
            date,
            stock_code,
            BrokerNo,
            SUM(CASE WHEN Dir = 2 THEN Volume ELSE 0 END) AS buy_vol,   -- assuming 2 is Buy (Up Red)
            SUM(CASE WHEN Dir = 1 THEN Volume ELSE 0 END) AS sell_vol,  -- assuming 1 is Sell (Down Green)
            SUM(CASE WHEN Dir = 2 THEN Amount ELSE -Amount END) as net_amount
        FROM raw_trades
        GROUP BY 1, 2, 3
    ),
    ranked_brokers AS (
        SELECT
            date,
            stock_code,
            BrokerNo,
            net_amount,
            ROW_NUMBER() OVER (PARTITION BY date, stock_code ORDER BY net_amount DESC) as rk
        FROM broker_stats
    ),
    stock_factors AS (
        SELECT
            date,
            stock_code,
            SUM(CASE WHEN rk <= 5 THEN net_amount ELSE 0 END) as top5_net_amount,
            COUNT(DISTINCT BrokerNo) as active_brokers
        FROM ranked_brokers
        GROUP BY 1, 2
    ),
    total_market AS (
        SELECT
            date,
            regexp_extract(source_file, '([0-9]{{5}})', 1) AS stock_code,
            SUM(Price * Volume) as total_turnover,
            -- Closing price approximation: last price of the day
            LAST_VALUE(Price) OVER (
                PARTITION BY date, regexp_extract(source_file, '([0-9]{{5}})', 1)
                ORDER BY SeqNum
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as close_price
        FROM read_parquet('{parquet_path}')
    )
    SELECT
        f.date,
        f.stock_code,
        f.top5_net_amount,
        f.active_brokers,
        m.total_turnover,
        m.close_price,
        CASE
            WHEN m.total_turnover > 0 THEN f.top5_net_amount / m.total_turnover
            ELSE 0
        END as top5_net_ratio
    FROM stock_factors f
    JOIN (SELECT DISTINCT date, stock_code, total_turnover, close_price FROM total_market) m
      ON f.date = m.date AND f.stock_code = m.stock_code
    WHERE m.total_turnover > 0
    ORDER BY f.date, f.stock_code
    """

    logging.info("Executing main factor query...")
    df_factors = con.execute(query_broker_flow).df()

    if df_factors.empty:
        logging.warning("Factor dataframe is empty.")
        return

    logging.info(f"Generated {len(df_factors)} stock-day factor records.")

    # Save the raw factor panel
    factor_path = os.path.join(OUTPUT_DIR, "factor_panel.parquet")
    df_factors.to_parquet(factor_path)
    logging.info(f"Saved factor panel to {factor_path}")

    # Step 2: Calculate IC if we have multiple days
    if mode == "full" or df_factors['date'].nunique() > 1:
        logging.info("Calculating forward returns and Rank IC...")

        # Pivot close prices to calculate returns
        df_prices = df_factors.pivot(index='date', columns='stock_code', values='close_price')

        # Calculate forward returns: fwd_ret_N = price(T+N) / price(T) - 1
        df_ret_1d = df_prices.shift(-1) / df_prices - 1
        df_ret_3d = df_prices.shift(-3) / df_prices - 1
        df_ret_5d = df_prices.shift(-5) / df_prices - 1

        # Melt back to long form
        df_ret_1d_lf = df_ret_1d.reset_index().melt(id_vars='date', value_name='fwd_ret_1d')
        df_ret_3d_lf = df_ret_3d.reset_index().melt(id_vars='date', value_name='fwd_ret_3d')
        df_ret_5d_lf = df_ret_5d.reset_index().melt(id_vars='date', value_name='fwd_ret_5d')

        # Merge back
        df_eval = df_factors.merge(df_ret_1d_lf, on=['date', 'stock_code'], how='left')
        df_eval = df_eval.merge(df_ret_3d_lf, on=['date', 'stock_code'], how='left')
        df_eval = df_eval.merge(df_ret_5d_lf, on=['date', 'stock_code'], how='left')

        # Calculate cross-sectional Rank IC
        metrics = []
        for dt, grp in df_eval.groupby('date'):
            if len(grp) < 10:  # Require at least 10 stocks for meaningful IC
                continue

            ic_1d = grp['top5_net_ratio'].corr(grp['fwd_ret_1d'], method='spearman')
            ic_3d = grp['top5_net_ratio'].corr(grp['fwd_ret_3d'], method='spearman')
            ic_5d = grp['top5_net_ratio'].corr(grp['fwd_ret_5d'], method='spearman')

            metrics.append({
                'date': dt,
                'ic_1d': ic_1d,
                'ic_3d': ic_3d,
                'ic_5d': ic_5d,
                'coverage': grp['top5_net_ratio'].notna().sum()
            })

        df_metrics = pd.DataFrame(metrics)

        if not df_metrics.empty:
            summary = {
                "factor": "top5_net_ratio",
                "days_tested": len(df_metrics),
                "avg_coverage": float(df_metrics['coverage'].mean()),
                "ic_1d_mean": float(df_metrics['ic_1d'].mean()),
                "ic_1d_tstat": float(df_metrics['ic_1d'].mean() / (df_metrics['ic_1d'].std() / np.sqrt(len(df_metrics)))) if df_metrics['ic_1d'].std() > 0 else 0.0,
                "ic_3d_mean": float(df_metrics['ic_3d'].mean()),
                "ic_3d_tstat": float(df_metrics['ic_3d'].mean() / (df_metrics['ic_3d'].std() / np.sqrt(len(df_metrics)))) if df_metrics['ic_3d'].std() > 0 else 0.0,
                "ic_5d_mean": float(df_metrics['ic_5d'].mean()),
                "ic_5d_tstat": float(df_metrics['ic_5d'].mean() / (df_metrics['ic_5d'].std() / np.sqrt(len(df_metrics)))) if df_metrics['ic_5d'].std() > 0 else 0.0,
            }

            with open(os.path.join(OUTPUT_DIR, "summary.json"), "w") as f:
                json.dump(summary, f, indent=2)

            logging.info("Factor Evaluation Summary:")
            for k, v in summary.items():
                logging.info(f"  {k}: {v}")

            # Write MarkDown report
            report_md = f"""# Alpha Probe 001: Broker Flow Candidate v0

## Evaluation Summary
- **Factor**: Top-5 Broker Net Buy Ratio (top5_net_amount / total_turnover)
- **Timeframe evaluated**: {df_metrics['date'].min()} to {df_metrics['date'].max()} ({len(df_metrics)} days)
- **Average cross-sectional coverage**: {summary['avg_coverage']:.0f} stocks

### Rank IC Metrics
| Horizon | Mean IC | t-stat |
|---|---|---|
| 1-Day | {summary['ic_1d_mean']:.4f} | {summary['ic_1d_tstat']:.2f} |
| 3-Day | {summary['ic_3d_mean']:.4f} | {summary['ic_3d_tstat']:.2f} |
| 5-Day | {summary['ic_5d_mean']:.4f} | {summary['ic_5d_tstat']:.2f} |

## Admissibility Notice
- `BrokerNo` relies on the candidate semantic dict (0 avoided).
- `Dir` mapping: `2` (Red Up) mapped to Buy, `1` (Green Down) mapped to Sell.
- This represents candidate research. Do not upload these metrics to the core validated layer until factor stability is further approved.
"""
            with open(os.path.join(OUTPUT_DIR, "report.md"), "w") as f:
                f.write(report_md)
            logging.info("Saved summary.json and report.md")
    else:
        logging.info("Smoke mode or insufficient dates. Skipping IC calculation.")
        print(df_factors.head().to_markdown())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--date", type=str, help="Date to smoke test, e.g. 2026-01-02", default="2026-01-02")
    args = parser.parse_args()

    con = setup_env()
    run_probe(con, args.mode, args.date)
