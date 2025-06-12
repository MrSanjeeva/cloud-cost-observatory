#!/usr/bin/env python
"""
pipeline_polars.py  ──›  Curated usage & KPI Parquet files
Run:   python pipeline_polars.py
"""

import pathlib
import polars as pl

RAW_FILE = pathlib.Path("data/raw/cost_usage_sample.csv")
CUR_DIR = pathlib.Path("data/curated")
CUR_DIR.mkdir(parents=True, exist_ok=True)

# ----- 1. Load & normalise -----
df = (
    pl.read_csv(RAW_FILE, try_parse_dates=True)
    .with_columns(
        # columns may already be auto‑parsed to correct dtypes;
        # just ensure numeric ones are floats.
        pl.col("cost_usd").cast(pl.Float64),
        pl.col("usage_amount").cast(pl.Float64),
    )
)

(df
 .write_parquet(CUR_DIR / "cur_usage.parquet")
 )

# ----- 2. Daily aggregate -----
daily = (
    df.group_by("usage_date")
    .agg(
        total_cost_usd=pl.col("cost_usd").sum(),
        total_usage=pl.col("usage_amount").sum(),
    )
    # fake idle %: treat usage < 0.5 as idle for demo purposes
    .with_columns(
        idle_pct=pl.when(pl.col("total_usage") < 0.5)
        .then(0.4)
        .otherwise(0.1)
    )
    .sort("usage_date")
)

# 30-day rolling forecast (simple moving avg × 30)
daily = daily.with_columns(
    forecast_30d=(
        pl.col("total_cost_usd")
          .rolling_mean(window_size=30)
          .fill_null(strategy="forward")
        * 30
    )
)

(daily
 .write_parquet(CUR_DIR / "kpi_daily.parquet")
 )

print("Wrote curated & KPI Parquet to data/curated/")
