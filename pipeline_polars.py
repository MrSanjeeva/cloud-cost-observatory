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

# ----- 2a. Cost by service -----
svc_daily = (
    df.group_by(["usage_date", "service"])
      .agg(cost_usd=pl.col("cost_usd").sum())
      .pivot(index="usage_date", on="service", values="cost_usd")
)
svc_daily.write_parquet(CUR_DIR / "svc_daily.parquet")

# ----- 2. Daily aggregate -----
daily = (
    df.group_by("usage_date")
    .agg(
        total_cost_usd=pl.col("cost_usd").sum(),
        total_usage=pl.col("usage_amount").sum(),
        row_cnt=pl.len(),                  # number of raw rows that day
    )
    .with_columns(
        avg_usage=pl.col("total_usage") / pl.col("row_cnt")   # per‑row average
    )
    # idle %: high (0.4) when average usage per row < 2.0 units
    .with_columns(
        idle_pct=pl.when(pl.col("avg_usage") < 2.0)
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

# ----- Anomaly flag (±2 σ) -----
mu = daily["total_cost_usd"].mean()
sigma = daily["total_cost_usd"].std()
daily = daily.with_columns(
    is_anomaly=(pl.col("total_cost_usd") > mu + 2 * sigma) |
               (pl.col("total_cost_usd") < mu - 2 * sigma)
)

(daily
 .write_parquet(CUR_DIR / "kpi_daily.parquet")
 )

print("Wrote curated, KPI, and service Parquet to data/curated/")
