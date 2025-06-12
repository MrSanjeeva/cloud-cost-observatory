import pathlib
import polars as pl


def test_kpi_outputs_exist():
    f = pathlib.Path("data/curated/kpi_daily.parquet")
    assert f.exists(), "kpi_daily.parquet missing"

    df = pl.read_parquet(f)
    expected_cols = {"total_cost_usd", "idle_pct", "forecast_30d"}
    assert expected_cols.issubset(df.columns)
    assert len(df) > 0
