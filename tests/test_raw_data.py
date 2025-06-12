import pathlib
import polars as pl


def test_sample_exists_and_schema():
    f = pathlib.Path("data/raw/cost_usage_sample.csv")
    assert f.exists(), "sample CSV missing"

    df = pl.read_csv(f, n_rows=5)
    expected_cols = {
        "usage_date",
        "account_id",
        "service",
        "region",
        "cost_usd",
        "usage_amount",
    }
    assert expected_cols.issubset(df.columns)
