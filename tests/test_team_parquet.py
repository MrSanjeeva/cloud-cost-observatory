import pathlib
import polars as pl


def test_team_parquet():
    f = pathlib.Path("data/curated/team_daily.parquet")
    assert f.exists()
    df = pl.read_parquet(f)
    assert len(df) > 0 and "alpha" in df.columns
