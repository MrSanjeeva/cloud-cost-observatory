# tests/test_service_parquet.py
import pathlib
import polars as pl


def test_service_parquet():
    f = pathlib.Path("data/curated/svc_daily.parquet")
    assert f.exists(), "service-level parquet missing"
    df = pl.read_parquet(f)
    # at least one known column should exist
    expected_any = {"EC2", "S3", "Lambda", "RDS", "CloudWatch"}
    assert expected_any.intersection(df.columns), "service columns not found"
    assert len(df) > 0
