#!/usr/bin/env python
"""
fetch_or_generate.py
--------------------
• Synthetic:  python fetch_or_generate.py --sample 90days
• Real AWS:   python fetch_or_generate.py --aws --days 90
"""

import argparse
import datetime as dt
import pathlib
import random
import polars as pl
import boto3
from faker import Faker
from tqdm import trange

RAW_DIR = pathlib.Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUTFILE = RAW_DIR / "cost_usage_sample.csv"

# ----- Synthetic Data Generation -----


def generate_synthetic(days: int = 90, rows_per_day: int = 60) -> None:
    fake = Faker()
    base = dt.date.today() - dt.timedelta(days=days)
    records = []
    for d in trange(days, desc="Generating synthetic CUR"):
        day = base + dt.timedelta(days=d)
        for _ in range(rows_per_day):
            weekday = day.weekday()  # 0 = Monday … 6 = Sunday
            # Lower utilisation on weekends to create idle spikes
            if weekday >= 5:  # Saturday or Sunday
                usage_amt = round(random.uniform(0.0, 1.0), 3)  # make weekends even lower
            else:  # Weekdays
                usage_amt = round(random.uniform(2.0, 10.0), 3)

            records.append(
                {
                    "usage_date": day.isoformat(),
                    "account_id": fake.random_int(100000000000, 999999999999),
                    "service": random.choice(
                        ["EC2", "S3", "RDS", "Lambda", "CloudWatch"]
                    ),
                    "region": random.choice(
                        ["us-east-1", "us-west-2", "eu-west-1"]
                    ),
                    "cost_usd": round(random.uniform(0.05, 25.0), 4),
                    "usage_amount": usage_amt,
                }
            )
    pl.DataFrame(records).write_csv(OUTFILE)
    print(
        f"Synthetic CUR written to {OUTFILE} ({OUTFILE.stat().st_size/1_000:.1f} KB)")

# ----- Real AWS path ------


def fetch_from_cost_explorer(days: int = 90) -> None:
    today = dt.date.today()
    start = (today - dt.timedelta(days=days)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    ce = boto3.client("ce")
    # Some botocore builds don’t expose a paginator for GetCostAndUsage.
    # Fall back to manual pagination using NextPageToken.
    kwargs = {
        "TimePeriod": {"Start": start, "End": end},
        "Granularity": "DAILY",
        "Metrics": ["UnblendedCost", "UsageQuantity"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }

    rows = []
    token = None
    while True:
        if token:
            kwargs["NextPageToken"] = token
        resp = ce.get_cost_and_usage(**kwargs)
        for result in resp["ResultsByTime"]:
            d = result["TimePeriod"]["Start"]
            for grp in result["Groups"]:
                rows.append(
                    {
                        "usage_date": d,
                        "account_id": 0,
                        "service": grp["Keys"][0],
                        "region": "multi",
                        "cost_usd": float(grp["Metrics"]["UnblendedCost"]["Amount"]),
                        "usage_amount": float(grp["Metrics"]["UsageQuantity"]["Amount"]),
                    }
                )
        token = resp.get("NextPageToken")
        if not token:
            break

    pl.DataFrame(rows).write_csv(OUTFILE)
    print(
        f"Pulled {len(rows)} rows from AWS → {OUTFILE} ({OUTFILE.stat().st_size/1_000:.1f} KB)")

# ------- main -------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate or fetch Cost & Usage CSV")
    mx = ap.add_mutually_exclusive_group(required=True)
    mx.add_argument("--sample", metavar="Ndays",
                    help="generate synthetic sample (e.g. 90days)")
    mx.add_argument("--aws", action="store_true",
                    help="pull from AWS Cost Explorer")
    ap.add_argument("--days", type=int, default=90,
                    help="look-back window for --aws")
    args = ap.parse_args()

    if args.sample:
        generate_synthetic(days=int(args.sample.replace("days", "")))
    elif args.aws:
        fetch_from_cost_explorer(days=args.days)


if __name__ == "__main__":
    main()
