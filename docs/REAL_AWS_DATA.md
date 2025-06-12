# Fetching Real AWS Cost Explorer Data

Follow these steps **locally** to replace the synthetic sample with live Cost Explorer usage.  
Nothing here is committed to Git, so your keys stay private.

## 1 Enable Cost Explorer

AWS Console → **Billing → Cost Explorer** → click **Enable** (one‑time per payer account).

## 2 Create / choose an IAM principal

Attach **ce:GetCostAndUsage** permission:

```json
{
  "Effect": "Allow",
  "Action": ["ce:GetCostAndUsage"],
  "Resource": "*"
}
```

## 3 Provide credentials (choose ONE)

### a) AWS CLI profile _(recommended)_

```bash
aws configure --profile cost-observatory      # fills ~/.aws/credentials
export AWS_PROFILE=cost-observatory
python fetch_or_generate.py --aws --days 90
```

### b) Environment variables

```bash
export AWS_ACCESS_KEY_ID=YOUR_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET
export AWS_DEFAULT_REGION=us-east-1
python fetch_or_generate.py --aws --days 90
```

### c) `.env` file _(never committed)_

```bash
cp .env.example .env
# edit .env and add:
# AWS_ACCESS_KEY_ID=YOUR_KEY
# AWS_SECRET_ACCESS_KEY=YOUR_SECRET
# AWS_DEFAULT_REGION=us-east-1
python fetch_or_generate.py --aws --days 90
```

> `.env` is listed in `.gitignore`, so credentials stay local.

## 4 Run the fetcher

```bash
python fetch_or_generate.py --aws --days 90
# → data/raw/cost_usage_sample.csv is overwritten with live data
```

To switch back to the demo sample:

```bash
python fetch_or_generate.py --sample 90days
```
