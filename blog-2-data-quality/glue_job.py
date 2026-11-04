import sys
import json
from datetime import datetime, timezone

import boto3
import yaml

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from rule_engine import apply_rules


args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET"]
)

bucket = args["BUCKET"]
job_name = args["JOB_NAME"]

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(job_name, args)

s3 = boto3.client("s3")

# 1. Read YAML rules from S3.
config_object = s3.get_object(
    Bucket=bucket,
    Key="config/transaction_rules.yaml"
)
config = yaml.safe_load(
    config_object["Body"].read().decode("utf-8")
)

# 2. Read raw financial transactions.
raw_path = f"s3://{bucket}/raw/transactions.csv"

raw_df = (
    spark.read
    .option("header", True)
    .option("mode", "FAILFAST")
    .csv(raw_path)
)

# 3. Execute reusable validation rules.
valid_df, invalid_df = apply_rules(raw_df, config)

# 4. Materialize the results for metrics.
valid_df = valid_df.cache()
invalid_df = invalid_df.cache()

raw_count = raw_df.count()
valid_count = valid_df.count()
invalid_count = invalid_df.count()

# 5. Create a run identifier.
run_id = datetime.now(timezone.utc).strftime(
    "%Y%m%dT%H%M%SZ"
)

# 6. Write valid records as Parquet.
curated_path = f"s3://{bucket}/curated/run_id={run_id}/"

(
    valid_df
    .drop("failure_reasons")
    .withColumn(
        "amount",
        F.col("amount").cast("decimal(18,2)")
    )
    .withColumn(
        "transaction_date",
        F.to_date("transaction_date", "yyyy-MM-dd")
    )
    .write
    .mode("errorifexists")
    .parquet(curated_path)
)

# 7. Write rejected records with failure reasons.
quarantine_path = (
    f"s3://{bucket}/quarantine/run_id={run_id}/"
)

(
    invalid_df
    .write
    .mode("errorifexists")
    .parquet(quarantine_path)
)

# 8. Build rule-level failure counts.
rule_metrics = {}

for rule in config["rules"]:
    rule_id = rule["id"]

    rule_metrics[rule_id] = invalid_df.filter(
        F.array_contains(
            F.col("failure_reasons"),
            rule_id
        )
    ).count()

# 9. Publish a JSON quality report.
report = {
    "dataset": config["dataset"],
    "run_id": run_id,
    "raw_records": raw_count,
    "valid_records": valid_count,
    "quarantined_records": invalid_count,
    "pass_rate": round(
        valid_count / raw_count * 100, 2
    ) if raw_count else 0,
    "rule_failures": rule_metrics
}

s3.put_object(
    Bucket=bucket,
    Key=f"metrics/run_id={run_id}/quality_report.json",
    Body=json.dumps(report, indent=2),
    ContentType="application/json"
)

print(json.dumps(report, indent=2))

valid_df.unpersist()
invalid_df.unpersist()

job.commit()
