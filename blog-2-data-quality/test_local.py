import yaml
from pyspark.sql import SparkSession

from rule_engine import apply_rules


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("FinancialDataQualityTest")
    .getOrCreate()
)

with open("transaction_rules.yaml", "r") as file:
    config = yaml.safe_load(file)

df = (
    spark.read
    .option("header", True)
    .csv("transactions.csv")
)

valid_df, invalid_df = apply_rules(df, config)

raw_count = df.count()
valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(f"Raw records: {raw_count}")
print(f"Valid records: {valid_count}")
print(f"Quarantined records: {invalid_count}")

invalid_df.select(
    "transaction_id",
    "failure_reasons"
).show(truncate=False)

assert raw_count == 15
assert valid_count == 8
assert invalid_count == 7

print("All validation checks passed!")

spark.stop()
