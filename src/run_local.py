"""Run the tutorial without AWS (Python 3.11 + Java 17 recommended)."""
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pipeline import SCHEMA, transform

ROOT = Path(__file__).resolve().parents[1]
spark = SparkSession.builder.master('local[2]').appName('FinancialPipelineDemo').getOrCreate()
try:
    raw = spark.read.option('header', 'true').schema(SCHEMA).csv(str(ROOT / 'data' / 'transactions.csv'))
    curated, metrics = transform(raw)
    target = ROOT / 'output' / 'curated'
    curated.coalesce(1).write.mode('overwrite').partitionBy('year', 'month').parquet(str(target))
    payment_total = (curated.filter((F.col('transaction_status') == 'COMPLETED') &
                                    (F.col('transaction_type') == 'PAYMENT'))
                     .agg(F.sum('transaction_amount').alias('amount')).first()['amount'])
    print('Metrics:', metrics)
    print('Completed payment total (GBP):', payment_total)
    print('Output:', target)
finally:
    spark.stop()
