"""AWS Glue 5.1 entry point; not executed as part of the local tutorial."""
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pipeline import SCHEMA, transform

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_PATH', 'TARGET_PATH'])
context = GlueContext(SparkContext.getOrCreate())
spark = context.spark_session
job = Job(context)
job.init(args['JOB_NAME'], args)
raw = spark.read.option('header', 'true').schema(SCHEMA).csv(args['SOURCE_PATH'])
curated, metrics = transform(raw)
print('Data quality metrics:', metrics)
(curated.repartition('year', 'month').write.mode('overwrite')
    .partitionBy('year', 'month').parquet(args['TARGET_PATH']))
job.commit()
