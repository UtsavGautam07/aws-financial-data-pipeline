"""Reusable Spark transformations for the financial transaction tutorial."""
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

SCHEMA = StructType([
    StructField('transaction_id', StringType(), True),
    StructField('account_id', StringType(), True),
    StructField('transaction_date', StringType(), True),
    StructField('transaction_amount', DecimalType(18, 2), True),
    StructField('transaction_type', StringType(), True),
    StructField('transaction_status', StringType(), True),
    StructField('currency', StringType(), True),
])

def transform(raw_df, max_invalid_rate=0.05):
    """Validate, normalize, and deduplicate synthetic transaction data.

    Note: conflicting duplicate IDs require a business rule; this demo assumes
    duplicates with the same ID are identical, and rejects conflicts.
    """
    parsed = raw_df.withColumn(
        'transaction_date',
        F.to_date(F.try_to_timestamp(F.col('transaction_date'), F.lit('yyyy-MM-dd')))
    )
    invalid = (
        F.col('transaction_id').isNull() |
        F.col('account_id').isNull() |
        F.col('transaction_date').isNull() |
        F.col('transaction_amount').isNull()
    )
    total = parsed.count()
    if total == 0:
        raise ValueError('Empty input: refusing to overwrite curated data')
    bad = parsed.filter(invalid).count()
    if bad / total > max_invalid_rate:
        raise ValueError(f'Invalid row rate {bad/total:.1%} exceeds {max_invalid_rate:.1%}')
    valid = (parsed.filter(~invalid)
        .withColumn('transaction_type', F.upper(F.trim('transaction_type')))
        .withColumn('transaction_status', F.upper(F.trim('transaction_status')))
        .withColumn('currency', F.upper(F.trim('currency'))))
    # Prevent nondeterministic selection of conflicting duplicates.
    conflict_count = (valid.groupBy('transaction_id')
        .agg(F.countDistinct(F.struct(*[F.col(c) for c in valid.columns])).alias('variants'))
        .filter('variants > 1').count())
    if conflict_count:
        raise ValueError(f'{conflict_count} transaction IDs contain conflicting records')
    unique = valid.dropDuplicates(['transaction_id'])
    curated = (unique
        .withColumn('year', F.date_format('transaction_date', 'yyyy'))
        .withColumn('month', F.date_format('transaction_date', 'MM')))
    metrics = {'raw_records': total, 'invalid_records': bad,
               'duplicate_records': valid.count() - unique.count(),
               'curated_records': unique.count()}
    return curated, metrics
