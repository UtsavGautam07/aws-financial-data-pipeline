from pathlib import Path
import sys
from decimal import Decimal
import pytest
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from pipeline import SCHEMA, transform

@pytest.fixture(scope='session')
def spark():
    session = SparkSession.builder.master('local[2]').appName('FinancialPipelineTests').getOrCreate()
    yield session
    session.stop()

def test_sample_transactions(spark):
    raw = spark.read.option('header', 'true').schema(SCHEMA).csv(str(ROOT / 'data' / 'transactions.csv'))
    curated, metrics = transform(raw)
    assert metrics == {'raw_records': 5, 'invalid_records': 0, 'duplicate_records': 1, 'curated_records': 4}
    amount = (curated.filter((F.col('transaction_type') == 'PAYMENT') &
                             (F.col('transaction_status') == 'COMPLETED'))
              .agg(F.sum('transaction_amount')).first()[0])
    assert amount == Decimal('150.00')

def test_empty_input_rejected(spark):
    empty = spark.createDataFrame([], SCHEMA)
    with pytest.raises(ValueError, match='Empty input'):
        transform(empty)
