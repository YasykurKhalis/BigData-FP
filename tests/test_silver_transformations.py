"""LUMBUNG — Unit tests Silver transformations. Owner: Yasykur"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import pytest

@pytest.fixture(scope="session")
def spark():
    # Simple local Spark session without delta for fast unit testing
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()

def test_deduplication_logic(spark):
    data = [
        {"source": "bapanas", "commodity": "beras", "date": "2026-06-20", "price": 10000},
        {"source": "bapanas", "commodity": "beras", "date": "2026-06-20", "price": 10000}, # Duplicate
        {"source": "pihps", "commodity": "jagung", "date": "2026-06-20", "price": 5000},
    ]
    
    df = spark.createDataFrame(data)
    df = df.withColumn("date_parsed", to_date(col("date")))
    
    # Logic from silver layer
    df_clean = df.select("source", "commodity", "date_parsed", "price") \
                 .dropDuplicates(["source", "commodity", "date_parsed"])
                 
    assert df_clean.count() == 2
    
    beras_count = df_clean.filter(col("commodity") == "beras").count()
    assert beras_count == 1
