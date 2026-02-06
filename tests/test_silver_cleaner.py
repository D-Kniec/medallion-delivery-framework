import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TestSilverLayer") \
        .getOrCreate()

def test_filter_negative_revenue(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("gross_revenue", DoubleType(), True)
    ])

    data = [
        ("ORD-001", 150.50),
        ("ORD-002", -20.00),
        ("ORD-003", 0.00)
    ]

    input_df = spark.createDataFrame(data, schema)

    cleaned_df = input_df.filter(F.col("gross_revenue") >= 0)

    assert cleaned_df.count() == 2
    
    rows = cleaned_df.collect()
    ids = [row.order_id for row in rows]
    
    assert "ORD-001" in ids
    assert "ORD-003" in ids
    assert "ORD-002" not in ids