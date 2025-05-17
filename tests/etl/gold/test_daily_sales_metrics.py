from datetime import datetime
from decimal import Decimal

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_capstone.etl.gold.daily_sales_metrics import get_daily_sales_metrics

def test_get_daily_sales_metrics(spark):
    """
    Test the get_daily_sales_metrics function.
    """

    # Create test data
    wide_sales_test_data = [
        (datetime(2023, 1, 1), Decimal("100.00"), Decimal("10.00")),
        (datetime(2023, 1, 1), Decimal("200.00"), Decimal("20.00")),
        (datetime(2023, 1, 1), Decimal("300.00"), Decimal("30.00")),
        (datetime(2023, 10, 1), Decimal("150.00"), Decimal("15.00")),
        (datetime(2023, 10, 1), Decimal("250.00"), Decimal("25.00"))
    ]

    wide_sales_schema = st.StructType([
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("SalesAmount", st.DecimalType(10, 2), True),
        st.StructField("Profit", st.DecimalType(10, 2), True)
    ])

    wide_sales_test = spark.createDataFrame(wide_sales_test_data, schema=wide_sales_schema)

    result = get_daily_sales_metrics(wide_sales_test)

    expected_schema = st.StructType([
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("SalesAmountSum", st.DecimalType(10, 2), True),
        st.StructField("SalesAmountAvg", st.DecimalType(10, 2), True),
        st.StructField("ProfitSum", st.DecimalType(10, 2), True),
        st.StructField("ProfitAvg", st.DecimalType(10, 2), True)
    ])

    expected_data = [
        (datetime(2023, 1, 1), Decimal("600.00"), Decimal("200.00"), Decimal("60.00"), Decimal("20.00")),
        (datetime(2023, 10, 1), Decimal("400.00"), Decimal("200.00"), Decimal("40.00"), Decimal("20.00"))
    ]

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    spark_testing.assertDataFrameEqual(result, expected_df)
    