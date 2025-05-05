from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_capstone.etl.silver.sales import get_sales


def test_get_sales(spark):

    """
    Positive test for the get_sales function.

    Args:
        spark (SparkSession): The SparkSession object.

    Returns:
        None
    """

    test_data = spark.createDataFrame(
        [   
            # include - sample to keep
            ("son_1", "2023-01-01",  "1","1",  "2023-01-01", "1", "extra_value"),
            # exclude - duplicate
            ("son_1", "2023-01-01",  "1","1",  "2023-01-01", "1", "extra_value")
        ],
        schema=[

    'son',
    'orderdate',
    'pk',
    'ck',
    'dateofshipping',
    'oquantity',
    "extra_column"
        ]
    )

    result = get_sales(sales_raw=test_data)

    expected_schema = st.StructType([
        st.StructField("SalesOrderNumber", st.StringType(), True),
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("ProductKey", st.IntegerType(), True),
        st.StructField("CustomerKey", st.IntegerType(), True),
        st.StructField("ShippedDate", st.DateType(), True),
        st.StructField("OrderQuantity", st.IntegerType(), True),
    ])



    expected = spark.createDataFrame(
        [
            (
                "son_1",
                datetime(2023, 1, 1),
                1,
                1,
                datetime(2023, 1, 1),
                1,
                
            ),

        ],
        schema=expected_schema
    )


    spark_testing.assertDataFrameEqual(result, expected)