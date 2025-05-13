from datetime import datetime
from decimal import Decimal

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_capstone.etl.silver.products import get_products


def test_get_products(spark):
    """Test the get_products function"""

    test_data = spark.createDataFrame(
        [   
            # include - sample to keep
            ("1", "1", "name_1", "10.1111", "14.1111111", "14.2222222", "color_1", "40", "NA", "1.65111111", "nameofmodel_1", "500", "desc_1", "extra_value"),
            # exclude - duplicate
            ("1", "1", "name_1", "10.1111", "14.1111111", "14.2222222", "color_1", "40", "NA", "1.65111111", "nameofmodel_1", "500", "desc_1", "extra_value")
        ],
        schema = [
            "pk",
            "psck",
            "name",
            "stancost",
            "dealerprice",
            "listprice",
            "color",
            "size",
            "range",
            "weight",
            "nameofmodel",
            "ssl",
            "desc",
            "extra_col"
        ]
    )

    result = get_products(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductKey", st.IntegerType(), True),
            st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
            st.StructField("ProductName", st.StringType(), True),
            st.StructField("StandardCost", st.DecimalType(10, 2), True),
            st.StructField("DealerPrice", st.DecimalType(10, 2), True),
            st.StructField("ListPrice", st.DecimalType(10, 2), True),
            st.StructField("Color", st.StringType(), True),
            st.StructField("Size", st.IntegerType(), True),
            st.StructField("SizeRange", st.StringType(), True),
            st.StructField("Weight", st.DecimalType(10, 2), True),
            st.StructField("ModelName", st.StringType(), True),
            st.StructField("SafetyStockLevel", st.IntegerType(), True),
            st.StructField("Description", st.StringType(), True),
            st.StructField("ProfitMargin", st.DecimalType(10, 2), True),
        ]
    )

    expected = spark.createDataFrame(
        [
            (
                1,
                1,
                "name_1",
                Decimal("10.11"),
                Decimal("14.11"),
                Decimal("14.22"),
                "color_1",
                40,
                None,
                Decimal("1.65"),
                "nameofmodel_1",
                500,
                "desc_1",
                Decimal("0.11")  # This should be the correct profit margin: 14.22 - 14.11 = 0.11
            )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)