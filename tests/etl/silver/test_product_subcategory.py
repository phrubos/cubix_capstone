from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_capstone.etl.silver.product_subcategory import get_product_subcategory


def test_get_product_subcategory(spark):
    """Test the get_product_subcategory function"""

    test_data = spark.createDataFrame(
        [   
            # include - sample to keep
            ("1", "1", "english_name_1", "spanish_name_1", "french_name_1", "extra_value"),
            # exclude - duplicate
            ("1", "1", "english_name_1", "spanish_name_1", "french_name_1", "extra_value")
        ],
        schema = [
            "psk",
            "pck",
            "espn",
            "spsn",
            "fpsn",
            "extra_col"
        ]
    )

    result = get_product_subcategory(test_data)

    expected_schema = st.StructType(
        [
            st.StructField("ProductSubcategoryKey", st.IntegerType(), True),
            st.StructField("ProductCategoryKey", st.IntegerType(), True),
            st.StructField("EnglishProductSubcategoryName", st.StringType(), True),
            st.StructField("SpanishProductSubcategoryName", st.StringType(), True),
            st.StructField("FrenchProductSubcategoryName", st.StringType(), True),

        ]
    )

    expected = spark.createDataFrame(
        [
            (
                1,
                1,
                "english_name_1",
                "spanish_name_1",
                "french_name_1"

            )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)