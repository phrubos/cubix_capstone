from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_capstone.etl.silver.customers import get_customers


def test_get_customers(spark):
    """
    Positive test for the get_customers function.

    Args:
        spark (SparkSession): The SparkSession object.

    Returns:
        None
    """

    test_data = spark.createDataFrame(
        [   
            # include - sample to keep
            ("1", "name_1", "2023-01-01", "M", "F", "50000", "1", "occ_1", "1", "1", "addr_1", "addr_2", "000-000-010", "extra_value"),
            # exclude - duplicate
            ("1", "name_1", "2023-01-01", "M", "F", "50000", "1", "occ_1", "1", "1", "addr_1", "addr_2", "000-000-010", "extra_value"),
            # include MaritalStatus / Gender = None, YearlyIncome = 50001
            ("2", "name_2", "2023-01-11", None, None, "50002", "1", "occ_2", "1", "1", "addr_3", "addr_4", "000-000-010", "extra_value")
        ],
        schema=[
            "ck",
            "name",
            "bdate",
            "ms",
            "gender",
            "income",
            "childrenhome",
            "occ",
            "hof",
            "nco",
            "addr1",
            "addr2",
            "phone",
            "extra_column"
        ]
    )

    result = get_customers(test_data)

    expected_schema = st.StructType([
        st.StructField("CustomerKey", st.IntegerType(), True),
        st.StructField("Name", st.StringType(), True),
        st.StructField("BirthDate", st.DateType(), True),
        st.StructField("MaritalStatus", st.IntegerType(), True),
        st.StructField("Gender", st.IntegerType(), True),
        st.StructField("YearlyIncome", st.IntegerType(), True),
        st.StructField("NumberChildrenAtHome", st.IntegerType(), True),
        st.StructField("Occupation", st.StringType(), True),
        st.StructField("HouseOwnerFlag", st.IntegerType(), True),
        st.StructField("NumberCarsOwned", st.IntegerType(), True),
        st.StructField("Addressline1", st.StringType(), True),
        st.StructField("Addressline2", st.StringType(), True),
        st.StructField("Phone", st.StringType(), True),
        st.StructField("FullAdress", st.StringType(), True),
        st.StructField("IncomeCategory", st.StringType(), True),
        st.StructField("BirthYear", st.IntegerType(), True)
    ])

    expected = spark.createDataFrame(
        [
            (
                1,
                "name_1",
                datetime(2023, 1, 1),
                1,
                0,
                50000,
                1,
                "occ_1",
                1,
                1,
                "addr_1",
                "addr_2",
                "000-000-010",
                "addr_1,addr_2",
                "Low",
                2023,
            ),
            (
                2,
                "name_2",
                datetime(2023, 1, 11),  # Corrected date to match input
                None,
                None,
                50002,
                1,
                "occ_2",
                1,
                1,
                "addr_3",
                "addr_4",
                "000-000-010",
                "addr_3,addr_4",
                "Medium",
                2023, 
            )
        ],
        schema=expected_schema
    )

    spark_testing.assertDataFrameEqual(result, expected)