from datetime import datetime

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_capstone.etl.silver.calendar import get_calendar


def test_get_calendar(spark):

    """
    Positive test for the get_calendar function.

    Args:
        spark (SparkSession): The SparkSession object.

    Returns:
        None
    """

    test_data = spark.createDataFrame(
        [
            ("2023-01-01", 1, "Sunday", "January", "1","1", "1", "1", "2023", "2023", "1", "1", "1", "1", "201701", "extra_value"),
            ("2023-01-01", 1, "Sunday", "January", "1","1", "1", "1", "2023", "2023", "1", "1", "1", "1", "201701", "extra_value")
        ],
        schema=[

            "Date", 
            "DayNumberOfWeek",
            "DayName", 
            "MonthName",
            "MonthNumberOfYear", 
            "DayNumberOfYear", 
            "WeekNumberOfYear",
            "CalendarQuarter",
            "CalendarYear", 
            "FiscalYear", 
            "FiscalSemester",
            "FiscalQuarter",
            "FinMonthNumberOfYear",
            "DayNumberOfMonth",
            "MonthID",
            "extra_column"
        ]
    )

    result = get_calendar(calendar_raw=test_data)

    expected_schema = st.StructType([
        st.StructField("Date", st.DateType(), True),
        st.StructField("DayNumberOfWeek", st.IntegerType(), True),
        st.StructField("DayName", st.StringType(), True),
        st.StructField("MonthName", st.StringType(), True),
        st.StructField("MonthNumberOfYear", st.IntegerType(), True),
        st.StructField("DayNumberOfYear", st.IntegerType(), True),
        st.StructField("WeekNumberOfYear", st.IntegerType(), True),
        st.StructField("CalendarQuarter", st.IntegerType(), True),
        st.StructField("CalendarYear", st.IntegerType(), True),
        st.StructField("FiscalYear", st.IntegerType(), True),
        st.StructField("FiscalSemester", st.IntegerType(), True),
        st.StructField("FiscalQuarter", st.IntegerType(), True),
        st.StructField("FinMonthNumberOfYear", st.IntegerType(), True),
        st.StructField("DayNumberOfMonth", st.IntegerType(), True),
        st.StructField("MonthID", st.IntegerType(), True)
    ])



# "2023-01-01", 1, "Sunday", "January", "1","1", "1", "1", "2023", "2023", "1", "1", "1", "1", "201701", "extra_value"
    expected = spark.createDataFrame(
        [
            (
                datetime(2023, 1, 1),
                1,
                "Sunday",
                "January",
                1,
                1,
                1,
                1,
                2023,
                2023,
                1,
                1,
                1,
                1,
                201701
            ),

        ],
        schema=expected_schema
    )


    spark_testing.assertDataFrameEqual(result, expected)