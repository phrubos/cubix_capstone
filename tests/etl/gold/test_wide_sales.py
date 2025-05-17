from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

import pyspark.sql.types as st
import pyspark.testing as spark_testing
from cubix_capstone.etl.gold.wide_sales import _join_master_tables, get_wide_sales


def test_join_master_tables(spark):
    """
    Test the _join_master_tables function.
    """

    # Create test data
    sales_master_test_data = [
        ("SO01", datetime(2017, 1, 1), 2, 3)
    ]
    sales_schema = st.StructType([
        st.StructField("SalesOrderNumber", st.StringType(), True),
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("CustomerKey", st.IntegerType(), True),
        st.StructField("ProductKey", st.IntegerType(), True)
    ])
    sales_master_test = spark.createDataFrame(sales_master_test_data, schema=sales_schema)

    calendar_master_test_data = [
        (datetime(2017, 1, 1), "Monday")
    ]
    calendar_schema = st.StructType([
        st.StructField("Date", st.DateType(), True),
        st.StructField("DayName", st.StringType(), True)
    ])
    calendar_master_test = spark.createDataFrame(calendar_master_test_data, schema=calendar_schema)


    customer_master_test_data = [        
        (2, "John Doe")
    ]
    customer_schema = st.StructType([
        st.StructField("CustomerKey", st.IntegerType(), True),
        st.StructField("Name", st.StringType(), True)
    ])
    customer_master_test = spark.createDataFrame(customer_master_test_data, schema=customer_schema)
    
    
    product_master_test_data = [
        (3,4, "Product A")
    ]
    product_schema = st.StructType([
        st.StructField("ProductKey", st.IntegerType(), True),
        st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
        st.StructField("ProductName", st.StringType(), True)
    ])
    product_master_test = spark.createDataFrame(product_master_test_data, schema=product_schema)

    product_subcategory_master_test_data = [
        (4,5, "Subcategory A")
    ]    
    product_subcategory_schema = st.StructType([
        st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
        st.StructField("ProductCategoryKey", st.IntegerType(), True),
        st.StructField("EnglishProductSubcategoryName", st.StringType(), True)
    ])
    product_subcategory_master_test = spark.createDataFrame(product_subcategory_master_test_data, schema=product_subcategory_schema)

    product_category_master_test_data = [
        (5, "Category A")
    ]
    product_category_schema = st.StructType([
        st.StructField("ProductCategoryKey", st.IntegerType(), True),
        st.StructField("EnglishProductCategoryName", st.StringType(), True)
    ])
    product_category_master_test = spark.createDataFrame(product_category_master_test_data, schema=product_category_schema)

    result = _join_master_tables(
            sales_master_test,
            calendar_master_test,
            customer_master_test,
            product_master_test, 
            product_subcategory_master_test,
            product_category_master_test
        )

    expected_schema = st.StructType([
        st.StructField("ProductKey", st.IntegerType(), True),  
        st.StructField("CustomerKey", st.IntegerType(), True),      
        st.StructField("SalesOrderNumber", st.StringType(), True),
        st.StructField("OrderDate", st.DateType(), True),
        st.StructField("DayName", st.StringType(), True),
        st.StructField("Name", st.StringType(), True),
        st.StructField("ProductSubCategoryKey", st.IntegerType(), True),
        st.StructField("ProductName", st.StringType(), True),
        st.StructField("ProductCategoryKey", st.IntegerType(), True),
        st.StructField("EnglishProductSubcategoryName", st.StringType(), True),
        st.StructField("EnglishProductCategoryName", st.StringType(), True)
    ])

    expected_data = [
        (3, 2, "SO01", datetime(2017, 1, 1), "Monday", "John Doe", 4, "Product A", 5, "Subcategory A", "Category A")
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    spark_testing.assertDataFrameEqual(result, expected_df)

@patch("cubix_capstone.etl.gold.wide_sales._join_master_tables")
def test_get_wide_sales(mock_join_master_tables, spark, some_df):
    """
    Test the get_wide_sales function.
    """
    # Mock the _join_master_tables function
    mock_join_master_dfs_data = [
            ("SO01", 2, Decimal("10.00"), Decimal("15.00"), 1, 0)
        ]
    mock_join_master_dfs_schema = st.StructType([
        st.StructField("SalesOrderNumber", st.StringType(), True),
        st.StructField("OrderQuantity", st.IntegerType(), True),
        st.StructField("StandardCost", st.DecimalType(10, 2), True),
        st.StructField("ListPrice", st.DecimalType(10, 2), True),
        st.StructField("MaritalStatus", st.IntegerType(), True),
        st.StructField("Gender", st.IntegerType(), True),
    ])

    mock_join_master_dfs = spark.createDataFrame(mock_join_master_dfs_data, schema=mock_join_master_dfs_schema)

    mock_join_master_tables.return_value = mock_join_master_dfs

    result = get_wide_sales(
        sales_master=some_df,
        calendar_master=some_df,
        customer_master=some_df,
        product_master=some_df,
        product_subcategory_master=some_df,
        product_category_master=some_df
    )

    expected_schema = st.StructType([
        st.StructField("SalesOrderNumber", st.StringType(), True),
        st.StructField("OrderQuantity", st.IntegerType(), True),
        st.StructField("StandardCost", st.DecimalType(10, 2), True),
        st.StructField("ListPrice", st.DecimalType(10, 2), True),
        st.StructField("MaritalStatus", st.StringType(), True),
        st.StructField("Gender", st.StringType(), True),
        st.StructField("SalesAmount", st.DecimalType(10, 2), True),
        st.StructField("HighValueOrder", st.BooleanType(), True),
        st.StructField("Profit", st.DecimalType(10, 2), True),
    ])

    expected_data = [
        ("SO01", 2, Decimal("10.00"), Decimal("15.00"), "Married", "Female", Decimal("30.00"), False, Decimal("10.00"))
    ]

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    spark_testing.assertDataFrameEqual(result, expected_df)
