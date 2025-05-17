import pyspark.sql.functions as sf
from pyspark.sql import DataFrame


def _join_master_tables(
    sales_master: DataFrame,
    calendar_master: DataFrame,
    customer_master: DataFrame,
    product_master: DataFrame,
    product_subcategory_master: DataFrame,
    product_category_master: DataFrame
    ) -> DataFrame:
    """
    Joins all master tables together.

    Parameters
    ----------
    sales_master : DataFrame
        DataFrame containing the sales data.
    calendar_master : DataFrame
        DataFrame containing the calendar data.
    customer_master : DataFrame
        DataFrame containing the customer data.
    product_master : DataFrame
        DataFrame containing the product data.
    product_subcategory_master : DataFrame
        DataFrame containing the product subcategory data.
    product_category_master : DataFrame
        DataFrame containing the product category data.


    Returns
    -------
    DataFrame
        DataFrame containing the joined master tables.
        """

    return (
        sales_master
        .join(calendar_master, sales_master["OrderDate"] == calendar_master["Date"], how="left")
        .drop(calendar_master["Date"])
        .join(customer_master, on="CustomerKey", how="left")
        .join(product_master, on="ProductKey", how="left")
        .join(
            product_subcategory_master,
            product_master["ProductSubCategoryKey"] == product_subcategory_master["ProductSubCategoryKey"],
            how="left")
        .drop(product_subcategory_master["ProductSubCategoryKey"])
        .join(
            product_category_master,
            product_category_master["ProductCategoryKey"] == product_subcategory_master["ProductCategoryKey"],
            how="left")
        .drop(product_category_master["ProductCategoryKey"])
    )


def get_wide_sales(
    sales_master: DataFrame,
    calendar_master: DataFrame,
    customer_master: DataFrame,
    product_master: DataFrame,
    product_subcategory_master: DataFrame,
    product_category_master: DataFrame

    ) -> DataFrame:
    """
    1. Joins all master tables together.
    2. Calculates the following columns:
        - SalesAmount: OrderQuantity * ListPrice
        - HighValueOrder: SalesAmount > 10000
        - Profit: SalesAmount - (StandardCost * OrderQuantity)
    3. Converts MaritalStatus and Gender to string.

    Parameters
    ----------
    sales_master : DataFrame
        DataFrame containing the sales data.
    calendar_master : DataFrame
        DataFrame containing the calendar data.
    customer_master : DataFrame
        DataFrame containing the customer data.
    product_master : DataFrame
        DataFrame containing the product data.
    product_category_master : DataFrame
        DataFrame containing the product category data.
    product_subcategory_master : DataFrame
        DataFrame containing the product subcategory data.

    Returns
    -------
    DataFrame
        DataFrame containing the wide sales data.
    """

    wide_sales_df = _join_master_tables(
        sales_master,
        calendar_master,
        customer_master,
        product_master,
        product_subcategory_master,
        product_category_master

    )

    calculate_sales_amount = sf.col("OrderQuantity") * sf.col("ListPrice")
    calculate_high_value_order = sf.col("SalesAmount") > 10000
    calculate_profit = sf.col("SalesAmount") - (sf.col("StandardCost") * sf.col("OrderQuantity"))

    return (
        wide_sales_df
        .withColumn(
            "MaritalStatus",
            sf.when(sf.col("MaritalStatus") == 1, "Married")
            .when(sf.col("MaritalStatus") == 0, "Single")
            .otherwise(None))
        .withColumn(
            "Gender",
            sf.when(sf.col("Gender") == 1, "Male")
            .when(sf.col("Gender") == 0, "Female")
            .otherwise(None))
        .withColumn("SalesAmount", calculate_sales_amount)
        .withColumn("HighValueOrder", calculate_high_value_order)
        .withColumn("Profit", calculate_profit)
    )
