import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

SALES_MAPPING = {
    'son': 'SalesOrderNumber',
    'orderdate': 'OrderDate',
    'pk': 'ProductKey',
    'ck': 'CustomerKey',
    'dateofshipping': 'ShippedDate',
    'oquantity': 'OrderQuantity'
}


def get_sales(sales_raw: DataFrame) -> DataFrame:
    """
    Mapping the raw sales data into a structured format.

    Args:
        sales_raw (DataFrame): The raw sales data.

    Returns:
        DataFrame: The transformed sales data.
    """

    return (
            sales_raw
                .select(
                    sf.col("son"),
                    sf.col("orderdate").cast("date"),
                    sf.col("pk").cast("int"),
                    sf.col("ck").cast("int"),
                    sf.col("dateofshipping").cast("date"),
                    sf.col("oquantity").cast("int")
                )
                .withColumnsRenamed(SALES_MAPPING)
                .dropDuplicates()
                )
