import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

def get_daily_sales_metrics(wide_sales: DataFrame) -> DataFrame:
    """
    Calculate daily sales metrics from the wide sales DataFrame.
    Args:
        wide_sales (DataFrame): DataFrame containing the wide sales data.

    Returns:
        DataFrame: DataFrame containing the daily sales metrics.
    """

    return (
        wide_sales
        .groupBy(sf.col("OrderDate"))
        .agg(
            sf.sum(sf.col("SalesAmount")).alias("SalesAmountSum"),
            sf.round(sf.avg(sf.col("SalesAmount")), 2).alias("SalesAmountAvg"),
            sf.sum(sf.col("Profit")).alias("ProfitSum"),
            sf.round(sf.avg(sf.col("Profit")), 2).alias("ProfitAvg"),
        )
    )
